#!/usr/bin/env python3
"""Orchestrator for the CRIU node-evacuation-to-serverless demo.

Runs as a one-shot Kubernetes Job and drives the whole cross-host migration:

  1. wait until all ranks are ACTIVE at epoch 0;
  2. `kubectl exec` `fmi-rank-agent evacuate-local` inside the machine-A pod: its ranks are
     criu-dumped in one consistent cut and the packed images staged in Redis (no restore, no
     promotion);
  3. scale machine A's Deployment to zero — the node is now truly evacuated;
  4. POST one /restore per evacuated rank to the fmi-restore Knative Service, in parallel:
     each request cold-starts an instance that criu-restores its rank and holds the request
     while the restored worker runs;
  5. wait until the CRIU registry shows every evacuated rank RUNNING on a NEW host (a
     fmi-restore pod, not machine-a), then promote the epoch (local `fmi-rank-agent promote`);
  6. wait for epoch 1 with all ranks ACTIVE, then verify: machine B's logs prove ranks 4..7
     rejoined, and the /restore responses prove ranks 0..3 finished with preserved state on
     serverless — the post-migration allreduce is only correct if their memory survived.

State is read straight from the Redis control plane (same key schema the runbook scripts use),
so the orchestrator image needs no FMI Python binding; the one control-plane WRITE (epoch
promotion) goes through the fmi-rank-agent binary shipped in this same image.
"""
import os
import sys
import time


# ----- pure helpers (unit-tested in tests/test_orchestrator.py) -----

def all_active_from_states(states, num_peers):
    """True iff ranks 0..num_peers-1 are all ACTIVE in a Redis states hash."""
    return all(states.get(str(i)) == "ACTIVE" for i in range(num_peers))


def parse_rank_results(log_text):
    """Scan pod logs for the worker's terminal lines, returning (ok_ranks, failed_ranks) as sets
    of rank-id strings. A worker prints `rank=<r> OK: application state survived ...` only when its
    post-migration allreduce matched (state preserved), and `rank=<r> FAIL: ...` otherwise."""
    ok, failed = set(), set()
    for line in log_text.splitlines():
        line = line.strip()
        if not line.startswith("rank="):
            continue
        rank = line.split()[0].split("=", 1)[1]
        if "OK: application state survived" in line:
            ok.add(rank)
        elif "FAIL:" in line:
            failed.add(rank)
    return ok, failed


def parse_evacuation_output(text):
    """Extract (staged_epoch, [ranks]) from evacuate-local's `staged_epoch=1 ranks=0,1,2,3`."""
    for line in text.splitlines():
        if line.startswith("staged_epoch="):
            fields = dict(part.split("=", 1) for part in line.split() if "=" in part)
            ranks = [int(r) for r in fields["ranks"].split(",") if r != ""]
            return int(fields["staged_epoch"]), ranks
    raise ValueError(f"no staged_epoch line in evacuate-local output: {text!r}")


def verify_relocations(original_hosts, registry_entries, ranks):
    """Failure strings unless every evacuated rank is RUNNING on a host different from the one
    it was dumped on. original_hosts / registry_entries map rank -> pre-dump host_id / registry
    hash ({'state': ..., 'host_id': ...})."""
    failures = []
    for rank in ranks:
        entry = registry_entries.get(rank) or {}
        state, host = entry.get("state"), entry.get("host_id", "")
        if state != "RUNNING":
            failures.append(f"rank {rank}: registry state is {state}, expected RUNNING")
        elif not host or host == original_hosts.get(rank):
            failures.append(f"rank {rank}: still on '{host}' (dumped on "
                            f"'{original_hosts.get(rank)}'), did not relocate")
    return failures


def verify_restore_responses(responses, ranks):
    """Failure strings unless every /restore response reports status=ok with an OK worker log."""
    failures = []
    for rank in ranks:
        response = responses.get(rank)
        if not isinstance(response, dict):
            failures.append(f"rank {rank}: no /restore response ({response!r})")
            continue
        if response.get("status") != "ok":
            failures.append(f"rank {rank}: /restore status={response.get('status')!r} "
                            f"(log tail: {response.get('log_tail', '')[-200:]!r})")
            continue
        ok, failed = parse_rank_results(response.get("log_tail", ""))
        if str(rank) in failed:
            failures.append(f"rank {rank}: worker reported state loss on the restore host")
        elif str(rank) not in ok:
            failures.append(f"rank {rank}: restored worker log has no terminal OK line")
    return failures


# ----- runtime (skipped during unit tests) -----

def main():
    import concurrent.futures
    import json
    import subprocess
    import urllib.error
    import urllib.request
    from string import Template

    import redis
    from kubernetes import client, config
    from kubernetes.stream import stream

    ns = os.environ.get("NAMESPACE", "fmi-criu")
    comm = os.environ["COMM_NAME"]
    num_peers = int(os.environ["NUM_PEERS"])
    redis_host = os.environ.get("REDIS_HOST", "fmi-redis")
    redis_port = int(os.environ.get("REDIS_PORT", "6379"))
    tcpunch_host = os.environ.get("TCPUNCH_HOST", "fmi-tcpunch")
    evacuate_selector = os.environ.get("EVACUATE_SELECTOR", "app=fmi-machine,fmi-machine=a")
    evacuate_deployment = os.environ.get("EVACUATE_DEPLOYMENT", "fmi-machine-a")
    all_selector = os.environ.get("ALL_MACHINES_SELECTOR", "app=fmi-machine")
    container = os.environ.get("WORKER_CONTAINER", "fmi-machine")
    agent_config = os.environ.get("AGENT_CONFIG", "/tmp/fmi.json")
    agent_bin = os.environ.get("FMI_AGENT_BIN", "/opt/fmi/bin/fmi-rank-agent")
    restore_url = os.environ.get("RESTORE_URL",
                                 "http://fmi-restore.fmi-criu.svc.cluster.local/restore")
    template_path = os.environ.get("CONFIG_TEMPLATE", "/opt/fmi/app/fmi-machine.json.tmpl")
    active_timeout_s = int(os.environ.get("ACTIVE_TIMEOUT_S", "180"))
    epoch_timeout_s = int(os.environ.get("EPOCH_TIMEOUT_S", "180"))
    relocate_timeout_s = int(os.environ.get("RELOCATE_TIMEOUT_S", "300"))
    restore_http_timeout_s = int(os.environ.get("RESTORE_HTTP_TIMEOUT_S", "600"))

    prefix = f"fmi:ft:{comm}:"
    r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)

    def log(msg):
        print(f"[orchestrator] {msg}", flush=True)

    def fail(msg):
        print(f"[orchestrator] FAIL: {msg}", file=sys.stderr, flush=True)
        sys.exit(1)

    def current_epoch():
        val = r.hget(f"{prefix}meta", "current_epoch")
        return int(val) if val is not None else 0

    def states(epoch):
        return r.hgetall(f"{prefix}epoch:{epoch}:states")

    def registry_entry(rank):
        return r.hgetall(f"{prefix}criu:rank:{rank}")

    def wait_until(predicate, timeout_s, what):
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            if predicate():
                return
            time.sleep(0.5)
        fail(f"timed out after {timeout_s}s waiting for {what}")

    def print_directory(epoch):
        st = states(epoch)
        pl = r.hgetall(f"{prefix}epoch:{epoch}:placement")
        log(f"--- epoch {epoch} directory ---")
        for i in range(num_peers):
            log(f"  rank {i}: state={st.get(str(i), '-')} placement={pl.get(str(i), '-')}")

    def find_pod(core, selector):
        pods = core.list_namespaced_pod(ns, label_selector=selector).items
        running = [p for p in pods if p.status.phase == "Running"]
        if not running:
            fail(f"no Running pod matches selector '{selector}' in namespace {ns}")
        return running[0].metadata.name

    def post_restore(rank):
        request = urllib.request.Request(
            restore_url,
            data=json.dumps({"rank": rank}).encode(),
            method="POST",
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(request, timeout=restore_http_timeout_s) as response:
                return json.loads(response.read().decode("utf-8", errors="replace"))
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            try:
                return json.loads(body)  # restore-server errors carry a structured body
            except json.JSONDecodeError:
                return {"status": f"http-{exc.code}", "log_tail": body}
        except urllib.error.URLError as exc:
            return {"status": "unreachable", "log_tail": str(exc.reason)}

    config.load_incluster_config()
    core = client.CoreV1Api()
    apps = client.AppsV1Api()

    log(f"comm={comm} num_peers={num_peers} namespace={ns}")
    log("waiting for all ranks ACTIVE at epoch 0")
    wait_until(lambda: all_active_from_states(states(0), num_peers), active_timeout_s,
               "all ranks ACTIVE at epoch 0")
    print_directory(0)

    # 1. Evacuate machine A: dump its ranks in one cut and stage the images in Redis.
    pod = find_pod(core, evacuate_selector)
    log(f"staging machine-A ranks: fmi-rank-agent evacuate-local in pod '{pod}'")
    cmd = " ".join([agent_bin, "evacuate-local", comm, str(num_peers), agent_config]) \
        + '; echo "__EXIT__=$?"'
    out = stream(core.connect_get_namespaced_pod_exec, pod, ns,
                 command=["sh", "-c", cmd], container=container,
                 stderr=True, stdin=False, stdout=True, tty=False, _preload_content=True)
    for line in out.splitlines():
        log(f"  [agent] {line}")
    if "__EXIT__=0" not in out:
        fail(f"evacuate-local did not exit cleanly:\n{out}")
    staged_epoch, evac_ranks = parse_evacuation_output(out)
    log(f"staged epoch {staged_epoch} for ranks {evac_ranks}")
    original_hosts = {rank: registry_entry(rank).get("host_id", "") for rank in evac_ranks}

    # 2. The dumped processes are dead; retire the machine itself. From here on nothing of
    #    ranks 0..3 exists on machine A — the restores must succeed somewhere else.
    log(f"scaling deploy/{evacuate_deployment} to 0 (machine A is evacuated)")
    apps.patch_namespaced_deployment_scale(evacuate_deployment, ns, {"spec": {"replicas": 0}})
    wait_until(lambda: not core.list_namespaced_pod(ns, label_selector=evacuate_selector).items,
               active_timeout_s, "machine-A pod to terminate")

    # 3. One /restore per rank, in parallel: Knative cold-starts one instance per request
    #    (containerConcurrency=1) and each request stays open while its restored worker runs.
    log(f"POSTing {len(evac_ranks)} parallel /restore requests to {restore_url}")
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=len(evac_ranks))
    futures = {rank: pool.submit(post_restore, rank) for rank in evac_ranks}

    # 4. Restores are confirmed via the control plane (the responses only arrive after the
    #    workers FINISH, which needs the promotion below — don't wait on them here).
    def all_relocated():
        entries = {rank: registry_entry(rank) for rank in evac_ranks}
        return not verify_relocations(original_hosts, entries, evac_ranks)

    log("waiting for every evacuated rank to be RUNNING on a new host")
    wait_until(all_relocated, relocate_timeout_s,
               "all evacuated ranks RUNNING on a restore host")
    for rank in evac_ranks:
        log(f"  rank {rank}: {original_hosts[rank]} -> {registry_entry(rank).get('host_id')}")

    # 5. Promote the epoch: survivors and the restored ranks rebuild channels at epoch N+1.
    with open(template_path) as f:
        rendered = Template(f.read()).safe_substitute(
            REDIS_HOST=redis_host, TCPUNCH_HOST=tcpunch_host, HOST_ID="orchestrator")
    with open("/tmp/fmi-orchestrator.json", "w") as f:
        f.write(rendered)
    promote = subprocess.run(
        [agent_bin, "promote", comm, str(num_peers), "/tmp/fmi-orchestrator.json"],
        capture_output=True, text=True)
    for line in (promote.stdout + promote.stderr).splitlines():
        log(f"  [agent] {line}")
    if promote.returncode != 0:
        fail(f"promote failed with exit code {promote.returncode}")

    log(f"waiting for epoch {staged_epoch} with all ranks ACTIVE")
    wait_until(lambda: current_epoch() >= staged_epoch
               and all_active_from_states(states(staged_epoch), num_peers),
               epoch_timeout_s, f"epoch {staged_epoch} with all ranks ACTIVE")
    print_directory(staged_epoch)

    # 6. Outcomes. Survivor ranks report in the remaining machine pods' logs; evacuated ranks
    #    report through the held /restore responses (their logs live in the restore pods).
    responses = {rank: future.result(timeout=restore_http_timeout_s)
                 for rank, future in futures.items()}
    pool.shutdown()
    response_failures = verify_restore_responses(responses, evac_ranks)
    if response_failures:
        fail("serverless restores did not complete cleanly:\n  " +
             "\n  ".join(response_failures))
    for rank in evac_ranks:
        log(f"  rank {rank} on {responses[rank].get('host')}: status=ok (state preserved)")

    ok, failed = set(str(rank) for rank in evac_ranks), set()
    for p in core.list_namespaced_pod(ns, label_selector=all_selector).items:
        try:
            logs = core.read_namespaced_pod_log(p.metadata.name, ns, container=container)
        except client.ApiException as e:
            log(f"could not read logs of {p.metadata.name}: {e.reason}")
            continue
        pod_ok, pod_fail = parse_rank_results(logs)
        ok |= pod_ok
        failed |= pod_fail

    expected = {str(i) for i in range(num_peers)}
    if failed:
        fail(f"ranks reported state loss: {sorted(failed)}")
    if ok != expected:
        fail(f"only ranks {sorted(ok)} confirmed preserved state; expected {sorted(expected)}")

    log(f"PASSED: machine A evacuated to serverless in one criu cut — "
        f"{len(evac_ranks)} ranks relocated to fmi-restore pods, all {num_peers} ranks "
        f"preserved state at epoch {current_epoch()}")


if __name__ == "__main__":
    main()
