#!/usr/bin/env python3
"""Orchestrator for the CRIU node-evacuation demo.

Runs as a one-shot Kubernetes Job. It does NOT touch the migration control plane itself — the
host-local `fmi-rank-agent migrate-local` owns the whole cut (discover this machine's ranks,
request their migration, criu dump/restore them in parallel, promote the epoch). The orchestrator
just drives and verifies the scenario:

  1. wait until all ranks are ACTIVE at epoch 0;
  2. `kubectl exec` `fmi-rank-agent migrate-local` inside the machine-A pod (same container as its
     ranks, so criu can checkpoint/restore them) to evacuate machine A in one cut;
  3. wait until the epoch advances to 1 and all ranks are ACTIVE again;
  4. read every machine pod's logs and assert all ranks finished with preserved state
     (phase-2 allreduce OK), proving CRIU carried the in-memory state across the evacuation.

State is read straight from the Redis control plane (same key schema the runbook scripts use), so
the orchestrator image needs no FMI Python binding.
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


# ----- runtime (skipped during unit tests) -----

def main():
    import redis
    from kubernetes import client, config
    from kubernetes.stream import stream

    ns = os.environ.get("NAMESPACE", "fmi-criu")
    comm = os.environ["COMM_NAME"]
    num_peers = int(os.environ["NUM_PEERS"])
    redis_host = os.environ.get("REDIS_HOST", "fmi-redis")
    redis_port = int(os.environ.get("REDIS_PORT", "6379"))
    evacuate_selector = os.environ.get("EVACUATE_SELECTOR", "app=fmi-machine,fmi-machine=a")
    all_selector = os.environ.get("ALL_MACHINES_SELECTOR", "app=fmi-machine")
    container = os.environ.get("WORKER_CONTAINER", "fmi-machine")
    agent_config = os.environ.get("AGENT_CONFIG", "/tmp/fmi.json")
    agent_bin = os.environ.get("FMI_AGENT_BIN", "/opt/fmi/bin/fmi-rank-agent")
    active_timeout_s = int(os.environ.get("ACTIVE_TIMEOUT_S", "180"))
    epoch_timeout_s = int(os.environ.get("EPOCH_TIMEOUT_S", "180"))

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

    config.load_incluster_config()
    core = client.CoreV1Api()

    log(f"comm={comm} num_peers={num_peers} namespace={ns}")
    log("waiting for all ranks ACTIVE at epoch 0")
    wait_until(lambda: all_active_from_states(states(0), num_peers), active_timeout_s,
               "all ranks ACTIVE at epoch 0")
    print_directory(0)

    pod = find_pod(core, evacuate_selector)
    log(f"evacuating machine-A pod '{pod}' via fmi-rank-agent migrate-local")
    cmd = " ".join([agent_bin, "migrate-local", comm, str(num_peers), agent_config]) + '; echo "__EXIT__=$?"'
    out = stream(core.connect_get_namespaced_pod_exec, pod, ns,
                 command=["sh", "-c", cmd], container=container,
                 stderr=True, stdin=False, stdout=True, tty=False, _preload_content=True)
    for line in out.splitlines():
        log(f"  [agent] {line}")
    if "__EXIT__=0" not in out:
        fail(f"migrate-local did not exit cleanly:\n{out}")

    log("waiting for epoch to advance to 1 and all ranks ACTIVE again")
    wait_until(lambda: current_epoch() >= 1 and all_active_from_states(states(1), num_peers),
               epoch_timeout_s, "epoch 1 with all ranks ACTIVE")
    print_directory(1)

    log("collecting per-rank results from machine pods")
    ok, failed = set(), set()
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

    log(f"PASSED: machine A evacuated in one criu cut; all {num_peers} ranks preserved state "
        f"at epoch {current_epoch()}")


if __name__ == "__main__":
    main()
