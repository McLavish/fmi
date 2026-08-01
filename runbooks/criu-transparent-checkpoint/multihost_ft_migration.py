#!/usr/bin/env python3
"""Cross-host CRIU migration of FMI ranks over ssh: the EC2/plain-VM mirror of
runbooks/k8s-criu-node-evacuation (kubectl exec -> ssh, Deployment scale -> nothing,
Knative POST /restore -> ssh restore-remote).

Flow, exactly the control-plane sequence the K8s orchestrator drives:
  1. launch every rank over ssh, round-robin across --nodes, with an FT config whose
     state_transfer is "criu" and whose preferred_data_backend names the transport under
     test (DirectTCP here); ranks register themselves in the CRIU registry keyed by
     their host's gethostname()
  2. wait until every rank is ACTIVE at epoch 0
  3. `fmi-rank-agent evacuate-local` ON the evacuated node (same host as its ranks --
     the agent discovers them by host_id): quiesces, dumps, packs, stages each rank's
     image in Redis, then the ranks are gone from that node
  4. `fmi-rank-agent restore-remote <rank>` on the DESTINATION node for each evacuated
     rank: fetches the image from Redis, untars at /, criu-restores at the dumped PID
  5. `fmi-rank-agent promote` once (from the driver's node): epoch N -> N+1; survivors
     reconfigure under the epoch-qualified comm name, the restored ranks resume out of
     their wait loop
  6. wait for the promoted epoch + every rank ACTIVE there, then for job completion,
     and compare every rank's final checksum against a clean baseline of the same shape

Environment requirements are the ones in the module doc of multihost_sweep.py, plus:
  * fmi-rank-agent and the subject must come from an FMI_ENABLE_CRIU=ON build
  * FMI_CRIU_EXTRA_ARGS=--unprivileged is exported around every agent invocation (the
    agent's criu calls do not pass it themselves; rootless criu-with-file-caps needs it)
  * fault_tolerance.criu.images_dir must exist / be creatable at the same absolute path
    on every node (default /tmp/fmi-criu-images is per-host local, which is fine: the
    image travels through Redis, not the filesystem)
"""
import argparse
import json
import os
import re
import shlex
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", ".."))

DONE_RE = re.compile(r"rank (\d+): DONE rounds=(\d+) checksum=(-?\d+)")


def ssh_base(key):
    base = ["ssh", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=accept-new",
            "-o", "ConnectTimeout=10"]
    if key:
        base += ["-i", key]
    return base


def run_on(base, user, node, cmd, timeout=120):
    return subprocess.run(base + [f"{user}@{node}", cmd], capture_output=True, text=True,
                          timeout=timeout)


def redis_cli(redis_host, *args):
    out = subprocess.run(["redis-cli", "-h", redis_host, *args], capture_output=True,
                         text=True)
    return out.stdout.strip()


def states(redis_host, comm, epoch):
    raw = redis_cli(redis_host, "HGETALL", f"fmi:ft:{comm}:epoch:{epoch}:states")
    lines = raw.splitlines()
    return dict(zip(lines[0::2], lines[1::2]))


def current_epoch(redis_host, comm):
    v = redis_cli(redis_host, "HGET", f"fmi:ft:{comm}:meta", "current_epoch")
    return int(v) if v else 0


def registry_host_entry(redis_host, comm, rank):
    raw = redis_cli(redis_host, "HGETALL", f"fmi:ft:{comm}:criu:rank:{rank}")
    lines = raw.splitlines()
    return dict(zip(lines[0::2], lines[1::2]))


def wait_until(what, pred, timeout_s, interval=1.0):
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if pred():
            return
        time.sleep(interval)
    raise TimeoutError(f"timed out waiting for {what}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--nodes", nargs="+", required=True)
    ap.add_argument("--evacuate-node", required=True,
                    help="node whose ranks get dumped (must be one of --nodes)")
    ap.add_argument("--restore-node", required=True,
                    help="node the dumped ranks are restored on (should differ)")
    ap.add_argument("--ssh-key", default=os.environ.get("FMI_SSH_KEY", ""))
    ap.add_argument("--ssh-user", default=os.environ.get("FMI_SSH_USER", "ec2-user"))
    ap.add_argument("--config", required=True, help="absolute path, same on every node")
    ap.add_argument("--subject", required=True, help="absolute path to the rank binary")
    ap.add_argument("--agent", required=True, help="absolute path to fmi-rank-agent")
    ap.add_argument("--peers", type=int, default=4)
    ap.add_argument("--rounds", type=int, default=20000)
    ap.add_argument("--ms", type=int, default=0)
    ap.add_argument("--shape", default="baseline")
    ap.add_argument("--print-every", type=int, default=25)
    ap.add_argument("--payload-ints", type=int, default=1)
    ap.add_argument("--comm", default=f"ftmh{os.getpid()}")
    ap.add_argument("--outdir", default="")
    ap.add_argument("--baseline-sums", default="",
                    help="JSON rank->checksum map from a clean run; empty = skip compare")
    ap.add_argument("--evacuate-after", type=float, default=3.0,
                    help="seconds after epoch-0 ACTIVE before evacuating")
    args = ap.parse_args()

    base = ssh_base(args.ssh_key)
    user = args.ssh_user
    comm = args.comm
    with open(args.config) as f:
        cfg = json.load(f)
    redis_host = cfg["fault_tolerance"]["control_host"]
    npeers = args.peers

    outdir = args.outdir or os.path.join(HERE, "sweep", f"ftmh{os.getpid()}", comm)
    os.makedirs(outdir, exist_ok=True)

    # A previous job's registry/control state under the same comm name would poison
    # discovery; comm names are unique per invocation, but clean anyway.
    for pat in (f"fmi:ft:{comm}:*", f"fmi:direct:{comm}*"):
        keys = redis_cli(redis_host, "--scan", "--pattern", pat).split()
        if keys:
            redis_cli(redis_host, "DEL", *keys)

    placement = {}
    for r in range(npeers):
        node = args.nodes[r % len(args.nodes)]
        log = os.path.join(outdir, f"r{r}.log")
        inner = (f"cd {shlex.quote(HERE)}; setsid {shlex.quote(args.subject)} {r} {npeers} "
                 f"{shlex.quote(args.config)} {shlex.quote(comm)} {args.rounds} {args.ms} "
                 f"{args.print_every} {args.payload_ints} --shape {shlex.quote(args.shape)} "
                 f"< /dev/null > {shlex.quote(log)} 2>&1 & echo LAUNCHED:$!")
        out = run_on(base, user, node, inner)
        m = re.search(r"LAUNCHED:(\d+)", out.stdout)
        if out.returncode != 0 or not m:
            print(f"launch rank {r} on {node} failed: {out.stdout} {out.stderr}",
                  file=sys.stderr)
            return 1
        placement[r] = (node, int(m.group(1)))
    print("placement:", " ".join(f"{r}@{n}" for r, (n, _) in placement.items()))

    wait_until("epoch-0 ACTIVE",
               lambda: len([s for s in states(redis_host, comm, 0).values()
                            if s == "ACTIVE"]) == npeers, 120)
    print("epoch 0: all ACTIVE")
    time.sleep(args.evacuate_after)

    evac_ranks = [r for r, (n, _) in placement.items() if n == args.evacuate_node]
    pre_hosts = {r: registry_host_entry(redis_host, comm, r).get("host_id", "")
                 for r in evac_ranks}
    print(f"evacuating {args.evacuate_node} (ranks {evac_ranks}, host_ids {pre_hosts})")

    agent_env = "FMI_CRIU_EXTRA_ARGS=--unprivileged"
    ev = run_on(base, user, args.evacuate_node,
                f"cd {shlex.quote(HERE)}; {agent_env} {shlex.quote(args.agent)} "
                f"evacuate-local {shlex.quote(comm)} {npeers} {shlex.quote(args.config)}",
                timeout=300)
    print("evacuate-local:", ev.stdout.strip(), ev.stderr.strip()[:400])
    m = re.search(r"staged_epoch=(\d+) ranks=([\d,]+)", ev.stdout)
    if ev.returncode != 0 or not m:
        return 2
    staged_epoch = int(m.group(1))
    staged = [int(x) for x in m.group(2).split(",")]

    for r in staged:
        rr = run_on(base, user, args.restore_node,
                    f"cd {shlex.quote(HERE)}; {agent_env} {shlex.quote(args.agent)} "
                    f"restore-remote {shlex.quote(comm)} {npeers} "
                    f"{shlex.quote(args.config)} {r}", timeout=300)
        print(f"restore-remote rank {r}:", rr.stdout.strip(), rr.stderr.strip()[:400])
        if rr.returncode != 0:
            return 3

    wait_until("relocation visible in CRIU registry",
               lambda: all(
                   registry_host_entry(redis_host, comm, r).get("state") == "RUNNING" and
                   registry_host_entry(redis_host, comm, r).get("host_id") != pre_hosts[r]
                   for r in staged), 120)
    print("all restored ranks RUNNING on a new host")

    pr = subprocess.run([args.agent, "promote", comm, str(npeers), args.config],
                        capture_output=True, text=True, cwd=HERE, timeout=120,
                        env={**os.environ, "FMI_CRIU_EXTRA_ARGS": "--unprivileged"})
    print("promote:", pr.stdout.strip(), pr.stderr.strip()[:400])
    if pr.returncode != 0:
        return 4

    wait_until(f"epoch >= {staged_epoch}",
               lambda: current_epoch(redis_host, comm) >= staged_epoch, 120)
    wait_until(f"all ACTIVE at epoch {staged_epoch}",
               lambda: len([s for s in states(redis_host, comm, staged_epoch).values()
                            if s == "ACTIVE"]) == npeers, 180)
    print(f"epoch {staged_epoch}: all ACTIVE — migration complete, waiting for DONE")

    def all_done():
        done = 0
        for r in range(npeers):
            try:
                text = open(os.path.join(outdir, f"r{r}.log")).read()
            except OSError:
                continue
            if DONE_RE.search(text) or "MISMATCH" in text or "terminate called" in text:
                done += 1
        return done == npeers
    wait_until("every rank DONE", all_done, 1800, interval=2.0)

    failures = []
    sums = {}
    for r in range(npeers):
        text = open(os.path.join(outdir, f"r{r}.log")).read()
        m = DONE_RE.search(text)
        if m:
            sums[r] = int(m.group(3))
        for bad in ("MISMATCH", "terminate called", "Segmentation", "stack smashing"):
            if bad in text:
                failures.append(f"rank {r}: {bad}")
    if args.baseline_sums:
        want = {int(k): v for k, v in json.loads(args.baseline_sums).items()}
        for r, w in want.items():
            if sums.get(r) != w:
                failures.append(f"rank {r} checksum {sums.get(r)} != baseline {w}")
    if len(sums) != npeers:
        failures.append(f"only {len(sums)}/{npeers} ranks reached DONE")

    print("checksums:", sums)
    if failures:
        print("FAIL:", "; ".join(failures))
        return 5
    print(f"PASS: cross-host migration {args.evacuate_node} -> {args.restore_node}, "
          f"ranks {staged}, epoch {staged_epoch}, all {npeers} ranks DONE")
    return 0


if __name__ == "__main__":
    sys.exit(main())
