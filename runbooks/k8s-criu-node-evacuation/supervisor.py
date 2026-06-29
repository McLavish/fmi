#!/usr/bin/env python3
"""Per-machine supervisor for the CRIU node-evacuation demo.

Runs as the container's main process in each "machine" pod. It:
  1. renders the FMI config from the template, pinning this machine's criu.host_id (so the rank
     agent can later discover exactly this pod's ranks) plus the shared Redis + tcpunchd
     endpoints;
  2. launches this machine's slice of ranks (BASE_PEER_ID .. BASE_PEER_ID+RANKS_HERE-1) as the
     transparent_state_transfer_demo worker, each in its own session, writing to a regular log
     file (criu can re-open a regular file on restore; a container stdout pipe could not);
  3. stays alive and tails the per-rank logs to its own stdout (so `kubectl logs` shows progress
     and the final per-rank OK/FAIL), so that workers restored detached by `fmi-rank-agent
     migrate-local` (run via `kubectl exec` into this same container) outlive it.

The supervisor never restarts a worker: a migrated rank is killed by criu dump and brought back
by criu restore as a detached process, so its exit here is expected and must not trigger a relaunch
(SIGCHLD is ignored so the transient zombie is auto-reaped).
"""
import os
import signal
import subprocess
import sys
from string import Template


def env(name, default=None, required=False):
    val = os.environ.get(name, default)
    if required and (val is None or val == ""):
        sys.exit(f"[supervisor] missing required env {name}")
    return val


def main():
    host_id = env("HOST_ID", required=True)            # e.g. machine-a
    num_peers = int(env("NUM_PEERS", required=True))   # total ranks across all machines
    base_peer_id = int(env("BASE_PEER_ID", required=True))
    ranks_here = int(env("RANKS_HERE", required=True)) # ranks this machine runs
    comm_name = env("COMM_NAME", required=True)
    redis_host = env("REDIS_HOST", "fmi-redis")
    tcpunch_host = env("TCPUNCH_HOST", "fmi-tcpunch")
    window_ms = env("WINDOW_MS", "20000")
    template_path = env("CONFIG_TEMPLATE", "/opt/fmi/app/fmi-machine.json.tmpl")
    config_path = env("CONFIG_PATH", "/tmp/fmi.json")
    worker = env("FMI_DEMO_BIN", "/opt/fmi/bin/transparent_state_transfer_demo")
    log_dir = env("LOG_DIR", "/tmp")

    # Render the per-machine config (only host_id / endpoints vary between machines).
    with open(template_path) as f:
        rendered = Template(f.read()).safe_substitute(
            REDIS_HOST=redis_host, TCPUNCH_HOST=tcpunch_host, HOST_ID=host_id)
    with open(config_path, "w") as f:
        f.write(rendered)

    print(f"[supervisor] host_id={host_id} comm={comm_name} num_peers={num_peers} "
          f"ranks={base_peer_id}..{base_peer_id + ranks_here - 1} redis={redis_host} "
          f"tcpunch={tcpunch_host} window_ms={window_ms} config={config_path}", flush=True)

    # A migrated rank is killed/restored by criu; never relaunch it, just auto-reap the zombie.
    signal.signal(signal.SIGCHLD, signal.SIG_IGN)

    log_files = []
    for peer_id in range(base_peer_id, base_peer_id + ranks_here):
        log_path = os.path.join(log_dir, f"rank-{peer_id}.log")
        log_files.append(log_path)
        # Truncate/create up front so `tail -F` below has something to follow.
        fh = open(log_path, "w")
        subprocess.Popen(
            [worker, str(peer_id), str(num_peers), config_path, comm_name, str(window_ms)],
            stdout=fh, stderr=subprocess.STDOUT, start_new_session=True)
        fh.close()
        print(f"[supervisor] launched rank {peer_id} -> {log_path}", flush=True)

    # Mirror every rank's log to our stdout and block forever. tail -F survives the file being
    # re-opened by a restored worker.
    tail = subprocess.Popen(["tail", "-n", "+1", "-F", *log_files], stdout=sys.stdout)
    tail.wait()


if __name__ == "__main__":
    main()
