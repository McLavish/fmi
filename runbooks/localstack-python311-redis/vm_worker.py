"""VM container entrypoint.

Reads runtime parameters from environment variables and runs the FMI
collective workload via worker_core.run_worker.  The process is long-lived:
it holds state across the sleep/barrier gap and exits naturally when the
Communicator quiesces on migration (or completes normally).
"""
import os
import sys

# The bundle is bind-mounted at /opt/fmi; PYTHONPATH and LD_LIBRARY_PATH are
# set by the docker run command in orchestrator.py so that "import fmi" works.
from worker_core import run_worker


def main():
    peer_id = int(os.environ["PEER_ID"])
    num_peers = int(os.environ["NUM_PEERS"])
    comm_name = os.environ["COMM_NAME"]
    worker_id = os.environ["WORKER_ID"]
    placement = os.environ.get("PLACEMENT", "vm")
    n = int(os.environ.get("N", "2"))
    gap_s = float(os.environ.get("GAP_S", "5.0"))

    result = run_worker(peer_id, num_peers, comm_name, worker_id, placement, n, gap_s)
    print(result, flush=True)
    status = result.get("status", "")
    if status != "ok" and not status.startswith("partial"):
        sys.exit(1)


if __name__ == "__main__":
    main()
