"""Lambda handler — thin wrapper that delegates to worker_core.run_worker."""
from worker_core import run_worker


def lambda_handler(event, context):
    peer_id = int(event["peer_id"])
    num_peers = int(event["num_peers"])
    comm_name = event["comm_name"]
    worker_id = event["worker_id"]
    placement = event["placement"]
    n = int(event.get("n", 2))
    gap_s = float(event.get("gap_s", 5.0))
    resume = bool(event.get("resume", False))

    return run_worker(peer_id, num_peers, comm_name, worker_id, placement, n, gap_s, resume=resume)
