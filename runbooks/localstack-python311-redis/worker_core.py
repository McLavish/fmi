"""Shared worker body for both Lambda and VM container substrates."""
import time
from pathlib import Path

import fmi


CONFIG_PATH = Path(__file__).with_name("fmi-worker.json")


def run_worker(peer_id, num_peers, comm_name, worker_id, placement, n, gap_s):
    """Run the FMI collective workload and return a status dict.

    Works as a Lambda invocation (called from lambda_function.py) and as a
    long-lived container process (called from vm_worker.py).  The config path
    is resolved relative to this file so it is found whether the file lives at
    /var/task/ (Lambda) or at a bind-mounted repo path (VM container).
    """
    try:
        comm = fmi.Communicator(
            peer_id,
            num_peers,
            str(CONFIG_PATH),
            comm_name,
            1024,
            worker_id,
            placement,
        )
        comm.hint(fmi.hints.fast)

        for _ in range(n):
            comm.allreduce(
                float(peer_id + 1),
                fmi.func(fmi.op.sum),
                fmi.types(fmi.datatypes.double),
            )

        time.sleep(gap_s)
        comm.barrier()

        for _ in range(n):
            comm.allreduce(
                float(peer_id + 1),
                fmi.func(fmi.op.sum),
                fmi.types(fmi.datatypes.double),
            )

    except Exception as e:
        return {"peer_id": peer_id, "placement": placement, "status": f"partial: {e}"}

    return {"peer_id": peer_id, "placement": placement, "status": "ok"}
