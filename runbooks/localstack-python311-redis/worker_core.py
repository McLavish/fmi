"""Shared worker body for both Lambda and VM container substrates."""
import time
from pathlib import Path

import fmi


CONFIG_PATH = Path(__file__).with_name("fmi-worker.json")


def run_worker(peer_id, num_peers, comm_name, worker_id, placement, n, gap_s, resume=False):
    """Run the FMI collective workload and return a status dict.

    Works as a Lambda invocation (called from lambda_function.py) and as a
    long-lived container process (called from vm_worker.py).  The config path
    is resolved relative to this file so it is found whether the file lives at
    /var/task/ (Lambda) or at a bind-mounted repo path (VM container).

    When resume=True the worker is a post-migration replacement: it skips the
    phase-1 allreduce loop and the sleep gap, jumping directly to the barrier
    and phase-2 allreduces.  This aligns its epoch-1 op sequence with the
    survivor's epoch-1 op sequence (which reconfigures at the barrier entry).

    The returned dict includes "result": the final allreduce value from the
    last phase-2 iteration.  For a 2-peer run the expected post-migration
    result is 3.0 (rank0 contributes 1.0, rank1 contributes 2.0).
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

        if not resume:
            # Phase 1: run allreduces before the migration window
            for _ in range(n):
                comm.allreduce(
                    float(peer_id + 1),
                    fmi.func(fmi.op.sum),
                    fmi.types(fmi.datatypes.double),
                )

            # Sleep gap: gives the orchestrator time to detect both ranks ACTIVE
            # and trigger migration before the barrier
            time.sleep(gap_s)

        # Barrier: survivor reconfigures to epoch 1 here; replacement (resume=True)
        # joins epoch 1 during Communicator construction and starts here too.
        comm.barrier()

        # Phase 2: post-migration allreduces — the key proof of live communication
        result = None
        for _ in range(n):
            result = comm.allreduce(
                float(peer_id + 1),
                fmi.func(fmi.op.sum),
                fmi.types(fmi.datatypes.double),
            )

        # Self-verify the post-migration result so BOTH substrates fail loudly on
        # a wrong collective — the surviving VM and the replacement Lambda, not
        # just the orchestrator-inspected return value.  Each rank p contributes
        # (p + 1), so a sum-allreduce over n peers must equal n*(n+1)/2.
        expected = num_peers * (num_peers + 1) / 2.0
        if result != expected:
            return {
                "peer_id": peer_id,
                "placement": placement,
                "status": f"wrong result: got {result}, expected {expected}",
                "result": result,
            }

    except Exception as e:
        return {"peer_id": peer_id, "placement": placement, "status": f"partial: {e}", "result": None}

    return {"peer_id": peer_id, "placement": placement, "status": "ok", "result": result}
