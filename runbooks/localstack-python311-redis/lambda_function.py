from pathlib import Path
import time

import fmi


CONFIG_PATH = Path(__file__).with_name("fmi-worker.json")


def lambda_handler(event, context):
    try:
        peer_id = int(event["peer_id"])
        num_peers = int(event["num_peers"])
        comm_name = event["comm_name"]
        worker_id = event["worker_id"]
        placement = event["placement"]
        n = int(event.get("n", 2))
        gap_s = float(event.get("gap_s", 5.0))

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
