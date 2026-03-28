from pathlib import Path

import fmi


CONFIG_PATH = str(Path(__file__).with_name("fmi.json"))


def lambda_handler(event, context):
    peer_id = int(event["peer_id"])
    num_peers = int(event["num_peers"])
    comm_name = event["comm_name"]
    faas_memory = int(event.get("faas_memory", 1024))

    comm = fmi.Communicator(peer_id, num_peers, CONFIG_PATH, comm_name, faas_memory)
    comm.hint(fmi.hints.fast)

    total = comm.allreduce(peer_id, fmi.func(fmi.op.sum), fmi.types(fmi.datatypes.int))

    if peer_id == 0:
        comm.bcast(42, 0, fmi.types(fmi.datatypes.int))
        bcast_value = 42
    else:
        bcast_value = comm.bcast(None, 0, fmi.types(fmi.datatypes.int))

    comm.barrier()

    return {
        "peer_id": peer_id,
        "num_peers": num_peers,
        "comm_name": comm_name,
        "sum_of_peer_ids": total,
        "bcast_value": bcast_value,
    }
