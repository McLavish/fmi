import argparse
import json
from pathlib import Path

import fmi


CONFIG_PATH = str(Path(__file__).with_name("fmi.json"))


def run_peer(peer_id, num_peers, comm_name, faas_memory):
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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--peer-id", type=int, required=True)
    parser.add_argument("--num-peers", type=int, required=True)
    parser.add_argument("--comm-name", required=True)
    parser.add_argument("--faas-memory", type=int, default=1024)
    args = parser.parse_args()

    result = run_peer(args.peer_id, args.num_peers, args.comm_name, args.faas_memory)
    print(json.dumps(result))


if __name__ == "__main__":
    main()
