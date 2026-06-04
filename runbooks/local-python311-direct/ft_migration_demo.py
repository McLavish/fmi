import argparse
import json
import shutil
import sys
import time
from pathlib import Path

import fmi


DEFAULT_CONFIG = Path(__file__).with_name("fmi-ft.json")
INT_TYPE = fmi.types(fmi.datatypes.int)
SUM_FUNC = fmi.func(fmi.op.sum)


def emit(payload):
    print(json.dumps(payload), flush=True)


def checkpoint_file(checkpoint_dir, comm_name, peer_id):
    return Path(checkpoint_dir) / comm_name / f"rank-{peer_id}.json"


def save_checkpoint(checkpoint_dir, comm_name, peer_id, state):
    path = checkpoint_file(checkpoint_dir, comm_name, peer_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state))


def load_checkpoint(checkpoint_dir, comm_name, peer_id):
    path = checkpoint_file(checkpoint_dir, comm_name, peer_id)
    if not path.exists():
        raise RuntimeError(f"checkpoint missing for rank {peer_id}: {path}")
    return json.loads(path.read_text())


def run_normal_peer(peer_id, num_peers, config_path, comm_name, worker_id, wait_ms, checkpoint_dir, faas_memory):
    session = fmi.FTSession(peer_id, num_peers, str(config_path), comm_name, worker_id, faas_memory)
    session.hint(fmi.hints.fast)

    phase1_sum = session.allreduce(peer_id + 1, SUM_FUNC, INT_TYPE)
    emit({
        "event": "phase1",
        "rank": peer_id,
        "epoch": session.epoch(),
        "sum": phase1_sum,
        "worker_id": worker_id,
    })

    time.sleep(wait_ms / 1000.0)

    event = session.safe_point()
    if event == fmi.ft_events.migrate_self:
        save_checkpoint(
            checkpoint_dir,
            comm_name,
            peer_id,
            {
                "phase1_sum": phase1_sum,
                "phase2_value": 100 + peer_id,
            },
        )
        emit({
            "event": "migrate_self",
            "rank": peer_id,
            "epoch": session.epoch(),
            "worker_id": worker_id,
        })
        return 100

    if event == fmi.ft_events.reconfigured:
        emit({
            "event": "reconfigured",
            "rank": peer_id,
            "epoch": session.epoch(),
            "worker_id": worker_id,
        })

    phase2_sum = session.allreduce(100 + peer_id, SUM_FUNC, INT_TYPE)
    emit({
        "event": "phase2",
        "rank": peer_id,
        "epoch": session.epoch(),
        "sum": phase2_sum,
        "worker_id": worker_id,
        "restored": False,
    })
    return 0


def run_replacement_peer(peer_id, num_peers, config_path, comm_name, worker_id, checkpoint_dir, faas_memory):
    state = load_checkpoint(checkpoint_dir, comm_name, peer_id)
    session = fmi.FTSession(peer_id, num_peers, str(config_path), comm_name, worker_id, faas_memory)

    event = session.safe_point()
    if event != fmi.ft_events.reconfigured:
        raise RuntimeError(f"replacement rank {peer_id} expected reconfigured, got {event}")

    session.hint(fmi.hints.fast)
    emit({
        "event": "reconfigured",
        "rank": peer_id,
        "epoch": session.epoch(),
        "worker_id": worker_id,
        "restored_phase1_sum": state["phase1_sum"],
    })

    phase2_sum = session.allreduce(state["phase2_value"], SUM_FUNC, INT_TYPE)
    emit({
        "event": "phase2",
        "rank": peer_id,
        "epoch": session.epoch(),
        "sum": phase2_sum,
        "worker_id": worker_id,
        "restored": True,
        "restored_phase1_sum": state["phase1_sum"],
    })
    return 0


def run_migration_request(rank, num_peers, config_path, comm_name):
    coordinator = fmi.FTCoordinator(str(config_path), comm_name, num_peers)
    coordinator.request_migration(rank)
    emit({
        "event": "migration_requested",
        "rank": rank,
        "epoch": coordinator.epoch(),
    })
    return 0


def run_promotion(num_peers, config_path, comm_name):
    coordinator = fmi.FTCoordinator(str(config_path), comm_name, num_peers)
    coordinator.promote_epoch()
    emit({
        "event": "epoch_promoted",
        "epoch": coordinator.epoch(),
    })
    return 0


def run_cleanup(num_peers, config_path, comm_name, checkpoint_dir):
    coordinator = fmi.FTCoordinator(str(config_path), comm_name, num_peers)
    coordinator.clear_job_state()

    checkpoint_root = Path(checkpoint_dir) / comm_name
    if checkpoint_root.exists():
        shutil.rmtree(checkpoint_root)

    emit({
        "event": "cleanup_done",
        "comm_name": comm_name,
    })
    return 0


def parse_args():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="mode", required=True)

    peer_parser = subparsers.add_parser("peer")
    peer_parser.add_argument("--peer-id", type=int, required=True)
    peer_parser.add_argument("--num-peers", type=int, required=True)
    peer_parser.add_argument("--comm-name", required=True)
    peer_parser.add_argument("--worker-id", required=True)
    peer_parser.add_argument("--role", choices=["normal", "replacement"], required=True)
    peer_parser.add_argument("--wait-ms", type=int, default=750)
    peer_parser.add_argument("--checkpoint-dir", default="/tmp/fmi-ft-checkpoints")
    peer_parser.add_argument("--faas-memory", type=int, default=1024)
    peer_parser.add_argument("--config", default=str(DEFAULT_CONFIG))

    migrate_parser = subparsers.add_parser("migrate")
    migrate_parser.add_argument("--rank", type=int, required=True)
    migrate_parser.add_argument("--num-peers", type=int, required=True)
    migrate_parser.add_argument("--comm-name", required=True)
    migrate_parser.add_argument("--config", default=str(DEFAULT_CONFIG))

    promote_parser = subparsers.add_parser("promote")
    promote_parser.add_argument("--num-peers", type=int, required=True)
    promote_parser.add_argument("--comm-name", required=True)
    promote_parser.add_argument("--config", default=str(DEFAULT_CONFIG))

    cleanup_parser = subparsers.add_parser("cleanup")
    cleanup_parser.add_argument("--num-peers", type=int, required=True)
    cleanup_parser.add_argument("--comm-name", required=True)
    cleanup_parser.add_argument("--checkpoint-dir", default="/tmp/fmi-ft-checkpoints")
    cleanup_parser.add_argument("--config", default=str(DEFAULT_CONFIG))

    return parser.parse_args()


def main():
    args = parse_args()
    config_path = Path(args.config)

    try:
        if args.mode == "peer":
            if args.role == "normal":
                return run_normal_peer(args.peer_id, args.num_peers, config_path, args.comm_name, args.worker_id, args.wait_ms,
                                       args.checkpoint_dir, args.faas_memory)
            return run_replacement_peer(args.peer_id, args.num_peers, config_path, args.comm_name, args.worker_id, args.checkpoint_dir,
                                        args.faas_memory)

        if args.mode == "migrate":
            return run_migration_request(args.rank, args.num_peers, config_path, args.comm_name)

        if args.mode == "promote":
            return run_promotion(args.num_peers, config_path, args.comm_name)

        if args.mode == "cleanup":
            return run_cleanup(args.num_peers, config_path, args.comm_name, args.checkpoint_dir)

        raise RuntimeError(f"unknown mode: {args.mode}")
    except Exception as exc:
        print(f"fatal: {exc}", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
