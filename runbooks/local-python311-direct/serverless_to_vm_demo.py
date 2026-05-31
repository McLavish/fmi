import argparse
import json
import os
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path

try:
    import fmi
except ImportError:
    repo_root = Path(__file__).resolve().parents[2]
    for build_dir in (repo_root / "python" / "build-native-debug", repo_root / "python" / "build-native"):
        if (build_dir / "fmi.so").exists():
            sys.path.insert(0, str(build_dir))
            break
    import fmi


DEFAULT_CONFIG = Path(__file__).with_name("fmi-ft.json")
INT_TYPE = fmi.types(fmi.datatypes.int)
SUM_FUNC = fmi.func(fmi.op.sum)


def emit(payload):
    print(json.dumps(payload), flush=True)


def run_worker(peer_id, num_peers, config_path, comm_name, placement, role, worker_id, wait_ms, faas_memory):
    session = fmi.FTSession(peer_id, num_peers, str(config_path), comm_name, worker_id, faas_memory, placement)

    if role == "normal":
        # A normal worker can use its communicator immediately; a replacement worker must
        # wait for safe_point() to reconfigure before it may touch the communicator at all.
        session.hint(fmi.hints.fast)
        phase1_sum = session.allreduce(peer_id + 1, SUM_FUNC, INT_TYPE)
        emit({
            "event": "phase1",
            "rank": peer_id,
            "epoch": session.epoch(),
            "sum": phase1_sum,
            "worker_id": worker_id,
            "placement": placement,
        })
        time.sleep(wait_ms / 1000.0)

    while True:
        event = session.safe_point()
        if event == fmi.ft_events.migrate_self:
            emit({
                "event": "migrate_self",
                "rank": peer_id,
                "epoch": session.epoch(),
                "worker_id": worker_id,
                "placement": placement,
            })
            return 0
        if event == fmi.ft_events.reconfigured:
            emit({
                "event": "reconfigured",
                "rank": peer_id,
                "epoch": session.epoch(),
                "worker_id": worker_id,
                "placement": placement,
            })
            break
        time.sleep(0.05)

    # safe_point() rebuilt the communicator for the new epoch, so (re-)apply the hint here.
    session.hint(fmi.hints.fast)
    phase2_sum = session.allreduce(100 + peer_id, SUM_FUNC, INT_TYPE)
    emit({
        "event": "phase2",
        "rank": peer_id,
        "epoch": session.epoch(),
        "sum": phase2_sum,
        "worker_id": worker_id,
        "placement": placement,
    })
    return 0


def start_reader(tag, process, events, events_lock):
    def read_stdout():
        for line in process.stdout:
            line = line.rstrip()
            if not line:
                continue
            try:
                event = json.loads(line)
                event["source"] = tag
                with events_lock:
                    events.append(event)
                print(f"[{tag}] {line}", flush=True)
            except json.JSONDecodeError:
                print(f"[{tag}:log] {line}", flush=True)

    thread = threading.Thread(target=read_stdout, daemon=True)
    thread.start()
    return thread


def wait_for(events, events_lock, predicate, timeout, what):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with events_lock:
            snapshot = list(events)
        if predicate(snapshot):
            return snapshot
        time.sleep(0.05)
    raise TimeoutError(what)


def launch_worker(peer_id, num_peers, config_path, comm_name, placement, role, worker_id, wait_ms, faas_memory, tag, events, events_lock):
    args = [
        sys.executable,
        str(Path(__file__).resolve()),
        "worker",
        "--peer-id", str(peer_id),
        "--num-peers", str(num_peers),
        "--comm-name", comm_name,
        "--placement", placement,
        "--role", role,
        "--worker-id", worker_id,
        "--config", str(config_path),
        "--wait-ms", str(wait_ms),
        "--faas-memory", str(faas_memory),
    ]
    process = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env=os.environ.copy())
    start_reader(tag, process, events, events_lock)
    return process


def format_directory(snapshot):
    by_rank = {entry.rank: entry for entry in snapshot}
    for rank in sorted(by_rank):
        entry = by_rank[rank]
        print(f"  rank={entry.rank} worker_id={entry.worker_id} placement={entry.placement} state={entry.state}")
    return by_rank


def print_check(label, passed):
    print(f"{'PASS' if passed else 'FAIL'} {label}")


def stop_processes(processes):
    for process in processes:
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.terminate()
            try:
                process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=2)


def run_orchestrator(comm_name, num_peers, config_path, wait_ms):
    if num_peers != 2:
        raise RuntimeError("serverless_to_vm_demo requires --num-peers 2")

    coordinator = fmi.FTCoordinator(str(config_path), comm_name, num_peers)
    coordinator.clear_job_state()
    emit({
        "event": "cleanup_done",
        "comm_name": comm_name,
    })

    uid = uuid.uuid4().hex[:8]
    uid2 = uuid.uuid4().hex[:8]
    orig0 = f"{comm_name}-rank0-serverless-{uid}"
    wid1 = f"{comm_name}-rank1-vm"
    repl0 = f"{comm_name}-rank0-vm-replacement-{uid2}"

    events = []
    events_lock = threading.Lock()
    processes = []

    try:
        processes.append(launch_worker(0, num_peers, config_path, comm_name, "serverless", "normal", orig0, wait_ms, 1024, "rank0",
                                       events, events_lock))
        processes.append(launch_worker(1, num_peers, config_path, comm_name, "vm", "normal", wid1, wait_ms, 1024, "rank1",
                                       events, events_lock))

        snapshot = wait_for(
            events,
            events_lock,
            lambda items: {event["rank"] for event in items if event.get("event") == "phase1"} == {0, 1},
            30,
            "timed out waiting for phase1 events from ranks 0 and 1",
        )
        phase1_sum = next(event["sum"] for event in snapshot if event.get("event") == "phase1")

        coordinator.request_migration(0)
        emit({
            "event": "migration_requested",
            "rank": 0,
            "epoch": coordinator.epoch(),
        })

        wait_for(
            events,
            events_lock,
            lambda items: any(event.get("event") == "migrate_self" and event.get("rank") == 0 for event in items),
            30,
            "timed out waiting for rank0 migrate_self",
        )

        processes.append(launch_worker(0, num_peers, config_path, comm_name, "vm", "replacement", repl0, wait_ms, 1024, "repl0",
                                       events, events_lock))

        snapshot = wait_for(
            events,
            events_lock,
            lambda items: {event["rank"] for event in items if event.get("event") == "phase2"} == {0, 1},
            30,
            "timed out waiting for phase2 events from ranks 0 and 1",
        )
        phase2_sum = next(event["sum"] for event in snapshot if event.get("event") == "phase2")
    finally:
        stop_processes(processes)

    dir0 = coordinator.directory_snapshot(0)
    dir1 = coordinator.directory_snapshot(1)
    final_epoch = coordinator.epoch()

    print("Epoch 0 directory:")
    dir0_by_rank = format_directory(dir0)
    print("Epoch 1 directory:")
    dir1_by_rank = format_directory(dir1)
    print(f"Phase 1 sum: {phase1_sum}")
    print(f"Phase 2 sum: {phase2_sum}")

    checks = [
        ("phase1 sum == sum(p+1 for p in range(num_peers))", phase1_sum == sum(p + 1 for p in range(num_peers))),
        ("final_epoch == 1", final_epoch == 1),
        ("phase2 sum == sum(100+p for p in range(num_peers))", phase2_sum == sum(100 + p for p in range(num_peers))),
        ("dir0 placements are rank0=serverless and rank1=vm",
         dir0_by_rank.get(0) is not None and dir0_by_rank.get(1) is not None and
         dir0_by_rank[0].placement == "serverless" and dir0_by_rank[1].placement == "vm"),
        ("dir1 placements are rank0=vm and rank1=vm",
         dir1_by_rank.get(0) is not None and dir1_by_rank.get(1) is not None and
         dir1_by_rank[0].placement == "vm" and dir1_by_rank[1].placement == "vm"),
        ("logical rank ids unchanged across the move", set(dir0_by_rank) == {0, 1} and set(dir1_by_rank) == {0, 1}),
        ("rank0 survived on a NEW worker",
         dir0_by_rank.get(0) is not None and dir1_by_rank.get(0) is not None and
         dir0_by_rank[0].worker_id != dir1_by_rank[0].worker_id),
        ("rank1 stayed the same worker",
         dir0_by_rank.get(1) is not None and dir1_by_rank.get(1) is not None and
         dir1_by_rank[1].worker_id == dir0_by_rank[1].worker_id),
    ]

    for label, passed in checks:
        print_check(label, passed)

    overall = all(passed for _, passed in checks)
    print(f"OVERALL: {'PASS' if overall else 'FAIL'}")
    return 0 if overall else 1


def parse_args():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="mode", required=True)

    worker_parser = subparsers.add_parser("worker")
    worker_parser.add_argument("--peer-id", type=int, required=True)
    worker_parser.add_argument("--num-peers", type=int, default=2)
    worker_parser.add_argument("--comm-name", required=True)
    worker_parser.add_argument("--placement", choices=["serverless", "vm"], required=True)
    worker_parser.add_argument("--role", choices=["normal", "replacement"], default="normal")
    worker_parser.add_argument("--worker-id", required=True)
    worker_parser.add_argument("--config", default=str(DEFAULT_CONFIG))
    worker_parser.add_argument("--wait-ms", type=int, default=300)
    worker_parser.add_argument("--faas-memory", type=int, default=1024)

    run_parser = subparsers.add_parser("run")
    run_parser.add_argument("--comm-name", required=True)
    run_parser.add_argument("--num-peers", type=int, default=2)
    run_parser.add_argument("--config", default=str(DEFAULT_CONFIG))
    run_parser.add_argument("--wait-ms", type=int, default=300)

    return parser.parse_args()


def main():
    args = parse_args()
    config_path = Path(args.config)

    try:
        if args.mode == "worker":
            return run_worker(args.peer_id, args.num_peers, config_path, args.comm_name, args.placement, args.role, args.worker_id,
                              args.wait_ms, args.faas_memory)

        if args.mode == "run":
            return run_orchestrator(args.comm_name, args.num_peers, config_path, args.wait_ms)

        raise RuntimeError(f"unknown mode: {args.mode}")
    except Exception as exc:
        print(f"fatal: {exc}", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
