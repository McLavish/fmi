#!/usr/bin/env python3
"""
Transparent migration demo: 2 ranks run a plain fmi.Communicator allreduce loop.
No safe_point() calls — migration is handled transparently by FMI at operation
boundaries. One rank is migrated mid-run; the demo verifies the rank directory
flip via FTCoordinator.directory_snapshot().

Usage:
    python3 transparent_migration_demo.py run [--comm-name NAME] [--iterations N]
    python3 transparent_migration_demo.py cleanup --comm-name NAME
"""

import argparse
import os
import subprocess
import sys
import time
import uuid

# Locate fmi.so relative to this script
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_BUILD_DIR = os.path.join(_SCRIPT_DIR, "../../python/build-native-debug")
sys.path.insert(0, _BUILD_DIR)

import fmi


CONFIG = os.path.join(_SCRIPT_DIR, "fmi-transparent-migration.json")
NUM_PEERS = 2
MIGRATION_RANK = 0  # rank that will be migrated
ITERATIONS = 10


# ---------------------------------------------------------------------------
# Worker: plain Communicator loop — byte-identical to a non-migration program
# ---------------------------------------------------------------------------

def run_worker(peer_id, num_peers, config, comm_name, worker_id, placement, iterations):
    print(f"[rank {peer_id}] starting worker_id={worker_id} placement={placement}", flush=True)
    comm = fmi.Communicator(peer_id, num_peers, config, comm_name, 128, worker_id, placement)
    comm.hint(fmi.hints.fast)

    for i in range(iterations):
        val = float(peer_id + 1)
        result = comm.allreduce(val, fmi.func(fmi.op.sum), fmi.types(fmi.datatypes.double))
        print(f"[rank {peer_id}] iter {i}: allreduce={result}", flush=True)
        time.sleep(0.1)

    print(f"[rank {peer_id}] done", flush=True)


# ---------------------------------------------------------------------------
# Orchestrator: triggers migration, relaunches the migrated rank, verifies
# ---------------------------------------------------------------------------

def run_orchestrator(comm_name, iterations):
    coordinator = fmi.FTCoordinator(CONFIG, comm_name, NUM_PEERS)
    coordinator.clear_job_state()

    # Unique worker IDs so the replacement is distinguishable
    wid_rank0 = f"rank0-vm-{uuid.uuid4().hex[:8]}"
    wid_rank1 = f"rank1-vm-{uuid.uuid4().hex[:8]}"
    wid_replacement = f"rank0-serverless-{uuid.uuid4().hex[:8]}"

    def spawn(peer_id, worker_id, placement):
        return subprocess.Popen([
            sys.executable, __file__, "worker",
            "--peer-id", str(peer_id),
            "--num-peers", str(NUM_PEERS),
            "--comm-name", comm_name,
            "--worker-id", worker_id,
            "--placement", placement,
            "--iterations", str(iterations),
        ])

    # Start both initial workers (both labelled "vm")
    proc0 = spawn(0, wid_rank0, "vm")
    proc1 = spawn(1, wid_rank1, "vm")

    # Wait a couple of iterations, then trigger migration of rank 0
    time.sleep(0.5)
    print(f"[orchestrator] requesting migration of rank {MIGRATION_RANK}", flush=True)
    coordinator.request_migration(MIGRATION_RANK)

    # Wait for rank0 to quiesce (state becomes QUIESCED in Redis)
    print("[orchestrator] waiting for rank 0 to quiesce ...", flush=True)
    deadline = time.time() + 10
    while time.time() < deadline:
        snap = coordinator.directory_snapshot(0)
        states = {e.rank: e.state for e in snap}
        if states.get(0) == "QUIESCED":
            break
        time.sleep(0.1)
    else:
        print("[orchestrator] ERROR: rank 0 did not quiesce in time", flush=True)
        proc0.wait(); proc1.wait()
        sys.exit(1)

    print("[orchestrator] promoting migration epoch", flush=True)
    coordinator.promote_epoch()

    print(f"[orchestrator] rank 0 quiesced — launching replacement (placement=serverless)", flush=True)
    proc_replacement = spawn(0, wid_replacement, "serverless")

    # Wait for both workers to finish
    proc0.wait()
    proc_replacement.wait()
    proc1.wait()

    # Verify the rank directory
    dir0 = coordinator.directory_snapshot(0)
    dir1 = coordinator.directory_snapshot(1)

    print("\n--- Epoch 0 directory ---")
    for e in dir0:
        print(f"  rank {e.rank}: worker_id={e.worker_id} placement={e.placement} state={e.state}")

    print("\n--- Epoch 1 directory ---")
    for e in dir1:
        print(f"  rank {e.rank}: worker_id={e.worker_id} placement={e.placement} state={e.state}")

    # Acceptance checks
    failures = []

    epoch0_by_rank = {e.rank: e for e in dir0}
    epoch1_by_rank = {e.rank: e for e in dir1}

    if epoch0_by_rank.get(0) and epoch0_by_rank[0].placement != "vm":
        failures.append(f"epoch0 rank0 placement expected 'vm', got '{epoch0_by_rank[0].placement}'")
    if epoch0_by_rank.get(1) and epoch0_by_rank[1].placement != "vm":
        failures.append(f"epoch0 rank1 placement expected 'vm', got '{epoch0_by_rank[1].placement}'")
    if epoch0_by_rank.get(0) and epoch0_by_rank[0].state != "QUIESCED":
        failures.append(f"epoch0 rank0 state expected 'QUIESCED', got '{epoch0_by_rank[0].state}'")

    if epoch1_by_rank.get(0) and epoch1_by_rank[0].placement != "serverless":
        failures.append(f"epoch1 rank0 placement expected 'serverless', got '{epoch1_by_rank[0].placement}'")
    if epoch1_by_rank.get(1) and epoch1_by_rank[1].placement != "vm":
        failures.append(f"epoch1 rank1 placement expected 'vm', got '{epoch1_by_rank[1].placement}'")
    if epoch1_by_rank.get(0) and epoch1_by_rank[0].worker_id == wid_rank0:
        failures.append("epoch1 rank0 still has original worker_id — replacement not detected")
    if epoch1_by_rank.get(1) and epoch1_by_rank[1].worker_id != wid_rank1:
        failures.append(f"epoch1 rank1 worker_id changed (survivor should stay): {epoch1_by_rank[1].worker_id}")

    if failures:
        print("\nFAILED:")
        for f in failures:
            print(f"  - {f}")
        sys.exit(1)

    print("\nPASSED: rank directory flip verified")
    coordinator.clear_job_state()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd")

    run_p = sub.add_parser("run")
    run_p.add_argument("--comm-name", default=f"demo-{int(time.time())}")
    run_p.add_argument("--iterations", type=int, default=ITERATIONS)

    worker_p = sub.add_parser("worker")
    worker_p.add_argument("--peer-id", type=int, required=True)
    worker_p.add_argument("--num-peers", type=int, default=NUM_PEERS)
    worker_p.add_argument("--comm-name", required=True)
    worker_p.add_argument("--worker-id", required=True)
    worker_p.add_argument("--placement", required=True)
    worker_p.add_argument("--iterations", type=int, default=ITERATIONS)

    cleanup_p = sub.add_parser("cleanup")
    cleanup_p.add_argument("--comm-name", required=True)

    args = parser.parse_args()

    if args.cmd == "run":
        run_orchestrator(args.comm_name, args.iterations)
    elif args.cmd == "worker":
        run_worker(args.peer_id, args.num_peers, CONFIG, args.comm_name,
                   args.worker_id, args.placement, args.iterations)
    elif args.cmd == "cleanup":
        fmi.FTCoordinator(CONFIG, args.comm_name, NUM_PEERS).clear_job_state()
        print("cleaned up")
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
