#!/usr/bin/env python3
import argparse
import json
import sys
import time
import uuid
from pathlib import Path

import boto3


RUNBOOK_DIR = Path(__file__).resolve().parent
REPO_ROOT = RUNBOOK_DIR.parents[1]
sys.path.insert(0, str(REPO_ROOT / "python" / "build-native-debug"))

import fmi


FUNCTION_NAME = "fmi-migration-worker"
HOST_CONFIG = RUNBOOK_DIR / "fmi-host.json"
NUM_PEERS = 2


def lambda_client():
    return boto3.client(
        "lambda",
        endpoint_url="http://localhost:4566",
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )


def invoke_worker(client, payload):
    response = client.invoke(
        FunctionName=FUNCTION_NAME,
        InvocationType="Event",
        Payload=json.dumps(payload).encode("utf-8"),
    )
    status_code = response.get("StatusCode")
    if status_code not in (202, 204):
        raise RuntimeError(f"unexpected invoke status for {payload}: {status_code}")


def snapshot_by_rank(coordinator, epoch):
    return {entry.rank: entry for entry in coordinator.directory_snapshot(epoch)}


def wait_for_snapshot(coordinator, epoch, predicate, description, timeout_s, interval_s=1.0):
    deadline = time.time() + timeout_s
    last_snapshot = {}
    while time.time() < deadline:
        last_snapshot = snapshot_by_rank(coordinator, epoch)
        if predicate(last_snapshot):
            return last_snapshot
        time.sleep(interval_s)
    raise TimeoutError(f"timed out waiting for {description}; last={format_snapshot(last_snapshot)}")


def format_snapshot(snapshot):
    if isinstance(snapshot, dict):
        entries = snapshot.values()
    else:
        entries = snapshot
    rows = []
    for entry in sorted(entries, key=lambda e: e.rank):
        rows.append(
            f"rank {entry.rank}: worker_id={entry.worker_id} "
            f"placement={entry.placement} state={entry.state}"
        )
    return "\n".join(rows) if rows else "(empty)"


def print_snapshot(title, snapshot):
    print(f"\n--- {title} ---")
    print(format_snapshot(snapshot))


def both_ranks_active(snapshot):
    return all(rank in snapshot and snapshot[rank].state == "ACTIVE" for rank in (0, 1))


def both_ranks_present(snapshot):
    return all(rank in snapshot for rank in (0, 1))


def run(comm_name, n):
    coordinator = fmi.FTCoordinator(str(HOST_CONFIG), comm_name, NUM_PEERS)
    coordinator.clear_job_state()
    client = lambda_client()

    rank0_vm_wid = f"rank0-vm-{uuid.uuid4().hex[:8]}"
    rank1_vm_wid = f"rank1-vm-{uuid.uuid4().hex[:8]}"
    rank0_sl_wid = f"rank0-serverless-{uuid.uuid4().hex[:8]}"

    invoke_worker(
        client,
        {
            "peer_id": 0,
            "num_peers": NUM_PEERS,
            "comm_name": comm_name,
            "worker_id": rank0_vm_wid,
            "placement": "vm",
            "n": n,
        },
    )
    invoke_worker(
        client,
        {
            "peer_id": 1,
            "num_peers": NUM_PEERS,
            "comm_name": comm_name,
            "worker_id": rank1_vm_wid,
            "placement": "vm",
            "n": n,
        },
    )

    epoch0 = wait_for_snapshot(
        coordinator,
        0,
        both_ranks_active,
        "epoch 0 ranks 0 and 1 ACTIVE",
        timeout_s=60,
    )

    print("[orchestrator] requesting migration of rank 0", flush=True)
    coordinator.request_migration(0)

    epoch0 = wait_for_snapshot(
        coordinator,
        0,
        lambda snap: 0 in snap and snap[0].state == "QUIESCED",
        "epoch 0 rank 0 QUIESCED",
        timeout_s=30,
    )

    invoke_worker(
        client,
        {
            "peer_id": 0,
            "num_peers": NUM_PEERS,
            "comm_name": comm_name,
            "worker_id": rank0_sl_wid,
            "placement": "serverless",
            "n": n,
        },
    )

    epoch1 = wait_for_snapshot(
        coordinator,
        1,
        both_ranks_present,
        "epoch 1 ranks 0 and 1 present",
        timeout_s=60,
    )

    print_snapshot("Epoch 0 directory", epoch0)
    print_snapshot("Epoch 1 directory", epoch1)

    failures = []

    if 0 not in epoch0:
        failures.append("epoch0 rank0 missing")
    else:
        if epoch0[0].placement != "vm":
            failures.append(f"epoch0 rank0 placement expected 'vm', got '{epoch0[0].placement}'")
        if epoch0[0].state != "QUIESCED":
            failures.append(f"epoch0 rank0 state expected 'QUIESCED', got '{epoch0[0].state}'")
        if epoch0[0].worker_id != rank0_vm_wid:
            failures.append(f"epoch0 rank0 worker_id expected '{rank0_vm_wid}', got '{epoch0[0].worker_id}'")

    if 1 not in epoch0:
        failures.append("epoch0 rank1 missing")
    else:
        if epoch0[1].placement != "vm":
            failures.append(f"epoch0 rank1 placement expected 'vm', got '{epoch0[1].placement}'")
        if epoch0[1].worker_id != rank1_vm_wid:
            failures.append(f"epoch0 rank1 worker_id expected '{rank1_vm_wid}', got '{epoch0[1].worker_id}'")

    if 0 not in epoch1:
        failures.append("epoch1 rank0 missing")
    else:
        if epoch1[0].placement != "serverless":
            failures.append(f"epoch1 rank0 placement expected 'serverless', got '{epoch1[0].placement}'")
        if epoch1[0].worker_id != rank0_sl_wid:
            failures.append(f"epoch1 rank0 worker_id expected '{rank0_sl_wid}', got '{epoch1[0].worker_id}'")
        if epoch1[0].worker_id == rank0_vm_wid:
            failures.append("epoch1 rank0 still has original worker_id")

    if 1 not in epoch1:
        failures.append("epoch1 rank1 missing")
    else:
        if epoch1[1].placement != "vm":
            failures.append(f"epoch1 rank1 placement expected 'vm', got '{epoch1[1].placement}'")
        if epoch1[1].worker_id != rank1_vm_wid:
            failures.append(f"epoch1 rank1 worker_id changed: {epoch1[1].worker_id}")

    if failures:
        print("\nFAILED:")
        for failure in failures:
            print(f"  - {failure}")
        sys.exit(1)

    print("\nPASSED: rank directory flip verified")
    coordinator.clear_job_state()


def cleanup(comm_name):
    fmi.FTCoordinator(str(HOST_CONFIG), comm_name, NUM_PEERS).clear_job_state()
    print(f"cleaned up {comm_name}")


def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd", required=True)

    run_p = sub.add_parser("run")
    run_p.add_argument("--comm-name", default=f"demo-{int(time.time())}")
    run_p.add_argument("--n", type=int, default=2)

    cleanup_p = sub.add_parser("cleanup")
    cleanup_p.add_argument("--comm-name", required=True)

    args = parser.parse_args()

    if args.cmd == "run":
        run(args.comm_name, args.n)
    elif args.cmd == "cleanup":
        cleanup(args.comm_name)


if __name__ == "__main__":
    main()
