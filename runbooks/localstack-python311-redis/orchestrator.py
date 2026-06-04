#!/usr/bin/env python3
import argparse
import concurrent.futures
import json
import os
import subprocess
import sys
import tempfile
import time
import uuid
import urllib.error
import urllib.request
from pathlib import Path


RUNBOOK_DIR = Path(__file__).resolve().parent
REPO_ROOT = RUNBOOK_DIR.parents[1]
sys.path.insert(0, str(REPO_ROOT / "python" / "build-native-debug"))

import fmi


FUNCTION_NAME = "fmi-migration-worker"
HOST_CONFIG = RUNBOOK_DIR / "fmi-host.json"
NUM_PEERS = 2
ENDPOINT_URL = "http://localhost:4566"
BUILD_IMAGE_TAG = "fmi-localstack-build:redis-gcc10"
AMI_ID = "ami-00000001"
BUNDLE_DIR = RUNBOOK_DIR / "build" / "bundle"

# Dummy credentials matching what setup.sh uses for LocalStack
_AWS_ENV = {
    **os.environ,
    "AWS_ACCESS_KEY_ID": os.environ.get("AWS_ACCESS_KEY_ID", "test"),
    "AWS_SECRET_ACCESS_KEY": os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
    "AWS_DEFAULT_REGION": os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
    "AWS_EC2_METADATA_DISABLED": "true",
}


def lambda_client():
    return ENDPOINT_URL


def invoke_worker_async(endpoint_url, payload):
    """Fire-and-forget Lambda invocation (Event mode) — used for epoch-0 VM ranks via Lambda."""
    url = f"{endpoint_url}/2015-03-31/functions/{FUNCTION_NAME}/invocations"
    request = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        method="POST",
        headers={
            "Content-Type": "application/json",
            "X-Amz-Invocation-Type": "Event",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            status_code = response.status
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"invoke failed for {payload}: HTTP {e.code}: {body}") from e

    if status_code not in (202, 204):
        raise RuntimeError(f"unexpected invoke status for {payload}: {status_code}")


def invoke_worker_sync(endpoint_url, payload, timeout=120):
    """Synchronous Lambda invocation (RequestResponse) — blocks until the function returns.

    Returns the parsed JSON response body dict.
    """
    url = f"{endpoint_url}/2015-03-31/functions/{FUNCTION_NAME}/invocations"
    request = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        method="POST",
        headers={
            "Content-Type": "application/json",
            "X-Amz-Invocation-Type": "RequestResponse",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            status_code = response.status
            body = response.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"sync invoke failed for {payload}: HTTP {e.code}: {body}") from e

    if status_code not in (200, 202):
        raise RuntimeError(f"unexpected sync invoke status for {payload}: {status_code}, body={body}")

    try:
        return json.loads(body)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"sync invoke returned non-JSON body: {body!r}") from exc


def launch_vm_rank(peer_id, worker_id, comm_name, num_peers, n, gap_s):
    """Launch a LocalStack EC2 instance (Docker VM Manager) as a long-lived VM rank.

    Uses aws ec2 run-instances against the LocalStack endpoint.  The instance
    boots from the AMI image localstack-ec2/fmi-vm:ami-00000001 which already
    contains fmi.so, vm_worker.py, and fmi-worker.json.  Per-rank parameters
    are injected via user-data which is executed on boot by LocalStack.
    """
    # Build a shell user-data script that sets env vars and starts the worker.
    # LD_LIBRARY_PATH: /usr/lib64 first so the AL2023 libpython3.11 (with the
    # correct PYTHONHOME prefix) is picked up before the Lambda-bundled one in
    # /var/task/lib.  The Python binary is the AL2023-native /usr/bin/python3.11.
    user_data_script = f"""\
#!/bin/sh
export PEER_ID={peer_id}
export NUM_PEERS={num_peers}
export COMM_NAME={comm_name}
export WORKER_ID={worker_id}
export PLACEMENT=vm
export N={n}
export GAP_S={gap_s}
export PYTHONPATH=/var/task
export LD_LIBRARY_PATH=/usr/lib64:/var/task/lib:/lib64
exec /usr/bin/python3.11 /var/task/vm_worker.py
"""

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".sh", delete=False, prefix=f"fmi-ud-rank{peer_id}-"
    ) as f:
        f.write(user_data_script)
        ud_path = f.name

    try:
        cmd = [
            "aws", "--endpoint-url", ENDPOINT_URL,
            "ec2", "run-instances",
            "--image-id", AMI_ID,
            "--count", "1",
            "--instance-type", "t2.micro",
            "--user-data", f"file://{ud_path}",
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, env=_AWS_ENV)
        if result.returncode != 0:
            raise RuntimeError(
                f"ec2 run-instances failed for rank {peer_id}: {result.stderr.strip()}"
            )
        reservation = json.loads(result.stdout)
        instance_id = reservation["Instances"][0]["InstanceId"]
        print(
            f"[orchestrator] launched EC2 instance {instance_id} for rank {peer_id}",
            flush=True,
        )
        return instance_id
    finally:
        os.unlink(ud_path)


def terminate_ec2_instances(instance_ids):
    """Terminate a list of EC2 instance IDs via the LocalStack endpoint."""
    if not instance_ids:
        return
    cmd = [
        "aws", "--endpoint-url", ENDPOINT_URL,
        "ec2", "terminate-instances",
        "--instance-ids", *instance_ids,
    ]
    subprocess.run(cmd, capture_output=True, env=_AWS_ENV)


def cleanup_leftover_ec2_containers():
    """Remove any leftover localstack-ec2.i-* containers on the host."""
    result = subprocess.run(
        ["docker", "ps", "-a", "--format", "{{.Names}}"],
        capture_output=True, text=True,
    )
    for name in result.stdout.splitlines():
        if name.startswith("localstack-ec2.i-"):
            subprocess.run(["docker", "rm", "-f", name], capture_output=True)


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

    instance_ids = []

    # Epoch 0: launch both ranks as genuine LocalStack EC2 instances.
    # Launch concurrently so both workers start within the Redis collective
    # timeout window (user-data runs immediately inside the container).
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        f0 = pool.submit(launch_vm_rank, 0, rank0_vm_wid, comm_name, NUM_PEERS, n, 5.0)
        f1 = pool.submit(launch_vm_rank, 1, rank1_vm_wid, comm_name, NUM_PEERS, n, 5.0)
        iid0 = f0.result()
        iid1 = f1.result()
    instance_ids.append(iid0)
    instance_ids.append(iid1)

    epoch0 = wait_for_snapshot(
        coordinator,
        0,
        both_ranks_active,
        "epoch 0 ranks 0 and 1 ACTIVE",
        timeout_s=90,
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

    # Epoch 1: replacement rank comes in as a Lambda (serverless substrate).
    # Invoke synchronously so we can inspect the return value and assert the
    # post-migration allreduce result.  resume=True tells worker_core to skip
    # phase-1 (already done by the original rank0) and start at the barrier,
    # aligning its epoch-1 op sequence with the survivor (rank1).
    print("[orchestrator] invoking replacement Lambda (synchronous, resume=True)", flush=True)
    replacement_payload = {
        "peer_id": 0,
        "num_peers": NUM_PEERS,
        "comm_name": comm_name,
        "worker_id": rank0_sl_wid,
        "placement": "serverless",
        "n": n,
        "resume": True,
    }
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        replacement = pool.submit(invoke_worker_sync, client, replacement_payload, 120)
        print("[orchestrator] promoting migration epoch", flush=True)
        coordinator.promote_epoch()
        lambda_response = replacement.result(timeout=180)
    print(f"[orchestrator] Lambda response: {lambda_response}", flush=True)

    epoch1 = wait_for_snapshot(
        coordinator,
        1,
        both_ranks_active,
        "epoch 1 ranks 0 and 1 ACTIVE",
        timeout_s=90,
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
        if epoch1[0].state != "ACTIVE":
            failures.append(f"epoch1 rank0 state expected 'ACTIVE', got '{epoch1[0].state}'")
        if epoch1[0].placement != "serverless":
            failures.append(f"epoch1 rank0 placement expected 'serverless', got '{epoch1[0].placement}'")
        if epoch1[0].worker_id != rank0_sl_wid:
            failures.append(f"epoch1 rank0 worker_id expected '{rank0_sl_wid}', got '{epoch1[0].worker_id}'")
        if epoch1[0].worker_id == rank0_vm_wid:
            failures.append("epoch1 rank0 still has original worker_id")

    if 1 not in epoch1:
        failures.append("epoch1 rank1 missing")
    else:
        if epoch1[1].state != "ACTIVE":
            failures.append(f"epoch1 rank1 state expected 'ACTIVE', got '{epoch1[1].state}'")
        if epoch1[1].placement != "vm":
            failures.append(f"epoch1 rank1 placement expected 'vm', got '{epoch1[1].placement}'")
        if epoch1[1].worker_id != rank1_vm_wid:
            failures.append(f"epoch1 rank1 worker_id changed: {epoch1[1].worker_id}")

    # Validate the post-migration communication result.
    # The replacement Lambda must have completed the epoch-1 allreduce with the
    # survivor (rank1).  rank0 contributes 1.0, rank1 contributes 2.0 → sum = 3.0.
    # This can only be 3.0 if the two ranks genuinely communicated after migration.
    lambda_status = lambda_response.get("status", "")
    lambda_result = lambda_response.get("result")
    EXPECTED_RESULT = 3.0

    if lambda_status != "ok":
        failures.append(f"Lambda replacement status expected 'ok', got '{lambda_status}'")
    else:
        if lambda_result is None:
            failures.append("Lambda replacement did not return a numeric result")
        elif abs(float(lambda_result) - EXPECTED_RESULT) > 1e-9:
            failures.append(
                f"Lambda replacement allreduce result expected {EXPECTED_RESULT}, "
                f"got {lambda_result!r} — post-migration communication did NOT succeed"
            )
        else:
            print(
                f"[orchestrator] post-migration allreduce result = {lambda_result} "
                f"(expected {EXPECTED_RESULT}) ✓",
                flush=True,
            )

    if failures:
        print("\nFAILED:")
        for failure in failures:
            print(f"  - {failure}")
        # Terminate EC2 instances before exit
        terminate_ec2_instances(instance_ids)
        cleanup_leftover_ec2_containers()
        sys.exit(1)

    print("\nPASSED: rank directory flip verified AND post-migration allreduce result == 3.0")
    coordinator.clear_job_state()

    # Clean up EC2 instances
    terminate_ec2_instances(instance_ids)
    cleanup_leftover_ec2_containers()


def cleanup(comm_name):
    fmi.FTCoordinator(str(HOST_CONFIG), comm_name, NUM_PEERS).clear_job_state()
    # Terminate any running EC2 instances via LocalStack
    result = subprocess.run(
        [
            "aws", "--endpoint-url", ENDPOINT_URL,
            "ec2", "describe-instances",
            "--query", "Reservations[].Instances[].InstanceId",
            "--output", "text",
        ],
        capture_output=True, text=True, env=_AWS_ENV,
    )
    instance_ids = result.stdout.split()
    if instance_ids:
        terminate_ec2_instances(instance_ids)
    cleanup_leftover_ec2_containers()
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
