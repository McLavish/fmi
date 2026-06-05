#!/usr/bin/env python3
import argparse
import concurrent.futures
import json
import os
import ssl
import sys
import time
import uuid
import urllib.error
import urllib.request
from pathlib import Path

import fmi


TASK_DIR = Path(__file__).resolve().parent
ORCHESTRATOR_CONFIG = Path(os.environ.get("FMI_ORCHESTRATOR_CONFIG", TASK_DIR / "fmi-orchestrator.json"))
NUM_PEERS = 2
EXPECTED_RESULT = 3.0
DEFAULT_IMAGE = "323756936843.dkr.ecr.eu-central-1.amazonaws.com/fmi-knative-migration:v1"
DEFAULT_SERVERLESS_URL = "http://fmi-serverless-rank.fmi.svc.cluster.local/invoke"


class KubernetesJobClient:
    def __init__(self, namespace=None):
        self.namespace = namespace or os.environ.get("FMI_NAMESPACE") or self._service_account_namespace()
        host = os.environ["KUBERNETES_SERVICE_HOST"]
        port = os.environ.get("KUBERNETES_SERVICE_PORT_HTTPS", "443")
        self.base_url = f"https://{host}:{port}"
        self.token = Path("/var/run/secrets/kubernetes.io/serviceaccount/token").read_text().strip()
        ca_path = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
        self.context = ssl.create_default_context(cafile=ca_path)

    @staticmethod
    def _service_account_namespace():
        return Path("/var/run/secrets/kubernetes.io/serviceaccount/namespace").read_text().strip()

    def create_job(self, manifest):
        name = manifest["metadata"]["name"]
        path = f"/apis/batch/v1/namespaces/{self.namespace}/jobs"
        print(f"[orchestrator] creating Job {name}", flush=True)
        self._request("POST", path, manifest, expected=(201,))

    def delete_job(self, name, wait=True):
        path = f"/apis/batch/v1/namespaces/{self.namespace}/jobs/{name}"
        body = {
            "apiVersion": "v1",
            "kind": "DeleteOptions",
            "gracePeriodSeconds": 0,
            "propagationPolicy": "Foreground",
        }
        print(f"[orchestrator] deleting Job {name}", flush=True)
        status, _ = self._request("DELETE", path, body, expected=(200, 202, 404))
        if wait and status != 404:
            self.wait_job_deleted(name, timeout_s=60)

    def wait_job_deleted(self, name, timeout_s):
        path = f"/apis/batch/v1/namespaces/{self.namespace}/jobs/{name}"
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            status, _ = self._request("GET", path, expected=(200, 404))
            if status == 404:
                return
            time.sleep(1)
        raise TimeoutError(f"timed out waiting for Job {name} to be deleted")

    def _request(self, method, path, body=None, expected=(200,)):
        data = None if body is None else json.dumps(body).encode("utf-8")
        request = urllib.request.Request(
            self.base_url + path,
            data=data,
            method=method,
            headers={
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
        )
        try:
            with urllib.request.urlopen(request, context=self.context, timeout=30) as response:
                raw = response.read().decode("utf-8", errors="replace")
                parsed = json.loads(raw) if raw else {}
                if response.status not in expected:
                    raise RuntimeError(f"{method} {path} returned HTTP {response.status}: {raw}")
                return response.status, parsed
        except urllib.error.HTTPError as exc:
            raw = exc.read().decode("utf-8", errors="replace")
            if exc.code in expected:
                return exc.code, json.loads(raw) if raw else {}
            raise RuntimeError(f"{method} {path} returned HTTP {exc.code}: {raw}") from exc


def build_vm_rank_job_manifest(name, image, peer_id, worker_id, comm_name, num_peers, n, gap_s):
    env = {
        "PEER_ID": peer_id,
        "NUM_PEERS": num_peers,
        "COMM_NAME": comm_name,
        "WORKER_ID": worker_id,
        "PLACEMENT": "vm",
        "N": n,
        "GAP_S": gap_s,
        "PYTHONPATH": "/var/task",
        "LD_LIBRARY_PATH": "/var/lang/lib:/usr/lib64:/lib64:/var/task:/var/task/lib",
    }
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": name, "labels": {"app": "fmi-vm-rank", "fmi-role": "vm-rank"}},
        "spec": {
            "backoffLimit": 0,
            "template": {
                "metadata": {"labels": {"app": "fmi-vm-rank", "fmi-role": "vm-rank", "rank": str(peer_id)}},
                "spec": {
                    "restartPolicy": "Never",
                    "containers": [
                        {
                            "name": "rank",
                            "image": image,
                            "imagePullPolicy": "Always",
                            "command": ["python3.11", "-u", "/var/task/vm_worker.py"],
                            "env": [{"name": key, "value": str(value)} for key, value in env.items()],
                            "resources": {
                                "requests": {"cpu": "50m", "memory": "128Mi"},
                                "limits": {"cpu": "500m", "memory": "512Mi"},
                            },
                        }
                    ],
                },
            },
        },
    }


def invoke_serverless(url, payload, timeout=240):
    request = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = response.read().decode("utf-8", errors="replace")
            status = response.status
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"serverless invoke failed: HTTP {exc.code}: {body}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"serverless invoke failed: {exc.reason}") from exc

    if status != 200:
        raise RuntimeError(f"serverless invoke returned HTTP {status}: {body}")
    try:
        return json.loads(body)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"serverless invoke returned non-JSON body: {body!r}") from exc


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
    rows = []
    for entry in sorted(snapshot.values(), key=lambda item: item.rank):
        rows.append(
            f"rank {entry.rank}: worker_id={entry.worker_id} "
            f"placement={entry.placement} state={entry.state}"
        )
    return "\n".join(rows) if rows else "(empty)"


def print_snapshot(title, snapshot):
    print(f"\n--- {title} ---", flush=True)
    print(format_snapshot(snapshot), flush=True)


def both_ranks_active(snapshot):
    return all(rank in snapshot and snapshot[rank].state == "ACTIVE" for rank in (0, 1))


def verify_migration(epoch0, epoch1, serverless_response, rank0_vm_wid, rank1_vm_wid, rank0_serverless_wid):
    failures = []

    if 0 not in epoch0:
        failures.append("epoch0 rank0 missing")
    else:
        if epoch0[0].placement != "vm":
            failures.append(f"epoch0 rank0 placement expected vm, got {epoch0[0].placement}")
        if epoch0[0].state != "QUIESCED":
            failures.append(f"epoch0 rank0 state expected QUIESCED, got {epoch0[0].state}")
        if epoch0[0].worker_id != rank0_vm_wid:
            failures.append(f"epoch0 rank0 worker_id expected {rank0_vm_wid}, got {epoch0[0].worker_id}")

    if 1 not in epoch0:
        failures.append("epoch0 rank1 missing")
    else:
        if epoch0[1].placement != "vm":
            failures.append(f"epoch0 rank1 placement expected vm, got {epoch0[1].placement}")
        if epoch0[1].state != "ACTIVE":
            failures.append(f"epoch0 rank1 state expected ACTIVE, got {epoch0[1].state}")
        if epoch0[1].worker_id != rank1_vm_wid:
            failures.append(f"epoch0 rank1 worker_id expected {rank1_vm_wid}, got {epoch0[1].worker_id}")

    if 0 not in epoch1:
        failures.append("epoch1 rank0 missing")
    else:
        if epoch1[0].placement != "serverless":
            failures.append(f"epoch1 rank0 placement expected serverless, got {epoch1[0].placement}")
        if epoch1[0].state != "ACTIVE":
            failures.append(f"epoch1 rank0 state expected ACTIVE, got {epoch1[0].state}")
        if epoch1[0].worker_id != rank0_serverless_wid:
            failures.append(f"epoch1 rank0 worker_id expected {rank0_serverless_wid}, got {epoch1[0].worker_id}")
        if epoch1[0].worker_id == rank0_vm_wid:
            failures.append("epoch1 rank0 still has original worker_id")

    if 1 not in epoch1:
        failures.append("epoch1 rank1 missing")
    else:
        if epoch1[1].placement != "vm":
            failures.append(f"epoch1 rank1 placement expected vm, got {epoch1[1].placement}")
        if epoch1[1].state != "ACTIVE":
            failures.append(f"epoch1 rank1 state expected ACTIVE, got {epoch1[1].state}")
        if epoch1[1].worker_id != rank1_vm_wid:
            failures.append(f"epoch1 rank1 worker_id changed: {epoch1[1].worker_id}")

    if serverless_response.get("status") != "ok":
        failures.append(f"serverless status expected ok, got {serverless_response.get('status')}")
    else:
        result = serverless_response.get("result")
        if result is None:
            failures.append("serverless did not return a result")
        elif abs(float(result) - EXPECTED_RESULT) > 1e-9:
            failures.append(f"serverless allreduce result expected {EXPECTED_RESULT}, got {result}")

    return failures


def run(comm_name, n, gap_s, image, serverless_url):
    coordinator = fmi.FTCoordinator(str(ORCHESTRATOR_CONFIG), comm_name, NUM_PEERS)
    coordinator.clear_job_state()
    kube = KubernetesJobClient()

    suffix = uuid.uuid4().hex[:8]
    rank0_job = f"fmi-vm-rank0-{suffix}"
    rank1_job = f"fmi-vm-rank1-{suffix}"
    rank0_vm_wid = f"rank0-vm-{suffix}"
    rank1_vm_wid = f"rank1-vm-{suffix}"
    rank0_serverless_wid = f"rank0-serverless-{suffix}"

    cleanup_jobs = [rank0_job, rank1_job]
    try:
        print(f"[orchestrator] comm_name={comm_name} suffix={suffix}", flush=True)
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            f0 = pool.submit(
                kube.create_job,
                build_vm_rank_job_manifest(rank0_job, image, 0, rank0_vm_wid, comm_name, NUM_PEERS, n, gap_s),
            )
            f1 = pool.submit(
                kube.create_job,
                build_vm_rank_job_manifest(rank1_job, image, 1, rank1_vm_wid, comm_name, NUM_PEERS, n, gap_s),
            )
            f0.result()
            f1.result()

        epoch0_active = wait_for_snapshot(
            coordinator,
            0,
            both_ranks_active,
            "epoch 0 ranks 0 and 1 ACTIVE",
            timeout_s=90,
        )
        print_snapshot("Epoch 0 active", epoch0_active)

        print("[orchestrator] requesting migration of rank 0", flush=True)
        coordinator.request_migration(0)

        epoch0 = wait_for_snapshot(
            coordinator,
            0,
            lambda snap: 0 in snap and snap[0].state == "QUIESCED",
            "epoch 0 rank 0 QUIESCED",
            timeout_s=30,
        )

        kube.delete_job(rank0_job, wait=True)

        payload = {
            "peer_id": 0,
            "num_peers": NUM_PEERS,
            "comm_name": comm_name,
            "worker_id": rank0_serverless_wid,
            "placement": "serverless",
            "n": n,
            "resume": True,
        }
        print(f"[orchestrator] invoking serverless replacement at {serverless_url}", flush=True)
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            replacement = pool.submit(invoke_serverless, serverless_url, payload)
            print("[orchestrator] promoting migration epoch", flush=True)
            coordinator.promote_epoch()
            serverless_response = replacement.result(timeout=260)
        print(f"[orchestrator] HTTP /invoke response {serverless_response}", flush=True)

        epoch1 = wait_for_snapshot(
            coordinator,
            1,
            both_ranks_active,
            "epoch 1 ranks 0 and 1 ACTIVE",
            timeout_s=90,
        )

        print_snapshot("Epoch 0 directory", epoch0)
        print_snapshot("Epoch 1 directory", epoch1)

        failures = verify_migration(
            epoch0,
            epoch1,
            serverless_response,
            rank0_vm_wid,
            rank1_vm_wid,
            rank0_serverless_wid,
        )
        if failures:
            print("\nFAILED:", flush=True)
            for failure in failures:
                print(f"  - {failure}", flush=True)
            sys.exit(1)

        print(
            f"[orchestrator] post-migration allreduce result = {serverless_response.get('result')} "
            f"(expected {EXPECTED_RESULT})",
            flush=True,
        )
        print("\nPASSED: rank directory flip verified AND post-migration allreduce result == 3.0", flush=True)
        coordinator.clear_job_state()
    finally:
        keep = os.environ.get("KEEP_JOBS_ON_FAILURE", "").lower() in ("1", "true", "yes")
        if keep and sys.exc_info()[0] is not None:
            print("[orchestrator] keeping VM rank Jobs for failure inspection", flush=True)
        else:
            for name in cleanup_jobs:
                try:
                    kube.delete_job(name, wait=False)
                except Exception as exc:
                    print(f"[orchestrator] cleanup warning for {name}: {exc}", flush=True)


def cleanup(comm_name):
    coordinator = fmi.FTCoordinator(str(ORCHESTRATOR_CONFIG), comm_name, NUM_PEERS)
    coordinator.clear_job_state()
    print(f"[orchestrator] cleared FMI state for {comm_name}", flush=True)


def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd", required=True)

    run_p = sub.add_parser("run")
    run_p.add_argument("--comm-name", default=f"demo-{int(time.time())}")
    run_p.add_argument("--n", type=int, default=int(os.environ.get("N", "2")))
    run_p.add_argument("--gap-s", type=float, default=float(os.environ.get("GAP_S", "5.0")))
    run_p.add_argument("--image", default=os.environ.get("FMI_IMAGE", DEFAULT_IMAGE))
    run_p.add_argument("--serverless-url", default=os.environ.get("SERVERLESS_URL", DEFAULT_SERVERLESS_URL))

    cleanup_p = sub.add_parser("cleanup")
    cleanup_p.add_argument("--comm-name", required=True)

    args = parser.parse_args()
    if args.cmd == "run":
        run(args.comm_name, args.n, args.gap_s, args.image, args.serverless_url)
    elif args.cmd == "cleanup":
        cleanup(args.comm_name)


if __name__ == "__main__":
    main()
