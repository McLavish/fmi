import importlib.util
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace


RUNBOOK_DIR = Path(__file__).resolve().parents[2]
ORCHESTRATOR_PATH = RUNBOOK_DIR / "knative-migration" / "orchestrator.py"


def load_orchestrator():
    spec = importlib.util.spec_from_file_location("knative_migration_orchestrator", ORCHESTRATOR_PATH)
    module = importlib.util.module_from_spec(spec)
    sys.modules.setdefault("fmi", SimpleNamespace())
    spec.loader.exec_module(module)
    return module


def entry(rank, worker_id, placement, state):
    return SimpleNamespace(rank=rank, worker_id=worker_id, placement=placement, state=state)


class OrchestratorTests(unittest.TestCase):
    def test_vm_rank_job_manifest_contains_rank_env_and_image(self):
        orchestrator = load_orchestrator()

        manifest = orchestrator.build_vm_rank_job_manifest(
            name="fmi-vm-rank0-deadbeef",
            image="123456789012.dkr.ecr.eu-central-1.amazonaws.com/fmi-knative-migration:v1",
            peer_id=0,
            worker_id="rank0-vm-deadbeef",
            comm_name="demo",
            num_peers=2,
            n=2,
            gap_s=5.0,
        )

        self.assertEqual(manifest["kind"], "Job")
        self.assertEqual(manifest["metadata"]["name"], "fmi-vm-rank0-deadbeef")
        container = manifest["spec"]["template"]["spec"]["containers"][0]
        self.assertEqual(
            container["image"],
            "123456789012.dkr.ecr.eu-central-1.amazonaws.com/fmi-knative-migration:v1",
        )
        self.assertEqual(container["command"], ["python3.11", "-u", "/var/task/vm_worker.py"])
        self.assertEqual(container["imagePullPolicy"], "Always")
        env = {item["name"]: item["value"] for item in container["env"]}
        self.assertEqual(env["PEER_ID"], "0")
        self.assertEqual(env["WORKER_ID"], "rank0-vm-deadbeef")
        self.assertEqual(env["PLACEMENT"], "vm")
        self.assertEqual(env["GAP_S"], "5.0")

    def test_vm_rank_job_manifest_honors_image_pull_policy(self):
        orchestrator = load_orchestrator()

        manifest = orchestrator.build_vm_rank_job_manifest(
            name="fmi-vm-rank0-deadbeef",
            image="fmi-knative-migration:local",
            peer_id=0,
            worker_id="rank0-vm-deadbeef",
            comm_name="demo",
            num_peers=2,
            n=2,
            gap_s=5.0,
            image_pull_policy="IfNotPresent",
        )

        container = manifest["spec"]["template"]["spec"]["containers"][0]
        self.assertEqual(container["imagePullPolicy"], "IfNotPresent")


    def test_verify_migration_accepts_directory_flip_and_result(self):
        orchestrator = load_orchestrator()

        failures = orchestrator.verify_migration(
            epoch0={
                0: entry(0, "rank0-vm-abc123", "vm", "QUIESCED"),
                1: entry(1, "rank1-vm-abc123", "vm", "ACTIVE"),
            },
            epoch1={
                0: entry(0, "rank0-serverless-abc123", "serverless", "ACTIVE"),
                1: entry(1, "rank1-vm-abc123", "vm", "ACTIVE"),
            },
            serverless_response={
                "peer_id": 0,
                "placement": "serverless",
                "status": "ok",
                "result": 3.0,
            },
            rank0_vm_wid="rank0-vm-abc123",
            rank1_vm_wid="rank1-vm-abc123",
            rank0_serverless_wid="rank0-serverless-abc123",
        )

        self.assertEqual(failures, [])


    def test_verify_migration_rejects_wrong_serverless_result(self):
        orchestrator = load_orchestrator()

        failures = orchestrator.verify_migration(
            epoch0={
                0: entry(0, "rank0-vm-abc123", "vm", "QUIESCED"),
                1: entry(1, "rank1-vm-abc123", "vm", "ACTIVE"),
            },
            epoch1={
                0: entry(0, "rank0-serverless-abc123", "serverless", "ACTIVE"),
                1: entry(1, "rank1-vm-abc123", "vm", "ACTIVE"),
            },
            serverless_response={
                "peer_id": 0,
                "placement": "serverless",
                "status": "ok",
                "result": 2.0,
            },
            rank0_vm_wid="rank0-vm-abc123",
            rank1_vm_wid="rank1-vm-abc123",
            rank0_serverless_wid="rank0-serverless-abc123",
        )

        self.assertEqual(failures, ["serverless allreduce result expected 3.0, got 2.0"])


if __name__ == "__main__":
    unittest.main()
