#!/usr/bin/env python3
"""Unit tests for the orchestrator's and restore server's pure verification logic.

These cover the parts that decide PASS/FAIL without needing a cluster, Redis, or criu: rank
state scanning, agent output parsing, relocation verification, and restore-response
verification. Run with `python3 tests/test_orchestrator.py` (no pytest needed).
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from orchestrator import (  # noqa: E402
    all_active_from_states,
    parse_evacuation_output,
    parse_rank_results,
    verify_relocations,
    verify_restore_responses,
)
from restore_server import parse_agent_output, worker_outcome  # noqa: E402


def test_all_active_true_when_every_rank_active():
    states = {str(i): "ACTIVE" for i in range(8)}
    assert all_active_from_states(states, 8)


def test_all_active_false_when_one_quiesced():
    states = {str(i): "ACTIVE" for i in range(8)}
    states["3"] = "QUIESCED"
    assert not all_active_from_states(states, 8)


def test_all_active_false_when_rank_missing():
    states = {str(i): "ACTIVE" for i in range(7)}  # rank 7 not present yet
    assert not all_active_from_states(states, 8)


def test_parse_results_collects_ok_and_fail():
    log = "\n".join([
        "[supervisor] launched rank 0 -> /tmp/rank-0.log",
        "rank=0 phase1_sum=36 state=1",
        "rank=0 OK: application state survived transparent migration",
        "rank=1 OK: application state survived transparent migration",
        "rank=2 FAIL: phase2_sum=2 != expected 836 — application state did NOT survive migration",
        "some unrelated line",
    ])
    ok, failed = parse_rank_results(log)
    assert ok == {"0", "1"}
    assert failed == {"2"}


def test_parse_results_ignores_progress_lines():
    log = "rank=5 pre_migration_state=105 (sleeping 20000ms for the migration window)"
    ok, failed = parse_rank_results(log)
    assert ok == set()
    assert failed == set()


def test_parse_evacuation_output_extracts_epoch_and_ranks():
    out = "\n".join([
        "some agent noise",
        "staged_epoch=1 ranks=0,1,2,3",
        "__EXIT__=0",
    ])
    assert parse_evacuation_output(out) == (1, [0, 1, 2, 3])


def test_parse_evacuation_output_rejects_missing_line():
    try:
        parse_evacuation_output("fatal: nothing staged\n__EXIT__=1")
    except ValueError:
        return
    raise AssertionError("expected ValueError for missing staged_epoch line")


def test_verify_relocations_passes_when_all_moved():
    original = {0: "machine-a", 1: "machine-a"}
    entries = {
        0: {"state": "RUNNING", "host_id": "fmi-restore-00001-deployment-abc"},
        1: {"state": "RUNNING", "host_id": "fmi-restore-00001-deployment-def"},
    }
    assert verify_relocations(original, entries, [0, 1]) == []


def test_verify_relocations_flags_unmoved_and_unrestored_ranks():
    original = {0: "machine-a", 1: "machine-a", 2: "machine-a"}
    entries = {
        0: {"state": "RUNNING", "host_id": "machine-a"},   # restored in place — not a relocation
        1: {"state": "QUIESCED", "host_id": "machine-a"},  # never restored
        # rank 2 missing entirely
    }
    failures = verify_relocations(original, entries, [0, 1, 2])
    assert len(failures) == 3
    assert any("rank 0" in f and "did not relocate" in f for f in failures)
    assert any("rank 1" in f and "QUIESCED" in f for f in failures)
    assert any("rank 2" in f for f in failures)


def test_verify_restore_responses_passes_on_ok_with_ok_log():
    responses = {
        0: {"status": "ok", "host": "pod-a",
            "log_tail": "rank=0 OK: application state survived transparent migration"},
    }
    assert verify_restore_responses(responses, [0]) == []


def test_verify_restore_responses_flags_bad_status_and_bad_log():
    responses = {
        0: {"status": "restore-failed", "log_tail": ""},
        1: {"status": "ok",
            "log_tail": "rank=1 FAIL: phase2_sum=2 != expected 836 — state did NOT survive"},
        2: {"status": "ok", "log_tail": "rank=2 phase1_sum=36 state=3"},  # no terminal line
        # rank 3: no response at all
    }
    failures = verify_restore_responses(responses, [0, 1, 2, 3])
    assert len(failures) == 4
    assert any("rank 0" in f and "restore-failed" in f for f in failures)
    assert any("rank 1" in f and "state loss" in f for f in failures)
    assert any("rank 2" in f and "no terminal OK" in f for f in failures)
    assert any("rank 3" in f and "no /restore response" in f for f in failures)


def test_restore_server_parses_agent_output():
    out = "restored_rank=2 pid=3004 host=fmi-restore-00001-deployment-abc\n"
    assert parse_agent_output(out) == (3004, "fmi-restore-00001-deployment-abc")


def test_restore_server_rejects_agent_output_without_marker():
    try:
        parse_agent_output("fatal: No staged CRIU image for rank 2 at epoch 1")
    except ValueError:
        return
    raise AssertionError("expected ValueError for missing restored_rank line")


def test_restore_server_worker_outcome():
    assert worker_outcome("rank=0 OK: application state survived transparent migration") == "ok"
    assert worker_outcome("rank=0 FAIL: phase2_sum=1 != expected 836") == "failed"
    assert worker_outcome("rank=0 phase1_sum=36 state=1") == "unknown"


def main():
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    for t in tests:
        t()
        print(f"  ok  {t.__name__}")
    print(f"PASSED {len(tests)} orchestrator unit tests")


if __name__ == "__main__":
    main()
