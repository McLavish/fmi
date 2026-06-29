#!/usr/bin/env python3
"""Unit tests for the orchestrator's pure verification logic.

These cover the parts that decide PASS/FAIL without needing a cluster or Redis: detecting that
all ranks are ACTIVE in a control-plane states hash, and extracting per-rank OK/FAIL outcomes
from machine-pod logs. Run with `python3 tests/test_orchestrator.py` (no pytest needed).
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from orchestrator import all_active_from_states, parse_rank_results  # noqa: E402


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


def test_parse_results_full_8_rank_pass():
    lines = []
    for r in range(8):
        lines.append(f"rank={r} OK: application state survived transparent migration")
    ok, failed = parse_rank_results("\n".join(lines))
    assert ok == {str(i) for i in range(8)}
    assert failed == set()


def main():
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    for t in tests:
        t()
        print(f"  ok  {t.__name__}")
    print(f"PASSED {len(tests)} orchestrator unit tests")


if __name__ == "__main__":
    main()
