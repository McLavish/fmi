#!/usr/bin/env bash
# Smoke test for the FMI example programs: runs the fast examples with small parameters.
# Usage: example_programs/run_examples.sh [config] [binary_dir]
#
# One of the built examples is deliberately NOT in the suite below:
#   - crashing:    by design a rank aborts at random (1% chance per iteration), so the run's
#                  outcome is non-deterministic and cannot be asserted on.
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONFIG="${1:-example_programs/config/fmi_examples_redis.json}"
BIN_DIR="${2:-build-examples/example_programs}"
RANKS="${RANKS:-4}"
TIMEOUT="${TIMEOUT:-120}"

cd "$REPO_ROOT" || exit 1

if [ ! -f "$CONFIG" ]; then
    echo "config not found: $CONFIG" >&2
    exit 1
fi
if [ ! -d "$BIN_DIR" ]; then
    echo "binary directory not found: $BIN_DIR (build with -DFMI_BUILD_EXAMPLES=ON)" >&2
    exit 1
fi

# name:extra flags (empty means none). Long-running examples are deliberately shortened.
# A --timeout in the extra flags wins over the suite-wide $TIMEOUT (last flag wins).
EXAMPLES=(
    "avg:"
    "communicating:"
    "ring:--num-iterations 2"
    "jacobi:"
    # --m 24 is the cheapest value that still has reference sums to verify against. The kernel
    # itself is fast (~0.3 s at 4 ranks), but this is the one example the live-test lane saw
    # exceed the suite-wide cap when the Direct backend was in play, so give it a generous
    # wall-clock cap: a slow TCPunch rendezvous should not be reported as a failed example.
    "npb_ep:--m 24 --timeout 360"
    "mantevo_hpccg:--nx 16 --ny 16 --nz 16 --max-iter 20"
    "mixed_workload:--num-iterations 2 --sleep-min 0 --sleep-max 1"
    "checkpoint_workload:--num-iterations 2 --sleep-seconds 0 --num-collectives 2"
    "long_communicating_checkpoint:--num-iterations 5"
    "fixed_size_ckpt_comm:--size-mb 1 --num-all-reduces 5"
    "fixed_size_ckpt_sleep:--size-mb 1 --sleep-minutes 0"
)

PASSED=()
FAILED=()
SKIPPED=()

for entry in "${EXAMPLES[@]}"; do
    name="${entry%%:*}"
    extra="${entry#*:}"
    binary="$BIN_DIR/$name"

    if [ ! -x "$binary" ]; then
        SKIPPED+=("$name")
        continue
    fi

    echo "=== $name $extra"
    # shellcheck disable=SC2086
    "$binary" --ranks "$RANKS" --config "$CONFIG" --timeout "$TIMEOUT" $extra
    if [ $? -eq 0 ]; then
        PASSED+=("$name")
    else
        FAILED+=("$name")
    fi
done

echo
echo "=== summary (config $CONFIG, ranks $RANKS)"
for name in "${PASSED[@]:-}"; do
    [ -n "$name" ] && echo "PASS $name"
done
for name in "${FAILED[@]:-}"; do
    [ -n "$name" ] && echo "FAIL $name"
done
for name in "${SKIPPED[@]:-}"; do
    [ -n "$name" ] && echo "SKIP $name (not built)"
done

if [ "${#FAILED[@]}" -gt 0 ]; then
    exit 1
fi
exit 0
