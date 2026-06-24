#!/usr/bin/env bash
# End-to-end demo of CRIU "migrate all local" — parallel checkpointing of every rank running on
# this host in a single epoch cut (no survivors).
#
# Flow:
#   1. launch NUM_PEERS transparent_state_transfer_demo ranks (plain FMI::Communicator)
#   2. wait until all are ACTIVE at epoch 0
#   3. run fmi-rank-agent migrate-local: it discovers the host-local ranks from the CRIU registry,
#      requests their migration, waits for ALL to quiesce, criu dump/restores them in parallel,
#      then promotes epoch 1 once
#   4. assert every rank finishes with the post-migration allreduce == expected (state survived)
#
# This is the no-survivor case: unlike run-demo.sh (one target, one survivor), here every rank is
# checkpointed and restored, so all of them are detached criu children by the end.
#
# Requires: a running Redis (control plane), a running tcpunchd on :10000 (Direct data plane),
# and a working criu. See README.md. The default runs criu privileged (root); for rootless criu
# set FMI_CRIU_EXTRA_ARGS="--unprivileged".
set -uo pipefail

RUNBOOK_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${RUNBOOK_DIR}/../.." && pwd)"
BUILD_DIR="${FMI_BUILD_DIR:-${REPO_ROOT}/build-unified-criu}"
CONFIG="${RUNBOOK_DIR}/fmi.json"

DEMO="${BUILD_DIR}/runbooks/local-criu-state-transfer/transparent_state_transfer_demo"
AGENT="${BUILD_DIR}/tools/fmi-rank-agent"
NUM_PEERS="${NUM_PEERS:-3}"
COMM_NAME="${COMM_NAME:-criu-stx-all-$(date +%s)}"
WINDOW_MS="${WINDOW_MS:-8000}"

# Redis access: prefer a host redis-cli, else the fmi-redis docker container.
if command -v redis-cli >/dev/null 2>&1; then
    redis() { redis-cli "$@"; }
else
    redis() { docker exec -i fmi-redis redis-cli "$@"; }
fi

PREFIX="fmi:ft:${COMM_NAME}:"
log() { echo "[driver] $*"; }
fail() { echo "[driver] FAIL: $*" >&2; exit 1; }

[ -x "${DEMO}" ] || fail "demo binary not found: ${DEMO} (build with FMI_ENABLE_CRIU=ON)"
[ -x "${AGENT}" ] || fail "rank agent not found: ${AGENT}"
command -v criu >/dev/null 2>&1 || fail "criu not on PATH"
redis ping >/dev/null 2>&1 || fail "Redis not reachable"

log "comm_name=${COMM_NAME} num_peers=${NUM_PEERS} window_ms=${WINDOW_MS} criu_extra='${FMI_CRIU_EXTRA_ARGS:-}'"

# Clean any stale control-plane keys + images for this comm.
mapfile -t STALE < <(redis --scan --pattern "${PREFIX}*" 2>/dev/null)
[ "${#STALE[@]}" -gt 0 ] && redis del "${STALE[@]}" >/dev/null 2>&1
rm -rf "/tmp/fmi-criu-images/${COMM_NAME}"

# Launch all ranks.
declare -a PIDS=()
declare -a LOGS=()
for r in $(seq 0 $((NUM_PEERS - 1))); do
    out="${RUNBOOK_DIR}/.last-run-all-rank${r}.log"
    LOGS[r]="${out}"
    : >"${out}"
    setsid "${DEMO}" "${r}" "${NUM_PEERS}" "${CONFIG}" "${COMM_NAME}" "${WINDOW_MS}" >"${out}" 2>&1 &
    PIDS[r]=$!
done
log "launched ${NUM_PEERS} ranks (logs: ${RUNBOOK_DIR}/.last-run-all-rank*.log)"

cleanup() { kill "${PIDS[@]}" 2>/dev/null; }
trap cleanup EXIT

# Wait for all ranks ACTIVE at epoch 0.
log "waiting for all ${NUM_PEERS} ranks ACTIVE at epoch 0"
for _ in $(seq 1 150); do
    all_active=1
    for r in $(seq 0 $((NUM_PEERS - 1))); do
        s="$(redis hget "${PREFIX}epoch:0:states" "${r}" 2>/dev/null)"
        [ "${s}" = "ACTIVE" ] || { all_active=0; break; }
    done
    [ "${all_active}" -eq 1 ] && break
    sleep 0.2
done
[ "${all_active}" -eq 1 ] || fail "not all ranks reached ACTIVE"
log "all ranks ACTIVE"

# Migrate every local rank in one cut. The agent discovers the host-local ranks, requests their
# migration, waits for all to quiesce, dump/restores them in parallel, and promotes epoch 1.
log "running rank agent: migrate-local (real criu dump/restore of all local ranks)"
"${AGENT}" migrate-local "${COMM_NAME}" "${NUM_PEERS}" "${CONFIG}"
AGENT_RC=$?
[ "${AGENT_RC}" -eq 0 ] || fail "rank agent exited ${AGENT_RC} (see /tmp/fmi-criu-images/${COMM_NAME}/epoch-1/rank-*/{dump,restore}.log)"

# All ranks were restored as detached criu children, so poll their logs for the result line.
log "waiting for all ranks to finish"
declare -a RC=()
for r in $(seq 0 $((NUM_PEERS - 1))); do RC[r]=1; done
for _ in $(seq 1 150); do
    done_count=0
    for r in $(seq 0 $((NUM_PEERS - 1))); do
        if grep -q "rank=${r} OK:" "${LOGS[r]}" 2>/dev/null; then RC[r]=0; fi
        if grep -q "rank=${r} FAIL" "${LOGS[r]}" 2>/dev/null; then RC[r]=1; fi
        [ "${RC[r]}" -eq 0 ] && done_count=$((done_count + 1))
    done
    [ "${done_count}" -eq "${NUM_PEERS}" ] && break
    sleep 0.2
done

for r in $(seq 0 $((NUM_PEERS - 1))); do
    echo "----- rank ${r} output -----"; cat "${LOGS[r]}"
done
EPOCH="$(redis hget "${PREFIX}meta" current_epoch 2>/dev/null)"
log "final epoch=${EPOCH}"

all_ok=1
for r in $(seq 0 $((NUM_PEERS - 1))); do
    [ "${RC[r]}" -eq 0 ] || { all_ok=0; log "rank ${r} did not report OK"; }
done
if [ "${all_ok}" -eq 1 ] && [ "${EPOCH}" = "1" ]; then
    log "PASS: all ${NUM_PEERS} ranks were criu-migrated in one cut; preserved state at epoch 1"
    exit 0
fi
fail "all_ok=${all_ok} epoch=${EPOCH}"
