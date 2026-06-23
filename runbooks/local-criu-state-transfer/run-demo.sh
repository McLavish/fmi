#!/usr/bin/env bash
# End-to-end demo of CRIU transparent state transfer for single-rank migration (same-host v1).
#
# Flow:
#   1. launch two transparent_state_transfer_demo ranks (plain FMI::Communicator)
#   2. wait until both are ACTIVE at epoch 0
#   3. request migration of rank 0 (control plane: Redis)
#   4. run fmi-rank-agent, which criu-dumps + criu-restores rank 0 and promotes epoch 1
#   5. assert both ranks finish with the post-migration allreduce == expected (state survived)
#
# Requires: a running Redis (control plane), a running tcpunchd on :10000 (Direct data plane),
# and a working criu. See README.md. Override criu flags for rootless runs with
#   FMI_CRIU_EXTRA_ARGS="--unprivileged"
set -uo pipefail

RUNBOOK_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${RUNBOOK_DIR}/../.." && pwd)"
BUILD_DIR="${FMI_BUILD_DIR:-${REPO_ROOT}/build-unified-criu}"
CONFIG="${RUNBOOK_DIR}/fmi.json"

DEMO="${BUILD_DIR}/runbooks/local-criu-state-transfer/transparent_state_transfer_demo"
AGENT="${BUILD_DIR}/tools/fmi-rank-agent"
NUM_PEERS=2
COMM_NAME="${COMM_NAME:-criu-stx-$(date +%s)}"
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

log "comm_name=${COMM_NAME} window_ms=${WINDOW_MS} criu_extra='${FMI_CRIU_EXTRA_ARGS:-}'"

# Clean any stale control-plane keys + images for this comm.
mapfile -t STALE < <(redis --scan --pattern "${PREFIX}*" 2>/dev/null)
[ "${#STALE[@]}" -gt 0 ] && redis del "${STALE[@]}" >/dev/null 2>&1
rm -rf "/tmp/fmi-criu-images/${COMM_NAME}"

OUT0="${RUNBOOK_DIR}/.last-run-rank0.log"; OUT1="${RUNBOOK_DIR}/.last-run-rank1.log"
: >"${OUT0}"; : >"${OUT1}"
log "launching rank 0 and rank 1 (logs: ${OUT0}, ${OUT1})"
setsid "${DEMO}" 0 "${NUM_PEERS}" "${CONFIG}" "${COMM_NAME}" "${WINDOW_MS}" >"${OUT0}" 2>&1 &
R0=$!
setsid "${DEMO}" 1 "${NUM_PEERS}" "${CONFIG}" "${COMM_NAME}" "${WINDOW_MS}" >"${OUT1}" 2>&1 &
R1=$!

cleanup() { kill "${R0}" "${R1}" 2>/dev/null; }
trap cleanup EXIT

# Wait for both ranks ACTIVE at epoch 0.
log "waiting for both ranks ACTIVE at epoch 0"
for _ in $(seq 1 100); do
    S0="$(redis hget "${PREFIX}epoch:0:states" 0 2>/dev/null)"
    S1="$(redis hget "${PREFIX}epoch:0:states" 1 2>/dev/null)"
    [ "${S0}" = "ACTIVE" ] && [ "${S1}" = "ACTIVE" ] && break
    sleep 0.2
done
[ "${S0}" = "ACTIVE" ] && [ "${S1}" = "ACTIVE" ] || fail "ranks did not reach ACTIVE (s0=${S0} s1=${S1})"
log "both ranks ACTIVE"

# Request migration of rank 0 (same effect as ControlPlane::request_migration).
log "requesting migration of rank 0"
redis sadd "${PREFIX}pending" 0 >/dev/null
redis hset "${PREFIX}epoch:0:states" 0 MIGRATION_PENDING >/dev/null

# Run the host-local rank agent: it waits for rank 0 to checkpoint-quiesce, then criu
# dump/restore + promote epoch 1.
log "running rank agent (real criu dump/restore)"
"${AGENT}" migrate "${COMM_NAME}" "${NUM_PEERS}" "${CONFIG}" 0
AGENT_RC=$?
[ "${AGENT_RC}" -eq 0 ] || fail "rank agent exited ${AGENT_RC} (see /tmp/fmi-criu-images/${COMM_NAME}/epoch-1/rank-0/{dump,restore}.log)"

# Wait for both ranks to finish.
log "waiting for ranks to finish"
wait "${R1}"; RC1=$?
# The restored rank 0 is a detached criu child, not our child anymore; poll its result line.
RC0=1
for _ in $(seq 1 100); do
    if grep -q "rank=0 OK:" "${OUT0}" 2>/dev/null; then RC0=0; break; fi
    if grep -q "rank=0 FAIL" "${OUT0}" 2>/dev/null; then RC0=1; break; fi
    sleep 0.2
done

echo "----- rank 0 output -----"; cat "${OUT0}"
echo "----- rank 1 output -----"; cat "${OUT1}"
EPOCH="$(redis hget "${PREFIX}meta" current_epoch 2>/dev/null)"
log "final epoch=${EPOCH}"

if [ "${RC0}" -eq 0 ] && [ "${RC1}" -eq 0 ] && [ "${EPOCH}" = "1" ]; then
    log "PASS: rank 0 was criu-migrated; both ranks completed phase 2 with preserved state at epoch 1"
    exit 0
fi
fail "rank0_rc=${RC0} rank1_rc=${RC1} epoch=${EPOCH}"
