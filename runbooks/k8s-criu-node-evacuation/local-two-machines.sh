#!/usr/bin/env bash
# Stage-0 local baseline for the multi-machine CRIU node-evacuation demo.
#
# Simulates TWO machines on a single host using criu.host_id overrides, then evacuates one
# machine's ranks with a single `fmi-rank-agent migrate-local` cut while the other machine's
# ranks SURVIVE at the barrier. This is the novel path the K8s demo relies on (batch
# migrate-local *with survivors*) — run-demo-all.sh has no survivors, run-demo.sh has only one.
#
# Flow:
#   1. render two configs from fmi-machine.json.tmpl that differ only in criu.host_id
#      (machine-a, machine-b); both share the same Redis control plane and tcpunchd rendezvous
#   2. launch NUM_PEERS transparent_state_transfer_demo ranks: the first RANKS_PER_MACHINE on
#      machine-a, the rest on machine-b
#   3. wait until all ranks are ACTIVE at epoch 0
#   4. run `fmi-rank-agent migrate-local <comm> <num_peers> <machine-a config>`: it discovers the
#      machine-a ranks from the CRIU registry, requests their migration, waits for ALL of them to
#      quiesce, criu dump/restores them in parallel, then promotes epoch 1 once. The machine-b
#      ranks park at the barrier and reconfigure to epoch 1.
#   5. assert every rank finishes with the post-migration allreduce == expected (state survived)
#      and the control plane is at epoch 1.
#
# Requires: a running Redis (control plane), a running tcpunchd on :10000 (Direct data plane),
# and a working criu. The default runs criu privileged (root); for rootless criu set
# FMI_CRIU_EXTRA_ARGS="--unprivileged".
set -uo pipefail

RUNBOOK_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${RUNBOOK_DIR}/../.." && pwd)"
BUILD_DIR="${FMI_BUILD_DIR:-${REPO_ROOT}/build-unified-criu}"
TEMPLATE="${RUNBOOK_DIR}/fmi-machine.json.tmpl"

# Binary locations default to a host cmake build tree, but can be overridden (e.g. when this
# script runs inside the demo container, where the binaries are staged elsewhere).
DEMO="${FMI_DEMO_BIN:-${BUILD_DIR}/runbooks/local-criu-state-transfer/transparent_state_transfer_demo}"
AGENT="${FMI_AGENT_BIN:-${BUILD_DIR}/tools/fmi-rank-agent}"

NUM_PEERS="${NUM_PEERS:-8}"
RANKS_PER_MACHINE="${RANKS_PER_MACHINE:-4}"
COMM_NAME="${COMM_NAME:-criu-evac-$(date +%s)}"
WINDOW_MS="${WINDOW_MS:-8000}"
REDIS_HOST="${REDIS_HOST:-127.0.0.1}"
TCPUNCH_HOST="${TCPUNCH_HOST:-127.0.0.1}"

# Redis access: prefer a host redis-cli, else the fmi-redis docker container.
if command -v redis-cli >/dev/null 2>&1; then
    redis() { redis-cli -h "${REDIS_HOST}" "$@"; }
else
    redis() { docker exec -i fmi-redis redis-cli "$@"; }
fi

PREFIX="fmi:ft:${COMM_NAME}:"
log() { echo "[driver] $*"; }
fail() { echo "[driver] FAIL: $*" >&2; exit 1; }

[ -x "${DEMO}" ] || fail "demo binary not found: ${DEMO} (build with FMI_ENABLE_CRIU=ON)"
[ -x "${AGENT}" ] || fail "rank agent not found: ${AGENT}"
[ -f "${TEMPLATE}" ] || fail "config template not found: ${TEMPLATE}"
command -v criu >/dev/null 2>&1 || fail "criu not on PATH"
command -v envsubst >/dev/null 2>&1 || fail "envsubst not on PATH (install gettext)"
redis ping >/dev/null 2>&1 || fail "Redis not reachable at ${REDIS_HOST}:6379"
# The Direct data plane needs a tcpunchd rendezvous server. Without it ranks still reach ACTIVE
# (a Redis-only write) and then hang at the first Direct allreduce, so check it up front.
(exec 3<>/dev/tcp/"${TCPUNCH_HOST}"/10000) 2>/dev/null \
    || fail "tcpunchd not reachable on ${TCPUNCH_HOST}:10000 (Direct rendezvous) — start it: ${REPO_ROOT}/extern/TCPunch/server/build/tcpunchd 10000"

[ $((NUM_PEERS % RANKS_PER_MACHINE)) -eq 0 ] || fail "NUM_PEERS (${NUM_PEERS}) must be a multiple of RANKS_PER_MACHINE (${RANKS_PER_MACHINE})"

# Render the per-machine configs (differ only in criu.host_id).
CONFIG_A="${RUNBOOK_DIR}/.fmi-machine-a.json"
CONFIG_B="${RUNBOOK_DIR}/.fmi-machine-b.json"
REDIS_HOST="${REDIS_HOST}" TCPUNCH_HOST="${TCPUNCH_HOST}" HOST_ID="machine-a" \
    envsubst '${REDIS_HOST} ${TCPUNCH_HOST} ${HOST_ID}' <"${TEMPLATE}" >"${CONFIG_A}"
REDIS_HOST="${REDIS_HOST}" TCPUNCH_HOST="${TCPUNCH_HOST}" HOST_ID="machine-b" \
    envsubst '${REDIS_HOST} ${TCPUNCH_HOST} ${HOST_ID}' <"${TEMPLATE}" >"${CONFIG_B}"

log "comm_name=${COMM_NAME} num_peers=${NUM_PEERS} ranks_per_machine=${RANKS_PER_MACHINE} window_ms=${WINDOW_MS} criu_extra='${FMI_CRIU_EXTRA_ARGS:-}'"
log "machine-a = ranks 0..$((RANKS_PER_MACHINE - 1)) (evacuated); machine-b = ranks ${RANKS_PER_MACHINE}..$((NUM_PEERS - 1)) (survivors)"

# Clean any stale control-plane keys + images for this comm.
mapfile -t STALE < <(redis --scan --pattern "${PREFIX}*" 2>/dev/null)
[ "${#STALE[@]}" -gt 0 ] && redis del "${STALE[@]}" >/dev/null 2>&1
rm -rf "/tmp/fmi-criu-images/${COMM_NAME}"

# Launch all ranks: the first RANKS_PER_MACHINE on machine-a, the rest on machine-b.
declare -a PIDS=()
declare -a LOGS=()
for r in $(seq 0 $((NUM_PEERS - 1))); do
    if [ "${r}" -lt "${RANKS_PER_MACHINE}" ]; then cfg="${CONFIG_A}"; else cfg="${CONFIG_B}"; fi
    out="${RUNBOOK_DIR}/.last-rank${r}.log"
    LOGS[r]="${out}"
    : >"${out}"
    setsid "${DEMO}" "${r}" "${NUM_PEERS}" "${cfg}" "${COMM_NAME}" "${WINDOW_MS}" >"${out}" 2>&1 &
    PIDS[r]=$!
done
log "launched ${NUM_PEERS} ranks (logs: ${RUNBOOK_DIR}/.last-rank*.log)"

cleanup() { kill "${PIDS[@]}" 2>/dev/null; }
trap cleanup EXIT

# Wait for all ranks ACTIVE at epoch 0.
log "waiting for all ${NUM_PEERS} ranks ACTIVE at epoch 0"
all_active=0
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

# Evacuate machine-a in one cut. The agent (run with machine-a's config so resolve_host_id ==
# "machine-a") discovers ranks 0..RANKS_PER_MACHINE-1, requests their migration, waits for all to
# quiesce, dump/restores them in parallel, and promotes epoch 1.
log "running rank agent: migrate-local on machine-a (real criu dump/restore of its ranks)"
"${AGENT}" migrate-local "${COMM_NAME}" "${NUM_PEERS}" "${CONFIG_A}"
AGENT_RC=$?
[ "${AGENT_RC}" -eq 0 ] || fail "rank agent exited ${AGENT_RC} (see /tmp/fmi-criu-images/${COMM_NAME}/epoch-1/rank-*/{dump,restore}.log)"

# machine-a ranks were restored as detached criu children; machine-b ranks reconfigured in place.
# Poll every rank's log for its result line.
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
    log "PASS: machine-a (${RANKS_PER_MACHINE} ranks) was evacuated in one criu cut; all ${NUM_PEERS} ranks preserved state at epoch 1"
    exit 0
fi
fail "all_ok=${all_ok} epoch=${EPOCH}"
