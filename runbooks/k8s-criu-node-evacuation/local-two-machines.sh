#!/usr/bin/env bash
# Stage-0 local baseline for the CRIU node-evacuation-to-serverless demo.
#
# Simulates the whole cross-host pipeline on a single host using criu.host_id overrides:
# machine-a's ranks are dumped and STAGED in Redis (`fmi-rank-agent evacuate-local`), the local
# staging is wiped to prove nothing survives outside the control plane, then per-rank agents with
# fake serverless identities (`fake-serverless-<r>`) fetch + restore them (`restore-remote`) and
# a final `promote` releases the cut. machine-b's ranks survive at the barrier throughout.
#
# Flow:
#   1. render two configs from fmi-machine.json.tmpl that differ only in criu.host_id
#      (machine-a, machine-b); both share the same Redis control plane and tcpunchd rendezvous
#   2. launch NUM_PEERS transparent_state_transfer_demo ranks: the first RANKS_PER_MACHINE on
#      machine-a, the rest on machine-b
#   3. wait until all ranks are ACTIVE at epoch 0
#   4. `evacuate-local` with machine-a's config: dumps its ranks in one consistent cut and stages
#      image archives (criu tree + each rank's log) in Redis — no restore, no promotion
#   5. wipe the local image tree + the evacuated ranks' logs, then run one `restore-remote` per
#      rank under a fake-serverless-<r> host identity: fetch from Redis, unpack, criu restore
#   6. verify the CRIU registry shows each rank RUNNING on its fake serverless host, `promote`,
#      and assert every rank finishes with the post-migration allreduce == expected (state
#      survived) at epoch 1
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
log "machine-a = ranks 0..$((RANKS_PER_MACHINE - 1)) (evacuated to fake serverless); machine-b = ranks ${RANKS_PER_MACHINE}..$((NUM_PEERS - 1)) (survivors)"

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

# Evacuate machine-a: dump its ranks in one cut and stage the archives (image tree + each rank's
# log, which criu re-opens on restore) in Redis. No restore, no promotion.
log "running rank agent: evacuate-local on machine-a (dump + stage in Redis, no restore)"
FMI_CRIU_EXTRA_FILES="${RUNBOOK_DIR}/.last-rank{rank}.log" \
    "${AGENT}" evacuate-local "${COMM_NAME}" "${NUM_PEERS}" "${CONFIG_A}"
AGENT_RC=$?
[ "${AGENT_RC}" -eq 0 ] || fail "evacuate-local exited ${AGENT_RC} (see /tmp/fmi-criu-images/${COMM_NAME}/epoch-1/rank-*/dump.log)"

# Prove the staged archives are the ONLY thing the restore side needs: wipe the local image tree
# and the evacuated ranks' logs before restoring.
rm -rf "/tmp/fmi-criu-images/${COMM_NAME}/epoch-1"
for r in $(seq 0 $((RANKS_PER_MACHINE - 1))); do rm -f "${LOGS[r]}"; done
log "local staging wiped; restoring each rank from Redis under a fake serverless identity"

# One restore-remote per rank, each with its own host identity (what a Knative pod hostname is
# in the real deployment), run in parallel like the orchestrator's parallel POSTs.
declare -a RESTORE_PIDS=()
for r in $(seq 0 $((RANKS_PER_MACHINE - 1))); do
    cfg_r="${RUNBOOK_DIR}/.fmi-serverless-${r}.json"
    REDIS_HOST="${REDIS_HOST}" TCPUNCH_HOST="${TCPUNCH_HOST}" HOST_ID="fake-serverless-${r}" \
        envsubst '${REDIS_HOST} ${TCPUNCH_HOST} ${HOST_ID}' <"${TEMPLATE}" >"${cfg_r}"
    "${AGENT}" restore-remote "${COMM_NAME}" "${NUM_PEERS}" "${cfg_r}" "${r}" &
    RESTORE_PIDS+=($!)
done
restore_ok=1
for pid in "${RESTORE_PIDS[@]}"; do wait "${pid}" || restore_ok=0; done
[ "${restore_ok}" -eq 1 ] || fail "a restore-remote failed (see /tmp/fmi-criu-images/${COMM_NAME}/epoch-1/rank-*/restore.log)"

# The registry must now show every evacuated rank RUNNING on its new (fake serverless) host.
for r in $(seq 0 $((RANKS_PER_MACHINE - 1))); do
    host="$(redis hget "${PREFIX}criu:rank:${r}" host_id 2>/dev/null)"
    state="$(redis hget "${PREFIX}criu:rank:${r}" state 2>/dev/null)"
    [ "${state}" = "RUNNING" ] && [ "${host}" = "fake-serverless-${r}" ] \
        || fail "rank ${r} did not relocate: state=${state} host=${host}"
    log "rank ${r} relocated: machine-a -> ${host}"
done

# The orchestrator's final act: one promotion releases the whole cut.
log "promoting the epoch"
"${AGENT}" promote "${COMM_NAME}" "${NUM_PEERS}" "${CONFIG_A}" || fail "promote failed"

# machine-a ranks now run as detached criu children under fake serverless identities; machine-b
# ranks reconfigured in place. Poll every rank's log for its result line.
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
    log "PASS: machine-a (${RANKS_PER_MACHINE} ranks) was evacuated through Redis-staged images and restored under new host identities; all ${NUM_PEERS} ranks preserved state at epoch 1"
    exit 0
fi
fail "all_ok=${all_ok} epoch=${EPOCH}"
