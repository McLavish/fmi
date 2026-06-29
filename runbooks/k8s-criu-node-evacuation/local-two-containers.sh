#!/usr/bin/env bash
# Cluster-free validation of the multi-machine CRIU node-evacuation demo.
#
# Runs the two "machines" as two separate privileged containers on a user-defined Docker bridge
# network, so — unlike local-two-machines.sh (one host, loopback) — the ranks pair over the
# Direct/TCPunch data plane between DISTINCT container IPs, using DNS service names (fmi-redis,
# fmi-tcpunch). This is the closest cluster-free proxy for the real K8s path: it exercises the
# image, the supervisor, cross-"node" Direct hole-punching, and evacuating one machine's ranks via
# `docker exec ... fmi-rank-agent migrate-local` (the stand-in for `kubectl exec`), while the other
# machine's ranks survive at the barrier.
#
# Requires: docker, and the demo image (default fmi-criu-evac:dev).
set -uo pipefail

IMAGE="${FMI_IMAGE:-fmi-criu-evac:dev}"
NET="${NET:-fmi-evac-net}"
NUM_PEERS="${NUM_PEERS:-8}"
RANKS_PER_MACHINE="${RANKS_PER_MACHINE:-4}"
COMM_NAME="${COMM_NAME:-criu-evac-ctr-$(date +%s)}"
WINDOW_MS="${WINDOW_MS:-20000}"

log() { echo "[driver] $*"; }
fail() { echo "[driver] FAIL: $*" >&2; exit 1; }

docker image inspect "${IMAGE}" >/dev/null 2>&1 || fail "image ${IMAGE} not found (build it first)"

NAMES=(fmi-redis fmi-tcpunch fmi-machine-a fmi-machine-b)
cleanup() {
    docker rm -f "${NAMES[@]}" >/dev/null 2>&1
    docker network rm "${NET}" >/dev/null 2>&1
}
trap cleanup EXIT
cleanup  # clear any stale leftovers from a previous run

docker network create "${NET}" >/dev/null || fail "could not create network ${NET}"

log "starting redis + tcpunch on ${NET}"
docker run -d --name fmi-redis --network "${NET}" redis:7 \
    --save "" --appendonly no >/dev/null || fail "redis start failed"
docker run -d --name fmi-tcpunch --network "${NET}" "${IMAGE}" \
    /opt/fmi/bin/tcpunchd 10000 >/dev/null || fail "tcpunch start failed"

run_machine() {  # name host_id base_peer_id
    docker run -d --name "$1" --network "${NET}" --privileged "${IMAGE}" \
        bash -lc "HOST_ID=$2 NUM_PEERS=${NUM_PEERS} BASE_PEER_ID=$3 RANKS_HERE=${RANKS_PER_MACHINE} \
            COMM_NAME=${COMM_NAME} REDIS_HOST=fmi-redis TCPUNCH_HOST=fmi-tcpunch \
            WINDOW_MS=${WINDOW_MS} CONFIG_PATH=/tmp/fmi.json python3 -u /opt/fmi/app/supervisor.py" \
        >/dev/null || fail "$1 start failed"
}
log "starting machine-a (ranks 0..$((RANKS_PER_MACHINE - 1))) and machine-b (ranks ${RANKS_PER_MACHINE}..$((NUM_PEERS - 1)))"
run_machine fmi-machine-a machine-a 0
run_machine fmi-machine-b machine-b "${RANKS_PER_MACHINE}"

redis() { docker exec fmi-redis redis-cli "$@"; }
PREFIX="fmi:ft:${COMM_NAME}:"

log "waiting for all ${NUM_PEERS} ranks ACTIVE at epoch 0"
active=0
for _ in $(seq 1 200); do
    active=1
    for r in $(seq 0 $((NUM_PEERS - 1))); do
        [ "$(redis hget "${PREFIX}epoch:0:states" "${r}" 2>/dev/null)" = "ACTIVE" ] || { active=0; break; }
    done
    [ "${active}" -eq 1 ] && break
    sleep 0.5
done
[ "${active}" -eq 1 ] || fail "not all ranks reached ACTIVE (check: docker logs fmi-machine-a)"
log "all ranks ACTIVE"

log "evacuating machine-a via docker exec fmi-rank-agent migrate-local"
docker exec fmi-machine-a /opt/fmi/bin/fmi-rank-agent migrate-local "${COMM_NAME}" "${NUM_PEERS}" /tmp/fmi.json
[ $? -eq 0 ] || fail "migrate-local failed (check: docker exec fmi-machine-a ls /tmp/fmi-criu-images/${COMM_NAME}/epoch-1/rank-*/)"

log "waiting for epoch 1 + all ranks finished"
ok_total=0
for _ in $(seq 1 120); do
    ok_total=0
    for m in fmi-machine-a fmi-machine-b; do
        ok_total=$((ok_total + $(docker logs "${m}" 2>&1 | grep -c "OK: application state survived")))
    done
    [ "${ok_total}" -ge "${NUM_PEERS}" ] && break
    sleep 0.5
done
EPOCH="$(redis hget "${PREFIX}meta" current_epoch 2>/dev/null)"

echo "----- machine-a log -----"; docker logs fmi-machine-a 2>&1 | grep -E "rank=[0-9]+ (OK|FAIL|phase1_sum)"
echo "----- machine-b log -----"; docker logs fmi-machine-b 2>&1 | grep -E "rank=[0-9]+ (OK|FAIL|phase1_sum)"
log "ok_ranks=${ok_total}/${NUM_PEERS} final_epoch=${EPOCH}"

if [ "${ok_total}" -ge "${NUM_PEERS}" ] && [ "${EPOCH}" = "1" ]; then
    log "PASS: machine-a evacuated across containers (distinct IPs); all ${NUM_PEERS} ranks preserved state at epoch 1"
    exit 0
fi
fail "ok_ranks=${ok_total} epoch=${EPOCH}"
