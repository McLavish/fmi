#!/usr/bin/env bash
# Cluster-free dress rehearsal of the CRIU node-evacuation-to-serverless demo.
#
# Runs the two "machines" as two privileged containers on a user-defined Docker bridge network,
# then plays the whole orchestrator flow against them:
#
#   1. evacuate machine-a's ranks in one cut (`docker exec ... fmi-rank-agent evacuate-local`,
#      the stand-in for `kubectl exec`): dumped + staged in Redis, no restore;
#   2. `docker stop fmi-machine-a` — the machine is GONE, like scaling its Deployment to zero;
#   3. start one fresh privileged container per evacuated rank running restore_server.py
#      (the stand-in for a cold-started fmi-restore Knative instance, distinct hostname + IP)
#      and POST /restore to each in parallel — each request criu-restores its rank and stays
#      open while the restored worker runs;
#   4. once the CRIU registry shows every rank RUNNING on its restore container, promote the
#      epoch from a one-off container (the orchestrator's role);
#   5. PASS iff all ranks report preserved state at epoch 1, the survivors in machine-b's logs
#      and the evacuated ranks in the /restore responses.
#
# This exercises everything the cluster path needs except Knative itself: the image, the
# supervisor's pid band, cross-container Direct hole-punching, Redis image shipping, and
# restore-into-a-fresh-container.
#
# Requires: docker, and the demo image (default fmi-criu-evac:dev).
set -uo pipefail

IMAGE="${FMI_IMAGE:-fmi-criu-evac:dev}"
NET="${NET:-fmi-evac-net}"
NUM_PEERS="${NUM_PEERS:-8}"
RANKS_PER_MACHINE="${RANKS_PER_MACHINE:-4}"
COMM_NAME="${COMM_NAME:-criu-evac-ctr-$(date +%s)}"
WINDOW_MS="${WINDOW_MS:-20000}"
RUNBOOK_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

log() { echo "[driver] $*"; }
fail() { echo "[driver] FAIL: $*" >&2; exit 1; }

docker image inspect "${IMAGE}" >/dev/null 2>&1 || fail "image ${IMAGE} not found (build it first)"

NAMES=(fmi-redis fmi-tcpunch fmi-machine-a fmi-machine-b)
for r in $(seq 0 $((RANKS_PER_MACHINE - 1))); do NAMES+=("fmi-restore-${r}"); done
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
            WINDOW_MS=${WINDOW_MS} CONFIG_PATH=/tmp/fmi.json PID_BASE=3000 \
            python3 -u /opt/fmi/app/supervisor.py" \
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

# 1. Evacuate machine-a: dump + stage in Redis (each rank's log travels with its image).
log "staging machine-a's ranks: docker exec fmi-rank-agent evacuate-local"
docker exec -e FMI_CRIU_EXTRA_FILES='/tmp/rank-{rank}.log' fmi-machine-a \
    /opt/fmi/bin/fmi-rank-agent evacuate-local "${COMM_NAME}" "${NUM_PEERS}" /tmp/fmi.json \
    || fail "evacuate-local failed (check: docker exec fmi-machine-a cat /tmp/fmi-criu-images/${COMM_NAME}/epoch-1/rank-0/dump.log)"

# 2. The machine is gone. Everything ranks 0..N-1 need from here on lives in Redis.
log "stopping fmi-machine-a (the evacuated node)"
docker stop -t 2 fmi-machine-a >/dev/null

# 3. One fresh restore container per rank (a cold-started serverless instance: fresh fs, fresh
#    pid ns, its own hostname/IP), then POST /restore to each in parallel. Responses are held
#    until the restored workers finish, so collect them in the background.
declare -a POST_PIDS=()
for r in $(seq 0 $((RANKS_PER_MACHINE - 1))); do
    docker run -d --name "fmi-restore-${r}" --hostname "fmi-restore-${r}" --network "${NET}" \
        --privileged \
        -e COMM_NAME="${COMM_NAME}" -e NUM_PEERS="${NUM_PEERS}" \
        -e REDIS_HOST=fmi-redis -e TCPUNCH_HOST=fmi-tcpunch \
        "${IMAGE}" python3 -u /opt/fmi/app/restore_server.py >/dev/null \
        || fail "fmi-restore-${r} start failed"
done
for r in $(seq 0 $((RANKS_PER_MACHINE - 1))); do
    up=0
    for _ in $(seq 1 60); do
        if docker exec "fmi-restore-${r}" python3 -c \
            "import urllib.request; urllib.request.urlopen('http://localhost:8080/healthz', timeout=2)" \
            >/dev/null 2>&1; then up=1; break; fi
        sleep 0.5
    done
    [ "${up}" -eq 1 ] || fail "fmi-restore-${r} never became healthy"
done
log "POSTing ${RANKS_PER_MACHINE} parallel /restore requests"
for r in $(seq 0 $((RANKS_PER_MACHINE - 1))); do
    docker exec "fmi-restore-${r}" python3 -c "
import json, urllib.error, urllib.request
req = urllib.request.Request('http://localhost:8080/restore',
                             data=json.dumps({'rank': ${r}}).encode(),
                             headers={'Content-Type': 'application/json'})
try:
    print(urllib.request.urlopen(req, timeout=560).read().decode())
except urllib.error.HTTPError as e:
    print(e.read().decode())
" >"${RUNBOOK_DIR}/.restore-rank${r}.json" 2>&1 &
    POST_PIDS+=($!)
done

# 4. Restores are confirmed via the registry (the held responses only complete after promotion).
log "waiting for every evacuated rank to be RUNNING on its restore container"
relocated=0
for _ in $(seq 1 120); do
    relocated=1
    for r in $(seq 0 $((RANKS_PER_MACHINE - 1))); do
        state="$(redis hget "${PREFIX}criu:rank:${r}" state 2>/dev/null)"
        host="$(redis hget "${PREFIX}criu:rank:${r}" host_id 2>/dev/null)"
        { [ "${state}" = "RUNNING" ] && [ "${host}" = "fmi-restore-${r}" ]; } || { relocated=0; break; }
    done
    [ "${relocated}" -eq 1 ] && break
    sleep 0.5
done
[ "${relocated}" -eq 1 ] || fail "ranks did not relocate (check: docker logs fmi-restore-0; docker exec fmi-restore-0 cat /tmp/fmi-criu-images/${COMM_NAME}/epoch-1/rank-0/restore.log)"
for r in $(seq 0 $((RANKS_PER_MACHINE - 1))); do
    log "rank ${r} relocated: machine-a -> $(redis hget "${PREFIX}criu:rank:${r}" host_id)"
done

# 5. Promote from a one-off container — the orchestrator Job's role in the cluster.
log "promoting the epoch"
docker run --rm --network "${NET}" "${IMAGE}" bash -lc \
    "REDIS_HOST=fmi-redis TCPUNCH_HOST=fmi-tcpunch HOST_ID=driver \
        envsubst '\${REDIS_HOST} \${TCPUNCH_HOST} \${HOST_ID}' \
        </opt/fmi/app/fmi-machine.json.tmpl >/tmp/fmi.json \
     && /opt/fmi/bin/fmi-rank-agent promote ${COMM_NAME} ${NUM_PEERS} /tmp/fmi.json" \
    || fail "promote failed"

log "waiting for the held /restore responses (workers finishing)"
for pid in "${POST_PIDS[@]}"; do wait "${pid}"; done

# Survivors report in machine-b's logs; evacuated ranks in their /restore responses.
survivors_ok="$(docker logs fmi-machine-b 2>&1 | grep -c "OK: application state survived")"
responses_ok=0
for r in $(seq 0 $((RANKS_PER_MACHINE - 1))); do
    if grep -q '"status": "ok"' "${RUNBOOK_DIR}/.restore-rank${r}.json" \
        && grep -q "OK: application state survived" "${RUNBOOK_DIR}/.restore-rank${r}.json"; then
        responses_ok=$((responses_ok + 1))
    else
        log "rank ${r} restore response not ok: $(cat "${RUNBOOK_DIR}/.restore-rank${r}.json")"
    fi
done
EPOCH="$(redis hget "${PREFIX}meta" current_epoch 2>/dev/null)"

echo "----- machine-b log -----"; docker logs fmi-machine-b 2>&1 | grep -E "rank=[0-9]+ (OK|FAIL|phase1_sum)"
for r in $(seq 0 $((RANKS_PER_MACHINE - 1))); do
    echo "----- restore response rank ${r} -----"
    python3 -m json.tool "${RUNBOOK_DIR}/.restore-rank${r}.json" 2>/dev/null | head -8 \
        || cat "${RUNBOOK_DIR}/.restore-rank${r}.json"
done
log "survivors_ok=${survivors_ok}/$((NUM_PEERS - RANKS_PER_MACHINE)) restored_ok=${responses_ok}/${RANKS_PER_MACHINE} final_epoch=${EPOCH}"

if [ "${survivors_ok}" -ge $((NUM_PEERS - RANKS_PER_MACHINE)) ] \
    && [ "${responses_ok}" -eq "${RANKS_PER_MACHINE}" ] && [ "${EPOCH}" = "1" ]; then
    log "PASS: machine-a's ${RANKS_PER_MACHINE} ranks were evacuated into fresh restore containers (machine-a stopped); all ${NUM_PEERS} ranks preserved state at epoch 1"
    exit 0
fi
fail "survivors_ok=${survivors_ok} restored_ok=${responses_ok} epoch=${EPOCH}"
