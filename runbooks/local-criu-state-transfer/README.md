# Local CRIU transparent state-transfer runbook (same-host v1)

This runbook demonstrates **CRIU-backed transparent state transfer** for single-rank
migration: a plain `FMI::Communicator` application keeps its in-memory application state across
a migration, with no application-level checkpoint code. It is the same-host v1 of the milestone
described in `PLANS.md` and `docs/fault-tolerance.md`.

It is verified end-to-end on this machine with **rootless** CRIU 4.2 (`--unprivileged`),
Ubuntu 24.04, kernel 6.17. A privileged Docker fallback is described at the end for hosts where
rootless CRIU cannot dump/restore.

## What it proves

Two ranks run a collective workload
(`runbooks/local-criu-state-transfer/transparent_state_transfer_demo.cpp`):

1. phase-1 `allreduce`,
2. each rank mutates a local `state` variable (`state += 100`) — memory that lives only in the
   process,
3. a `barrier()` (the migration quiesce point),
4. phase-2 `allreduce` over `state`.

The orchestrator migrates **rank 0** between phases. With `state` preserved, phase-2 must equal
`num_peers*(num_peers+1)/2 + 100*num_peers` (= **203** for 2 ranks: rank 0 contributes 101,
rank 1 contributes 102). That result is only reachable if rank 0's in-memory `state` survived a
criu dump/restore — the restored image resumes **inside the barrier** (after phase 1), so it
never recomputes.

## How it works

```
rank 0 (target)                    rank agent (fmi-rank-agent)        rank 1 (survivor)
  barrier(): enter_operation
   sees pending migration of self
   prctl(PR_SET_PTRACER, ANY)
   close Direct sockets
   publish QUIESCED@epoch1 (+pid)   ── observes ready rank ──▶
   block in promotion-wait loop      criu dump  -t <pid> --tcp-close
        (process frozen) ◀──────────  (kills + reaps the rank)
        (restored image) ◀──────────  criu restore --tcp-close --restore-detached
                                       promote_epoch(1) ─────────────────────────▶ sees epoch 1
   reconnects Redis (lazy)                                                          reconfigure→epoch1
   sees epoch 1 → reconfigure→epoch1
  barrier() completes  ◀───────────────── Direct re-pair @ epoch=1 ──────────────▶  barrier() completes
  phase-2 allreduce over preserved state (=101)  ───────────────────────────────▶  phase-2 (=102) → 203
```

Key points:
- The rank is a plain `FMI::Communicator` app. It never calls criu; it only signals readiness
  through the Redis control plane (`fault_tolerance.state_transfer="criu"`).
- `prctl(PR_SET_PTRACER, PR_SET_PTRACER_ANY)` lets the non-parent rank agent's criu ptrace-seize
  the rank under `yama ptrace_scope=1` (the host default).
- Direct sockets are closed before the dump; the restored rank re-pairs lazily under the
  epoch-`N+1` communicator name. The Redis control connection is closed by `--tcp-close` and
  the ControlPlane reconnects lazily (SIGPIPE is ignored so that reconnect is not fatal).

## Prerequisites

- Build with CRIU enabled (produces the demo + `fmi-rank-agent`):
  see the `build-unified-criu` flags in the repo, or `CLAUDE.md`. The driver defaults to
  `FMI_BUILD_DIR=<repo>/build-unified-criu`.
- A running **Redis** control plane on `127.0.0.1:6379` (e.g. `docker run -d --name fmi-redis
  -p 127.0.0.1:6379:6379 redis:7`).
- A running **tcpunchd** rendezvous on port `10000`
  (`./extern/TCPunch/server/build/tcpunchd 10000`).
- **criu** on `PATH`. For rootless operation, `criu check --unprivileged` should report
  "Looks good".

## Run

```bash
# rootless criu
FMI_CRIU_EXTRA_ARGS="--unprivileged" bash runbooks/local-criu-state-transfer/run-demo.sh
```

Expected tail:

```
[driver] running rank agent (real criu dump/restore)
migrated_rank=0 promoted_epoch=1
rank=0 post_migration_state=101 phase2_sum=203 expected=203
rank=0 OK: application state survived transparent migration
rank=1 post_migration_state=102 phase2_sum=203 expected=203
rank=1 OK: application state survived transparent migration
[driver] PASS: rank 0 was criu-migrated; both ranks completed phase 2 with preserved state at epoch 1
```

Useful overrides: `COMM_NAME`, `WINDOW_MS` (migration window after phase 1), `FMI_BUILD_DIR`,
`FMI_CRIU_EXTRA_ARGS` (extra criu flags). Per-rank logs are written to
`runbooks/local-criu-state-transfer/.last-run-rank{0,1}.log`.

## Privileged Docker fallback

If rootless CRIU cannot dump/restore on your host (older kernel, restricted seccomp, TCP
repair limits), run the same flow inside a privileged container where criu has full
capabilities — then no `--unprivileged` flag is needed:

```bash
docker run --rm -it --privileged --name fmi-criu \
  -v "$PWD":/work -w /work \
  ubuntu:24.04 bash
# inside: install criu + redis-server + build deps, start redis + tcpunchd, then
#   FMI_CRIU_EXTRA_ARGS="" bash runbooks/local-criu-state-transfer/run-demo.sh
```

## v1 limitations

- Same host only (criu restores the process on the host that dumped it).
- `Direct` is the only supported data backend; `Redis` is the control plane.
- One targeted rank per migration; in-flight collectives are not preserved (migration happens
  only at operation boundaries).
- No Python binding for the CRIU path (the demo is C++).
- No rank-agent-failure recovery in the library: the rank agent must complete dump → restore →
  `promote_epoch`. Survivor ranks wait for promotion **indefinitely** — there is no library-side
  timeout — so a rank agent that dies mid-migration parks the job forever. Detecting and resolving
  that is the orchestrator's job: this demo's driver does it by aborting on the agent's non-zero
  exit (`run-demo.sh` checks `AGENT_RC`). Run the rank agent under process supervision for planned
  migrations.
- `FMI_CRIU_EXTRA_ARGS` is split on whitespace with no quoting; individual flags must not
  contain spaces.
