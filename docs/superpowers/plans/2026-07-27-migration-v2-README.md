# Migration Protocol v2 — Document Index and Running Order

**Status:** index. Not normative; the spec is. If this file and the spec disagree, the spec wins.

## Start here (read this paragraph first if you are picking this up cold)

FMI's transparent migration is unsound for divergent point-to-point schedules: the current protocol
aligns operation *counts* against one global `cut_index`, not operation *identities*, so a
post-migration `recv` can silently consume a pre-migration message. Four documents fix that. One
spec pins the contracts; three plans implement disjoint slices. **You almost certainly want Plan A.**
It is the only slice that is independently valuable, fully reversible, needs no new architecture, and
ends with the headline counterexample binary exiting `0` instead of `2` — on the *existing* epoch
protocol. Read the spec's contract 1 and then work Plan A task by task. Do not start Plan C past
stage C1 until Plans A and B have both reported; C2 is a one-way door. Before your first build:
`git submodule update --init --recursive`, then
`cmake -S . -B build -DFMI_BUILD_TESTS=ON -DFMI_ENABLE_CRIU=ON` — **`FMI_ENABLE_CRIU=ON` is required
or neither counterexample binary is built** — and run `Boost_Tests_run` from `build/tests`, because
`tests/communicator.cpp:12` hardcodes a relative config path.

## The four documents

| Document | What it is |
|---|---|
| `../specs/2026-07-27-sequenced-incarnation-links-design.md` | **The spec. Normative.** Four contracts (message identity, transport durability, membership state machine, checkpoint mechanics), the config surface, the staging map, the acceptance oracles, and four open items. It pins *what* and *why*, and deliberately schedules nothing. |
| `2026-07-27-a-message-identity.md` | **Plan A — stages A1, A2, A3.** 9 tasks. Hardens TCPunch pairing; gives the ClientServer data plane job-lifetime, identity-qualified keys; installs the RAII operation-identity scope above the `Channel` interface. Converts silent substitution into correct delivery or a loud `IdentityMismatch`. |
| `2026-07-27-b-checkpoint-mechanics.md` | **Plan B — stage B.** 12 tasks. A toy target with no FMI in it, seven measured CRIU experiments (pid reclaim, `--tcp-close`, held-mutex freeze, `PR_SET_PTRACER`, SIGPIPE/EINTR, freeze cost vs RSS, request→frozen latency), then the resident agent: verified process identity, freeze/dump/stage/verify/CAS, restore under lease, batched evacuation. |
| `2026-07-27-c-sequenced-link-layer.md` | **Plan C — stages C1–C4.** Written just-in-time: **only C1 (7 tasks) is executable.** C1 is the offline, socket-free link layer (`LinkFrame`, `SequencedLink`, `ProgressHooks`, `fault_tolerance.link`). C2–C4 are scoped outlines with entry criteria, because their detail depends on facts Plan B has not measured yet. |

Also relevant: `2026-07-22-migration-counterexamples.md` (the plan that built the corpus these plans
are graded against) and its spec `../specs/2026-07-22-migration-counterexamples-design.md`.

## Dependency order

```
                    spec (normative, no work)
                             │
        ┌────────────────────┼────────────────────┐
        │                    │                    │
    Plan A               Plan B                Plan C / C1
   A1 → A2 → A3      12 tasks, agent +      7 tasks, offline
   (9 tasks)         7 experiments          link layer
        │                    │                    │
        │  A1 tasks 1-2 ─────┼──► C1 task 7 (shares tests/channels.cpp)
        │                    │
        └──── identity seam ─┴──── measured CRIU findings ────┐
                                                              ▼
                                              ╔══════════════════════════════╗
                                              ║  C2  ⚠ COMMITMENT POINT      ║
                                              ║  engine owns the sockets     ║
                                              ╚══════════════════════════════╝
                                                              │
                                                    C3 (irreversible)
                                                    epochs → incarnations
                                                              │
                                                             C4
```

### What runs in parallel

**Plans A, B and C1 run concurrently.** They touch disjoint production file sets — `extern/TCPunch`
+ `src/comm/` (A), `src/ft/experimental/` + `tools/` (B), new files + `src/utils/Configuration.cpp`
(C1). Three shared-file exceptions, all resolved in the plans:

1. **`tests/channels.cpp` / `tests/communicator.cpp` — Plan A tasks 1–2 before Plan C task 7.**
   A adds two `Channels` cases; C's task 7 rewrites every `get_channel` call through a variant
   overlay and diffs the case list. Running C7 first guarantees a conflict and an invalid baseline.
   After A1 the baseline is 17 + 10 = 27 cases, not 25.
2. **`include/ft/ControlPlane.h` / `src/ft/ControlPlane.cpp` — Plan A task 6 only.** One additive
   defaulted parameter on `clear_job_state`. Plan B is forbidden the file; Plan C stage C3 rewrites
   it and must carry the change forward (C3 named work item 6).
3. **`CMakeLists.txt` and `tests/CMakeLists.txt` — all three append.** Resolve by union.

### What is strictly sequential

- **C2 requires:** Plan B's two-thread-dump and frozen-duration-vs-RSS findings; Plan A's landed
  identity seam; the inline-send-vs-full-engine benchmark **run before the request interface is
  frozen**; C1 fully green.
- **C3 requires:** C2's vertical slice demonstrated, **and Plan B landed** — C3 absorbs Plan B's
  shadow `ckpt:` directory into `ControlPlane` v2 (C3 named work item 8).
- **C4 requires:** C3.

## The commitment point

**Stage C2.** Everything before it is additive and independently valuable even if the progress
engine is never built. C2 takes ownership of the file descriptors, and socket ownership is not
divisible — there is no partial retreat. The spec is explicit that the C2 decision is deferred
"until B has reported what CRIU actually does on the target kernel." Plan C therefore leaves C2–C4
as outlines with written entry criteria rather than fabricating task detail against unmeasured
facts. **Do not expand C2 into tasks until every entry criterion is satisfied and recorded.**

C2 deliberately keeps epoch-qualified pairing names, so the threading model and retention/replay are
proven before the membership authority is swapped. C3 is then largely a naming and authority swap —
but it is irreversible.

## Single-owner map for the contested items

These were double-owned or unowned across drafts and are now assigned exactly once.

| Item | Owner |
|---|---|
| `Classification::LoudFail`, `aggregate_exit`, `--group message-identity` | **Plan A task 9.** C4 verifies, never re-implements. |
| `tests/migration_cut_model.cpp` disposition (`--expect` change-detector) | **Plan A task 9.** C4 re-targets it at incarnations later. |
| ClientServer deadline-suspension **seam** (`deadline_suspended()`) | **Plan A task 6** records it; **C3 work item 3** overrides it. |
| Job-wide policy fingerprint (startup assertion) | **Plan A task 8**; C task 5's handshake consumes the same value. |
| `--tcp-close` on both criu legs | **Plan B task 4.** |
| Foreign-fd (`/proc/<pid>/fd`) preflight | **Plan B task 12.** |
| `PR_SET_PTRACER` re-arm after restore | **B measures** (task 6); **C2 work item 10** fixes it. |
| EINTR in `Direct::send_object`/`recv_object` | **B measures** (task 6); **C2 work item 3** fixes it. |
| SIGPIPE `pthread_sigmask` on the engine thread | **C2 work item 4.** |
| Stale `--tcp-close` claim in `include/ft/ControlPlane.h:86-92` | **C3 work item 7** (B found it, cannot edit that file). |
| Contract-3 agent-driven edges | **B task 8** builds a shadow `ckpt:` directory; **C3 work item 8** absorbs it. One authority survives. |
| Custom-channel capability advertisement | **C3 work item 9.** |

## The acceptance oracle, and where it flips

`tests/migration_p2p_cut_counterexample.cpp` is the headline gate. It hardcodes
`config/fmi_ft_stress_redis_test.json` (`:267-268`), which disables `Direct` and enables `Redis`, so
its failure mechanism is the **ClientServer counter reset**. It therefore flips at **Plan A stage
A2** — no stage may gate on it earlier, no Plan C milestone may claim it, and no stage may loudly
reject ClientServer-under-FT while it remains the gate.

Note that the ClientServer data plane has **no `Boost_Tests_run` coverage today** (Redis is
commented out of `tests/channels.cpp:63-64` and disabled in `config/fmi_test.json:11`), so for A2/A3
"the suite stays green" is a no-regression check, never evidence. Each such task carries its own
Redis-backed case.

## Machine facts (this host)

- Redis native at `127.0.0.1:6379` — `redis-cli ping` → `PONG`.
- `tcpunchd`: `extern/TCPunch/server/build-fresh/tcpunchd 10000`. The `build/` and `build-debug/`
  copies are **stale pre-fix builds** — do not use them.
- criu 4.2 at `/usr/local/sbin/criu` with `cap_sys_ptrace=eip`.
- `config/fmi_test.json:19` holds the stale LAN address `192.168.0.166` for `backends.Direct.host`.
  Override it to `127.0.0.1` locally for a green Direct run and **never commit the override**.
- Run `Boost_Tests_run` with `cwd = build/tests`.
- Use `uv` for any Python, never bare `pip`.
- Stage and commit only files belonging to the current task; the working tree carries pre-existing
  unrelated changes that must be preserved.
