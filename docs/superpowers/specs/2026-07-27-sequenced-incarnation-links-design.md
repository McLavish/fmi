# Sequenced Incarnation Links — Migration Protocol v2 Design

**Status:** design contract. Normative for all downstream implementation plans.
**Supersedes:** `docs/consensus-cut.md` (global epoch cut) once Stage C lands.
**Downstream plans:** `docs/superpowers/plans/2026-07-27-{a-message-identity,b-checkpoint-mechanics,c-sequenced-link-layer}.md`
**Index and running order:** `docs/superpowers/plans/2026-07-27-migration-v2-README.md`

## Purpose

Replace FMI's externally driven global epoch cut with per-rank incarnations over a sequenced
link layer, so that transparent migration is correct for **arbitrary divergent point-to-point
schedules**, not only for identical per-rank instruction streams.

This document pins the contracts that must be decided **once**, before any implementation
begins: the wire envelope, the transport invariants, the membership state machine, and the
config surface. It does not schedule work. Each downstream plan owns a disjoint slice and
refers back here for the shared contract.

## Why the current protocol is unsound

The cut aligns operation **counts**, not operation **identities**. `cut_index` drags the target
forward until `boundary_index >= cut_index` (`src/ft/TransparentMigrationRuntime.cpp:104-108`)
before it quiesces, and the whole safety story is one scalar per rank against one global
`cut_index` (`src/ft/ControlPlane.cpp:459-534`).

Two concrete mechanisms follow:

- **ClientServer counter reset.** Message identity lives only in the object key, built from
  instance-local per-directed-pair counters (`src/comm/ClientServer.cpp:6-31`). Those counters
  are plain members of the channel object (`include/comm/ClientServer.h:62`). Because
  `Channel::reconfigure_for_epoch` defaults to `false` (`include/comm/Channel.h:118-120`),
  `Communicator::reconfigure_to_epoch` finalizes and **rebuilds** the ClientServer channel
  (`src/Communicator.cpp:105-119`), resetting the counters to their initializer. A post-migration
  `recv` then reads the key of a pre-migration message. This is the proven silent-substitution
  path and the mechanism behind `tests/migration_p2p_cut_counterexample.cpp`.
- **Direct has no message identity at all.** Send is a partial-write loop and recv reads exactly
  `buf.len` bytes known a priori (`src/comm/Direct.cpp:28-72`). Matching is socket plus arrival
  order — no tag, sequence, or discriminator in the bytes. Collective fragments and application
  p2p share one socket, because every collective in `src/comm/PeerToPeer.cpp` calls the same
  virtual `send(channel_data, peer_num)` an application calls (`include/comm/Channel.h:43`).

Structurally, some divergent schedules have **no** valid runtime-choosable park point: a rank
blocked in `recv()` whose matching send has not been issued can never reach a boundary.

## Decomposition: four orthogonal axes

The work separates along four independent concerns. This decomposition is normative — the
downstream plans are cut along these lines, not along protocol layers.

| Axis | Question it answers | Primary files | Plan |
|---|---|---|---|
| **A. Message identity** | *Which* message is this? | `include/comm/Channel.h`, `src/comm/PeerToPeer.cpp`, `src/comm/Channel.cpp`, `src/comm/ClientServer.cpp`, `include/Communicator.h` | A |
| **B. Checkpoint mechanics** | How is a process frozen, staged, restored? | `src/ft/experimental/LocalRankAgent.cpp`, `tools/rank_agent.cpp` | B |
| **C. Transport durability** | How does a stream survive a connection break? | `include/comm/LinkFrame.h`, `SequencedLink`, `ProgressEngine`, `src/comm/Direct.cpp` | C |
| **D. Membership** | Who is rank 3 *right now*? | `src/ft/ControlPlane.cpp`, `src/ft/TransparentMigrationRuntime.cpp`, `src/Communicator.cpp` | C |

Axis A is independently valuable: it converts silent substitution into correct delivery or a
loud failure **on the existing protocol**, with no new architecture. Axis B is verifiable
against a toy process with no FMI in it. Axes C and D carry the majority of the cost and are the
only irreversible commitment.

## Normative contract 1 — message identity

### Envelope fields (Direct frames; the same tuple qualifies ClientServer keys)

Every DATA frame carries:

| Field | Width | Meaning |
|---|---|---|
| `wire_version` | u16 | Protocol generation. A framed rank must never pair with a raw one. |
| `lane` | u8 | `P2P` or `COLLECTIVE`. Separate FIFO and drain queue per lane. |
| `op_kind` | u8 | `send`/`bcast`/`barrier`/`gather`/`scatter`/`reduce`/`allreduce`/`scan`. |
| `collective_index` | u64 | Per-**Communicator** monotonic collective ordinal. Zero and unused on the `P2P` lane. |
| `root` | u32 | Collective root, or the peer id for p2p. |
| `message_id` | u64 | Per-lane, per-directed-pair job-lifetime ordinal. Never reset. |
| `fragment_index` | u32 | Fragment ordinal within a logical message. |
| `total_length` | u64 | Logical message length in bytes. |
| `transport_seq` | u64 | Link-scoped, job-lifetime reliability sequence (Axis C only). |

**Why `(collective_index, op_kind, root)` and not just a lane ordinal.** The per-lane
`message_id` is a FIFO position, so it cannot detect reordering. Counterexample at `N=2`:

```
rank0: bcast(root=0); barrier()
rank1: barrier();     bcast(root=0)
```

`bcast` at `N=2` sends one 1-byte frame 0→1 (`src/comm/PeerToPeer.cpp:14-27`, `rounds=1`).
`barrier` is a 1-byte `allreduce` with a `{commutative=true, associative=true}` nop
(`src/comm/PeerToPeer.cpp:29-33`), so it takes `allreduce_no_order`; at `N=2` rank0 takes the
recv-then-send branch and rank1 the send-then-recv branch (`src/comm/PeerToPeer.cpp:108-118`).
On the 0→1 direction rank0 emits `[bcast][barrier]` while rank1 consumes expecting
barrier-then-bcast. Lane, `total_length`, and ordinal all match on both sides — **silent wrong
delivery**, which is the exact failure class this protocol exists to eliminate.

A bare per-communicator collective counter is also insufficient: rank0's `bcast` and rank1's
`barrier` are each that rank's collective #0. Detection requires the receiver to independently
compute an expected value, so `op_kind` and `root` must both be present and validated.

**The counter must live on the `Communicator`, not the channel.** With a per-channel counter,
`bcast → Redis; barrier → Direct` yields Direct-collective-count 0 on both ranks in either issue
order and detects nothing.

### Where the identity is produced

`Channel::send(channel_data, peer_num)` has no lane parameter, and the 27 internal call sites in
`src/comm/PeerToPeer.cpp` route collectives through the same virtual an application calls.
The identity is therefore produced **above** the channel interface:

> **Normative:** an RAII lane-and-instance scope, set by `Communicator`'s operation entry points
> (natural home: the existing `OperationGuard`), publishes `(lane, op_kind, collective_index,
> root)` for the duration of one logical operation. Channels read the active scope. The
> `Channel` virtuals do **not** gain a lane parameter — that would break custom channels
> satisfying the existing interface.

### Job-lifetime keys (ClientServer)

Data-plane keys are qualified by the identity tuple and by a job-lifetime per-directed-pair
sequence. They are **never** epoch-qualified and never reset. `comm_name` for data keys is
decoupled from the epoch-qualified name used for Direct pairing.

- Consumer-delete only in single-consumer namespaces: p2p, the new `gather`/`scatter` namespace
  (`<comm>:gather:<n>:<rank>`), reduce contributions.
- `bcast`/`scan` use per-consumer keys (`…:for:<j>`) or a refcount.
- Receive discipline is checkpoint-consistent: repeatable `GET` → commit the payload into
  process memory → idempotent `DEL`. **Never `GETDEL`** — a freeze between the server-side
  delete and the process-side copy destroys the only copy.
- Exact object length is validated against the expected size before touching the destination.
  The two backends fail differently and both must be fixed: `Redis::download_object` silently copies
  `std::min(buf.len, reply->len)` and returns success (`src/comm/Redis.cpp:72`), while
  `S3::download_object` issues `s.read(buf.buf, buf.len)` and ignores `gcount()`
  (`src/comm/S3.cpp:50`), leaving the destination tail untouched with no error.
- Uploads are write-once and retry-safe: create-if-absent; if the key exists, validate length
  and digest and return success.
- Barrier arrival is the **deduplicated rank set** over per-rank keys
  `<comm>:barrier:<n>:<rank>`, never a raw count. Keys are retained until job cleanup — deleting
  after one rank's pass strands slower ranks. The suffix-scan implementation
  (`src/comm/ClientServer.cpp:44-65`) is deleted.

  Two distinct defects are being fixed here and they must not be conflated. The one that exists
  **today** is scope: arrival is a `count_if` over a *suffix* match (`_barrier_<n>`,
  `src/comm/ClientServer.cpp:54-55`) applied to an unscoped listing of the entire store —
  `Redis::get_object_names` issues `KEYS *` (`src/comm/Redis.cpp:88`) and `S3::get_object_names` a
  whole-bucket `ListObjects` (`src/comm/S3.cpp:79-93`) — so a foreign communicator's or a dead
  epoch's key counts as an arrival. The one that appears **only after the fix** is duplication:
  prefix scoping forces `KEYS` to become `SCAN`, and `SCAN` may return the same key more than once.
  The first requires the prefix; the second requires the set. Neither alone is sufficient.

### Policy uniformity

The following must be identical across ranks or the job is ill-formed. Mismatches select
different algorithms and therefore different message patterns, producing hangs the identity
check would otherwise report as corruption:

- `hint`, `faas_price`, model parameter hash, `num_peers`, `wire_version` — job-wide, validated
  once (handshake fingerprint under Axis C; startup assertion before that).
- **The reduction function's `commutative` and `associative` flags** — per operation, not
  job-wide. `left_to_right = !(f.commutative && f.associative)` selects between wholly different
  algorithms at `include/Communicator.h:103` and `:128` (`reduce_ltr`/gather vs binomial
  `reduce_no_order`; `scan_ltr` vs `scan_no_order`; ltr allreduce vs recursive doubling).
  These flags therefore belong in the **envelope**, alongside `op_kind`, and are validated per
  collective — not in a static job-wide fingerprint.

## Normative contract 2 — transport durability (Axis C)

### Two watermarks

Per receiver, per directed pair:

- `next_received` — highest contiguous `transport_seq` accepted. Drives dedup and replay.
- `ack_safe` — highest seq the sender may **prune**. CRIU mode: received-and-durably-held, so
  it equals `next_received` (drain buffers are inside the image). `none` mode: application-consumed
  only.

The handshake carries both, per direction, plus `lowest_retained`. A peer requesting frames older
than `lowest_retained`, or claiming `next_received > next_send`, is an impossible state → loud abort.

### Ordering invariants

1. An immutable retained copy exists before any DATA byte reaches the socket.
2. A complete frame is committed to the drain queue before its ACK becomes writable.
3. Retention is reclaimed only on a validated cumulative ACK.

These three are what make an arbitrary-instant freeze sound: because an ACK is only issued for a
durably-held frame, **everything in a kernel socket buffer is unacked by construction** and will
be replayed from peer retention after `criu dump --tcp-close`. All four freeze positions relative
to drain-commit and ACK-write were enumerated; none produces loss or duplication.

### ACK versus CREDIT

ACK releases sender retention. CREDIT (byte-based) admits payload into the receiver's drain
buffers and is returned only as the application consumes. Without the split, ack-at-drain lets a
divergent peer grow an unbounded receiver backlog. Control frames are credit-exempt. Egress is
serialized per socket: a partially written DATA frame completes before any ACK or control frame
begins.

**Drain reserve per source ≥ the peer's window `W`**, otherwise ACKs queue behind undrainable
data on the shared FIFO (ack-behind-data deadlock).

**Open item (must be pinned before Plan C task 1):** CREDIT's initial value relative to `W` and
the drain reserve, and the behavior when `retention_limit_bytes` is reached. Both are currently
unspecified and both can silently violate the delivery obligation. Either derive retention from
the window and reject configs that cannot honor the obligation, or specify that retention is
never discarded and a cap hit blocks admission of *new* sends only.

**Known regression risk:** bounding buffering at `W` where TCP socket buffers are effectively
unbounded today means programs that work now can deadlock under v2. Plan C must include a
backpressure regression test of the shape the existing `tests/channels.cpp` matrix exercises — but
it must be a **new, fork-based** case, not a modification of one of the existing ones.
`sending_receiving` and `sending_receiving_mult_times` (`tests/channels.cpp:70-129`) run their two
peers as OpenMP threads in a single process; a window-bounded deadlock introduced there would hang
`Boost_Tests_run` itself rather than fail it. Use the `ForkedRankGuard` pattern the other thirteen
cases already use, plus a hard in-child `alarm()`, so a framed regression reports a timeout.

### Delivery obligation

After `send()` returns, either the receiver owns the message or an autonomous sender component
retains and services it until acknowledged. Replay to a restored peer proceeds while the
application computes, with no dependence on the application's next FMI call — a completed send
may be the last time the application ever touches that link.

### Repair is directory-driven

> **Normative:** on observing a peer's incarnation change, or its state leaving `ACTIVE`, in its
> own directory poll, the engine proactively closes the link and enters repair. It **never**
> waits for EOF, RST, or a timeout to discover that a peer moved.

`--tcp-close` closes the connection on the *dumped* side; the peer receives FIN-or-nothing, and
in the leave-stopped case the original's sockets remain ESTABLISHED and silent. Combined with
suspended deadlines, a socket-driven repair path waits forever. Plan C must include a test that
a survivor repairs while its old socket is still ESTABLISHED.

## Normative contract 3 — membership state machine (Axis D)

Directory prefix `fmi:ft:<comm>:`. Every mutation is CAS-guarded on
`(run_id, rank, expected incarnation, expected state)` and carries a caller token. `dirgen` is
bumped by every mutating script.

### States and transitions

> **Normative rule: every state must have at least one outbound edge that an external actor can
> drive.** The failure edges below are not optional; without them a failed `criu dump` — which
> today's code treats as routine (three attempts then rethrow,
> `src/ft/experimental/LocalRankAgent.cpp:420-437`) — wedges the job.

| From | To | Script | Driven by |
|---|---|---|---|
| — | `STARTING` | `join` (create-at-incarnation-0 only if absent) | rank |
| `STARTING` | `ACTIVE` | `mark_active` | rank, first poll |
| `ACTIVE` | `PAUSING` | `request_pause(ranks…, migration_id)` | orchestrator |
| `PAUSING` | `CHECKPOINTED` | `mark_checkpointed` (only after image durably staged **and** digest-verified) | agent |
| `PAUSING` | `ACTIVE` | **`abort_pause`** | agent, on dump failure |
| `CHECKPOINTED` | `RESTORE_RESERVED` | `acquire_restore_lease(rank, attempt_id)` (SET-NX-EX) | agent |
| `CHECKPOINTED` | `ACTIVE` | **`abort_pause`** | orchestrator, migration cancelled |
| `RESTORE_RESERVED` | `ACTIVATING` | `commit_restore(rank, expected_inc, attempt_id)` — CAS incarnation+1 | restoring agent |
| `RESTORE_RESERVED` | `CHECKPOINTED` | **`abort_restore`** / lease expiry | agent or lease TTL |
| `ACTIVATING` | `ACTIVE` | `mark_active` — restored process's first poll; **clears the pause entry** | rank |
| `ACTIVATING` | `CHECKPOINTED` | **`abort_activation`** (fenced; agent died before `SIGCONT`) | orchestrator |

`abort_pause` is idempotent by `migration_id` and clears both the pause-set entry and any lease.
`acquire_restore_lease`'s CAS must accept `RESTORE_RESERVED`-with-expired-lease as well as
`CHECKPOINTED`.

### Restore commit discipline

Duplicate restores are byte-identical processes, so arbitration must happen before either runs:

- Restore runs under the lease with
  `criu restore --tcp-close --leave-stopped --restore-detached --pidfile <attempt>`.
  CRIU 4.2 records `--tcp-close` in the image and **requires it again at restore** (verified:
  restore without it fails `criu/image.c:94: Need to set the --tcp-close options`).
- The **restoring agent**, not the process, attempts `commit_restore`.
- Only a winner receives `SIGCONT`. A loser or lease-less straggler is `SIGKILL`ed **while still
  stopped**, so a twin can never pair, ack, or touch the control plane.
- The restored process learns its new incarnation from its first `poll`, never from image memory.
- The image blob is deleted only after `ACTIVATING → ACTIVE`. If the host dies between commit
  and `SIGCONT`, the image survives for a fresh attempt at the next incarnation.

### Deadline suspension

> **Normative:** deadline suspension is **per link**, keyed on link state. A link in
> `DEAD`/`REPAIRING` suspends its own clock until the handshake completes and replay drains.

A global `migrations_in_flight` flag derived from the pause set releases suspension at
`commit_restore` — strictly *before* any peer is permitted to re-pair, handshake, or replay. A
survivor mid-operation then throws `Utils::Timeout`, and since an exception unwinding a sliced
operation poisons the communicator, a textbook-clean migration would intermittently poison the
job. The pause entry is therefore cleared at `ACTIVATING → ACTIVE`, not at `commit_restore`.

ClientServer's four poll loops also expire at `max_timeout` with no suspension hook; Plan A
records the hook, Plan C wires it.

Documented consequence: genuine peer death hangs rather than timing out. The orchestrator owns
failure detection. This is in scope for planned migration only.

## Normative contract 4 — checkpoint mechanics (Axis B)

- A resident per-node agent subscribes to a control-plane pub/sub channel for pause requests,
  with a tight-interval poll of the pause set as fallback. The agent is never checkpointed and
  may hold sockets freely.
- Per local target: **verify process identity first** (pidfd + start time + registered nonce —
  never freeze a recycled PID) → freeze → dump → stage the image to a durable artifact store and
  verify its digest → only then CAS `CHECKPOINTED`. Redis holds the immutable manifest; the
  Redis-blob path (`ControlPlane::criu_image_put`, `include/ft/ControlPlane.h:123-125`) survives
  as a small-image fallback for local runbooks.
- Batched evacuation: freeze **all** local targets first, then run the slow dumps. Mutual
  in-flight traffic is unacked and therefore replayed; there is no wait-for-quiesce gate.
- **Frozen-original abort recovery is cross-host only.** Same-host restore reclaims the dumped
  pid: `src/ft/experimental/LocalRankAgent.cpp:417` states the current discipline verbatim ("No
  `--leave-stopped`: criu ptrace-seizes, dumps, then kills and reaps the task, freeing the pid so
  the immediate restore can reclaim it"), and `restore_rank` (`:441-453`) passes no
  pid-namespace or remap flag. Reproduced on criu 4.2: restore after `--leave-stopped` fails
  `Can't fork for <pid>: File exists`; killing the original first makes the identical restore
  succeed. Plan B must state the fate of the same-host `migrate`/`migrate-local` verbs explicitly.

### Process-level restore hygiene

Three items that are unreachable today (a quiesced rank holds no sockets) but become the normal
case once a rank is frozen mid-syscall:

- **SIGPIPE.** The engine's dedicated hiredis connection writes to a dead socket on first
  post-restore use. The repo deliberately avoids a process-global `SIG_IGN`, so the fix is
  `pthread_sigmask(SIG_BLOCK, {SIGPIPE})` on the engine thread.
- **EINTR.** Freeze delivers EINTR to blocking socket calls. `Direct::send_object` and
  `recv_object` currently treat it as fatal (`src/comm/Direct.cpp:36-43`, `:62-69`).
- **`PR_SET_PTRACER` does not survive restore.** Setting it once at `Communicator` construction
  means a rank can be migrated once and never again on the rootless path. Currently masked
  because `/usr/local/sbin/criu` carries `cap_sys_ptrace=eip`.

`--tcp-close` transparency covers FMI-managed connections only. An application holding its own
sockets needs its own reconnect story; the agent preflights `/proc/<pid>/fd` and warns or rejects
per policy.

### Deployment

A DaemonSet cannot dump a rank in another pod (different PID and mount namespaces; criu reopens
the executable by path) and cannot perform the restore leg on Knative at all. The property
actually required — never checkpointed, free to hold sockets — is satisfied by a **per-pod
sidecar**, which also keeps the Knative restore inside the pod where it already works.

## `none` mode — cooperative restart profile

`none` is **not** transparent and is relabeled accordingly. A fresh process cannot recover a
program counter, so the application must arrange its own resume point — the LocalStack flow
already does this with `resume=True` (`runbooks/localstack-python311-redis/worker_core.py:19-23`).
Seeded counters make divergence **loud** (`FatalGap`); they do not create transparency.

- Retained because it is the only serverless-without-CRIU path.
- Quiesce happens at guard boundaries only; the guard certifies no-operation-in-flight.
- The flush contract is **bounded** and aborts loudly rather than hanging: ack-on-consume means a
  CLOSING flush could otherwise wait on a receive that occurs later in a divergent program.
- Aligned-collective applications only. Batch-evacuating `none`-mode ranks that communicate with
  each other is rejected loudly.

## Config surface (validated at parse)

```
fault_tolerance.link = {
  window_frames,          # W
  max_frame_bytes,
  retention_limit_bytes,  # see open item above
  slice_ms,
  drain_reserve_frames    # must be >= window_frames
}
backends.Direct.framed    # Plan C rollout gate; removed at Plan C completion
```

Memory bound ≈ `2·(N−1)·W` per rank per backend (retention + drain reserve). CRIU images grow by
exactly those buffers. **`W` must also be sized against dump duration**, not only memory: frozen
duration is `O(RSS)` (~0.4 s/GB measured), and because survivors keep sending during a dump,
frozen duration is charged directly against each link's window. This is a feedback loop — larger
`W` means larger RSS means longer dump means larger `W` required. Either bring `--pre-dump` /
`--track-mem` into scope or document that survivors block for the dump window.

Custom channels must advertise sequencing, snapshot, retry, and progress capabilities to be
enabled under FT. No such capability surface exists on `Channel` today; it must be added.

**Plumbing gap, to be decided in Plan C stage C2 before any framing code.** The two config keys
above reach a channel by different routes, and one of them does not reach it at all.
`backends.Direct.framed` arrives for free, because `Configuration::get_active_channels()`
(`src/utils/Configuration.cpp:12-38`) flattens every key under a backend's JSON block into the
`params` map that `Channel::get_channel(name, params, model_params)`
(`src/comm/Channel.cpp:11-30`) hands to the constructor. `fault_tolerance.link` does **not**:
`Communicator::register_channel` (`src/Communicator.cpp:125-130`) sets only `peer_id`, `num_peers`
and `comm_name`, so there is no path from the `fault_tolerance` block to a transport. Either inject
the link parameters into the `params` map or add a setter called from `register_channel` — the
latter is the natural home for the capability query above.

## Staging map

| Stage | Axis | Delivers | Reversible? |
|---|---|---|---|
| **A1** TCPunch hardening | — | Fixes an existing hang and thread/fd leak | Yes |
| **A2** ClientServer job-lifetime keys | A | **Flips the p2p counterexample and the Redis corpus on the current protocol** | Yes |
| **A3** Operation identity seam | A | Divergent-schedule mismatches become loud | Yes |
| **B** Daemon + CRIU lifecycle | B | Freeze/dump/stage/restore harness, validated against a toy process | Yes |
| **C1** Link layer offline | C | `LinkFrame` + `SequencedLink`, property-tested, no sockets | Yes |
| **C2** Progress engine + framed Direct **under epochs** | C | Retention/replay across a killed connection | ⚠️ **Commitment point** |
| **C3** Epochs → incarnations | D | Deletes the cut machinery | No |
| **C4** Integration, `none` contract, fuzz, docs | — | Corpus green, runbooks ported | No |

**A1, A2/A3, and B touch disjoint *production* file sets** (`extern/TCPunch`, `src/comm/`,
`src/ft/experimental/` + `tools/`) and run in parallel. Three qualifications, measured against the
plans as written:

- **A2 additionally touches `include/ft/ControlPlane.h` / `src/ft/ControlPlane.cpp`** — one additive
  defaulted parameter on `clear_job_state`, so a reused communicator name cannot inherit a previous
  job's data-plane messages. B is forbidden that file, so there is no A↔B collision, but C3 must
  carry the change forward rather than rewrite over it.
- **A1 and C1 share `tests/channels.cpp`.** A1 appends two pairing cases; C1's last task routes every
  `get_channel` call in `tests/channels.cpp` and `tests/communicator.cpp` through a transport-variant
  overlay and diffs the case list. **A1 lands first**; C1's baseline is then 17 + 10, not 15 + 10.
- **`CMakeLists.txt` and `tests/CMakeLists.txt` are appended to by all three plans.** Resolve by
  union.

Everything else in the parallel window is genuinely disjoint.

**C2 is the commitment point.** Socket ownership is not divisible — once the engine owns the
fds there is no partial retreat. Everything before C2 is additive and independently valuable
even if the engine is never built. Defer the C2 decision until B has reported what CRIU actually
does on the target kernel.

**C2 deliberately keeps epoch-qualified pairing names.** The epoch acts as a coarse incarnation,
which lets the threading model and retention/replay be proven without simultaneously swapping
the membership authority. C3 is then largely a naming and authority swap.

## Acceptance oracles

The counterexample corpus is the acceptance oracle, but its milestone mapping in the superseded
plan was wrong and is corrected here:

- `tests/migration_p2p_cut_counterexample.cpp:267-268` hardcodes
  `config/fmi_ft_stress_redis_test.json`, which sets `backends.Direct.enabled=false`,
  `backends.Redis.enabled=true`, `preferred_data_backend="Redis"`. Its failure mechanism is the
  **ClientServer** counter reset. It therefore flips at **A2**, not at any Direct milestone, and
  no stage may gate on it before A2.
- Any stage that loudly rejects ClientServer-under-FT would make this binary exit 1 at
  construction. No stage may do so while it remains the headline gate.
- `Classification::LoudFail` must be added: the corpus specifies loud-fail scenarios *and* an
  "aggregate exit 0" gate, but `aggregate_exit` (`tests/migration_counterexample_runner.cpp:1825-1836`)
  passes only `Preserved` and `InfrastructureSkip`. These are mutually exclusive as written.
- `transparent_migration_cut_timing_stress` and `selective_repair_keeps_survivor_links` are
  **rewrites, not controls** — both are built on the epoch/promotion surface being deleted. The
  genuinely untouched controls are the `Channels` and `Communicator` suites, which never
  construct FT.
- `tests/migration_cut_model.cpp` (623 LOC, plus `--explore`) needs an explicit disposition:
  after the fix its explorer can never be green — it either still reports counterexamples
  (exit 2) or reports none found (exit 1).
- The verification recipe must pass `-DFMI_ENABLE_CRIU=ON`; without it neither counterexample
  binary is built.
- **The ClientServer data plane has no `Boost_Tests_run` coverage at all today**, so for stages A2
  and A3 "the suite stays green" is a no-regression check and never evidence that the new code
  works. `tests/channels.cpp:62-66` has `S3` and `Redis` commented out of its `backends` map
  (`:63-64`), leaving the `Channels` suite `Direct`-only, and `config/fmi_test.json` disables Redis
  (`:11`) and enables only `Direct` (`:18`), so the `Communicator` suite is `Direct`-only too. Every
  A2/A3 change must therefore carry its own explicitly Redis-backed case. The counterexample corpus
  is the acceptance oracle precisely because the ordinary suite cannot be one here.

## Decision record

Carried forward from the superseded design, plus corrections from adversarial review.

- **Retain the global epoch and add a link layer underneath** — rejected: builds the hard part
  anyway while keeping the machinery it obsoletes, plus global disruption per migration.
- **Operation-signature validation alone** — rejected as *the* solution (cannot migrate divergent
  p2p); its spirit survives as the envelope identity tuple and the handshake fingerprint.
- **CRIU `--tcp-established`** — rejected: cross-host address preservation is impractical on
  Knative. `--tcp-close` alone was a dead end for the *old* protocol because it loses kernel
  buffers; with sender retention that loss is exactly the unacked set, which is what makes it the
  enabler of immediate dumps.
- **In-process signal handler for immediate checkpoint** — rejected: async-signal-safety forbids
  touching link state, hiredis, or fds from a handler racing the main thread.
- **Dropping `none` mode** — rejected: only serverless-without-CRIU path; relabeled instead.
- **Redis handoff spool as the primary path** — rejected: gating the dump on every peer's seal ack
  reintroduces a global pre-dump barrier, unbounded against a compute-bound peer. Recorded as a
  compatible future extension: with stable message ids, retention can overflow into a Redis spool
  during long restores so senders complete instead of blocking at window-full.
- **App-visible safe points / `migration_point()`** — rejected by the user (transparency requirement).
- **Threadless sliced loops** — rejected in favor of an autonomous progress engine. **Open
  alternative not yet evaluated:** app thread performs the send inline under a per-link mutex
  *after* retaining its copy, with the engine owning only ACK consumption, replay, repair, and
  pairing. This still satisfies the delivery obligation (the retained copy exists before any byte
  hits the socket) while keeping the short path for small messages. Plan C must benchmark this
  against the full-engine design **before** freezing the request interface.

## Open items to resolve before Plan C task 1

1. CREDIT initial value and `retention_limit_bytes` cap behavior (see Axis C above).
2. The quiesce-snapshot schema. `links:<r>` is per-link, but the collective index is
   per-rank/per-channel. Specify it as an opaque versioned per-**channel** blob produced by each
   channel's `quiesce_links()`, and enumerate the required contents.
3. Inline-send versus full-engine, decided on Plan B's measured freeze data and a Plan C
   micro-benchmark, not after the engine is built.
4. Whether `--pre-dump`/`--track-mem` enters scope, decided on Plan B's `O(RSS)` measurements.
