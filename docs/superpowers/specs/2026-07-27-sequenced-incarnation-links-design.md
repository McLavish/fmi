# Sequenced Incarnation Links — Migration Protocol v2 Design

**Status:** design contract. Normative for all downstream implementation plans.
**Supersedes:** `docs/consensus-cut.md` (global epoch cut) once Stage C lands.
**Downstream plans:** `docs/superpowers/plans/2026-07-27-{a-message-identity,b-checkpoint-mechanics,c-sequenced-link-layer}.md`
**Index and running order:** `docs/superpowers/plans/2026-07-27-migration-v2-README.md`
**Machine-checked companion:** `docs/tla/README.md` — three TLA+ modules, 36 TLC 2.19 configurations,
all exhausted. Contracts 1, 2 and 3 each have a module; see [Verification status](#verification-status)
for what is proven, what is only assumed, and the composition gap that none of them closes.

**Citation baseline:** every `file:line` reference in this document and in the three downstream
plans resolves against this branch, `new_migration_protocol`, as of `9c7e533`. The design documents
and the code were briefly on separate branches (`docs/migration-protocol-v2` and
`exp/checkpoint-mechanics`, diverged at `523128a`); they were merged in `c733d02` and the branch
renamed, so citations now resolve natively in the working tree. Plan A has already begun landing
here — `1aa9beb`, `9c7e533`, and `7460d68` in the `extern/TCPunch` submodule. Re-resolve citations
after any further rebase; `tests/channels.cpp` and `src/comm/ClientServer.cpp` are the two files
whose line numbers move most.

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

### `run_id` — the universal name fence

`grep -rn run_id include/ src/ tools/ tests/` returns nothing today. `comm_name` is the sole fence,
and it is reused verbatim across four independent namespaces: the control-plane key prefix
(`src/ft/ControlPlane.cpp:200`), the CRIU image directory
(`src/ft/experimental/LocalRankAgent.cpp:91`), every ClientServer data key
(`src/comm/ClientServer.cpp:14`), and every Direct pairing name (`src/comm/Direct.cpp:29`). Two runs
of the same job name therefore share all four.

> **Normative:** a `run_id` — a fresh random value minted once at job creation — is a mandatory
> prefix of every ClientServer data key, every Direct pairing name (through the hashed rendezvous
> tuple below), every CRIU image and manifest path, and every control-plane CAS. A script whose
> `run_id` argument does not match the directory's rejects rather than mutating.

One indirection is required and is not optional: `fmi:ft:name:<comm_name> → run_id`, written once at
job creation and read by every name-addressed tool. `fmi-rank-agent`, both shell demos and both
Python orchestrators address a job by **name**; without the index, `run_id` is unreachable from the
entire CLI surface that actually drives migrations.

With `run_id` in place, `clear_job_state`'s cross-run collision role demotes from a correctness
precondition to hygiene: a previous run's keys become unreachable rather than merely unlikely to be
hit.

### Envelope fields (Direct frames; the same tuple qualifies ClientServer keys)

Every DATA frame carries:

> **As built, this table diverges from the shipped header — see `include/comm/LinkFrame.h`.**
> `run_id`, `fragment_offset` and `fragment_length` were never implemented. The transport shipped
> a `fragment_index` instead, which is exactly what the normative paragraph below rejects, and a
> `payload_length` alongside `total_length`. Since no fragmentation feature ever landed, all of
> that was dead weight, and `frame_wire_version` 4 removed it: `message_id`, `fragment_index`,
> `total_length` and every padding byte are gone, `payload_length` absorbed the length term of the
> identity, and the header is **42 bytes** rather than the 72 it grew to. The handshake lost
> `policy_fingerprint` and the incarnation pair in the same change and is **30 bytes**. The
> identity tuple this document argues for — lane, op_kind, collective_index, root, reduce_flags,
> length — is intact and unchanged; only the fields nothing read were removed.

| Field | Width | Meaning |
|---|---|---|
| `wire_version` | u16 | Protocol generation. A framed rank must never pair with a raw one. |
| `lane` | u8 | `P2P` or `COLLECTIVE`. Separate FIFO and drain queue per lane. |
| `op_kind` | u8 | `send`/`bcast`/`barrier`/`gather`/`scatter`/`reduce`/`allreduce`/`scan`. |
| `collective_index` | u64 | Per-**Communicator** monotonic collective ordinal. Zero and unused on the `P2P` lane. |
| `root` | u32 | Collective root. On the `P2P` lane it is the **destination rank**, written identically by both ends. |
| `reduce_flags` | u8 | `commutative` and `associative` of the reduction function, one bit each. Zero on non-reducing ops. |
| `message_id` | u64 | Per-lane, per-directed-pair job-lifetime ordinal. Never reset. |
| `fragment_offset` | u64 | Byte offset of this fragment within the logical message. |
| `fragment_length` | u32 | Payload bytes in this fragment. |
| `total_length` | u64 | Logical message length in bytes. |
| `transport_seq` | u64 | Link-scoped, job-lifetime reliability sequence (Axis C only). |

**`root` on the `P2P` lane is the destination rank, not "the peer".** "The peer id" is ambiguous: if
each side writes *the other end*, the sender writes `1` and the receiver expects `0`, and every legal
p2p receive becomes a loud abort. `MessageIdentity.tla` resolves it to the destination rank on both
sides, and `MessageIdentity_FullTuple_aligned` (`NoLoudAbort` holds over all 258 `Compatible` pairs)
depends on that resolution.

**Fragmentation is described by offset and length, never by an index.** With a `fragment_index`,
reassembly offset is `index × max_frame_bytes`, so `max_frame_bytes` becomes a silent part of the
wire contract. A rank restored on a host whose config carries a different `max_frame_bytes`
reassembles every fragment at the wrong offset, and no field in the frame could detect it.
`fragment_offset` + `fragment_length` is self-describing: a mismatch is arithmetic, not
configuration.

**Why `(collective_index, op_kind, root)` and not just a lane ordinal.** The per-lane
`message_id` is a FIFO position, so it cannot detect reordering. Counterexample at `N=2`:

```
rank0: bcast(root=0); barrier()
rank1: barrier();     bcast(root=0)
```

`bcast` at `N=2` sends one frame 0→1 (`src/comm/PeerToPeer.cpp:14-27`, `rounds=1`) carrying
`buf.len` bytes — whatever the application passed. `barrier` is a 1-byte `allreduce` with a
`{commutative=true, associative=true}` nop (`src/comm/PeerToPeer.cpp:29-33`), so it takes
`allreduce_no_order`; at `N=2` rank0 takes the recv-then-send branch and rank1 the send-then-recv
branch (`src/comm/PeerToPeer.cpp:108-118`). On the 0→1 direction rank0 emits `[bcast][barrier]`
while rank1 consumes expecting barrier-then-bcast. Lane and ordinal match on both sides; a 1-byte
`bcast` payload — the case chosen here so `total_length` matches the barrier's nop allreduce too —
makes the match total, and the result is **silent wrong delivery**, the exact failure class this
protocol exists to eliminate. (`total_length` alone discriminates a large `bcast`; it does not
discriminate the collective-versus-collective collisions below, which really are all 1 byte.)

A bare per-communicator collective counter is also insufficient: rank0's `bcast` and rank1's
`barrier` are each that rank's collective #0. Detection requires the receiver to independently
compute an expected value, so `op_kind` and `root` must both be present and validated.

**This is machine-checked, over all divergent program pairs rather than this one.** Across
`MessageIdentity.tla` (programs of length ≤ 2 over a 6-symbol alphabet, ≤ 3 in two configs):
today's unframed `Direct` admits silent substitution (`MessageIdentity_OrdinalOnly`,
`NoCrossLaneSubstitution` violated); lane + per-lane FIFO is not enough
(`MessageIdentity_LaneOnly`, `NoCollectiveCollision` violated); lane + `collective_index` is still
not enough (`MessageIdentity_LaneAndIndex`, `NoEqualIndexCollision` violated, with
`collective_index` equal on both sides in the trace); and the full envelope never rejects a
well-formed pair (`MessageIdentity_FullTuple_aligned`, `NoLoudAbort` holds;
`MessageIdentity_FullTuple_len3`, no error, 34,581 distinct states). `root` is carried per this
contract but its *necessity* is not exhibited at `N=2` and needs `N ≥ 3`.

**`op_kind` and the reduction flags are each independently necessary.** `reduce` and `reduce_nc`
collide with identical `op_kind`, `root`, `collective_index` and length; only `commutative` /
`associative` separate them. Neither field may be dropped as an optimisation, and neither can be
folded into a static job-wide policy digest — see [Policy uniformity](#policy-uniformity).

> **Normative:** no FMI collective places two messages on one directed pair within a single
> logical operation. This is what makes `(collective_index, op_kind, root)` plus per-directed-pair
> FIFO sufficient, and it is why no algorithm-phase or substep field is required in the envelope.
> A future optimized collective that violated it would silently break identity, so any new or
> overridden collective must be checked against this invariant.

**The counter must live on the `Communicator`, not the channel.** With a per-channel counter,
`bcast → Redis; barrier → Direct` yields Direct-collective-count 0 on both ranks in either issue
order and detects nothing.

### Where the identity is produced

`Channel::send(channel_data, peer_num)` has no lane parameter, and the 27 internal call sites in
`src/comm/PeerToPeer.cpp` route collectives through the same virtual an application calls.
The identity is therefore produced **above** the channel interface:

> **Normative:** an RAII lane-and-instance scope, set by `Communicator`'s operation entry points
> (natural home: the existing `OperationGuard`), publishes `(lane, op_kind, collective_index,
> root, reduce_flags)` for the duration of one logical operation. Channels read the active scope.
> The `Channel` virtuals do **not** gain a lane parameter — that would break custom channels
> satisfying the existing interface.

> **Normative:** that same RAII scope is the **single owner** of the per-lane, per-directed-pair
> `message_id` counter, for **both** channel families. ClientServer already maintains exactly this
> counter today, in `num_operations["send"+dest]` / `["recv"+dest]`
> (`src/comm/ClientServer.cpp:7,16,21,30`; declared `include/comm/ClientServer.h:62`), and a framed
> `Direct` would otherwise grow a second, independent copy inside the link layer. Two counters for
> one concept is exactly the divergence this contract exists to prevent: the scope owns it, and both
> families read it.

The counter must **not** be hoisted into a new "logical message" layer sitting above the channels.
The collectives are implemented *inside* the `Channel` subclasses — `PeerToPeer` implements
`bcast`/`barrier`/`gather`/`scatter`/`reduce`/`allreduce`/`scan`
(`src/comm/PeerToPeer.cpp:14,29,35,86,132,186,241`) and `ClientServer` implements its own
(`src/comm/ClientServer.cpp:34,44,106,153`), with `include/comm/Channel.h:56-57` explicitly inviting
subclasses to supply optimized versions and `Communicator::register_channel`
(`include/Communicator.h:166`) as the documented out-of-tree extension point. A layer owning identity
for both families would have to sit *below* those implementations, which means either changing the
pure-virtual signatures or hoisting the collectives out of both subclasses — a strictly larger
breakage than the lane parameter this contract already forbids.

### Rendezvous naming (Direct)

Today the pairing name is built at the **call site** and the two ends do not agree on it.
`Direct::send_object` registers `comm_name + peer_id + "_" + rcpt_id`
(`src/comm/Direct.cpp:29`) while `Direct::recv_object` registers
`comm_name + sender_id + "_" + peer_id` (`src/comm/Direct.cpp:49`), and `check_socket` pairs under
whichever of the two arrives first. A legal divergent program in which A's first touch of B is a
`send` **and** B's first touch of A is also a `send` therefore registers `comm A_B` against
`comm B_A`: the two names never meet, both endpoints block to `max_timeout`, and both throw. Today's
collectives happen to agree on first-touch direction on every pair, which is why this is invisible —
and it becomes reachable under exactly the divergent schedules v2 exists to support.

> **Normative:** the rendezvous name is a fixed-size hash over the canonical tuple
> `(run_id, wire_version, low_rank, low_incarnation, high_rank, high_incarnation)`, rank-ordered so
> both ends compute the identical string regardless of which one registers first. The full tuple is
> carried and authenticated in `HANDSHAKE`; a peer whose tuple does not match the local one is a
> loud abort, not a retry.

This subsumes three separate naming defects at once:

- The direction disagreement above disappears, because the name no longer depends on who calls first.
- The `@epoch=` digit-merge collision disappears. `epoch_comm_name` produces `base + "@epoch=" + N`
  (`include/ft/Common.h:55-57`) and `Direct::send_object` concatenates ids onto it with no separator
  (`src/comm/Direct.cpp:29`), so `X@epoch=1` with `(peer=10, rcpt=1)` and `X@epoch=11` with
  `(peer=0, rcpt=1)` both yield the pairing name `X@epoch=110_1` — two different epochs pairing
  under one name.
- The rendezvous server's `MAX_PAIRING_NAME = 100` truncation
  (`extern/TCPunch/server/hole_punching_server.cpp:24`) stops being a silent correctness cliff for
  long communicator names, because a hash is fixed-width by construction.

> **Normative:** the rendezvous server records **which side** registered under a name. A retrying
> endpoint must never be paired with its own abandoned registration.

### Job-lifetime keys (ClientServer)

Data-plane keys are prefixed by `run_id`, then qualified by the identity tuple and by a job-lifetime
per-directed-pair sequence. They are **never** epoch-qualified and never reset. `comm_name` for data
keys is decoupled from the epoch-qualified name used for Direct pairing; `run_id` fences them against
a previous run of the same job name, which `comm_name` alone does not
(`src/comm/ClientServer.cpp:14`).

- Consumer-delete only in single-consumer namespaces: p2p, the new `gather`/`scatter` namespace
  (`<run_id>:<comm>:gather:<n>:<rank>`), reduce contributions.
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
- Barrier arrival is a **direct probe of the `N` known keys**
  `<run_id>:<comm>:barrier:<n>:<rank>` for `rank ∈ [0, num_peers)` — an `O(N)` `MGET`/`EXISTS` (or
  `HeadObject` fan-out on S3) — never a listing, never a count over a listing. Keys are retained
  until job cleanup; deleting after one rank's pass strands slower ranks. The suffix-scan
  implementation (`src/comm/ClientServer.cpp:44-65`) is deleted.

  The names are fully determined by `(comm, generation, rank)` — that is already true today
  (`src/comm/ClientServer.cpp:46-47`) — so nothing has to be discovered. Probing the known set
  eliminates the whole class rather than patching it. The defect that exists **today** is scope:
  arrival is a `count_if` over a *suffix* match (`_barrier_<n>`, `src/comm/ClientServer.cpp:54-56`)
  applied to an unscoped listing of the entire store — `Redis::get_object_names` issues `KEYS *`
  (`src/comm/Redis.cpp:88`) and `S3::get_object_names` a whole-bucket `ListObjects`
  (`src/comm/S3.cpp:79-93`) — so a foreign communicator's or a dead epoch's key counts as an
  arrival. The defect that would appear if the listing were merely *prefix-scoped* is duplication:
  scoping forces `KEYS` to become `SCAN`, and `SCAN` may return the same key more than once, so the
  count would have to become a deduplicated rank set. The direct probe removes both, and removes the
  S3 `ListObjects` pagination requirement with them.

  **Rejected: a two-phase arrived/departed barrier.** The departure phase is a second rendezvous over
  the same medium with the same termination requirement, so it merely moves the hazard: rank 2 dumped
  mid-barrier lets rank 0 observe all departures and reclaim, while rank 1 polls late and blocks to
  `max_timeout`. It also buys no correctness that is missing — the keys are already
  generation-qualified (`_barrier_<n>`, `src/comm/ClientServer.cpp:46-47`), so a stale generation's
  keys can never be miscounted as this generation's arrivals.

### Policy uniformity

The following must be identical across ranks or the job is ill-formed. Mismatches select
different algorithms and therefore different message patterns, producing hangs the identity
check would otherwise report as corruption:

- `run_id`, `faas_price`, model parameter hash, `num_peers`, `wire_version`, and the **effective**
  policy fingerprint — job-wide, validated once (handshake fingerprint under Axis C; startup
  assertion before that).
- **The reduction function's `commutative` and `associative` flags** — per operation, not
  job-wide. `left_to_right = !(f.commutative && f.associative)` selects between wholly different
  algorithms at `include/Communicator.h:103` and `:128` (`reduce_ltr`/gather vs binomial
  `reduce_no_order`; `scan_ltr` vs `scan_no_order`; ltr allreduce vs recursive doubling).
  These flags therefore belong in the **envelope**, alongside `op_kind`, and are validated per
  collective — not in a static job-wide fingerprint. A `policy_digest` **cannot** carry them,
  because `left_to_right` is computed per call from the caller's `Function<T>`, not from any
  configured policy. `MessageIdentity.tla` confirms they are independently necessary: `reduce` and
  `reduce_nc` collide on every other field.

**The policy fingerprint is frozen at the first guarded operation, not at construction.** `hint()`
and `set_channel_policy()` are public runtime mutators (`src/Communicator.cpp:145-152`), so a digest
taken during construction records the *default* hint and never the effective one.

> **Normative:** the policy fingerprint — model parameter hash plus the **effective** `hint` and
> `preferred_data_backend` — is computed lazily inside the first `OperationGuard`, published to the
> **control plane** (not to a ClientServer data key), and compared there. `hint()` and
> `set_channel_policy()` throw once the fingerprint is frozen.

Rejecting mutation "after the rank has joined" is the right hazard named at the wrong point: the rank
joins inside the `Communicator` constructor (`src/Communicator.cpp:64`), and every FT application in
this repo calls `hint()` *after* construction —
`runbooks/localstack-python311-redis/worker_core.py:38`,
`runbooks/local-criu-state-transfer/transparent_state_transfer_demo.cpp:42`,
`runbooks/aws-python311-s3/function/lambda_function.py:16`. A join-time freeze would reject all of
them. First-guarded-operation is the earliest point that is both after configuration and before any
message.

**`Communicator::scan` is flag-blind.** `include/Communicator.h:153` calls
`policy->get_channel({Utils::scan, sendbuf.size_in_bytes()})` with no `left_to_right` argument, unlike
`reduce` (`:103`) and `allreduce` (`:128`). Consequence: a `commutative`/`associative` divergence on
`scan` produces an *algorithm* mismatch — caught by the envelope's `reduce_flags` — but can never
split the two ranks onto different **backends**, because the backend choice does not see the flag.
Whether to make `scan` consistent with `reduce`/`allreduce` is an [open item](#open-items-to-resolve-before-plan-c-task-1); this
contract does not depend on the answer either way, but the asymmetry must not be discovered by
accident later.

## Normative contract 2 — transport durability (Axis C)

### Two watermarks

Per receiver, per directed pair:

- `next_received` — highest contiguous `transport_seq` accepted. Drives dedup and replay.
- `ack_safe` — highest seq the sender may **prune**. CRIU mode: received-and-durably-held, so
  it equals `next_received` (drain buffers are inside the image). `none` mode: application-consumed
  only.

The handshake carries both, per direction, plus `lowest_retained` and the peer's **available receive
credit in bytes**. A peer requesting frames older than `lowest_retained`, or claiming
`next_received > next_send`, is an impossible state → loud abort.

> **Normative:** on handshake the sender **adopts** the peer's advertised credit outright. It never
> carries a decremented credit counter across the break. A decremented counter is a guess about how
> much of what was in flight the peer actually admitted, and after `--tcp-close` the sender cannot
> know. The receiver knows exactly, so the receiver is authoritative.
>
> Adoption is safe because of one invariant the receiver must maintain when it computes the value it
> advertises: `replayable ≤ limit − buffered`, where `buffered` is what the drain queues already
> hold and `replayable` is the set the sender is about to resend. A receiver that advertises credit
> without subtracting what it is still holding can be overrun by its own replay.

### Durable versus connection-scoped state

> **Normative:** **link state** — both watermarks, `lowest_retained`, the retention set, the drain
> queues, and the credit accounting — is *durable*. It belongs to the link, survives the freeze, is
> captured in the CRIU image, and is reconciled by the handshake.
>
> **Connection state** — the partial-frame parse buffer, the partial-frame egress cursor, and the
> socket fd itself — is *connection-scoped*. It MUST be discarded on connection death and on
> restore, and MUST NOT be relied on after either. A frame that was half-parsed or half-written when
> the connection died is not a frame; its bytes are unacked by construction and come back through
> replay.

This split is what makes the freeze analysis tractable: everything volatile is precisely the
connection-scoped set, and everything connection-scoped is reconstructible from the durable set.

### Ordering invariants

1. An immutable retained copy exists before any DATA byte reaches the socket.
2. A complete frame is committed to the drain queue before its ACK becomes writable.
3. Retention is reclaimed only on a validated cumulative ACK.

These three are what make an arbitrary-instant freeze sound: because an ACK is only issued for a
durably-held frame, **everything in a kernel socket buffer is unacked by construction** and will
be replayed from peer retention after `criu dump --tcp-close`.

Six freeze positions must be shown loss-free and duplication-free, not four. The two extra ones fall
out of the durable/connection-scoped split above and were previously folded silently into their
neighbours:

| # | Freeze lands… | Volatile at that instant | Recovered by |
|---|---|---|---|
| 1 | after retention, before any byte on the wire | nothing on the wire | replay from retention |
| 2 | **mid-frame on egress** — a DATA frame partly written | the egress cursor and the written prefix | cursor discarded; whole frame replayed |
| 3 | frame on the wire, not yet parsed | the kernel buffer | replay from retention |
| 4 | **mid-frame on ingress** — a DATA frame partly parsed | the parse buffer | buffer discarded; whole frame replayed |
| 5 | parsed and committed to the drain queue, ACK not yet written | nothing (drain queue is durable) | `next_received` in the handshake suppresses re-delivery |
| 6 | ACK on the wire | the kernel buffer | `ack_safe` in the handshake re-establishes the prune point |

Positions 2 and 4 are sound **only** because the partial buffers are connection-scoped and are
thrown away rather than resumed; a design that tried to resume a half-written frame after restore
would reorder its bytes against the replayed copy.

`SequencedLink.tla` enables `Freeze` at *every* reachable non-aborted state and finds no loss,
duplication, reordering or missed delivery obligation: `SequencedLink_correct` (2,932 distinct
states) and `SequencedLink_correct_large` (96,431 generated, 30,567 distinct, `Reserve > W`,
`MaxMsg=4`, `W=3`). Its own four freeze-position probes —
`Probe_FreezePos1_FrameOnTheWire`, `Probe_FreezePos2_ParsedNotCommitted`,
`Probe_FreezePos3_CommittedNotAcked`, `Probe_FreezePos4_AckOnTheWire` — are invariants asserted in
order to be *refuted*, and all four are refuted, so each of those instants is actually reached and
the clean results are not vacuous. Three further probes
(`Probe_CompletesAcrossEveryFreeze`, `Probe_WindowIsExercised`, `Probe_FreezeMidStream`) are likewise
refuted.

Those four model positions all fall inside rows 3–6 of the table above. Rows **2 and 4 — the
partial-frame ones — are not checked at all**: the model treats frames as atomic on the wire and
stages at most one frame in the parser, which understates the volatile set. Both rows, and the
egress-serialisation rule that a partially written DATA frame completes before any ACK begins, remain
unchecked implementation obligations.

**Receiver-side dedup is dead code.** `DedupUnreachable` holds in every `SequencedLink`
configuration: because the handshake replays exactly `[next_received, next_send)`, no duplicate ever
reaches the receiver on a single directed link. The dedup branch must still exist as a loud
assertion, but it is defensive, cannot be reached by driving the real handshake, and no test may
claim to cover it by doing so. (Proven for one direction with one restore at a time; a
double-restore race is not modelled.)

### ACK versus CREDIT

ACK releases sender retention. CREDIT (byte-based) admits payload into the receiver's drain
buffers and is returned only as the application consumes. Without the split, ack-at-drain lets a
divergent peer grow an unbounded receiver backlog. Control frames are credit-exempt. Egress is
serialized per socket: a partially written DATA frame completes before any ACK or control frame
begins.

**Drain reserve per source ≥ the peer's window `W`**, otherwise ACKs queue behind undrainable
data on the shared FIFO (ack-behind-data deadlock).

That constraint is a **config constraint only, and it is not sufficient.** An implementer can satisfy
`drain_reserve_frames >= window_frames` at parse time and still write a serializer that blocks. The
structural rule is therefore separate and normative:

> **Normative:** every engine-owned socket is `O_NONBLOCK`, with `SO_SNDTIMEO`/`SO_RCVTIMEO`
> **removed** rather than tuned, and the engine never blocks inside `write`. A short write parks the
> egress cursor and returns to the poll loop; it never waits for the peer to drain.

`poll()` returns `EINTR` across a freeze/restore boundary and must be retried, not treated as an
error — the same rule as [EINTR](#process-level-restore-hygiene) below, but on the engine's own loop
rather than on `Direct`'s blocking calls.

`SequencedLink.tla` **does not verify** the drain-reserve constraint:
`SequencedLink_reserve_below_window` sets `Reserve = 1, W = 3` — a config this contract's parse-time
validation rejects — and passes clean. That is not a counterexample; it is evidence that the
ack-behind-data hazard is genuinely *bidirectional* and cannot be exhibited without two DATA
directions sharing one egress FIFO, which the single-direction model does not have. The constraint
remains an unchecked implementation obligation.

**Retention cap behaviour is now decided: the cap is an admission threshold, never an eviction
threshold.** Reaching `retention_limit_bytes` blocks admission of *new* sends; nothing already
retained is ever discarded, because discarding it would break the delivery obligation directly.
`SequencedLink`'s results are only valid under this reading — the model collapses bytes into frames
and `retention_limit_bytes` into `W`, which is sound *only* for an admission threshold — so this is
the reading the machine-checked result actually supports.

**Still open (must be pinned before Plan C task 1):** CREDIT's initial value relative to `W` and the
drain reserve. Unspecified today, and a value that is too small silently converts a legal program
into a window-full stall.

**Known regression risk:** bounding buffering at `W` where TCP socket buffers are effectively
unbounded today means programs that work now can deadlock under v2. Plan C must include a
backpressure regression test of the shape the existing `tests/channels.cpp` matrix exercises — but
it must be a **new, fork-based** case, not a modification of one of the existing ones.
`sending_receiving` and `sending_receiving_mult_times` (`tests/channels.cpp:76-134`) run their two
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

This rule is load-bearing for the machine-checked delivery result, not decoration.
`SequencedLink`'s `DeliveryObligation` holds only under `WF_vars(Restore)` and `WF_vars(Handshake)`
— i.e. under the assumption that a survivor *learns* its peer moved. Socket-driven discovery does not
satisfy that assumption in the `--tcp-close --leave-stopped` case, so the liveness result would fail.

## Normative contract 3 — membership state machine (Axis D)

Directory prefix `fmi:ft:<run_id>:<comm>:`, reached from a name by
`fmi:ft:name:<comm_name> → run_id`. Every mutation is CAS-guarded on
`(run_id, rank, expected incarnation, expected state)` and carries a caller token.

### `dirgen` is a topology generation

> **Normative:** `dirgen` is bumped **only** by a script that changes the *topology* — a rank's
> incarnation, state, or placement. Heartbeats, lease renewals and any other liveness bookkeeping do
> **not** bump it.

This is not a refinement; the alternative is broken. Heartbeats are mutating scripts: the rank
registration path `HSET`s `last_heartbeat_ms` on every write (`src/ft/ControlPlane.cpp:720`). Were
`dirgen` bumped by every mutating script, each rank's each poll would invalidate every peer's cached
directory, and the repair rule ("on observing a peer's incarnation change… enter repair") would fire
continuously against ordinary liveness churn.

> **Normative:** one Lua execution returns `dirgen` **and** every rank record the caller needs. A
> caller must never read `dirgen` and the records in two round trips — that pair is not atomic and
> can observe a topology that never existed.

### Idempotent operation tokens

Every mutating script carries an operation token, and the directory records the token together with
the result it produced.

> **Normative:** a retry presenting a token already recorded returns the **original recorded result**
> and performs no mutation. It must never be re-evaluated as a fresh CAS.

This is required, not defensive. `ControlPlane::command` treats a `nullptr` reply as a transport
fault: it reconnects and re-issues the *identical* command unconditionally, including for mutating
`EVAL`s (`src/ft/ControlPlane.cpp:141-148`). A lost reply to a `commit_restore` that in fact
succeeded therefore replays it: the retry now sees incarnation `i+1` against expected `i`, reports a
CAS failure, and under the [restore commit discipline](#restore-commit-discipline) the agent
`SIGKILL`s the process that legitimately owns `i+1`. Tokens convert that from a lost job into a
no-op.

### Concurrent per-rank migrations

> **Normative:** migration is **per rank**. A `request_pause` naming a rank that is not `ACTIVE`
> returns `Busy` for that rank. It is never silently merged into an earlier request, and no
> completion of an earlier request may clear a later one's state.

Today's control plane does the opposite in both directions, and both directions are load-bearing
because node evacuation batches four ranks at once:

- `request_migrations` **rejects the whole call** if the pending set holds any rank outside the
  requested set (`src/ft/ControlPlane.cpp:346-353`), and is deliberately idempotent for a
  re-requested subset (`:339-341`) — i.e. it silently merges a second request into the first.
- `promote_epoch` `DEL`s the *entire* pending set (`src/ft/ControlPlane.cpp:678`), so one migration
  completing erases the marks of any other.

Both behaviours are correct for a single global cut and wrong for per-rank incarnations. `Busy` plus
per-rank state is the replacement; neither the merge nor the global clear survives C3.

### States and transitions

> **Normative rule: every reachable strongly-connected component of the state graph must have at
> least one outbound edge that an external actor can drive.** The weaker per-state form of this rule
> is insufficient and was shown so by model checking: a two-state cycle in which each state has an
> outbound edge *back into the cycle* satisfies "every state has an escape" while the job never
> escapes. The failure edges below are not optional; without them a failed `criu dump` — which
> today's code treats as routine (three attempts then rethrow,
> `src/ft/experimental/LocalRankAgent.cpp:420-437`) — wedges the job.

The rule is machine-checked in both of its forms. Removing the abort edges wedges the job at
`PAUSING ∧ dump_failed` (`MembershipNoAbort`, `NoDeadEnd` violated at depth 9) — that is the
per-state form, and it is real. Keeping every abort edge but restricting `abort_pause` from
`PAUSING` to the agent alone *still* wedges the job (`MembershipTableLiteral`, below). The
**strengthening to components** comes from a third result: `CHECKPOINTED ⇄ RESTORE_RESERVED` is a
cycle in which *every state has an outbound edge* and the job still never escapes
(`Membership.cfg`, temporal properties violated). A per-state rule accepts that cycle; the
component form rejects it.

| From | To | Script | Driven by |
|---|---|---|---|
| — | `STARTING` | `join` (create-at-incarnation-0 only if absent) | rank |
| `STARTING` | `ACTIVE` | `mark_active` | rank, first poll |
| `ACTIVE` | `PAUSING` | `request_pause(rank, migration_id)` — `Busy` if not `ACTIVE` | orchestrator |
| `PAUSING` | `CHECKPOINTED` | `mark_checkpointed` (only after image durably staged **and** digest-verified) | agent |
| `PAUSING` | `ACTIVE` | **`abort_pause`** — CAS incarnation+1 | agent on dump failure, **or orchestrator on agent-liveness loss** |
| `CHECKPOINTED` | `RESTORE_RESERVED` | `acquire_restore_lease(rank, attempt_id)` (SET-NX-EX) | agent |
| `CHECKPOINTED` | `ACTIVE` | **`abort_pause`** — CAS incarnation+1 | orchestrator, migration cancelled |
| `CHECKPOINTED` | `LOST` | **`declare_lost(rank, incarnation)`** — attempt budget exhausted | orchestrator |
| `RESTORE_RESERVED` | `ACTIVATING` | `commit_restore(rank, expected_inc, attempt_id)` — CAS incarnation+1 | restoring agent |
| `RESTORE_RESERVED` | `CHECKPOINTED` | **`abort_restore`**; also realised **lazily** by the next `acquire_restore_lease` over an expired lease | agent |
| `RESTORE_RESERVED` | `LOST` | **`declare_lost(rank, incarnation)`** — attempt budget exhausted | orchestrator |
| `ACTIVATING` | `ACTIVE` | `mark_active` — restored process's first poll; **clears the pause entry** | rank |
| `ACTIVATING` | `CHECKPOINTED` | **`abort_activation`** (fenced; agent died before `SIGCONT`) | orchestrator |

`abort_pause` is idempotent by `migration_id`, on **both** rows, and clears both the pause-set entry
and any lease. `acquire_restore_lease`'s CAS must accept `RESTORE_RESERVED`-with-expired-lease as
well as `CHECKPOINTED`.

**Lease expiry is not itself a transition.** A Redis key TTL cannot mutate the `state` field; only a
script run by an actor can. The `RESTORE_RESERVED → CHECKPOINTED` move is therefore realised lazily,
by the next `acquire_restore_lease` accepting an expired lease. An implementer who reads the table as
TTL-driven will either build a sweeper nobody specified, or build nothing and turn
`RESTORE_RESERVED` into a genuine dead end.

#### `abort_pause` must be orchestrator-drivable from `PAUSING`

Contract 4 specifies one sidecar per pod. If that single agent dies *after* freezing its target and
*before* completing or failing the dump, then with `abort_pause` available only to the agent — whose
own precondition is a *failed* dump that never occurred — no actor has any edge out of `PAUSING`. The
rank stays frozen with its pause entry set, which by the [deadline suspension](#deadline-suspension)
rule below suspends deadlines job-wide.

`MembershipTableLiteral.cfg` is exactly this table with the orchestrator's `abort_pause` withheld
from `PAUSING` and a single agent, and it violates `NoDeadEnd` at depth 15 (354 states generated, 179
distinct), with `dumpfail = FALSE` in the wedged state — strictly worse than the
`MembershipNoAbort` wedge, because there even the agent's edge had a live precondition.

Recovery on this edge still needs someone to `SIGCONT` the frozen original. On the cross-host path
that is a *different node's* replacement agent, so this edge carries a **resume responsibility** that
must be assigned explicitly in Plan B.

#### `abort_pause` must bump the incarnation

> **Normative:** `abort_pause` increments the incarnation, on both rows, exactly like
> `commit_restore`. The rank resumes as incarnation `i+1`, not as `i`.

Without the bump, teardown is asymmetric and both ends hang. The survivor observed the target's state
leave `ACTIVE` and, per the [directory-driven repair](#repair-is-directory-driven) rule, closed its
link. The frozen target observed nothing: it returns at incarnation `i`, believes its `ESTABLISHED`
socket is healthy, never re-registers with the rendezvous server, and waits forever — against a
survivor that is waiting for a pairing that will never be requested. Bumping the incarnation makes
the abort visible on exactly the same channel every other membership change uses.

#### `LOST` — the terminal state

`LOST(rank, incarnation)` is terminal. It exists because the restore phase is otherwise a cycle, not
a chain: `CHECKPOINTED ⇄ RESTORE_RESERVED` is reachable indefinitely through lease expiry or
`abort_restore`, with the pause entry set throughout. Both those states individually have outbound
edges, which is why the per-state form of the normative rule above does not catch it and the
SCC form does.

> **Normative:** entering `LOST` **clears the rank's pause entry and its lease**, and releases the
> job-wide deadline suspension. A terminal state that kept the pause entry set would preserve exactly
> the wedge it exists to break. `LOST(rank, incarnation)` is final for that incarnation: the rank is
> never re-restored under it, its image blob is reclaimed, and its peers' links to it stay `DEAD`.
> Whether the job as a whole then fails or continues degraded is the orchestrator's decision, not
> the directory's.

`Membership.cfg` — the configuration expected to be clean — passes every invariant and the deadlock
check and then **violates its temporal properties** (9,869 states generated, 2,919 distinct, depth
25), on two distinct lassos: `AcquireRestoreLease ↔ LeaseExpirySpurious` and
`AcquireRestoreLease ↔ AbortRestore`. Both cycle with `paused = TRUE`, so the job-wide deadline
suspension never releases. The documented consequence of suspension ("genuine peer death hangs rather
than timing out") was scoped to a *dead* peer; this is a *live* rank whose restore never converges,
which nothing in the contract obliged anyone to detect.

`MembershipLivenessSF.cfg` isolates the cause to a single missing sentence. It is `Membership.cfg`
with **nothing removed** — TLC confirms the identical 9,869 / 2,919 / depth-25 state space — and only
strong instead of weak fairness on `RestoreProcess`/`CommitRestore`; liveness then holds. The entire
gap is the unstated assumption *"a restore that can proceed eventually does"*, i.e. that the lease
outlives the `criu restore`.

> **Normative:** (a) the restore lease TTL must provably exceed worst-case `criu restore` duration,
> **or** the restoring agent renews its lease for the duration of the restore. Contract 4 measures
> frozen duration as `O(RSS)` (~0.4 s/GB), so this is a computable requirement, not a hand-wave.
> (b) The restore phase carries a bounded attempt budget `k`. After `k` failed attempts the
> orchestrator drives either `abort_pause` back to `ACTIVE` at incarnation `i+1` (releasing the pause
> entry, and hence the job-wide suspension) or `declare_lost` if the rank cannot be resumed at all.
> Neither `CHECKPOINTED` nor `RESTORE_RESERVED` may be re-entered without consuming budget.

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

**Lease exclusion is not the arbitrator; the `commit_restore` CAS is.** `LeaseExclusion` holds in
`Membership.tla` but is near-vacuous — Redis `SET NX` makes it true by construction. Its useful
content is negative: it is *not* sufficient for twin safety, because a straggler keeps its restored
process after its lease expires. Only the CAS decides.

**The `SIGCONT` fence is not a belt-and-braces measure.** Remove it and `MembershipUnfencedSigCont`
produces a twin — `AtMostOneRunning` violated at depth 16 — with **`crashed = {}`**. No crash is
required; a merely slow-but-alive agent suffices. With the fence present, `AtMostOneRunning` holds
across 2,919 reachable states, and across 305,576 in `MembershipSafetyLarge` (2 migratable ranks,
2 migrations) and 26,868 in `MembershipSafety3Agents` (3 agents contending).

### Deadline suspension

> **Normative:** deadline suspension is **per link**, keyed on link state. A link in
> `DEAD`/`REPAIRING` suspends its own clock until the handshake completes and replay drains.

A global `migrations_in_flight` flag derived from the pause set releases suspension at
`commit_restore` — strictly *before* any peer is permitted to re-pair, handshake, or replay. A
survivor mid-operation then throws `Utils::Timeout`, and since an exception unwinding a sliced
operation poisons the communicator, a textbook-clean migration would intermittently poison the
job. The pause entry is therefore cleared at `ACTIVATING → ACTIVE`, not at `commit_restore`.

The reachability half of that argument is machine-checked: `MembershipClearAtCommit` violates
`NoPoison` at depth 13, i.e. clearing at `commit_restore` makes the `ACTIVATING ∧ ¬paused` window
genuinely reachable, whereas under this contract's ordering it is unreachable. The *consequence* of
that window — that a survivor inside it actually throws `Utils::Timeout` — is this contract's own
argument encoded, not checked: `Membership.tla` has no links, no deadlines and no operations, so
`SurvivorTimeout` is an assumed action.

ClientServer's four poll loops also expire at `max_timeout` with no suspension hook; Plan A
records the hook, Plan C wires it.

Documented consequence: genuine peer death hangs rather than timing out. The orchestrator owns
failure detection. This is in scope for planned migration only.

## Normative contract 4 — checkpoint mechanics (Axis B)

- The agent subscribes to a control-plane pub/sub channel for pause requests, with a tight-interval
  poll of the pause set as fallback. The agent is never checkpointed and may hold sockets freely.
  Its deployment shape is pinned under [Deployment](#deployment) below — "resident per node" and
  "one per pod" are different shapes with different failure modes, and contract 3's `PAUSING`
  dead-end analysis assumes the latter.
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

  Model checking confirms and strengthens this scoping rather than merely agreeing with it:
  `MembershipSameHost` violates `ActiveHasProcess` at depth 9 — on the same-host shape the directory
  advertises a rank as `ACTIVE` with **no process in existence** after an abort, because the process
  the abort would have resumed was killed and reaped by the dump. Cross-host-only is therefore a
  safety requirement, not a convenience.

### Process-level restore hygiene

Three items that are unreachable today (a quiesced rank holds no sockets) but become the normal
case once a rank is frozen mid-syscall:

- **SIGPIPE.** The engine's dedicated hiredis connection writes to a dead socket on first
  post-restore use. The repo deliberately avoids a process-global `SIG_IGN`, so the fix is
  `pthread_sigmask(SIG_BLOCK, {SIGPIPE})` on the engine thread.
- **EINTR.** Freeze delivers EINTR to blocking socket calls. `Direct::send_object` and
  `recv_object` currently treat it as fatal (`src/comm/Direct.cpp:36-43`, `:62-69`).
- **`PR_SET_PTRACER` does not survive restore — a forward obligation, not a present bug.** Today's
  call site is inside the quiesce path, at `src/ft/TransparentMigrationRuntime.cpp:155` in
  `checkpoint_and_wait_for_restore()`, so the current code re-arms it on **every** migration and is
  correct as written. v2 deletes that quiesce point: nothing on the new path re-arms `PR_SET_PTRACER`
  after a restore, so a rank would be migratable once and never again on the rootless path. Plan B
  must place a re-arm on the post-restore path. The gap is currently masked anyway, because
  `/usr/local/sbin/criu` carries `cap_sys_ptrace=eip`, which is exactly why it would not be caught by
  running the demos.

`--tcp-close` transparency covers FMI-managed connections only. An application holding its own
sockets needs its own reconnect story; the agent preflights `/proc/<pid>/fd` and warns or rejects
per policy.

### Deployment

A DaemonSet cannot dump a rank in another pod (different PID and mount namespaces; criu reopens
the executable by path) and cannot perform the restore leg on Knative at all. The property
actually required — never checkpointed, free to hold sockets — is satisfied by a **per-pod
sidecar**, which also keeps the Knative restore inside the pod where it already works.

**Neither shipped runbook does this today.** `grep -rn -iE "daemonset|sidecar|shareProcessNamespace|hostPID" runbooks/`
returns nothing: both `runbooks/localstack-python311-redis/knative-migration/` and
`runbooks/k8s-criu-node-evacuation/` run the agent **inside the ranks' own container**, which is the
only shape that has actually been cluster-verified. The contract must not silently normalise an
unverified shape, so both are pinned:

> **Normative:** the **in-container agent** is the verified baseline shape and remains supported. The
> per-pod **sidecar** is permitted only where all three of its prerequisites are met, and a
> deployment that claims the sidecar shape without them is ill-formed:
>
> 1. `shareProcessNamespace: true` on the pod, so the sidecar can see and `ptrace`-seize the rank.
> 2. An **identical mount layout** in both containers for every path criu reopens — the executable,
>    every mapped library, the images directory, and any file the rank holds open. criu reopens by
>    path; a differing layout fails at restore, not at dump.
> 3. The additional Knative feature gate required to set `shareProcessNamespace` on a Knative
>    Service, on top of the gates the evacuation runbook already enables.
>
> The in-container agent has one property the sidecar does not: it dies with its rank. Contract 3's
> `PAUSING` dead-end (`MembershipTableLiteral`) is the sidecar's failure mode specifically, which is
> why the orchestrator-driven `abort_pause` edge is mandatory before the sidecar shape is used.

Which shape becomes the default for the v2 runbooks is an [open item](#open-items-to-resolve-before-plan-c-task-1) and must be
settled by Plan B on measured behaviour, not by preference.

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
  retention_limit_bytes,  # admission threshold, never eviction
  slice_ms,
  drain_reserve_frames    # must be >= window_frames
}
backends.Direct.framed    # Plan C rollout gate; removed at Plan C completion
```

`drain_reserve_frames >= window_frames` is validated at parse, but parse-time validation is
**necessary and not sufficient** — see [ACK versus CREDIT](#ack-versus-credit) for the non-blocking
egress rule that must hold alongside it, and for why `SequencedLink.tla` cannot check this constraint
at all.

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
| **A1** TCPunch hardening | — | Fixed an existing hang and thread/fd leak — **landed** | Yes |
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
- **A1 has landed and is no longer a scheduling constraint.** `extern/TCPunch` commits `b938efd`
  ("fix stack-use-after-return and thread/fd leak in `pair()`"), `3a6e986` ("decide the pairing
  before stopping the listener") and `7460d68` ("server: sweep dead and idle pairing registrations")
  replaced the two file-scope atomics with a per-pairing `ListenContext` struct — see the
  explanatory comment at `extern/TCPunch/client/tcpunch.cpp:22-34`. Pairing state is already
  per-call; there is no process-global pairing state left to remove and no helper-listener rewrite
  outstanding. A1 also appended two pairing cases to `tests/channels.cpp`, so C1's baseline is
  17 + 10, not 15 + 10. What remains under the A1 heading is **only** the hashed rendezvous key of
  [Rendezvous naming](#rendezvous-naming-direct) — a naming change on both client and server, not a
  concurrency fix.
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
- The verification recipe had to pass `-DFMI_ENABLE_CRIU=ON`; without it neither counterexample
  binary was built. That option and both binaries were removed with the epoch migration
  protocol, so this recipe is a historical record rather than a runnable one.
- **ClientServer collective coverage is thin, but it is not absent.** The backends *matrix* is
  `Direct`-only: `tests/channels.cpp:68-71` has `S3` and `Redis` commented out of its `backends` map
  (`:69-70`), and `config/fmi_test.json` disables Redis (`:11`) and enables only `Direct` (`:18`), so
  the `Communicator` suite is `Direct`-only too. But one case does construct a real Redis channel and
  exercise a ClientServer collective directly: `scan_ltr_client_server_ordering`
  (`tests/channels.cpp:812`, guarded `#if FMI_ENABLE_REDIS`), which is what caught the ClientServer
  scan fold-order bug fixed in commit `523128a`. So `scan` is the **sole covered ClientServer path**;
  `send`/`recv`/`bcast`/`barrier`/`reduce` over ClientServer are uncovered. For A2 and A3 "the suite
  stays green" therefore remains a no-regression check rather than evidence the new code works, and
  every A2/A3 change must still carry its own explicitly Redis-backed case — but the correct model to
  copy already exists in-tree.
- Model checking does not fill that hole either: **`ClientServer` is not modelled at all**. Every A2
  claim — job-lifetime keys, consumer-delete namespaces, `GET`/commit/`DEL` never `GETDEL`, the
  barrier direct-probe fix, `KEYS *` scoping — is unverified, and `MessageIdentity.tla` models a FIFO
  stream, not a keyed store. This matters precisely because the headline acceptance oracle fails
  through the ClientServer path.

## Verification status

> **What is built, and the rules the built thing must obey, are in
> [`2026-07-30-sequenced-links-implementation.md`](2026-07-30-sequenced-links-implementation.md).**
> Contracts 1 and 2 are implemented on the *blocking* transport rather than on the progress
> engine this document specifies; contract 3 has its incarnation and fencing rules and none of
> its state machine; contract 4 is exercised against real criu by
> `runbooks/criu-transparent-checkpoint`. Four rules the implementation must hold that this
> document does not state — because it assumed an event loop — are normative there.
>
> One of them was found exactly where this section says the model does not look. "Freeze
> positions 2 and 4 (partial egress / partial parse) are abstracted away" below is not a
> footnote: the transport committed its receive watermark at header-parse time, so a freeze in
> that window let the peer prune a message it had never delivered, silently. The model could
> not have caught it, and did not.

Three TLA+ modules, **36 TLC 2.19 configurations, all exhausted**, live in `docs/tla/`;
`docs/tla/README.md` is the results document and is authoritative over any summary here. Runs are
sequential and take ≈ 2 minutes wall clock on 16 workers.

| Contract | Module | Status |
|---|---|---|
| 1 — message identity | `MessageIdentity.tla` | Machine-checked over all divergent program pairs of length ≤ 2 (≤ 3 in two configs). Necessity of `lane`, `collective_index`, `op_kind` and the reduction flags each exhibited; `NoFalseAbort` holds over all 258 `Compatible` pairs. |
| 2 — transport durability | `SequencedLink.tla` | Machine-checked on one directed link with `Freeze` enabled at every reachable non-aborted state, up to 2 freezes: no loss, duplication or reordering, delivery obligation met (`SequencedLink_correct_large`: 30,567 distinct states). Six injected defects each caught; seven non-vacuity probes all refuted. |
| 3 — membership | `Membership.tla` | Safety machine-checked across 2,919 states (305,576 safety-only). Liveness **fails as originally written** — that failure is what produced the `LOST` state, the SCC-form normative rule, and the lease/attempt-budget rule above. |
| 4 — checkpoint mechanics | — | Not modelled as such. Only the process-layer facts contract 3's restore discipline depends on (`SIGCONT` fence, same-host abort, per-pod sidecar) appear, inside `Membership.tla`. Measured instead, on criu 4.2: [mechanics](2026-07-30-criu-mechanics-findings.md), and end to end against an unmodified FMI program in `runbooks/criu-transparent-checkpoint`. |

Every result above must be read as *"no counterexample exists at these bounds"*. The largest
configurations are 2 ranks / 3 agents / 2 migrations; 4 messages with a 3-frame window and 2 freezes;
programs of length 3 over a 4-symbol alphabet.

### What is explicitly NOT proven

- **The composition gap — the central claim is unchecked.** No module states anything about a rank
  migrating *in the middle of* a divergent schedule while its peer replays frames that are then
  matched against the restored process's expectations. Each module removes exactly what the others
  contain: `MessageIdentity` has divergent schedules and no migration, `SequencedLink` has one
  directed stream and no envelope, `Membership` has no messages at all. Their conjunction does not
  imply the claim. A fourth module — two ranks, divergent schedules, two sequenced links, one
  incarnation change — is the highest-value next piece of verification work.
- `ClientServer` is entirely unmodelled, which is where the headline acceptance oracle actually
  fails.
- `run_id` and `dirgen` are absent from `Membership.tla` (single run assumed), so the fence in
  [`run_id`](#run_id--the-universal-name-fence) and the topology-generation rule are unverified.
- `drain_reserve_frames >= window_frames` cannot be checked by a single-direction model; it needs a
  bidirectional one.
- `root` necessity needs `N ≥ 3` and is not exhibited at `N = 2`.
- Freeze positions 2 and 4 (partial egress / partial parse) are abstracted away: frames are atomic on
  the wire and the parser stages at most one frame.
- `none`-mode counter seeding, `wire_version`, fragmentation and partial writes are modelled nowhere.
- `Membership` has **no clock**: it answers "safe for every possible lease-expiry instant" and cannot
  answer "is the TTL long enough", which is exactly the question the lease rule above poses.

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
- **Dropping `none` mode** — rejected, twice: it is the only serverless-without-CRIU path, and the
  LocalStack flow already carries the application-side resume it requires
  (`runbooks/localstack-python311-redis/worker_core.py:19-23`). Relabelled as a cooperative restart
  profile instead of removed. Re-proposed by external review B on the grounds that it is not
  transparent; that is the reason for the relabel, not a reason for removal.
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

### Adopted from external review B

- **Order-independent hashed rendezvous key** — adopted. Building the pairing name at the call site
  is a latent hang under exactly the divergent schedules v2 exists to support, and hashing a
  rank-ordered tuple also retires the `@epoch=` digit merge and the `MAX_PAIRING_NAME` truncation.
  See [Rendezvous naming](#rendezvous-naming-direct).
- **`run_id` as a universal fence** — adopted, with one addition review B omits: the
  `fmi:ft:name:<comm_name> → run_id` index, without which every name-addressed tool in the repo
  cannot reach the fence.
- **`fragment_offset` + `fragment_length` instead of `fragment_index`** — adopted; an index makes
  `max_frame_bytes` part of the wire contract with no field that could detect a mismatch.
- **Receive credit advertised in `HANDSHAKE` and adopted rather than carried** — adopted, with the
  `replayable ≤ limit − buffered` invariant that makes adoption safe.
- **Idempotent operation tokens** — adopted; without them the existing unconditional retry
  (`src/ft/ControlPlane.cpp:141-148`) turns a lost reply into a `SIGKILL` of a legitimate process.
- **`dirgen` returned together with the rank records in one Lua execution** — adopted; two round
  trips can observe a topology that never existed.

### Rejected from external review B

- **A separate "logical message" layer owning message identity** — rejected. The collectives are
  implemented *inside* the `Channel` subclasses (`src/comm/PeerToPeer.cpp:14,29,35,86,132,186,241`;
  `src/comm/ClientServer.cpp:34,44,106,153`), below where B places the layer, with
  `include/comm/Channel.h:56-57` inviting subclass overrides and `Communicator::register_channel`
  (`include/Communicator.h:166`) as the documented out-of-tree extension point. A layer owning
  identity for both families must therefore either change the pure-virtual signatures or hoist the
  collectives out of both subclasses — strictly larger breakage than the lane parameter this contract
  already forbids. The RAII identity scope owns the counter instead.
- **A two-phase arrived/departed barrier** — rejected. The departure phase is a second rendezvous
  over the same medium with the same termination requirement, so a rank dumped mid-barrier lets one
  survivor observe all departures and reclaim while another polls late and blocks to `max_timeout`.
  The keys are already generation-qualified (`src/comm/ClientServer.cpp:46-47`), so it buys no
  correctness either. The direct `O(N)` key probe removes the underlying scan hazards outright.
- **Reject `hint()` after the rank has joined** — rejected as written. The rank joins inside the
  `Communicator` constructor (`src/Communicator.cpp:64`) and every FT application in this repo calls
  `hint()` after construction, so the rule would reject all of them. The hazard is real; the correct
  freeze point is the first guarded operation. See [Policy uniformity](#policy-uniformity).
- **A `policy_digest` carrying the reduction flags** — rejected as impossible, not merely
  undesirable: `left_to_right` is computed per call from the caller's `Function<T>`
  (`include/Communicator.h:103`, `:128`), so no static digest can carry it. The flags stay in the
  envelope, where `MessageIdentity.tla` shows them independently necessary.
- **An algorithm-phase / substep field in the envelope** — rejected as unnecessary, given the
  normative invariant that no collective places two messages on one directed pair within a single
  logical operation.
- **Unconditional `--leave-stopped` on the dump** — rejected *as unconditional*. Leaving the original
  frozen is what makes frozen-original abort recovery possible and it is required on the cross-host
  path; on the same-host in-place path it makes the immediate restore fail
  `Can't fork for <pid>: File exists` (reproduced on criu 4.2), because that restore reclaims the
  dumped pid. The flag is conditional on migration shape, and `MembershipSameHost` shows the
  same-host shape is unsafe for abort recovery in any case.

## Open items to resolve before Plan C task 1

Resolved since the first draft, and no longer open: the `retention_limit_bytes` cap is an
**admission** threshold and never an eviction threshold (this is also the only reading under which
`SequencedLink`'s results hold); credit reconciliation across a connection break is settled by the
handshake-adoption rule in [Two watermarks](#two-watermarks); `abort_pause` drivers, the abort
incarnation bump, the pause-clear ordering and the `SIGCONT` fence are all machine-checked in
`Membership.tla`.

1. **CREDIT's initial value** relative to `W` and the drain reserve. Still unspecified; too small a
   value silently converts a legal program into a window-full stall.
2. The quiesce-snapshot schema. `links:<r>` is per-link, but the collective index is
   per-rank/per-channel. Specify it as an opaque versioned per-**channel** blob produced by each
   channel's `quiesce_links()`, and enumerate the required contents.
3. Inline-send versus full-engine, decided on Plan B's measured freeze data and a Plan C
   micro-benchmark, not after the engine is built.
4. Whether `--pre-dump`/`--track-mem` enters scope, decided on Plan B's `O(RSS)` measurements.
5. **The lease TTL constant and the restore attempt budget `k`** (contract 3). The *rule* is now
   normative — TTL provably exceeds worst-case restore duration, or the agent renews; plus a bounded
   budget with terminal escalation — but the numbers are not. They must come from Plan B's measured
   `criu restore` durations, not from a guess, and `Membership.tla` has no clock so it cannot supply
   them. Also assign the **resume responsibility** the orchestrator-driven `abort_pause` edge
   implies: someone must `SIGCONT` the frozen original, and on the cross-host path that is a
   different node's agent.
6. **Agent deployment shape** (contract 4). The in-container agent is the verified baseline; the
   per-pod sidecar is what contract 3's `PAUSING` analysis assumes and what Knative restore wants.
   Decide which is normative for the v2 runbooks, and if the sidecar wins, verify its three
   prerequisites (`shareProcessNamespace`, identical mount layout, the extra Knative feature gate) on
   a real cluster before any plan depends on it.
7. **Whether `Communicator::scan` should become flag-aware.** `include/Communicator.h:153` omits
   `left_to_right` from its policy call, unlike `reduce` (`:103`) and `allreduce` (`:128`), so a
   flags divergence on `scan` can never split the backend. Making it consistent is a one-line change
   with a policy-behaviour consequence; leaving it is defensible. Decide deliberately and record it,
   rather than leaving the asymmetry to be rediscovered.
8. **Whether to build the composition module.** Nothing currently checks contracts 1, 2 and 3
   together, and that conjunction *is* the central claim of v2. Two ranks, divergent schedules, two
   sequenced links, one incarnation change — see `docs/tla/README.md`, "Extending the models".
