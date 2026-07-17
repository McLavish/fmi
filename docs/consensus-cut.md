# The Consensus Cut

How FMI decides *where* in the operation stream a migration epoch cut happens. The mechanism
lives in `ControlPlane::observe_operation` (src/ft/ControlPlane.cpp) and
`TransparentMigrationRuntime::enter_operation` (src/ft/TransparentMigrationRuntime.cpp);
promotion-side enforcement is in `ControlPlane::promote_epoch`.

## The problem

A migration must cut over between operations, but "between operations" is per-rank: without
coordination, each rank stops at *its own* next operation boundary when it notices the pending
request. That races mid-collective — rank A parks at boundary k while rank B is already blocked
*inside* operation k+1, waiting for a contribution the migration target will never send (or
reading EOF from its closed sockets). The stress test
`FaultTolerance/transparent_migration_cut_timing_stress` reproduces exactly this.

## The mechanism

Every rank counts the operations it has **completed** (`boundary_index`, bumped at
`exit_operation`). At every `enter_operation` it makes one atomic control-plane call — a single
Redis Lua script — that simultaneously:

1. publishes the rank's current boundary into a per-epoch boundaries hash,
2. checks the pending-migration set,
3. and, if a migration is pending and no cut exists yet, **fixes `cut_index`** — the first
   caller to get here becomes the proposer; everyone after it just reads the stored value.

**Proposal rule.** The proposer starts at its own boundary `B` and bumps the cut past any
*other* rank that has published a boundary `>= B`, to `max_published + 1`. The reasoning:
publishing is atomic with the pending check, so a rank that published `J` without parking must
have seen an *empty* pending set in that same call — it may be inside operation `J` right now,
needing the target's participation. Cutting above `J` keeps everyone (target included)
executing through operation `J`. Ranks published *below* `B` are only inside operations the
proposer already completed, so whatever they still need from the target is already in flight —
no deferral required. A lone rank therefore quiesces immediately at its own boundary.
(Note that this reasoning identifies operations across ranks by their per-rank index — it is
only sound within the scope described in
[Scope: aligned operation streams](#scope-aligned-operation-streams).)

**Parking.** Given a fixed cut `C`, every rank keeps executing while `boundary_index < C` and
stops exactly at `C`: the target quiesces (exits, or CRIU-checkpoints under
`state_transfer=criu`), survivors park awaiting promotion. Every rank completes exactly `C`
operations in the old epoch and resumes the new epoch at the same operation index. Because no
operation is in flight anywhere at the cut, the quiescing rank can close all its data-plane
sockets — which is what makes the socket-free CRIU dump and selective re-pair safe.

**Enforcement.** `promote_epoch` (one atomic script on its side) refuses to advance the epoch
until every pending rank is QUIESCED *and*, when a cut was fixed, every member has published a
boundary `>= C`. Both gates clear on their own — ranks below the cut can always finish to it,
since their peers already completed those operations — so callers simply retry promotion until
it succeeds (`LocalRankAgent::promote_when_gate_opens`, and the orchestrators'
`promote_when_gate_opens`). Promotion deletes `cut_index` and the boundaries hash atomically
with the epoch bump: the cut belongs to the epoch being left, and boundary counters reset to 0
on rejoin.

**Guards worth knowing about.** The cut is transported as `-1` for "none" because `0` is a
legitimate cut (a request that lands before any operation ran). An epoch guard keeps a rank
that slept through a promotion from polluting the new epoch's boundary hash with its stale
counter — it just rejoins. Only one cut can be in flight at a time; overlapping requests for a
different rank set are rejected.

## Worked example

Three ranks, migration requested for rank 2 while everyone is around their 8th operation:

| step | what happens |
|------|--------------|
| rank 1 enters op 8 (published boundary 7) | pending set still empty in its atomic call → proceeds, now *inside* op 8 |
| orchestrator calls `request_migration(2)` | pending = {2} |
| rank 0 hits boundary 7 | first to observe pending; proposes B=7, sees rank 1 published 7 ≥ 7 → fixes `cut_index = 8` |
| ranks 0 and 2 | boundary 7 < 8 → both execute op 8, so rank 1's in-flight op 8 completes with full participation |
| all ranks at boundary 8 | rank 2 quiesces; ranks 0 and 1 park |
| orchestrator retries `promote_epoch` | gate opens (target QUIESCED, all boundaries ≥ 8) → epoch N+1; everyone resumes at the same index |

## Scope: aligned operation streams

The cut is one global index compared against each rank's *local* count of guarded operations.
The identification "operation `k` on rank X is the same operation as operation `k` on rank Y"
is an **assumption about the application**, not something the protocol establishes. The
mechanism is sound exactly when every matched communication sits at the same boundary index on
all of its participants — in practice, when every rank issues its guarded FMI operations in
the same order (the SPMD/collectives shape; MPI-style semantics already require identically
ordered collectives).

General point-to-point schedules break the assumption. `send`/`recv` are guarded and counted
like any other operation, and nothing forces a matching send and recv to share an index:

- rank 1's op 0 is `recv(from 0)`; the matching `send(to 1)` is rank 0's op 5
- rank 1 publishes boundary 0 (pending set still empty in that call) and blocks inside the recv
- the request lands; rank 0 proposes at its boundary 5 → `cut_index = 5` (rank 1's published 0
  is *below* 5 and is ignored, per the proposal rule)
- rank 0 parks at 5 without ever executing the send; rank 1 can never leave op 0, so it never
  reaches the cut

The proposal rule's "ranks published below `B` are only inside operations the proposer already
completed" is exactly what fails: with unaligned streams, a low published boundary says nothing
about *which* operation that rank is blocked inside. Note that even a fully symmetric
neighbour exchange violates the assumption *mid-phase* — each rank walks its own neighbour
list, so a matched pair sits at different in-phase offsets on its two endpoints — and
per-rank neighbour counts that differ (interior vs. boundary ranks of a stencil) misalign the
counters permanently.

**Failure mode: fail-stop, never corruption.** The promotion gate refuses while any member's
published boundary is below the cut, so a misaligned cut leaves the job parked/blocked with
promotion failing loudly ("has not reached the consensus cut boundary"). That hang is the
orchestrator's to detect and resolve (see the liveness section of
[fault-tolerance.md](fault-tolerance.md)); what can never happen is a silently misaligned
epoch.

The practical contract, until the protocol either validates alignment (e.g. publishing an
operation signature — kind/root/size — with each boundary and rejecting mismatched cuts) or
counts only collectives as cuttable boundaries:

- **collectives-only workloads** may have migration requested at any time;
- **workloads using `send`/`recv`** must only have migration requested while all ranks are at
  an aligned point — e.g. the application holds every rank immediately before the same
  collective for the duration of the request window.

## Centralized or decentralized?

Both, in different places. The **proposal is decentralized**: no leader, no coordinator — the
proposer is simply whichever rank's boundary script executes first in Redis after the request
lands. The **arbitration is centralized**: agreement is delegated to Redis, whose
single-threaded atomic script execution makes first-writer-wins trivially race-free. Ranks
never negotiate with each other; they agree *through a shared linearizable store*, not among
peers — which is why no Paxos/Raft-style protocol is needed. This adds no new single point of
failure: Redis is already the FT control plane for epochs, membership, and quiescence, and the
cut negotiation rides inside the one control-plane round-trip each rank already pays per
operation boundary (zero extra RTTs).

## Lineage

The ingredients are classical: coordinated (blocking) checkpointing at an agreed barrier from
MPI checkpoint/restart; consistent cuts from Chandy–Lamport/Mattern-style distributed
snapshots (epoch-aligned barrier snapshotting, as in stream processors); a `max observed + 1`
selection rule that is essentially Lamport-clock timestamp assignment; and first-writer-wins
arbitration through a linearizable register. The FMI-specific part is the fit: agreement folded
into the existing per-operation round-trip, and the asymmetric proposal rule that only defers
the cut past ranks that could actually be inside an operation.
