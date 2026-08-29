# TLA+ specifications — the sequenced link layer

Machine-checked models of the normative link-layer contracts in
[`docs/design/2026-07-27-sequenced-incarnation-links-design.md`](../design/2026-07-27-sequenced-incarnation-links-design.md).

Two modules, 26 configurations, all exhaustively checked. Every number in this file was
observed by running TLC; nothing here is extrapolated. **Read [What is NOT proven](#what-is-not-proven)
before citing any of it** — the models are small, deliberately partial, and one of them records
an under-specification the design document still carries.

That design document also defines a contract 3, a Redis-coordinated membership state machine,
and this directory once held a `Membership.tla` for it. The protocol it modelled has been
deleted from the library and the module went with it; see
[the note at the end of the change log](#removal-of-the-membership-model).

---

## Modules and the contracts they map to

| Module | Design contract | Axis | What it answers |
|---|---|---|---|
| [`MessageIdentity.tla`](MessageIdentity.tla) | Contract 1 — message identity | A | Which envelope fields are *necessary* to make silent substitution impossible on the unframed `Direct` transport under divergent programs? |
| [`SequencedLink.tla`](SequencedLink.tla) | Contract 2 — transport durability | C | Does one directed sequenced link lose, duplicate or reorder anything when `criu dump --tcp-close` can fire at an arbitrary instant? |

Neither module models more than its own contract. **Nothing checks the two together** — see
[the composition gap](#the-composition-gap-the-biggest-hole).

---

## Reproducing

Toolchain: TLC2 version 2.19 (`tla2tools.jar`), Java 21. No Toolbox; command line only.
Run from **inside this directory**.

```bash
cd /home/luca/fmi/docs/tla

# every configuration, sequentially
for cfg in *.cfg; do
  name="${cfg%.cfg}"
  case "$name" in
    MessageIdentity*) mod=MessageIdentity ;;
    SequencedLink*)   mod=SequencedLink ;;
  esac
  java -XX:+UseParallelGC -cp ~/.local/share/tla/tla2tools.jar tlc2.TLC \
       -workers auto -cleanup -config "$cfg" "$mod.tla"
done
```

A single configuration:

```bash
java -XX:+UseParallelGC -cp ~/.local/share/tla/tla2tools.jar tlc2.TLC \
     -workers auto -cleanup -config SequencedLink_correct.cfg SequencedLink.tla
```

Notes on the command line:

* `-config <name>.cfg` is required because each module carries many configurations, every one
  of them named after the property it checks rather than after its module; TLC's default
  search for `<Module>.cfg` therefore finds nothing.
* `-cleanup` is safe **only when runs are sequential** — it wipes `states/`, which both
  modules in this directory share. Concurrent runs need distinct `-metadir`.
* Run all 26 sequentially: **≈ 1 minute wall clock** on 16 workers. The slowest single
  configuration is `MessageIdentity_FullTuple_len3` at 30 s; every other one is under 5 s.
* Exit codes: `0` = no error, `12` = invariant violated, `13` = temporal property violated.
  For the deliberately-broken and probe configurations, a **non-zero exit is the pass
  condition**.

### Reproducibility caveat

Runs that **complete** (`Model checking completed`) are exhaustive and their state counts are
exactly reproducible. Runs that **stop at a violation** are not: TLC's parallel workers reach
the counterexample by different routes each time, so `states generated`, `distinct states` and
the reported search depth vary run to run, and the trace itself may differ. The *outcome* —
which invariant or property is violated — is stable. Numbers below for violating runs are from
one observed run and are illustrative; numbers for completing runs are exact.

---

## Results

All 26 configurations, real output.

### `SequencedLink.tla` — contract 2

| Config | Outcome | States gen. | Distinct | Depth |
|---|---|---|---|---|
| `SequencedLink_correct` | **No error** — the headline theorem | 7,991 | 2,932 | 29 |
| `SequencedLink_correct_large` | **No error** — `Reserve > W`, `MaxMsg=4`, `W=3` | 96,431 | 30,567 | 34 |
| `SequencedLink_reserve_below_window` | No error *(`Reserve < W`; see finding SL-2)* | 3,738 | 1,551 | 39 |
| `SequencedLink_broken_ackonreceipt` | `OrderingInv2` VIOLATED *(expected)* | 40 | 33 | 9 |
| `SequencedLink_broken_retention` | `RetentionSafety` VIOLATED *(expected)* | 573 | 313 | 14 |
| `SequencedLink_broken_loss` | `NoLossSafety` VIOLATED *(expected)* | 247 | 134 | 11 |
| `SequencedLink_broken_volatile_drainq` | `RetentionSafety` VIOLATED *(audit pass)* | 536 | 291 | 15 |
| `SequencedLink_broken_prune_on_transmit` | `OrderingInv1` VIOLATED *(audit pass)* | 24 | 22 | 7 |
| `SequencedLink_broken_prune_on_transmit_loss` | `NoLossSafety` VIOLATED *(audit pass, stage 2)* | 218 | 129 | 11 |
| `SequencedLink_probe_Probe_CompletesAcrossEveryFreeze` | VIOLATED *(**required** — non-triviality)* | 7,084 | 2,726 | 26 |
| `SequencedLink_probe_Probe_WindowIsExercised` | VIOLATED *(required)* | 337 | 186 | 13 |
| `SequencedLink_probe_Probe_FreezeMidStream` | VIOLATED *(required)* | 227 | 130 | 13 |
| `SequencedLink_probe_Probe_FreezePos1_FrameOnTheWire` | VIOLATED *(required)* | 49 | 39 | 8 |
| `SequencedLink_probe_Probe_FreezePos2_ParsedNotCommitted` | VIOLATED *(required)* | 84 | 62 | 10 |
| `SequencedLink_probe_Probe_FreezePos3_CommittedNotAcked` | VIOLATED *(required)* | 126 | 81 | 11 |
| `SequencedLink_probe_Probe_FreezePos4_AckOnTheWire` | VIOLATED *(required)* | 305 | 189 | 14 |

The seven probes are invariants **asserted in order to be refuted**. A probe that *passes*
means the model never reaches the state of interest and the corresponding "no error found"
result is partly vacuous. All seven are refuted.

Independent non-vacuity evidence, from `-coverage 1` on `SequencedLink_correct` (format is
`distinct:generated`):

```
<AppSend>:        105:731     <Deliver>:        124:316     <Freeze>:     86:1836
<Transmit>:        66:309     <Commit>:         162:329     <Restore>:   103:145
<Replay>:          37:115     <PruneRetention>: 142:1315    <Handshake>:  36:139
<AppConsume>:     869:1194    <EmitAck>:        673:987     <AbortedEnd>:   0:0
```

Every action fires, including `Replay` and `Handshake`; `AbortedEnd` never fires, which is
`NoAbort` restated.

### `MessageIdentity.tla` — contract 1

| Config | Envelope | Outcome | States gen. | Distinct | Depth |
|---|---|---|---|---|---|
| `MessageIdentity_FullTuple` | full | **No error** | 7,593 | 6,673 | 9 |
| `MessageIdentity_FullTuple_len3` | full, programs ≤ 3 | **No error** | 41,450 | 34,581 | 13 |
| `MessageIdentity_FullTuple_aligned` | full, well-formed pairs only | **No error** (`NoLoudAbort` holds) | 68,822 | 2,234 | 13 |
| `MessageIdentity_FullTuple_witness` | full | `NoLoudAbort` VIOLATED *(**required** — witness)* | 3,727 | 3,671 | 3 |
| `MessageIdentity_LaneAndIndex` | + `collective_index` | `NoEqualIndexCollision` VIOLATED | 3,726 | 3,670 | 3 |
| `MessageIdentity_LaneAndIndex_specprogram` | + `collective_index` | `NoEqualIndexCollision` VIOLATED | 42 | 6 | 4 |
| `MessageIdentity_LaneOnly` | lane + ordinal | `NoCollectiveCollision` VIOLATED | 3,725 | 3,669 | 3 |
| `MessageIdentity_LaneOnly_specprogram` | lane + ordinal | `NoSilentSubstitution` VIOLATED | 41 | 5 | 4 |
| `MessageIdentity_OrdinalOnly` | today's `Direct` | `NoCrossLaneSubstitution` VIOLATED | 3,630 | 3,602 | 3 |
| `MessageIdentity_OrdinalOnly_p2p_vs_barrier` | today's `Direct` | `NoCrossLaneSubstitution` VIOLATED | 368 | 352 | 3 |

---

## Bounds, and why

Every bound is small on purpose: an exhausted small model is worth more than a timed-out
large one. Nothing below is a symmetry-reduction or a state constraint that could hide a
counterexample; all bounds are either enabling-condition budgets or `TypeOK` assertions.

| Module | Bound | Value | Why this value |
|---|---|---|---|
| SequencedLink | `MaxMsg` | 3 (4 large) | Only has to exceed `W` so the window actually blocks. |
| | `W` | 2 (3 large) | Smallest window with >1 frame in flight, so one `Freeze` destroys several frames at once. |
| | `Reserve` | 2 (4 large, 1 in the below-window config) | 2 is the boundary case `Reserve = W`; 4 is the `Reserve > W` case the spec's config validation requires. |
| | `MaxFreezes` | 2 (1 in the broken configs) | 2 permits a second freeze *during* repair of the first. |
| MessageIdentity | ranks | 2 | The spec's counterexample is at N=2, and it is the only size where `bcast` and `barrier` both collapse to one 1-byte frame on the same directed pair. |
| | `MaxProgLen` | 2 (3 in two configs) | Length 2 already exhibits all three verdicts. Length 3 over the 6-symbol alphabet is 66,564 program pairs, so length 3 uses a 4-symbol alphabet. |
| | frame length | always 1 byte | Deliberate: the collision of interest is between frames of *equal* length, so `total_length` must not be allowed to do the discriminating. |

---

## What is PROVEN

Read every one of these as prefixed by *"within the bounds in the table above, and modulo the
abstractions in the next section"*.

**Contract 1 (`MessageIdentity`).** Over **all** divergent program pairs of length ≤ 2 (and
≤ 3 in two configs) drawn from a 6-symbol alphabet — not a hand-picked pair:

1. Today's unframed `Direct` admits silent substitution at depth 3.
2. Adding `lane` + per-lane FIFO drain queues is **not** enough: two *different collectives*
   still collide.
3. Adding `collective_index` as well is **not** enough — machine-checked confirmation of the
   spec's claim that "a bare per-communicator collective counter is also insufficient", with
   `collective_index` equal on both sides in the trace.
4. `op_kind` and the `commutative`/`associative` flags are each **independently** necessary:
   `reduce` vs `reduce_nc` collide with identical `op_kind`, `root`, `collective_index` and
   length; only the flags separate them. Neither may be dropped as an optimisation.
5. The full envelope never rejects a well-formed program pair (`NoFalseAbort` /
   `NoLoudAbort` over all 258 distinct `Compatible` pairs of length ≤ 3). This is the one
   result here that is not true by construction.
6. The validation actually fires (`NoLoudAbort` witness), rather than hanging or accepting.

**Contract 2 (`SequencedLink`).** On one directed link, with `Freeze` (`criu dump
--tcp-close`) enabled at **every** reachable non-aborted state below the bound:

7. Nothing posted is lost, duplicated or reordered (`NoLossSafety`, `RetentionSafety`,
   `NoDuplication`, `FIFO`), and every message whose `send()` returned is eventually consumed
   (`DeliveryObligation`), across up to 2 freezes with replay and handshake reconciliation.
8. The three ordering invariants hold, and each is load-bearing: five separate fault
   injections (ACK-on-receipt in three stages, volatile drain queue, prune-on-transmit in two
   stages) are each caught, and three of the five surface as *actual data loss*, not merely as
   a violated internal invariant.
9. Receiver-side dedup is unreachable (`DedupUnreachable` holds everywhere): because the
   handshake replays exactly `[next_received, next_send)`, no duplicate ever reaches the
   receiver on a single directed link. The `Classification::Duplicate` branch in Plan C is
   defensive dead code and its test case cannot be reached by driving the real handshake.
10. No impossible-state abort ever fires in the correct configuration (`NoAbort`), while the
    broken configurations do drive the handshake into a loud abort — so the abort path is
    reachable but never spuriously taken.

---

## Where model checking CONTRADICTED the design spec

One item. Findings D-1, D-2 and D-3 were all against contract 3's membership state machine and
were removed with it; the numbering below is left alone so that citations elsewhere in the repo
still resolve to what they meant. **The design document has not been edited**; what follows is
recorded here for the authors to act on.

### D-4 (minor). Contract 1's `root` field is under-specified for the P2P lane

The envelope table defines `root` as *"Collective root, or the peer id for p2p"*. "The peer
id" is ambiguous: if each side writes "the other end", the sender writes `1` and the receiver
expects `0`, and every legal p2p receive becomes a loud abort. `MessageIdentity.tla` silently
resolved this to **`root` = destination rank** on both sides, which is the only self-consistent
choice; `NoFalseAbort` holding depends on that resolution. Contract 1 should say
"the destination rank" explicitly.

---

## What is NOT proven

This section is the point of the document. A formal spec that quietly assumes away the hard
part is worse than none.

### The composition gap (the biggest hole)

**The claim the link layer exists to support — that a rank can be frozen and restored at an
arbitrary instant while its peers are running *arbitrary divergent point-to-point schedules* —
is not checked by either of these modules, and is not implied by their conjunction.**

Each module removes exactly what the other contains:

| | freeze / restore | divergent schedules | envelope | link durability |
|---|---|---|---|---|
| `MessageIdentity` | ✗ none | ✓ exhaustive | ✓ | ✗ assumes perfect FIFO |
| `SequencedLink` | ~ abstract `Freeze`/`Restore` steps | ✗ one directed stream | ✗ | ✓ |

So there is **no** machine-checked statement about a rank being frozen *in the middle of* a
divergent schedule and its peer replaying frames that are then matched against the restored
process's expectations. Specifically unverified:

* Whether a replayed frame's `collective_index` / `message_id`, restored from a CRIU image,
  still agrees with a survivor that kept advancing during the freeze.
* **Counter seeding for a process that takes a rank over from scratch** rather than being
  restored from an image (`SequencedLink::seed`). The design says seeded counters make
  divergence loud; no module models a fresh process, a reset counter, or the seeding.
* The interaction of contract 1's identity validation with contract 2's replay: a frame
  replayed after repair is validated by contract 1 machinery that no module exercises on a
  replayed frame.

A third module composing a 2-rank divergent schedule over two sequenced links with one
freeze/restore would be the highest-value next piece of work. Neither of the current two
substitutes for it.

**Partially closed by implementation tests, which are evidence but not proof.** The third
bullet — identity validation applied to a *replayed* frame — is now exercised directly against
the real transport by
`CheckpointFreezePoints/identity_is_still_enforced_on_a_frame_that_arrives_by_replay`: a frame
is put on the wire a second time after a repair, belonging to a different logical operation
than the one the receiver awaits, and is refused rather than delivered. That is one path
through the composition, chosen because it is the one where a frame reaches the receiver
without the sender having just produced it. It says nothing about the first two bullets, and a
test over one path is not a check over all of them.

The freeze positions this file lists as abstracted away (partial egress, partial parse) are
likewise covered only by implementation tests — and that abstraction hid a real defect: the
transport advanced its receive watermark at header-parse time, so a freeze between a header and
its payload let the peer prune a message it had never delivered. See
`docs/design/2026-07-30-sequenced-links-implementation.md`, rule R1.

### `MessageIdentity`

* **No freeze, no restore, no incarnation change, no counter reset.** Consequence:
  `message_id` discriminates nothing here. Its "job-lifetime, never reset" property matters
  against the ClientServer counter reset, which is a *keyed store*, not a FIFO stream — a
  different transport model, entirely absent.
* **ClientServer is not modelled at all.** Every A2-stage claim in the spec — job-lifetime
  keys, consumer-delete namespaces, `GET`/commit/`DEL` never `GETDEL`, the barrier
  deduplicated-rank-set fix, `KEYS *` scoping — is unverified, and the counterexample corpus
  that used to drive that path was removed with the epoch protocol, so nothing exercises it
  now either.
* **`root` necessity is not exhibited.** At N=2 no two collectives agree on lane, `op_kind`,
  `collective_index` and length while disagreeing on `root`. `root` is carried per the spec;
  this module does not show it necessary. That needs N ≥ 3.
* **`wire_version` and `fragment_index` are absent**, as is fragmentation and any partial
  write.
* **All frames are 1 byte.** This is what makes the p2p-vs-collective collision fire; a
  real application sending a large p2p message would not collide with a 1-byte barrier
  fragment on length alone. The *collective-vs-collective* collisions are unconditional (those
  frames really are all 1 byte); the *cross-lane* one requires a 1-byte application message.
* **`NoFalseAbort` is vacuous in three of four configs.** Under `OrdinalOnly` the checked
  field set is empty, so `LoudAbort` can never fire; under `LaneOnly`/`LaneAndIndex` the
  checked fields always agree. It is a real result only under `FullTuple`, and the dedicated
  evidence is `MessageIdentity_FullTuple_aligned`.
* **"FullTuple holds" is true by construction**, because ground-truth identity is defined as
  exactly the FullTuple field set. The load-bearing outputs are the three *necessity* results,
  `NoFalseAbort`, and the `LoudAbort` witness — not that one.
* **`Terminates` is trivial**: every action strictly increases `pc[0]+pc[1]`, so the state
  graph is finite and acyclic. It would only catch a modelling error.
* **N=2 understates the reduce-flag divergence.** At N=2 `reduce_ltr` and `reduce_no_order`
  emit the same pattern *and* compute the same answer, so a flags mismatch is a latent
  divergence; the wrong arithmetic only materialises at N ≥ 3.

### `SequencedLink`

* **One DATA direction only.** Therefore the `drain_reserve_frames >= window_frames`
  constraint — which exists to prevent the ack-behind-data deadlock — is **NOT verified**.
  `SequencedLink_reserve_below_window.cfg` sets `Reserve = 1, W = 3`, a config the spec's
  parse-time validation rejects, and it passes clean (3,738 / 1,551, depth 39, liveness holds).
  That is not a counterexample to the spec; it is evidence that the hazard is genuinely
  bidirectional and cannot be exhibited without two DATA directions sharing one egress FIFO.
  **The constraint remains an unchecked implementation obligation.** If it is worth gating
  config parsing, it deserves its own bidirectional module.
* **No lanes, no fragments, no message identity** — contract 1 is out of scope. (Reassembly
  would sit between `Deliver` and `Commit` and only widen the volatile window, so the omission
  is conservative.)
* **Bytes are frames.** `max_frame_bytes` and `retention_limit_bytes` collapse into the frame
  count `W`. Valid only under the Plan C decision that the retention cap is an *admission*,
  never an *eviction*, threshold. **The spec's open item 1 (cap behaviour at
  `retention_limit_bytes`) is therefore assumed resolved, not resolved.**
* **Frames are atomic on the wire.** The spec's egress-serialisation rule (a partially written
  DATA frame completes before any ACK begins) is a framing-corruption rule and framing
  corruption is abstracted away. Sound for the durability argument specifically; the
  interleaving hazard itself is an unchecked implementation obligation.
* **The handshake is atomic** (one step, not one RTT), and **the parser stages at most one
  frame**. Both understate the volatile set, which is conservative in the correct
  configuration.
* **No time, no timeouts, no coordinator.** However a survivor comes to learn that its peer
  was restored collapses into a nondeterministic, weakly fair `Restore`, and this module says
  nothing about that mechanism. **The liveness result is conditional on `WF_vars(Restore)` and
  `WF_vars(Handshake)`** — i.e. on repair discovery never waiting for EOF/RST/timeout. If
  discovery were socket-driven, `Restore` would not be weakly fair in the `--tcp-close
  --leave-stopped` case (the survivor's socket stays ESTABLISHED and silent) and
  `DeliveryObligation` would fail. There is no directory left to discharge that assumption, so
  the whole of it now rests on the transport: the restored process re-establishes toward its
  peers, and a survivor accepts that connection from `service_transport`, which runs from
  inside every blocking read and write rather than only from `build_mesh`.
* **`Freeze` is a *planned* checkpoint.** No crash-without-image, no permanent peer death, no
  image corruption, no double-restore race.
* **An ack-on-consume policy is not modelled** (it prunes strictly less, so the modelled case
  is the harder one).
* **`DedupUnreachable` is proven only for a single direction with one restore at a time.** A
  double-restore race is not modelled.

### Bound-scope honesty

Nothing here is proven for unbounded N, unbounded messages, unbounded freezes or unbounded
program length. The largest configurations are 4 messages with a 3-frame window and 2 freezes,
and programs of length 3 over a 4-symbol alphabet. Read every result as *"no counterexample
exists at these bounds"*. Small-scope arguments make that persuasive for the *classes* of bug
modelled here (interleaving, ordering, arbitration); they say nothing about bugs whose smallest
instance is larger — and finding D-4's `root` necessity gap is a concrete example of exactly
such a bug living at N ≥ 3.

---

## Changes made by the integration/audit pass

All are additive and gated on new constants defaulting to the previously-modelled behaviour.
Re-running every pre-existing config after these edits reproduced its previous outcome, and
every exhaustive run reproduced its exact state count.

| File | Change |
|---|---|
| `SequencedLink.tla` | New `CONSTANT DrainQueueVolatile` (Freeze destroys the drain queue) and `CONSTANT PruneOnTransmit` (retention released at socket-write). Both `FALSE` = the spec. |
| all `SequencedLink*.cfg` | New constants assigned their spec values. |
| new: `SequencedLink_broken_volatile_drainq.cfg` | Teeth test: a defect that respects ordering invariant 2 but lies about durability. |
| new: `SequencedLink_broken_prune_on_transmit{,_loss}.cfg` | Teeth test: breaks ordering invariant 3 directly; stage 2 shows it reaching actual loss. |

### Removal of the membership model

`Membership.tla` and its 11 configurations were deleted together with the Redis-coordinated
epoch migration protocol they modelled. Contract 3 described that protocol's directory,
leases and pause entries; with the control plane gone from the library there is nothing left
for the module to be a model *of*, and keeping it would have asserted machine-checked support
for code that no longer exists. Findings D-1, D-2 and D-3 went with it. Nothing in contracts 1
and 2 depended on it — the two surviving modules never referenced `Membership.tla`, and every
remaining configuration reproduces the outcome and, where the run completes, the exact state
count recorded above.

---

## Extending the models

1. **Compose them.** The highest-value next module is the one described under
   [the composition gap](#the-composition-gap-the-biggest-hole): two ranks running divergent
   schedules over two sequenced links, with one freeze and restore in the middle. Take
   `MessageIdentity`'s program/`Compile` machinery, replace its perfect FIFO `chan` with
   `SequencedLink`'s `dataChan`/`retained`/`drainQ`, and let `SequencedLink`'s `Freeze` and
   `Restore` drive the cut. Expect to need `MaxProgLen = 2` and `MaxMsg = 2` to stay
   exhaustible.
2. **Bidirectional `SequencedLink`.** Add a second DATA direction sharing one egress FIFO per
   socket. This is the *only* way to check `drain_reserve_frames >= window_frames`; a model
   that cannot exhibit the deadlock the constraint prevents cannot validate the constraint.
3. **N ≥ 3 in `MessageIdentity`.** Needed for `root` necessity and for the reduce-flag
   divergence to become a wrong *answer* rather than a latent identity mismatch. The state
   space grows fast; drop the alphabet to `{send, recv, bcast, reduce}` first.
4. **ClientServer identity.** A keyed-store model (keys, not a stream) for the A2 claims.
   `S3` and `Redis` carry real traffic and nothing — model or test — checks their identity
   discipline.

When adding a configuration: if it is expected to *fail*, say so in a comment at the top of
the `.cfg` and add it to the results table with the required outcome, so a future reader can
tell a regression from an intentional refutation.
