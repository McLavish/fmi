# TLA+ models for sequenced links

This directory contains two models and 26 TLC configurations. The tables below
record earlier completed runs; they have not been regenerated for this
 documentation rewrite. Read [What is NOT proven](#what-is-not-proven) before
using the results to support a correctness claim.

The models address separate parts of the
[sequenced-link design](../design/2026-07-27-sequenced-incarnation-links-design.md):

| Module | Contract | Question |
|---|---|---|
| [MessageIdentity.tla](MessageIdentity.tla) | 1: identity | Which fields distinguish incompatible operation schedules on a FIFO stream? |
| [SequencedLink.tla](SequencedLink.tla) | 2: durability | Can one directed link preserve ordered delivery through abstract freeze and restore steps? |

Neither model composes identity checks with replay. Neither models Local Drain.

## Status against the code (wire version 4)

The models were written for the July design and were not rerun after the August
27 wire-format change. The following differences matter when comparing them
with the implementation:

- `MessageIdentity` calls its FIFO position `mid` and its length `len`. The wire
  fields `message_id` and `total_length` were removed as redundant; the current
  code uses sequence-ordered receive queues and `payload_length`.
- `SequencedLink` models receiver CREDIT and a separate `ackSafe` watermark.
  The implementation has no CREDIT protocol and keeps `ack_safe_seq` equal to
  `next_received` in production. Model results involving those extra mechanisms
  are not validation of an implemented receiver-memory limit.
- The model handshake includes `next_send`, `next_expected`, `ack_safe`, and
  `lowest_retained`. The version-4 wire handshake carries the first, second, and
  fourth, in a 30-byte payload.
- The old sequenced membership protocol and `Membership.tla` were removed.
  Incarnation references in the original model commentary concern that retired
  protocol, not the current sequenced wire format.

## Reproducing

Recorded toolchain: TLC2 2.19 (`tla2tools.jar`) and Java 21. Run from this directory
and adjust the JAR path for your installation.

```bash
cd docs/tla        # from the repository root

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


Specify `-config`: the configurations have descriptive names, so TLC's default
`<Module>.cfg` lookup will not find them. Run sequentially when using `-cleanup`,
which removes the shared `states/` directory. Concurrent runs need separate
`-metadir` paths.

The recorded complete sweep took about one minute with 16 workers. TLC exit code
0 means no error; 12 means an invariant violation; 13 means a temporal-property
violation. Broken and probe configurations deliberately require a violation.

### Reproducibility caveat

A run ending with `Model checking completed` exhausts its configured state space.
For the recorded toolchain and settings, those completed runs reproduced their
state counts. Parallel runs that stop at a counterexample can take different
paths, so their counts, depths, and traces can differ. The required violated
property is the result to compare; the violating-run counts below describe one
observed run.

## Results

### `SequencedLink.tla` — contract 2

| Config | Outcome | States gen. | Distinct | Depth |
|---|---|---|---|---|
| `SequencedLink_correct` | **No error** — within the stated bounds | 7,991 | 2,932 | 29 |
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

The seven probes deliberately assert that a state of interest is unreachable.
Each is expected to fail, showing that the model actually reaches that state.
All seven produced the required counterexample.

Independent non-vacuity evidence, from `-coverage 1` on `SequencedLink_correct` (format is
`distinct:generated`):

```
<AppSend>:        105:731     <Deliver>:        124:316     <Freeze>:     86:1836
<Transmit>:        66:309     <Commit>:         162:329     <Restore>:   103:145
<Replay>:          37:115     <PruneRetention>: 142:1315    <Handshake>:  36:139
<AppConsume>:     869:1194    <EmitAck>:        673:987     <AbortedEnd>:   0:0
```

`Replay` and `Handshake` both execute. `AbortedEnd` never executes, consistent
with `NoAbort`.

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
| `MessageIdentity_OrdinalOnly` | unframed `Direct` | `NoCrossLaneSubstitution` VIOLATED | 3,630 | 3,602 | 3 |
| `MessageIdentity_OrdinalOnly_p2p_vs_barrier` | unframed `Direct` | `NoCrossLaneSubstitution` VIOLATED | 368 | 352 | 3 |


## Bounds, and why

The bounds allow complete exploration of the selected state spaces. They are
enabling-condition budgets or type assertions, not symmetry reductions or
extra state constraints used to discard executions.

| Module | Bound | Value | Why this value |
|---|---|---|---|
| SequencedLink | `MaxMsg` | 3 (4 large) | Only has to exceed `W` so the window actually blocks. |
| | `W` | 2 (3 large) | Smallest window with >1 frame in flight, so one `Freeze` destroys several frames at once. |
| | `Reserve` | 2 (4 large, 1 in the below-window config) | 2 is the boundary case `Reserve = W`; 4 is the `Reserve > W` case the original proposal required. |
| | `MaxFreezes` | 2 (1 in the broken configs) | 2 permits a second freeze *during* repair of the first. |
| MessageIdentity | ranks | 2 | The spec's counterexample is at N=2, and it is the only size where `bcast` and `barrier` both collapse to one 1-byte frame on the same directed pair. |
| | `MaxProgLen` | 2 (3 in two configs) | Length 2 already exhibits all three verdicts. Length 3 over the 6-symbol alphabet is 66,564 program pairs, so length 3 uses a 4-symbol alphabet. |
| | frame length | always 1 byte | Deliberate: the collision of interest is between frames of *equal* length, so payload length must not be allowed to do the discriminating. |


## What is PROVEN

These statements apply **within the configured bounds and abstractions**.

`MessageIdentity` shows that unframed matching can silently substitute data from
a different operation. Adding a lane still allows collective collisions;
adding a collective index still allows different operations at that index to
collide. Operation kind and reduction flags distinguish the modeled cases.
The aligned full-tuple configuration accepts all 258 compatible program pairs
within its scope, and a separate witness shows that mismatches can be rejected.
The two-rank model does not establish the necessity of `root`.

`SequencedLink` preserves no-loss, retention, non-duplication, and FIFO invariants
for one directed stream. Under its fairness assumptions, completed sends are
eventually consumed across the modeled freezes. Deliberately broken ACK,
retention, and drain-queue variants violate their required properties. The
correct configurations never take the impossible-state abort path.

`DedupUnreachable` holds in that directed-link model because the handshake
replays exactly the needed suffix. It is not evidence that a production dedup
check is unnecessary under executions the model does not include.

## Design clarification: P2P root

The original text described `root` as “the peer,” which is ambiguous because the
two endpoints have different peers. The model uses the **destination rank** at
both ends. The design now states that definition explicitly. This was recorded
as finding D-4; D-1 through D-3 concerned the removed membership model.

## What is NOT proven

### The composition gap

No model checks a rank frozen in the middle of a divergent P2P schedule while
replayed frames are matched against its restored operation state.

| Model | Divergent schedules | Freeze/restore | Identity checks | Reliable link |
|---|---|---|---|---|
| MessageIdentity | Yes, within bounds | No | Yes | Assumes perfect FIFO |
| SequencedLink | No; one directed stream | Abstract actions | No | Yes |

Consequently, the two results do not establish that restored operation counters
agree with advancing survivors, that identity checks are correct on all replay
paths, or that a fresh process can take over a rank. Fresh-process replacement
is not an implemented substitute for checkpoint restore.

The C++ test
`CheckpointFreezePoints/identity_is_still_enforced_on_a_frame_that_arrives_by_replay`
checks one identity/replay interaction. Partial-frame tests also cover a gap that
hid the original header-before-payload watermark bug. These are useful tests of
specific paths, not exhaustive checks of the composition.

### MessageIdentity limitations

- No checkpoint, counter reset, partial I/O, or keyed store is modeled.
- Every frame is one byte. This deliberately exposes equal-length collisions;
  the cross-lane example requires a one-byte application message.
- At two ranks, `root` necessity is not exhibited, and some reduction-flag
  mismatches do not yet change the arithmetic result. Both need larger cases.
- `FullTuple` is also the model's definition of ground-truth identity, so its
  no-substitution result follows from that definition. The weaker-envelope
  counterexamples, aligned-program check, and rejection witness supply the
  additional evidence.
- `NoFalseAbort` is vacuous for weaker envelopes whose checked fields always
  agree. `Terminates` follows from the finite, acyclic program-counter graph.

### SequencedLink limitations

- **One DATA direction.** The model cannot exhibit a bidirectional ACK-behind-DATA
  deadlock. The below-window reserve configuration passing does not validate the
  original reserve constraint. That CREDIT/reserve scheme is not implemented.
- **Atomic frames and handshake.** Partial headers, partial payloads, and DATA/ACK
  byte interleaving are absent. A real stream must serialize complete frames.
- **Frame-count memory model.** Byte limits collapse into window size `W`.
  Retention is assumed to be an admission limit, never an eviction policy.
- **No time, deadlines, or discovery.** Liveness assumes weakly fair `Restore`
  and `Handshake` actions. Actual socket-driven discovery must supply progress;
  a silent stopped peer does not automatically satisfy that assumption.
- **No permanent failure.** `Freeze` represents a planned checkpoint with an
  image. Lost images, crashes without images, image corruption, and competing
  restores are outside the model.
- **No combined lane or operation identity.** Receiver dedup being unreachable
  here applies only to this single-direction execution model.

### Finite scope

No result covers unbounded ranks, messages, freezes, or program length. The
largest message configuration has four messages and a three-frame window;
configurations allow at most two freezes. The longer identity configurations
use programs of length three over a four-symbol alphabet. A bug whose smallest
example exceeds those bounds can remain undetected.

## Extending the models

1. Compose two ranks, divergent schedules, two directed links, and a freeze/restore.
   Start with small program and message bounds so TLC can exhaust the model.
2. Add bidirectional DATA with shared per-socket egress to examine ACK progress
   and any proposed receive-credit policy.
3. Extend identity cases to at least three ranks to test root selection and
   reduction-order effects on answers.
4. Model Redis and S3 keys, checkpointed counters, retries, and cleanup separately.
   Existing store implementation tests do not replace a keyed-store model.

For a new configuration, document its expected outcome in the `.cfg`. Record the
required violation for deliberately broken cases so it is distinguishable from
a regression.
