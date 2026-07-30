# Sequenced links as implemented — what holds, how it was checked, what is still open

Companion to [the design](2026-07-27-sequenced-incarnation-links-design.md). The design
describes the protocol the four contracts define; this describes **what is actually built on
`feat/sequenced-links`**, the rules the built thing must obey that the design did not state
because it assumed a different execution model, and the evidence for each claim.

Read the design for *why* the protocol is shaped this way. Read this before changing the
transport, because several of the rules below look like implementation detail and are not.

## The shape that is built

The design specifies an autonomous progress engine: non-blocking sockets, an event loop, an
egress cursor, standalone ACK and CREDIT frames. **That engine does not exist.** What exists is
the sequenced link layer driven by FMI's existing *blocking, application-threaded* transport:
`TcpChannelBase` calls `send`/`recv` on the application's thread, exactly where the unframed
transport used to.

This is a deliberate staging choice, not an oversight. It buys message identity (contract 1)
and transport durability (contract 2) without the one-way door the design flags — the engine
"is the irreversible commitment point" — and it carries an arbitrary FMI program across a real
checkpoint.

Carrying one at **arbitrary rank counts** took six rounds and one idea: *a rank that is waiting
must keep meeting every obligation it has.* See [what it took](#checkpointing-at-arbitrary-rank-counts-what-it-took).
What is not here is the rest of what the engine would buy — replay proceeding while the
application computes, CREDIT, non-blocking egress — listed under [what the blocking shape cannot
do](#what-the-blocking-shape-cannot-do).

## Rules the blocking transport must obey

Each of these was found by an experiment that failed, and each is load-bearing.

### R1 — The receive watermark advances only after the payload is delivered

> **Normative:** `next_received` may not advance until the frame's payload is in the
> application's buffer. `SequencedLink::classify` decides what an arriving frame is;
> `SequencedLink::commit_inline` moves the watermark; the transport must call them in that
> order with the payload read in between.

The watermark is what the peer is told in the next handshake, and what it prunes retention
against. Advancing it at header-parse time means a freeze in the microseconds before the
payload is read leaves the receiver claiming a message whose bytes were still in a kernel
socket buffer — which no checkpoint image captures. The peer prunes it and never replays it.
Nothing anywhere reports an error; the message is simply gone.

This is freeze position 2 in `docs/tla/SequencedLink.tla`, and the transport had it wrong.
Pinned by `CheckpointFreezePoints/a_freeze_between_the_header_and_its_payload_does_not_consume_the_message`,
which asserts on the handshake the receiver sends after the break.

### R2 — Any break inside a frame is repairable, not fatal

A frame is header + payload, and the connection can die between them or part way through
either. All three reads go through one guarded path that repairs and restarts from the header;
the partial bytes already copied into the application's buffer are overwritten by the replay.
This is safe precisely because of R1: the frame was never acknowledged, so the peer still holds
it.

### R3 — Retention must be released on links that carry traffic in one direction only

> **Normative:** a receiver issues a standalone `FrameType::Ack` after `link_ack_interval`
> commits, and `link_ack_interval` must be strictly below `link_window_frames`. A sender whose
> window is full collects acks (`drain_acks`) before reporting the link exhausted.

Piggybacking alone is sufficient only when every link carries traffic both ways. Two ranks
always do — which is why this held through the entire two-rank test suite — and **three do
not**: a binomial tree leaves directed links with no return path at all. The sender's window
fills at frame `W` and never drains, and the job stops permanently with
`link to peer N is at its retention limit`. This is exactly how the first three-rank run failed.

`drain_acks` uses `MSG_PEEK`, so a data frame it finds is left in the stream for the normal
receive path; only payload-free ack frames are consumed. The peeked frame's `cumulative_ack` is
applied either way, so even a data frame that stays where it is prunes retention.

Pinned by `LinkLiveness`, whose `link_window_frames` is 4 so the condition is reached in a
handful of messages rather than 256.

### R4 — A library that owns a hiredis connection must not leave `SIGPIPE` fatal

> **Normative:** `FMI::Utils::suppress_sigpipe()` runs before any hiredis context is used —
> at construction of the object that owns it, not on the reconnect path.

FMI's own writes pass `MSG_NOSIGNAL`; hiredis's do not, and a library user cannot reach those
sockets. After a restore, the first registry or control-plane command is issued on the context
the image captured, whose socket the restore already dropped. With the default disposition that
write kills the process — before it re-establishes a single link, before it prints anything.
The recovery machinery is worth nothing if the process is not alive to run it, and this alone
was the difference between a working restore and a silent death the first time an FMI rank was
checkpointed for real.

Installed only when the current disposition is `SIG_DFL`, so an application's own handler is
never displaced. Pinned by
`CheckpointFreezePoints/a_restored_rank_survives_writing_to_a_connection_the_restore_dropped`.

### R5 — Lineage is judged before sequences

> **Normative:** `reconcile` compares incarnations first. A peer whose incarnation is *below*
> the one this side has already reconciled with is refused. A peer that reports talking to a
> *higher* incarnation of this rank than this process holds means this process is the zombie,
> and it refuses to serve. A peer whose incarnation is *above* what this side recorded has
> restarted from nothing, so the stream resets on both sides.

Order matters because a replacement legitimately arrives expecting sequence 0 while this side
has pruned well past it. Judged as a same-lineage handshake that is an impossible state, and
the legitimate replacement is rejected. Pinned by
`LinkIncarnations/a_replacements_zero_sequences_would_be_rejected_without_the_lineage_check`.

## Contract 3 as implemented

An **incarnation** names a *lineage of link state*, not a process and not a rank.

`ControlPlane::claim_incarnation` is called exactly once per process, in the `Communicator`
constructor. That single placement is what makes the number answer the right question:

| event | runs the constructor? | incarnation | what peers must do |
| --- | --- | --- | --- |
| first process for a rank | yes | 0 | nothing; the stream starts empty |
| criu restore | **no** | unchanged | reconcile — its sequences and retention are intact |
| survivor rejoining a later epoch | **no** | unchanged | reconcile |
| fresh replacement rank | yes | previous + 1 | reset the stream; the frames owed to its predecessor are discharged |
| a superseded process still running | — | below current | refuse the link |

The counter is job-scoped rather than epoch-scoped in Redis (`fmi:ft:<comm>:incarnations`),
because a lineage must stay fenced across every epoch it lives through; a per-epoch counter
would hand a rank's second replacement the number its first one already used.

Without fault tolerance there is no control plane, every incarnation is 0, and the lineage
rules hold trivially.

**What contract 3 does *not* have yet:** the design's full membership state machine — `dirgen`,
`PAUSING`/`QUIESCED`/`CHECKPOINTED`/`RESTORE_RESERVED`/`LOST`, idempotent operation tokens,
directory-driven repair, the restore budget. Those are specified and model-checked
(`docs/tla/Membership.tla`, and see the bounded-restore fix) but not written. What is
implemented is the part the link layer needs to be unambiguous: the incarnation, its fencing
rules, and where the number comes from.

## Evidence

| claim | how it is checked |
| --- | --- |
| identity fields are each necessary | `docs/tla/MessageIdentity.tla`, 10 configs; TLC exhibits a silent substitution for every weaker envelope |
| the link state machine is safe and live | `docs/tla/SequencedLink.tla`, 15 configs including 4 freeze positions and 6 deliberately-broken variants |
| the membership contract is live | `docs/tla/Membership.tla`; `Membership.cfg` violates its temporal properties, `MembershipBudget.cfg` is clean over 2,436 distinct states |
| identity flows from every collective with no application change | `OperationIdentity`, `FramedTransport` |
| a divergent schedule fails loudly rather than substituting | `FramedTransport`, `ProtocolEdgeCases`, `ProtocolFuzz` (framed: 0 silent completions of a divergent program; unframed: 17) |
| a severed link loses and duplicates nothing | `LinkRecovery`, `TransportRecovery` |
| every freeze position is recoverable | `CheckpointFreezePoints` |
| one-way links keep flowing | `LinkLiveness` |
| lineage rules fence a zombie | `LinkIncarnations` |
| **an unmodified FMI program survives a real checkpoint** | `runbooks/criu-transparent-checkpoint`, criu 4.2 |
| the tests detect the faults they claim to | `tests/tools/mutation_sweep.py` |

The mutation sweep is the load-bearing one. The suite's first version passed with 9 of 21
injected faults still in place — every test varied two fields at once, so some other comparison
always caught the break and no test pinned what it appeared to. Any new claim in the table
above should arrive with a mutation that the new test kills.

## Checkpointing at arbitrary rank counts: what it took

**48 randomized trials across 2, 3, 5, 7, 8 and 16 ranks; 48 passed.** Each trial checkpoints a
randomly chosen rank at a random instant and requires every rank to finish with the checksum a
clean run of the same shape produced, so a lost, duplicated or substituted message fails the
trial rather than being absorbed into a plausible answer.

Six rounds got there, and rounds 1–5 all left the 4-rank rate at exactly 7 of 10. That
invariance is what eventually identified the shape of the problem: it was never one bug.

| # | what a waiting rank was not doing | fix | 4-rank rate |
| --- | --- | --- | --- |
| 1 | reading its other links | `service_established_links` drains whole frames | 7/10 |
| 2 | reconciling a link the peer replaced | `note_link_replaced` in `accept_one` | 7/10 |
| 3 | tolerating a half-read message | a partial read that stalls is a broken link, not silence | 7/10 |
| 4 | letting the peer's handshake through | handshake carried as a frame, one way | 7/10 |
| 5 | accepting at all | `pump` services the listener from inside every wait | 10/10 |
| 6 | **finishing what it accepted, and paying what it owed** | `adopt_link` + reconcile from servicing | **all clean** |

**One idea underneath all six: a rank that is waiting must keep meeting every obligation it
has.** Not some of them, and not eventually — a rank blocked on one peer must still accept
connections, still drain its other links, and still hand over any handshake and replay it owes.
Miss any one and the wait-for relation that makes lazy establishment safe stops being acyclic
the moment a restore leaves several links needing attention at once.

Mechanically: every blocking read and write goes through `pump()`, which polls the descriptor
the caller needs in short slices and, between them, services the listener, drains the rank's
other links, and reconciles any link still owing a handshake. `adopt_link` completes an accepted
connection there and then rather than parking it until the application happens to touch that
peer.

**This is the progress engine's property without its thread.** The design specifies an
autonomous component owning every socket; what the deadlock actually needed was not concurrency
but the guarantee that waiting is never exclusive. Staying single-threaded keeps the lock-free
link state and the `thread_local` operation scope exactly as they were, and it is reversible in
a way the engine is not. The engine's *other* benefits — replay proceeding while the application
computes, CREDIT, non-blocking egress — remain absent; see below.

Non-powers of two are measured on their own account, not as an afterthought: FMI's collectives
are binomial trees, which take a different shape when the rank count is not a power of two, and
repo coverage before this branch was only ever 2 and 4 ranks — both powers of two.

A dump that criu itself refuses (`External socket is used`, seen intermittently on this host) is
counted as skipped rather than failed: it says nothing about the protocol.

### How each round was found

Not by reasoning — by photographing a wedged job (`ss` plus `/proc/*/wchan`), and from round 6
by having the transport report its own state. A rank that has waited more than three seconds
prints what it believes it holds on every link. That produced the decisive line:

```
[FMI] rank 0 has waited 39017ms for peer 3; p1{fd=5 rq=0 rec=0} p2{fd=8 rq=0 rec=1} ...
```

`rec=1` — rank 0 had held peer 2's link marked for reconciliation for thirty-nine seconds while
peer 2 waited for exactly the replay that mark represents. The diagnostic is kept, unconditional,
and silent in a healthy run because nothing waits that long.

## What the blocking shape cannot do

Honest limits of what is built, all of which the design's engine would address:

- **Replay does not proceed while the application computes.** The design requires that "a
  completed send may be the last time the application ever touches that link" — retention is
  serviced by an autonomous component. Here, repair happens inside the next `send` or `recv` on
  that link. A rank that finishes its last collective and then computes for an hour will not
  service a peer's replay during that hour.
- **Repair is socket-driven, not directory-driven.** The design requires a rank to close and
  repair on *observing* a peer's incarnation change in its directory poll, never on EOF or
  timeout. Here, a break is discovered when a read or write fails. In the
  `--tcp-close --leave-stopped` case the original's sockets stay ESTABLISHED and silent, and a
  socket-driven survivor would wait for its receive timeout rather than repairing promptly.
  The runbook's flow avoids this because the dump kills the original, so the peer sees the
  connection go.
- **No CREDIT.** Receiver-side buffering is bounded by the kernel socket buffer, as before; the
  design's separate credit dimension is not implemented, so a divergent peer's backlog is
  bounded by TCP rather than by policy.
- **Standalone acks are best effort.** An ack that will not fit in the socket is dropped and
  re-offered after the next commit; a *partial* ack is completed with a blocking write, bounded
  by `SO_SNDTIMEO`, because a half-written frame would desynchronise the peer.
- **The window is a new bound on unreceived sends**, and this is the behavioural regression the
  design warns about. Unframed, "how many messages may a rank send before its peer receives
  any" was bounded by the kernel socket buffer — effectively unbounded for small messages.
  Framed with recovery it is `link_window_frames` (default 256), because every unacknowledged
  send is retained. Exceeding it is a loud error naming the link, never a hang and never a
  dropped message; `LinkLiveness/the_window_is_the_bound_on_unreceived_sends_and_it_fails_loudly`
  pins both halves. A program that legitimately needs more must raise the window rather than
  discover the limit in production.
- **Single host only.** A restored rank re-uses the listening socket and advertised address its
  image captured. Correct on the same machine, wrong on any other.
- **`ClientServer` keys are not identity-qualified.** Contract 1 specifies that the same tuple
  qualifies `Redis`/`S3` object names. Only the TCP transports carry the envelope today.

## Also fixed on the way

Two defects in the *test harness* that were producing false results, both pre-existing:

- `ForkedRankGuard` now drains foreign zombies on construction and waits for the specific
  children it forked. A case whose rank construction throws unwinds without waiting for the
  children it already forked; a later case's `wait(nullptr)` then reaped one of those instead
  of its own rank child and checked shared result flags the rank had not written yet. This
  reported two `FramedTransport` failures that had not happened.
- `config/fmi_test.json` pointed `Direct` at a LAN address (`192.168.0.166`) that no longer
  exists, so `Communicator/sending_receiving` could not pass on any machine but one.
