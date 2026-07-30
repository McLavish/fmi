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
checkpoint at two ranks.

It does **not** carry one reliably above two ranks: see [the open deadlock](#open-a-job-deadlocks-when-a-rank-is-checkpointed-at-three-ranks-or-more),
which is the price of not having the engine and is diagnosed there.

It also costs things, listed under [What the blocking shape cannot
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

## Open: a job deadlocks when a rank is checkpointed at three ranks or more

**Not fixed. Diagnosed.** This is the largest gap between what the runbook claims and what it
delivers, and it is a design-level defect in lazy establishment, not a coding slip.

### What was measured

`runbooks/criu-transparent-checkpoint/sweep.py` on this host:

| shape | trials | passed | fixes in this section |
| --- | --- | --- | --- |
| 2 peers, one checkpoint | 10 | **10** | after |
| 4 peers, one checkpoint | 10 | 7 | after |
| 8 peers, one checkpoint | 8 | 6 | after |
| 8 peers, one checkpoint | 6 | 4 | before |
| 3 and 4 peers, up to 3 checkpoints | 8 valid | 6 | before |
| 4 peers, 256 KiB messages | 8 | 6 | before |
| 4 peers, checkpoint during mesh establishment | 8 | 4 | before |

**The rate did not move**, and that is the point of the last column: the two fixes below
address real defects, and neither is this one. Only two ranks is clean.

A failing trial ends with every rank throwing `Timeout` after its full deadline, the restored
rank having logged nothing since its restore.

### The mechanism

A wedged 4-rank job, caught live (`ss` plus `/proc/<pid>/wchan`), shows:

* rank 0 and the restored rank blocked in `poll` — both inside `build_mesh`, each establishing
  one link;
* the other two blocked in a socket read;
* **and data sitting unread on connections nobody is looking at**: 76 bytes (one whole frame)
  queued for rank 0, 56 bytes (one whole handshake) queued for another rank, 504 for a third.

Every rank is waiting, and every rank is holding something another rank needs.

`build_mesh` blocks the entire rank while it establishes ONE link, servicing only its listener
meanwhile — it does not read any of its other, already-established links. Before a checkpoint
that is safe, and `build_mesh` says why: a rank only ever waits for a *higher* rank to connect
to it, so the wait-for relation runs strictly upward and cannot close a cycle.

**A restore breaks that argument.** Several links must be rebuilt at once, and the order in
which each rank gets round to rebuilding each one is driven by its own collective schedule, not
by rank order. The wait-for relation is no longer upward-only, and it closes.

### Narrowed, by draining the other links

A rank inside `build_mesh` now takes whole frames off its *other* established links
(`TcpChannelBase::service_established_links`) into the per-lane drain queues, and `recv_object`
prefers a queued frame to the socket — a frame buffered that way is older than anything still
on the wire, so delivering the socket first would reorder the stream. Only complete frames are
taken, so a link is never left half-read.

The drain's *mechanism* is pinned by
`CheckpointFreezePoints/a_frame_taken_while_establishing_another_link_is_still_delivered`, which
severs the socket after the drain so a delivery can only have come from the queue. Its *call
site* is **not**: deleting `service_established_links(target)` from `build_mesh` survives the
whole suite, because the drain only has an observable effect while a rank is genuinely blocked
in establishment with a peer sending to it — the same race that deadlocks, and so the same thing
that resists a deterministic test. The mutation is kept in the sweep as a known survivor rather
than dropped.

**Measured effect on the pass rate: none** — 4 peers stayed at 6–7 of 10, which is the point.
**Measured effect on a wedged job: every data link drains to zero.** Before, a wedged 4-rank
job held whole frames on three separate links. After, `ss` shows `rq=0` everywhere except one
link holding exactly 56 bytes — a **handshake**, which is the one thing `service_established_links`
cannot consume, because a link awaiting reconciliation opens with a bare handshake preamble
rather than a frame and only the blocking `exchange_handshake` inside `check_socket` can read it.

So the remaining cycle is: rank A is establishing link X; rank B has offered A a handshake on a
replaced link and is blocked in `exchange_handshake` waiting for A's in return; A will not
handshake with B until it finishes with X.

**Closing that means making the handshake one-way** — carried as a frame so any reader can
consume it, with neither end waiting for the other. That is attempt three, and it is not done:
the first version of it desynchronised the stream (fixed since, by the partial-read change) and
the second regressed `TransportRecovery` and was reverted rather than shipped half-verified. The
change is well understood and bounded; it needs a careful pass over the two suites that
impersonate the far end of a link, both of which encode the current wire format.

### What was fixed along the way, and what was not

Two real defects found while chasing this, both fixed and both worth having regardless:

* `DirectTCP::accept_one` rejected a reconnect from any rank it still held a descriptor for,
  alive or not. It now probes `TCP_INFO` and replaces a socket that is no longer ESTABLISHED,
  marking the link so the next establishment reconciles rather than starting clean.
* `read_all` reported a *partial* read that then stalled as `Timeout` — a soft condition — even
  though the bytes already taken are gone from the stream and every subsequent frame boundary
  is off by that many. It now reports a broken link, which is true and recoverable.

Neither closes the deadlock, because neither changes the fact that a rank inside `build_mesh`
reads nothing else.

An attempt to fix it by making the handshake one-way and asynchronous was **reverted**: it made
the failure rate worse (2 of 8 passing) and introduced stream desynchronisation through exactly
the partial-read path described above.

### What would close it

The design's own answer, and it needs both halves:

* **a progress engine** — one component owning every socket, never blocking the rank on a
  single link, so a rank rebuilding one link keeps draining the others;
* **directory-driven repair** — a rank closes and re-establishes on *observing* a peer's
  incarnation change, rather than waiting to trip over a dead descriptor.

The design document states both as normative and explains that the machine-checked delivery
result depends on the second. Neither is implemented, and the deadlock is the price.

**Until then: the checkpoint story is verified at two ranks and deadlocks intermittently above
that.**

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
