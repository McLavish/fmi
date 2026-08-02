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

### R6 — Delivery order is the lane's, not the socket's

> **Normative:** whenever a link's drain queue is non-empty, the application receives from
> the queue, oldest first. The inline read path may hand a frame to the application only if
> the queue for that lane is empty at the moment of delivery — checked again after every
> wait, not once at entry — and a frame read off the socket while older frames sit queued is
> parked behind them, never delivered.

The skip argument that stops `service_established_links` draining the link its caller waits
on is not transitive: a nested pump — entered, say, from a handshake replay to a third rank —
carries a *different* skip, and legitimately drains the very link the application is
mid-receive on. That is not a defect; it is the waiting-rank obligation doing its job. What
was a defect is that `recv_object` consulted the drain queue once, at entry. A frame drained
during the wait advanced `next_received`, so the *next* frame off the socket classified as
Delivered — and with p2p identity carrying no per-operation counter (see below), nothing else
could object. The application received round N+1 while round N sat in local memory.

Found by the diverse-shape sweep this spec previously lacked: p2p_ring at 7 ranks with 2 MB
payloads failed 3 of 30 trials, every one a silent one-or-two-round substitution on a link
between two ranks that were **not** checkpointed. A payload-carrying trace
(`FMI_LINK_TRACE=1`) pinned the interleaving — `accept->lane seq=93` directly followed by
`inline-deliver seq=94` — and a four-angle independent code audit converged on the same
mechanism from the source alone. After the fix the same seed runs 30/30 with 29 lane commits
each matched by an in-order lane delivery.

Mutations: `lane_is_checked_once_at_entry`, `inline_path_ignores_the_drain_queue`,
`header_read_never_yields_to_the_lane`.

### R7 — A replaced connection restarts mid-flight operations at a frame boundary

> **Normative:** `read_all` and `write_all` must fail with `LinkReplaced` — not continue —
> when the transport swaps the socket beneath them, and their callers resume from a frame
> boundary on the new connection without charging the repair budget.

`accept_one` may adopt a replacement connection from inside a nested pump while the
application is mid-frame on the old one. Both loops re-read `sockets[peer]` on every
iteration, so without the guard they would keep filling one buffer from two different byte
streams — a splice no later check can catch, because the result is a well-formed buffer of
garbage. The replaced link needs no repair: the peer that dialed replays from its retention,
so the reader restarts at the header and the writer's admitted frame travels via the replay.

Mutation: `replaced_link_keeps_the_old_byte_count`.

### R8 — Establishment must survive a freeze landing inside it

Three obligations, each found from a traced wedge specimen of a job checkpointed during
round 0 — while the mesh was still being built lazily:

> **Normative (a):** an incoming hello for a rank with a *parked pending connection* replaces
> the parked descriptor, exactly as it replaces a held socket. The reconnect is the evidence;
> a dialer only dials again after abandoning its previous attempt, and rejecting the retry on
> the strength of the corpse it left behind wedges both ranks.

> **Normative (b):** `build_mesh` adopts accepted connections for every rank EXCEPT its
> target while it waits. The target's pending entry is the wait's completion condition,
> popped by `establish()` — adopting it from build_mesh's own accept path broke that contract
> loudly (13 suite failures the one time it was tried). Everyone else was acknowledged at
> accept and has moved on to sending frames nobody would otherwise read. The pop contract is
> deliberately *tolerant* now, not exclusive: `service_transport` — reachable from INSIDE an
> in-flight establishment through servicing → reconcile → `write_all` → pump — excludes
> nothing, so the target can legitimately be adopted out from under its own establishment
> (observed in the trial-0 specimen of the post-restore stall corpus). `establish()` treats
> "no pending entry but the socket is held" as a completed establishment and returns the
> held socket; skipping the target in `service_transport` instead was considered and
> rejected, because it strands the target's sender — acknowledged at accept, its handshake
> and replay pushed into a connection nobody reads for as long as the establishment lasts.

> **Normative (c):** a link that servicing has observed dead for over a second, whose DIALER
> this rank is, gets one bounded re-dial attempt per pump slice — never from inside an
> establishment (`establishment_depth` guards), with the dead descriptor closed only at the
> moment its replacement is dialed. The listener side's whole obligation remains serving its
> accept queue: DirectTCP dials down, so every dead link has exactly one responsible dialer.

The cycle (c) breaks: a restored rank that may only *accept* a link waits for its dialer; the
dialer only touches its dead descriptor when its application does; and the dialer's
application is blocked in a chain rooted at the waiting rank. Servicing had peeked EOF on the
dead link every 20 ms and treated it as "nothing to drain".

A caution that cost a full round: the FIRST version of (c) closed dead links at observation
time and re-dialed from inside `service_established_links` — the loop `build_mesh` itself
drives. It took the failing shape from 4/6 to 1/8. Both mistakes are structural, not tuning:
observation-time closing forces readers through re-establishment for deaths that need no
repair at all (a peer that finalized after its last message — which is also why the drain
queue must be consulted *before* `check_socket`), and establishment reentrancy was never
proven safe. Measured after the corrected version, honestly: **the wedge was reduced, not
closed.** Pooled across seeds, the failing shape went from 19/26 (73%) before the re-dial to
20/24 (83%) with it — and the remaining ~15-25% post-restore stalls at 7 ranks on
`variable_payloads` stood as an open defect until their specimens were finally traced (four
failing trials, seed 71, `FMI_LINK_TRACE=1 --keep`). The residual mechanism was **not** an
establishment gap at all — it is what R9 below fixes, and (c) itself was implicated: a redial
repairs a link from inside a wait on a *third* peer, then leaves the repaired link readable
by nobody but servicing, which until R9 could not take a frame larger than the socket buffer.
Suspicion hygiene that the specimens also forced: a replaced link sheds the dead connection's
`link_suspect_since` mark (`note_link_replaced`), and a link the application is mid-frame on
is never a redial candidate — the application actively consuming it is stronger liveness
evidence than any peek, and without both rules the aged redial tears down healthy links.

### R9 — Servicing must be able to drain a frame larger than the socket buffer

> **Normative:** `service_established_links` may take a frame in two ways: whole, exactly as
> before, when every byte is already buffered; or **staged** — header consumed, payload
> accumulated across servicing passes in a per-link `InboundStage`, never blocking, committed
> (`accept` → lane, ack) only when the last byte is in local memory. A frame is staged only
> when it can *never* be wholly buffered (its size exceeds a conservative floor of the
> stopped arriving while incomplete — FIONREAD unchanged across at least one full pump
> slice, the closed-window signature; the time floor matters because two servicing passes
> can be microseconds apart inside an establishment loop); every frame still growing keeps
> the whole-frame path byte for byte. Deliberately NOT judged against `SO_RCVBUF`: receive autotuning inflates the reported
> size while a reader drains fast, and trusting it stranded a 2 MiB frame behind a ~512 KB
> closed window, silently, in the first attempt.
> An incomplete stage is unacked by construction and is **discarded whenever the connection
> is replaced** (`note_link_replaced`) — the peer still retains the frame and replays it
> whole. While the application is mid-frame on a link (`app_owns_stream`, armed at the first
> header byte, released at every frame boundary), servicing must not touch that socket at
> all; symmetrically, the application's header read finishes an active stage before
> consuming a raw byte, and `drain_acks` advances a stage rather than peeking through it.

The failure this fixes is the R8 residual — the last ~15-25% of post-restore stalls at 7
ranks on `variable_payloads`. All four traced specimens share one triple, and the six passing
trials of the same run lack it: (1) a rank blocked in `write_all` pushing a post-restore
replay that contains a 2 MiB frame, (2) the receiving end's diagnostic showing that frame
jammed behind a ~128 KB socket buffer (`rq≈98k-127k`, never moving), (3) the receiver parked
in a wait on a *third* rank — an arrangement the aged redial (R8(c)) and accept-replace both
create routinely. The old whole-frame rule made such a frame undrainable by anyone except the
application receiving on that exact link, so the writer jammed forever and the wait-for graph
closed. The passes passed because every peer happened to repair the link from its own
application receive path, where the replay drains inline.

Three obligations that make staging compose, each found by adversarial review before it could
be found by a wedge:

- **Waits must wake for staged bytes.** `build_mesh`'s poll now includes the established
  links; a listener-side establishment used to sleep until its deadline, which was harmless
  when servicing left oversized frames alone and is fatal once a peer's half-delivered frame
  is waiting on this loop's servicing pass. Deterministically pinned by
  `LinkLiveness/a_frame_larger_than_the_socket_buffer_drains_while_its_receiver_waits_elsewhere`,
  which wedged on exactly this before the poll change.
- **A POLLOUT pump does not skip its own peer.** The pump's skip protects the caller's
  inbound cursor, which only a POLLIN wait owns; `app_owns_stream` protects it on every other
  path. With the skip in place for writers, two ranks replaying oversized suffixes to each
  other would each block writing and neither would ever stage the other's frames — a mutual
  wedge staging alone cannot break.
- **`fill_stage` is bounded per call** (256 KB): against a blocked writer on loopback the
  sender refills as fast as the receiver drains, and an until-EAGAIN drain inside someone
  else's 20 ms slice would consume the aged redial's entire budget in one visit.
- **Nothing may write to a peer that any in-flight `write_all` is mid-frame toward**
  (`outbound_frozen`, a per-peer DEPTH — a scalar slot was tried first and adversarial
  review produced the counterexample: a nested write to a different peer un-froze the outer
  peer, and the splice happened one level down). Servicing wants to write — a standalone ack
  after a drained frame, a handshake-plus-replay for a reconcile debt — and once a POLLOUT
  pump services its own peer, those writes would land in the MIDDLE of the frame the caller
  is writing. Both are deferrable by contract: the ack is best-effort and re-offered, the
  reconcile mark stays set. The old skip was silently providing this guarantee alongside the
  inbound one; splitting the two (claim for inbound, freeze for outbound) is what makes the
  POLLOUT no-skip rule sound.
- **A deferred ack must have a re-offer path that does not depend on more traffic.** Every
  commit-time ack offer fires only when another frame arrives, and when the suppressed ack
  is precisely what opens the peer's window, no frame ever does. Acks are therefore
  re-offered (self-gated by `ack_due` and the freeze) from every servicing pass, from lane
  deliveries, and by a receiver immediately before it parks in a header wait — that last one
  because a POLLIN wait's servicing skips its own peer, so nothing else would ever ack the
  very link being waited on.
- **Mid-operation replacement is detected by a per-link GENERATION, not by fd number.** The
  kernel reuses the lowest free descriptor number, and an accept-close-accept inside one
  servicing pass can hand the replacement the same number the dead link had — the forensics
  corpus caught a redial doing exactly this. `note_link_replaced`, `adopt_link` and
  `check_socket` bump the generation; `write_all`/`read_all`/`read_header_yielding` compare
  it alongside the fd.
- **Decode-failure suspicion is capped at two per connection.** A desynchronised stream
  heals through the redial-and-replay the suspicion triggers; a content-level rejection (a
  version or size the peer legitimately produced) reproduces identically after every replay,
  and an uncapped mark turns one loud error into an endless redial storm. After two
  attempts servicing falls silent and the application's own read reports it the loud way.
- **An establishment wait polls the established links only after a servicing pass that made
  progress**, and sleeps at most one slice otherwise: a readable socket servicing refused to
  drain (a growing frame, an app-owned stream, a capped decode failure) would otherwise
  turn the poll into a busy loop, while never coming back at all is how the original
  listener-side stall slept through a jammed frame.
- **`build_mesh` re-tests `have_link(target)` after its servicing pass, not only at the loop
  top.** The servicing pass nests pumps, pumps run `service_transport`, and `adopt_pending`
  can complete the very establishment the loop is waiting on from inside its own body. The
  traced specimen (variable_payloads, 5 ranks, seed 113): a restored rank's frozen bcast
  receive repaired into `build_mesh(3)`; one nested sweep adopted link 3, paid every debt,
  and parked the exact frame the application wanted in `links[3].lanes[Collective]` — then
  the body fell through to its poll with 58.6 s of budget, every peer socket drained, and
  four ranks transitively blocked on the sleeper. Their 60 s receive deadlines expired
  ~50 ms before the sleeper's own deadline would have woken it. No cycle, no loss: a sleep
  on an already-satisfied condition. Predates R9 — the same top-only test was always there —
  and is plausibly part of the original residual family.
- **Replay never iterates the live retention.** Writing a replay frame pumps; the pump
  services; an inbound frame's piggybacked ack — or a nested reconcile — prunes the very
  deque being iterated, and the dangling reference puts freed-heap bytes on the wire as a
  perfectly silent stream corruption (found via a hexdump of a 60-second `BadVersion` decode
  loop: heap pointers where a header should be). `exchange_handshake` replays by sequence,
  one re-validated `copy_retained` frame at a time; a frame pruned before its turn is one
  the peer just declared it holds, so skipping it is correct. This hazard predates R9 (any
  nested pump with a different skip could reach the prune) but the POLLOUT rule made it
  routine.
- **A full 72-byte peek that fails to decode marks the link suspect.** The same bytes will
  be there forever — waiting cannot realign a stream — so servicing hands the link to the
  aged redial (dialer side) while the application's own read of the same bytes throws loudly
  and repairs (listener side). Before this, a desynchronised link wedged silently for the
  job's whole deadline with 400 KB sitting unread.

Receiver-side memory: a staged frame plus its lane copy live on the heap, so the kernel
socket buffer is no longer the receive-side bound for oversized frames. The true bound is
transitive: a sender admits nothing past `link_retention_limit_bytes`, so no link can ever
have more unacked bytes in flight — staged, laned, or buffered — than the sender's own
retention cap.

### A property R6 leans on: p2p identity does not number operations

`p2p_identity(dest)` is the same for every same-size message between the same pair —
`same_identity` distinguishes lanes, op kinds, collectives, roots and lengths, but not the
first send to a peer from the thousandth. For repeated point-to-point traffic, **transport
sequence order is the entire defence** against wrong-round delivery; that is what makes R6
normative rather than defensive. Numbering p2p operations in the envelope would add a second,
independent check — at the cost of forbidding legitimate schedule drift between sender and
receiver of the kind collectives never produce. Not done on this branch; recorded so nobody
mistakes the silence of `same_identity` on p2p streams for coverage.

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
directory-driven repair, the restore budget. Those were specified and model-checked at the
time (in `docs/tla/Membership.tla`, since deleted with the epoch protocol) but never written,
and the contract they belonged to no longer exists. What is
implemented is the part the link layer needs to be unambiguous: the incarnation, its fencing
rules, and where the number comes from.

## Where the suites can actually run

Worth knowing before reading a red result as a regression, because two different things produce
one here.

The `-DFMI_ENABLE_TCPUNCH=OFF` build runs **13 of the 15 suites clean**, including every suite
this branch adds. `Communicator` and `FaultTolerance` are the exceptions: both ask for the
`Direct` backend, which that build does not register, so `Channel::get_channel` throws
`"Direct backend was disabled at build time"` inside a forked rank and the process aborts
rather than failing a case. That is **pre-existing** — the same two suites abort identically at
the branch point (`428b9cb`), verified by building it in the same configuration. It is a
property of the test configuration, not of this work, and it is the reason the mutation sweep
runs a named suite list rather than the whole binary.

Those two suites therefore have to run in a `TCPUNCH=ON` build, where they inherit TCPunch's
documented flakiness. Measured on this machine, four full runs each of
`Channels,Communicator,FaultTolerance`:

| | run 1 | run 2 | run 3 | run 4 |
| --- | --- | --- | --- | --- |
| branch point `428b9cb` | 0 | 19 | 29 | 42 |
| `feat/sequenced-links` | 3 | 5 | 7 | 30 |

The failing case names differ almost completely between consecutive runs of the *same* binary,
and every one of them is a `Direct` collective. The branch's numbers are lower, but with a
spread this wide and n=4 that is **not** a claim of improvement — the honest reading is that
both are dominated by the same pre-existing flakiness and neither is distinguishable from the
other. What can be said is that no failure in either column implicates the sequenced link
layer: with TCPunch out of the build, the same collectives pass.

## Evidence

| claim | how it is checked |
| --- | --- |
| identity fields are each necessary | `docs/tla/MessageIdentity.tla`, 10 configs; TLC exhibits a silent substitution for every weaker envelope |
| the link state machine is safe and live | `docs/tla/SequencedLink.tla`, 15 configs including 4 freeze positions and 6 deliberately-broken variants |
| ~~the membership contract is live~~ | *Retired.* `Membership.tla` and its 11 configurations were deleted with the Redis-coordinated epoch migration protocol they modelled; the library no longer has a membership contract. |
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

Two cautions from running it, both worth more than the score. First, a test can pass for the
wrong reason and look identical to one that passes for the right one: the case added to pin
"a blocked rank still accepts" originally let `build_mesh` do the accepting, so the mutation it
was written to kill sailed straight through. Only reordering it — establish the first link
*before* blocking — earned the kill. Second, `establishment_reads_no_other_link` was carried as
a documented known survivor on the reasoning that no test could observe it. The criu sweep kills
it 0/8 at both 4 and 8 ranks. The label was a statement about the tests that existed, not about
the code, and it is worth suspecting any other survivor of the same thing.

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

Each of those six obligations is now pinned by a mutation, and the pinning is unambiguous:
removing any single one takes the 4-rank sweep from 8 of 8 to **0 of 8**. Not a degraded
success rate — no run survives a checkpoint at all. Five of the six are killed by the criu
sweep rather than by the Boost suite, which is the honest home for them, since the property
only bites once a restore forces several links to be rebuilt at once.

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

## What the diverse-shape sweep then found

The 48/48 result above was real but narrow: one program shape, small payloads. Parameterising
the checkpoint subject by app shape (six new shapes, `runbooks/criu-transparent-checkpoint/shapes/`)
and pushing payloads to 2 MB found two safety defects and one composition defect that the
original battery could not see:

1. **Silent round substitution under nested servicing** — R6 above. ~10–20% of large-payload
   ring trials at 7 ranks; zero at small payloads, which is why 48/48 missed it.
2. **Stream splice on mid-frame link replacement** — R7 above. Latent; found by audit rather
   than sweep.
3. **Framing was incompatible with FT migration**: `reconfigure_for_epoch` reset a survivor's
   link *to* a moved rank but let the moved rank itself — in-place or CRIU-restored, appearing
   in its own moved set — keep stale counters toward every survivor (`sequence gap: expected 0
   but received 31`). Fixed by the self-moved branch: a rank that finds itself in the moved
   set resets every link. Pinned by
   `LinkLiveness/a_moved_rank_starts_every_link_afresh_after_reconfigure` and mutation
   `moved_rank_keeps_links_to_survivors`. Verified: the whole `FaultTolerance` suite is green
   with framing off (repeatedly), and with framing on the pre-fix failures — deterministic
   `sequence gap: expected 0 but received 31` every run — are gone. What framing-on shows
   instead is an INTERMITTENT wedge (~1 in 8 runs of the suite,
   `transparent_migration_cut_timing_stress`, a rank wedged in a Timeout waiting on a peer
   mid-cut): the same post-freeze establishment-liveness family as R8's open residual, on the
   `Direct` backend, which has no R8(c) re-dial (TCPunch pairing establishes differently).
   Tracked with the R8 residual as one open defect.

The general lesson mirrors the mutation-sweep cautions: the evidence for "arbitrary FMI
programs" must come from programs the branch did not write its configs around. The sweep now
defaults to enough rounds for a checkpoint to land and scores a trial where none landed as
SKIP, not FAIL — three shape authors independently misread the old scoring as protocol
failures.

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
- **No CREDIT.** The design's separate credit dimension is not implemented, so a divergent
  peer's backlog is bounded by TCP plus, since R9, the heap that staging and the lanes may
  hold — transitively capped by the sender's `link_retention_limit_bytes`, not by receiver
  policy.
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
