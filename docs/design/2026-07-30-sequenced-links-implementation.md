# Sequenced-link implementation rules

Direct and DirectTCP share their framed transport in `TcpChannelBase`.
`SequencedLink` manages sequence state and retention; `LinkFrame.h` defines the
wire codec. This document explains the implementation rules behind
Retain-and-Replay. The R1–R9 labels are retained so existing references remain
useful. The old membership implementation and superseded test instructions have
been removed.

Read the [design](2026-07-27-sequenced-incarnation-links-design.md) for the identity
and durability contracts, and [CLAUDE.md](../../CLAUDE.md) for builds and tests.
The current header is 42 bytes at wire version 4; the handshake payload is
30 bytes. Both `framed` and `recover_links` are optional, default-off flags.

## R1. Commit the payload before advancing the receive watermark

Header parsing establishes what data should arrive. It does not establish that
the receiver owns the data. Classify the header, read the full payload, then
commit it to the application buffer or a receive queue. Only then may
`next_received` advance and an ACK release sender retention.

Otherwise, a checkpoint between header and payload lets the handshake report a
message as received when its payload existed only in a discarded kernel buffer.
The sender can then prune the only remaining copy.

`CheckpointFreezePoints/a_freeze_between_the_header_and_its_payload_does_not_consume_the_message`
checks this ordering. The TLA+ model uses atomic frames and cannot expose this
partial-read defect by itself.

## R2. Restart a replaced frame at its header

A connection can fail during a header or payload read or write. Discard partial
connection state and replay from a complete frame boundary. On an inline receive,
restarting the copy may overwrite a partially filled application buffer; that is
safe because the operation has not yet returned and the receive watermark has
not advanced.

Never resume an old byte cursor on a new connection alongside whole-frame replay.
That would combine a partial original frame with a complete replayed frame.

## R3. Acknowledge one-way traffic

Piggybacked ACKs are insufficient when traffic flows in only one direction.
Binomial collectives create such links from three ranks onward. Without
standalone `FrameType::Ack` messages, successful receives eventually leave the
sender's retention window full.

`link_ack_interval` must allow an ACK before the window fills. While waiting for
space, `drain_acks` consumes ACK frames. When it peeks a DATA header, it may apply
the cumulative ACK carried by that header, but must leave the DATA frame for its
normal receive path. The link-liveness tests exercise small windows and one-way
traffic.

## R4. Handle SIGPIPE before creating hiredis connections

A restored process can first discover a dead Redis connection by writing to it.
Call `suppress_sigpipe()` before creating the hiredis context so the write reports
an error instead of terminating the process. The helper changes a default
SIGPIPE disposition and preserves an existing application handler or `SIG_IGN`.
See [TODO.md](../../TODO.md) for the separate S3 handler's inconsistent treatment
of `SIG_IGN`.

## R5. Retired incarnation rule

Earlier versions placed incarnation and policy fields in the sequenced handshake.
Wire version 4 removed them. There is no sequenced-link membership authority to
claim a new incarnation from, and restoring a process does not call the deleted
checkpoint hooks. `on_registered` initializes a channel after rank, size, and
communicator name have been assigned.

This change does not remove DrainTCP's independent incarnation checks. It also
does not make a fresh process a substitute for a checkpoint image.

## R6. Consume queued frames before newer inline frames

Socket servicing may receive a frame before the application asks for it and put
it in a lane queue. A subsequent receive must consume that queue before reading
a newer frame directly into the application's buffer. Recheck the queue after
every nested wait or servicing call because those calls may have filled it.

This matters especially for P2P traffic: consecutive same-sized sends can have
the same logical identity. FIFO queue order distinguishes their positions. An
identity comparison alone cannot detect delivery of frame 94 before queued
frame 93.

## R7. Treat connection replacement separately from ordinary I/O failure

Nested servicing can install a new socket while a read or write is in progress.
`LinkReplaced` tells the caller to restart at the frame header without charging
an ordinary repair attempt for work already completed by the servicing path.

Check a connection generation as well as its descriptor: the operating system
can reuse the same descriptor number for the replacement. Descriptor equality
alone does not prove that the stream is unchanged.

## R8. Preserve replacement connections during establishment

Replacement sockets can arrive while another operation is establishing a link.
The implementation must preserve three cases:

1. A replacement parked as pending survives a retry; retry cleanup must not
   discard a connection the peer already considers established.
2. `build_mesh` avoids directly adopting its target while establishing it, but
   nested `service_transport` can still adopt that target. Recheck whether the
   link is already installed rather than waiting only for a pending socket.
3. An aged suspect dialer needs bounded redial opportunities. Schedule attempts
   per servicing slice, not recursively inside `establish`. Keep the current
   socket until replacement succeeds, then clear suspicion. Do not replace a
   stream that an application call currently owns mid-frame.

These rules prevent both lost replacement connections and recursive
establishment loops.

## R9. Service large frames without blocking or corrupting the stream

### Stage partial input

A complete frame may be larger than the available socket receive buffer. Waiting
for the entire frame to become readable without consuming any bytes can prevent
the sender from completing it. The service path therefore retains partial header
and payload state across nonblocking calls.

When the readable-byte count stops changing across a poll slice, staged reads
allow progress. The decision must not depend on an autotuned `SO_RCVBUF` value
that can exceed the capacity actually available at that point. `fill_stage`
limits work to 256 KiB per call so one large frame does not monopolize servicing.

Only a complete payload enters a lane queue and advances the receive watermark.
Discard partial staging on connection replacement; replay supplies the frame
again.

### Give each stream one owner

`app_owns_stream` prevents the service path from consuming input belonging to an
active application receive. `outbound_frozen` is a **nesting depth**, not a boolean:
nested calls must not insert an ACK or another frame into a partly written frame.

A `POLLOUT` wait must still service the same peer when safe; otherwise a full
send buffer can prevent the reverse traffic needed to unblock it. A `POLLIN`
wait must avoid a second reader on the stream it owns. Deferred ACKs must be
reoffered on later servicing calls, lane delivery, and before another header wait.

### Bound repair and avoid busy loops

Parse only after a full header has arrived. Decode failures can justify bounded
replacement attempts, but must not create unlimited redial loops; the current
path allows two suspicion marks per connection.

An establishment loop should poll established sockets for further input only
when the previous servicing pass made progress. Otherwise, already-readable
input that the loop cannot consume causes a busy loop. After servicing, check
again whether the target link has been installed before waiting for it.

### Replay from stable copies

Nested servicing can process ACKs and prune the retained deque during replay.
Do not keep an iterator or reference into that deque across I/O. Fetch a stable
copy by sequence with `copy_retained`, revalidate each next sequence, and replay
one frame at a time.

## What the blocking implementation cannot do

Progress occurs during application communication calls. There is no background
engine servicing links throughout a compute phase. Discovery is socket-driven,
and a stopped peer can leave an established socket silent until a timeout or a
replacement connection causes progress.

Retention is bounded. A completed send remains an obligation until acknowledged;
an unacknowledged frame cannot be evicted merely to make room. Exhausting the
window can block a sender and eventually raise `Utils::Timeout`, which is
terminal for the communicator.

The implementation has no receiver CREDIT protocol or separate receive-reserve
budget. Complete queued frames and partial staged payloads consume process
memory. Do not infer a receiver-memory bound solely from the sender's retention
window.

See [TODO.md](../../TODO.md) for remaining defects, including an ACK write outside
its servicing exception handler and incomplete repair-state initialization.

## Verification

The Boost suites cover framing, recovery, operation identity, protocol validation,
liveness, and checkpoint positions. In particular,
`CheckpointFreezePoints/identity_is_still_enforced_on_a_frame_that_arrives_by_replay`
checks rejection of replayed data belonging to a different operation.

The [TLA+ models](../tla/README.md) check finite message-identity and directed-link
models separately. They do not check the full socket implementation or compose
identity with replay. End-to-end CRIU evidence and runnable commands are maintained
in [fmi-spot-migration](https://github.com/McLavish/fmi-spot-migration/tree/main/benchmarks/migration/criu-transparent).
