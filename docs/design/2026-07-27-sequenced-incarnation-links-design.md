# Sequenced links: identity and Retain-and-Replay

This document retains the reasoning behind the July 2026 design and describes
how it relates to the current library. The original proposal also specified a
Redis membership protocol, an autonomous progress engine, and several deployment
modes. Those parts were removed or never implemented; their obsolete plans have
been cut here. The dated filename is retained for existing references.

For socket-level requirements, read the
[implementation rules](2026-07-30-sequenced-links-implementation.md). For formal
results, read the [TLA+ guide](../tla/README.md): there are **two models and 26
configurations**, and they do not prove the complete migration system correct.

## Purpose

A checkpoint can interrupt a rank inside a send, receive, or collective. Restoring
its memory does not restore the connection as a usable communication channel:
data may have been in kernel buffers when the connection was discarded.
Retain-and-Replay keeps enough information in process memory to resend that data
and distinguish it from messages already received.

Message identity is a separate problem. A sequence number establishes order on a
link; it does not establish that both ranks are executing compatible operations.
The receiver must also check which operation a frame belongs to.

Waiting for every rank to reach a collective boundary cannot solve the general
case. A rank may already be blocked in a receive whose matching send has not yet
been issued. The migration mechanism must support that execution state.

## Normative contract 1 — message identity

The communicator establishes an `OperationScope` around each public operation.
The channel reads its thread-local identity without adding parameters to the
`Channel` interface. Framed TCP checks the following tuple:

| Field | Meaning |
|---|---|
| `lane` | Application point-to-point traffic or collective traffic |
| `op_kind` | Send, broadcast, barrier, gather, scatter, reduce, all-reduce, or scan |
| `collective_index` | Per-communicator collective ordinal; zero on the P2P lane |
| `root` | Collective root, or the **destination rank** for P2P |
| reduction flags | The operation's commutativity and associativity |
| `payload_length` | Expected payload size |

On the P2P lane, both endpoints must use the destination for `root`. “The peer”
would mean different ranks at the two ends and reject a valid receive.

The collective counter belongs to the communicator because its successive
operations can use different backends. Counters on individual channels would
lose the order between operations sent through different channels.

### Why a sequence number is insufficient

Consider these incompatible operation orders:

```
rank0: bcast(root=0); barrier()
rank1: barrier();     bcast(root=0)
```

At two ranks, a one-byte broadcast and a barrier can produce same-sized traffic
on the same directed link. Both are the rank's first collective, so a lane and
collective index alone cannot distinguish them. `op_kind` detects the mismatch.
Reduction flags matter too: they can select different algorithms even when the
operation name, root, index, and size agree.

Within its finite bounds, `MessageIdentity.tla` finds counterexamples for the
weaker envelopes. The two-rank model does not establish that `root` is necessary;
that requires a larger model. Its full-tuple result must also be read alongside
the aligned-program and rejection-witness checks described in the TLA+ guide.

The identity scheme assumes the existing collective algorithms' message order.
If an optimized collective introduces additional messages on one directed pair,
check whether the tuple and FIFO order still identify each internal step. There
is no algorithm-phase field in the current header.

### Job identity and policy agreement

Use a unique `comm_name` for every job run. The original proposal's separate
universal `run_id` and policy-fingerprint service were not implemented. A reused
name can collide with retained store objects or discovery state.

Ranks must agree on communicator size, compatible backend settings, message
sizes, and reduction flags. Framed identity checks detect mismatched traffic
that reaches a receiver; they cannot make ranks that selected different
backends communicate. The removed fingerprint handshake does not validate
job-wide policy agreement.

### Store backends

Redis and S3 use operation-qualified object keys rather than TCP frames. Under
`recover: true`, checkpointed process counters resume with the process, writes
can be retried, and finalize leaves objects for slower peers. Redis uses TTLs;
S3 requires bucket lifecycle expiration. Barrier recovery probes the exact
marker keys for the ranks instead of counting an unscoped listing.

The TLA+ message-identity model uses a FIFO stream. It does not model these keyed
stores, their expiry, or retry behavior. See [CLAUDE.md](../../CLAUDE.md) for the
implemented store contract and its configuration.

## Normative contract 2 — transport durability (Axis C)

Each directed link has send and receive sequence counters and a retained set of
sent frames. The receive watermark is an **exclusive upper bound**: every
sequence below `next_received` is held by the receiver. A cumulative ACK permits
the sender to release that prefix.

### Durable versus connection-scoped state

Here, “durable” means held in process memory that the checkpoint captures. It
does not mean written to independent stable storage.

| Preserved in the checkpoint | Discarded when a connection is replaced |
|---|---|
| Sequence counters and retained payloads | Old socket descriptor |
| Complete frames in receive queues | Partial-frame parser state |
| Application buffers and call state | Partial-frame write cursor |

After reconnect, the endpoints exchange sequence state. The sender replays the
suffix the receiver still needs. A request below the sender's retained range, or
beyond what it has sent, is an impossible state and must fail explicitly.

### Ordering invariants

1. Retain an immutable payload copy before writing any of its DATA bytes.
2. Acknowledge a frame only after its complete payload is in application memory
   or a receive queue captured by the checkpoint.
3. Release retention only after validating the peer's cumulative ACK.

A parsed header is not a received payload. Advancing the watermark at header
parse would allow the sender to discard the only complete copy before the
receiver has read the data.

| Checkpoint position | Recovery |
|---|---|
| Payload retained, transmission not started | Replay the retained frame |
| Header or payload partly written | Discard the connection cursor and replay the whole frame |
| Data in the network or kernel receive buffer | Replay anything not acknowledged |
| Header or payload partly read | Discard partial parser state and restart at the frame header |
| Whole payload committed, ACK not sent | Reconcile the receive watermark; do not deliver it twice |
| ACK in the network | Reconcile the watermark and release the acknowledged prefix |

The model treats wire frames atomically. Partial headers, partial payloads, and
interleaved writes therefore require implementation tests in addition to model
checking.

### ACKs, memory limits, and progress

ACKs normally accompany reverse traffic. One-way links also need standalone ACKs;
otherwise the sender eventually fills its retention window despite successful
receives. FIFO receive queues must be drained before newer matching frames are
delivered directly to an application buffer.

The retention limit is an **admission limit**, never permission to evict an
unacknowledged message. A sender waits for space and eventually times out if space
cannot be recovered. The default frame window is 256. Payload-size and byte
limits also apply; see [TODO.md](../../TODO.md) for configuration and diagnostic
gaps around oversized messages.

The original design separated ACK from receiver CREDIT and reserved capacity for
bidirectional draining. The implementation has no CREDIT protocol or separately
configured receiver reserve. Do not use the original proposed memory-bound
formula as a bound on the implemented receive queues.

Progress is application-driven. Blocking socket operations service other links,
accept replacement connections, and process ACKs. There is no autonomous engine
that progresses every connection while a rank performs arbitrary computation.
The model's fair restore and handshake actions are assumptions, not proof that
this progress mechanism always meets them.

## Normative contract 3 — membership state machine (Axis D)

**Retired.** The original Redis epoch/incarnation membership protocol and its
`Membership.tla` model were removed. The sequenced path now discovers peers through
its transport, and wire version 4 carries no incarnation pair or policy
fingerprint. Local Drain has its own control protocol and still uses
incarnations; see [its design](2026-08-11-neighborhood-drain-protocol.md).

A fresh process cannot replace a checkpoint restore: it lacks the application
state, operation counters, and retained messages. Retain-and-Replay also does not
handle a permanently lost rank, a lost image, or competing restores of one rank.

## Normative contract 4 — checkpoint mechanics (Axis B)

The external driver performs CRIU dump, image transfer when needed, restore, and
resume. Retain-and-Replay experiments use `--tcp-close` on dump and restore so
FMI rebuilds the discarded connections. These guarantees concern FMI-managed
connections; application-owned sockets need their own recovery mechanism.

The initial [CRIU experiments](2026-07-30-criu-mechanics-findings.md) established
same-host process behavior. Current same-host and cross-host campaigns are in
[fmi-spot-migration](https://github.com/McLavish/fmi-spot-migration/tree/main/benchmarks/migration).
DirectTCP's relocation handling refreshes the listener, registry client, and
advertised address after a cross-host restore. Leave `advertise_host` empty for
that path.

## Current wire format and configuration

`include/comm/LinkFrame.h` defines a little-endian, **42-byte version-4 header**:
magic, version, lane, operation kind, reduction flags, frame type, collective
index, root, payload length, transport sequence, and cumulative ACK. Handshake
frames carry a **30-byte payload** containing magic, version, `next_send_seq`,
`next_expected_seq`, and `lowest_retained`. Link snapshots use version 3.

Set `framed: true` and `recover_links: true` in the selected Direct or DirectTCP
backend block. These remain optional and default to false. The old
`fault_tolerance.link` configuration and staged rollout instructions are obsolete.
Do not mix wire versions within a job; rebuild and deploy all ranks together.

## Verification status

The [TLA+ results](../tla/README.md) cover message identity and a single directed
reliable link separately, at finite bounds. They do not check their composition,
actual socket I/O, deadlines, discovery, or Local Drain. C++ tests cover additional
paths, including partial frames and identity checks on replayed traffic. Migration
campaigns provide end-to-end evidence for the workloads and environments tested.
None of these results establishes correctness for arbitrary unbounded programs.
