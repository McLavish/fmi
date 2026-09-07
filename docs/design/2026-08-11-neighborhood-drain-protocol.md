# Local Drain: the neighborhood-drain protocol

`DrainTCP` supports planned checkpoint and migration by moving socket data into
process memory before CRIU runs. This guide describes the implemented channel,
control protocol, and same-host and cross-host migration paths. Known defects
from the September 2026 review are identified below and tracked in
[TODO.md](../../TODO.md).

## 1. What this is

Local Drain sends ordinary payload bytes during normal operation. When a rank
migrates, that rank and its directly connected peers half-close their connections
and read them to EOF. Any data the application has not yet consumed goes into
process memory, where CRIU can capture it. The rank then stops with its FMI
sockets closed.

The application and collective algorithms need no checkpoint calls. A survivor
pauses communication with the migrating rank while retaining its other links.
Ranks without a connection to the migrator record the leave notice so future
connection attempts wait for restore.

This avoids Retain-and-Replay's per-message frame headers, ACKs, and retained
copies. It requires cooperation before a connection is lost and does not recover
an unplanned process failure. “Socketless” describes the supported rank process:
an application that opens unrelated sockets must arrange their handling too.

## 2. Where it sits in the code

```
Channel → PeerToPeer → DrainTCP
```

`PeerToPeer` implements collectives using send and receive primitives. DrainTCP
implements those primitives with byte-offset resume across a drained connection.
`TcpChannelBase` instead restarts a replaced framed operation at its header; its
I/O loops cannot directly supply the drain contract. Its framing is optional,
however, so avoiding framing alone does not require a separate base class.

| Component | Responsibility |
|---|---|
| `DrainTCP` | Peer sockets, byte counters, drained buffers, and migration transitions |
| `DrainProtocol.h` | The reconnect record and `DrainParticipant` interface |
| `MigrationTrigger` | Process-wide signal handling and migration-request thread |
| `DrainCoordinator` | Control-plane interface, with a Redis implementation and test fakes |
| `PeerRegistry` | Hiredis operations for discovery, events, and leases |
| `TcpEndpoint` | Address, nonce, socket, and codec helpers |

Some listener and connection-establishment code is still duplicated between
DirectTCP and DrainTCP. See TODO §2 for proposed shared helpers.

## 3. Steady state

### 3.1 Data path

Each peer has a `LinkState` containing a mutex, condition variable, descriptor,
byte counters, drained `inbound` data and cursor, migration state, peer incarnation
and epoch, and an error for the application thread to report.

`send_object` and `recv_object` hold the link lock for one nonblocking syscall or
one copy from `inbound`, then release it before waiting. When they detect a
possible drain, they can also capture already-readable bytes and close the socket
under that lock. The operation's stack variable `moved` records its byte offset.
After migration, the loop checks the current link state and continues there.

Receives consume `inbound` before opening or reading a socket. Once consumed, the
buffer is freed. `bytes_sent` counts bytes accepted by the kernel;
`bytes_received` counts bytes copied into the application or `inbound`. These
counts describe completed transfers, not requested transfers.

### 3.2 The hello record

Each endpoint sends a 56-byte, big-endian `ResumeRecord` when establishing or
re-establishing a connection:

| Field | Check |
|---|---|
| Magic `FMID` and version | Recognized protocol |
| Sender and receiver rank | Intended endpoints and direction |
| Hash of `comm_name\|lo-hi` | Intended communicator and rank pair |
| Listener nonce | The dialer reached the advertised listener, rather than a recycled port |
| Sender incarnation | The connection does not belong to a superseded process incarnation |
| `bytes_sent`, `bytes_received` | Each endpoint's sent count equals the other's received count |

The counters detect loss or duplication at a migration boundary. They do not
provide per-message identity checks. The record is sent once per connection;
normal messages have no additional wire header.

### 3.3 Establishment and the control thread

Discovery uses Redis hash `fmi:drain:<comm_name>`, mapping ranks to
`ip:port:nonce`. Higher ranks dial lower ranks. Connection attempts poll for the
peer's registry entry and use `max_timeout` as their normal budget.

A per-channel control thread accepts connections and refreshes registry state.
It waits on the listener and a wake pipe, so a compute-bound application can
still accept peers. An armed channel also has a trigger thread that consumes
control events and migration requests.

Dialing holds a shared `establish_gate`; draining requires it exclusively. The
drain also parks the accept thread and closes the listener before locking links.
This prevents a new connection from being installed during the checkpoint cut.

### 3.4 What the steady state costs

Local Drain adds no per-message protocol bytes or retention copy. It does add
per-chunk lock operations, byte accounting, a control thread, and—when armed—a
trigger thread. Discovery and control-plane polling also cost work. Their measured
overheads are in fmi-spot-migration's `benchmarks/overhead/`; “zero added wire
bytes” is not a claim of zero total overhead.

## 4. The migration runtime

### 4.1 Trigger

`MigrationTrigger` is process-wide. Only one armed drain channel per process is
supported; a second raises `std::logic_error`. `on_registered` arms the channel
once its rank, peer count, and communicator name are known.

`trigger` selects signals, control events, both, or neither. A signal request uses
`sigqueue(pid, SIGRTMIN + drain_signal_offset, epoch)`. A handler protects threads
that existed before arming; the trigger thread uses `sigtimedwait` and checks the
handler's recorded request. Signal requests from older epochs are discarded.
Pending signals are cleared when attaching and finally detaching.

A control request is a `migrate` event in the coordinator stream. **Its epoch is
currently not checked**, so an unread command for an old epoch can cause another
migration after restore. The saved stream cursor only protects already-consumed
entries. This is an open bug, not an intended exception to epoch filtering.

The atfork handler disarms the child. A separate open defect concerns an inherited
mutex that it does not reset; see TODO §1 before relying on child re-arming.

### 4.2 Control plane

The Redis coordinator maintains three keys:

| Key | Purpose |
|---|---|
| `fmi:drain:<comm>:members` | Published rank, epoch, address, and nonce records |
| `fmi:drain:<comm>:events` | Stream of `leaving`, `sealed`, `restored`, and `migrate` events |
| `fmi:drain:<comm>:batch` | A `SET NX PX` lease allowing one migration batch at a time |

A stream preserves events that a stopped process cannot consume immediately.
The restored process resumes from its saved cursor. Leave notices use incarnation
and epoch checks to avoid applying old state to a new connection; local migrate
requests still need the missing epoch check described above.

A self-initiated migration obtains and releases its own lease. If the request
names a batch, the driver owns the lease and the rank neither takes nor releases
it. The lease uses `batch_lease_ms`; registry and event keys use `registry_ttl_s`.

### 4.3 The migrator sequence

The trigger thread performs these steps:

1. Acquire `establish_gate` exclusively within `establish_yield_ms`.
2. Publish `leaving`; fail the migration if the control plane is unavailable.
3. Park the control thread and close the listener.
4. Lock every peer link in ascending rank order, including unestablished links.
   Half-close **all** live sockets with `shutdown(SHUT_WR)`, then use one poll loop
   to read **all** of them to EOF, appending bytes to `inbound`. Close each socket
   after EOF.
5. Publish `sealed`, including byte counters and the last-read stream ID.
6. Disconnect the registry and coordinator.
7. Call `malloc_trim(0)` and flush standard I/O.
8. Raise `SIGSTOP` while retaining all link locks.

Half-closing all sockets before draining avoids two migrating ranks waiting on
each other in opposite pairwise order. TCP delivers FIN after preceding bytes;
reading to EOF therefore captures that direction's remaining data.

Silence is **not** EOF. A drain that exceeds its quiet-period or overall limit
fails with an incomplete-drain error and breaks the channel; it must not proceed
to a checkpoint. At a successful stop, each peer's sent count equals the local
received count. Reconnect validates the equality again.

### 4.4 Survivors

A survivor receiving `leaving` locks only that peer's link, half-closes it, drains
it to `inbound`, and closes it. Two message orderings need explicit handling:

- **FIN before the notice:** record a tentative drain, preserve readable bytes,
  and wait for confirmation. If no notice arrives within `drain_grace_ms`, report
  an unplanned connection failure.
- **Notice after restore:** reject an old leave notice when the link or recorded
  peer epoch already belongs to a newer incarnation. Otherwise, it could tear
  down a valid replacement connection.

A dialer rechecks draining state before installing a connection. The acceptor
allows the restored peer's new connection. Application threads retain their byte
offsets while waiting. `migration_max_ms` bounds the migration wait.

### 4.5 Restore

Execution continues after `raise(SIGSTOP)` when the driver restores and resumes
the process. The trigger thread:

1. Advances the epoch and incarnation.
2. Binds a fresh listener and resolves the advertised address before publication.
3. Republishes membership and emits `restored`, reconnecting Redis clients as
   needed.
4. Resumes the control thread and clears local drain state. Connections remain
   closed until application traffic needs them. Links to still-migrating batch
   peers keep their migration window open until those peers restore.
5. Releases link locks last, then lets application threads resume.

Restore errors are stored on links for the application thread to throw.
`rehearse_migration_in_place(hold_ms)` follows the same path with an optional hold
instead of an external CRIU stop and restore.

### 4.6 Migration time accounting

The intended policy excludes migration time from `max_timeout`. Each link's
`migration_ms_total` accumulates completed migration windows; each operation
compares that count with its entry baseline and credits the difference.

This handles operations already waiting when the migration begins. A September 7
reproduction found a gap: an operation can start its clock before taking the
link mutex but record its baseline only after a drain releases it. Time blocked
on that mutex is then charged to the normal timeout. A 600 ms rehearsal with a
200 ms transport budget reproduced immediate post-restore timeouts for send and
receive. See TODO §1 for the proposed fix and regression cases.

## 5. Unsafe windows

| Request timing | Handling |
|---|---|
| Before arming | Pending signals are discarded; the driver must target an armed rank |
| During establishment | Drain waits for the exclusive gate; dial retries yield when a drain is pending |
| During another migration | The channel's migration mutex serializes execution; the batch lease limits overlap |
| During finalize | Finalize joins the trigger thread before tearing down the channel |
| In a forked child before re-arming | The trigger is disarmed, subject to the known mutex-reset defect |

The protocol can drain between chunks of a send or receive, inside a collective,
or while application mutexes are held. Successful draining does not require all
ranks to reach the same collective boundary.

## 6. Failure model

All participating ranks must remain alive through the planned drain. There is no
abort protocol and no retained copy for reconstructing an undrained lost stream.
An unavailable control plane, incomplete drain, occupied batch lease, or expired
migration wait causes an explicit failure. A lost process image, permanent rank
failure, or unrelated application socket is outside this recovery contract.

Normal staggered finalize also has a known defect: a surviving armed peer can
mistake an abruptly closed final connection for a migration and then an unplanned
death. Do not interpret that case as successful graceful shutdown.

## 7. Relationship to GapRunner

The design follows GapRunner's per-chunk locking, byte-offset resume,
`SHUT_WR`/read-to-EOF drain, and externally driven checkpoint. FMI adds per-link
locking, validated reconnect counters, dynamically sized peer state, Redis event
replay, and explicit drain-timeout errors. Survivors wait for the migrating peer
rather than triggering their own checkpoint cascade.

## 8. Compared with sequenced links

| Property | Retain-and-Replay | Local Drain |
|---|---|---|
| Normal traffic | Frame header, retained payload, ACKs | Raw payload with locking and byte counters |
| Checkpoint preparation | External freeze | Coordinated leave and drain |
| Message identity | Checked per frame | No per-message identity check |
| Lost connection | Rebuild and replay retained data | Terminal unless part of a confirmed drain |
| FMI sockets in image | Discarded with `--tcp-close` | Closed before dump |
| Cross-host migration | DirectTCP relocation handling | Fresh listener and address during restore |

Both have measured campaigns. Compare the dedicated overhead experiments rather
than timing samples from different migration sessions.

## 9. Verification

`tests/drain_transport.cpp` covers raw collectives, multi-chunk messages, and
rejected hello records. `tests/drain_migration.cpp` covers rehearsals, control
orderings, stale leave notices, leases, repeated migration, byte-exact resume,
and socket scans during a sealed hold. These tests do not cover every execution
ordering: the two September 7 defects reproduced despite the existing suite
passing.

The real drivers and evidence are in
[fmi-spot-migration's drain campaign](https://github.com/McLavish/fmi-spot-migration/tree/main/benchmarks/migration/drain).
They include single-rank, whole-host, and two-host batch cuts, with socket scans,
byte-counter cross-checks, and application checksums. LULESH campaigns compare
`Final Origin Energy` with the clean baseline.

**Cross-host clock requirement:** library deadlines use `CLOCK_MONOTONIC`, whose
origin differs between hosts. CRIU needs sufficient privilege to restore a time
namespace preserving the source clock. Run the documented privileged restore
path; `--unprivileged` can inherit the destination's clock and expire deadlines
immediately. A recorded 74-hour uptime difference caused this failure; the same
cut passed with privileged CRIU. Deadline rebasing remains future work.

The current TLA+ modules cover sequenced links, not Local Drain. A formal drain
model and a dedicated per-rank measurement of how much non-neighbor progress is
disturbed remain open work.

## 10. Configuration

Configure the `DrainTCP` backend; [config/fmi_drain_tcp.json](../../config/fmi_drain_tcp.json)
is an example.

| key | default | meaning |
|---|---|---|
| `registry_host` / `registry_port` | required | Redis registry and control plane |
| `bind_host` / `advertise_host` | `0.0.0.0` / auto | listener and advertised address |
| `max_timeout` | required | per-operation budget, ms |
| `registry_poll_interval_ms` / `connect_retry_interval_ms` | 5 / 10 | establishment pacing |
| `registry_ttl_s` | 3600 | TTL on the registry hash, members hash, and event stream (the lease expires via `batch_lease_ms` instead) |
| `drain` | false | arm the migration runtime; off = plain raw channel |
| `trigger` | both | `signal`, `control`, `both`, `none` |
| `drain_signal_offset` | 3 | signal = `SIGRTMIN + offset` |
| `drain_grace_ms` | 5000 | quiet-period limit during drain and grace period for FIN before a leave notice, ms |
| `migration_max_ms` | 120000 | maximum wait for a migrating peer, ms |
| `establish_yield_ms` | 2000 | how long a drain waits for an in-flight establishment |
| `batch_lease_ms` | 120000 | batch lease duration, ms |
| `control_poll_interval_ms` | 20 | wait slice: data-path polls, cv waits, trigger tick |
| `drain_rehearsal_only` | false | both trigger paths rehearse instead of stopping (tests) |
| `drain_hold_ms` | 0 | hold between seal and restore for an event- or signal-driven rehearsal (tests; `rehearse_migration_in_place`'s argument covers direct calls) |


Use a unique communicator name, one rank and armed drain channel per process,
and control-event handling on every participating rank (`trigger: both` is the
default). Campaign configurations enable only the backend being measured so
channel selection cannot silently route the workload elsewhere.

## 11. Known limits and future work

- Fix migration-time credit for operations entered during a drain and reject
  stale control commands by epoch.
- Fix establishment-gate failure cleanup, staggered finalize, and fork-child
  mutex handling; see TODO §1.
- Bound the `inbound` buffer, which currently has no explicit size cap.
- Define scheduling for overlapping eviction requests; the current lease refuses
  overlap instead of queueing it.
- Measure migration-frequency effects and non-neighbor progress separately from
  steady-state overhead.
- Model drain ordering, byte preservation, and socket closure formally.
- Rebase deadlines if unprivileged cross-host restore is to be supported.
- Add a rendezvous reconnect path for environments without inbound connectivity.
