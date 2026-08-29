# The neighborhood-drain migration protocol (DrainTCP)

**Status:** as-built design record. Describes what is implemented on this branch and why.
**Scope:** Stages 1–2 — the channel, the migration runtime, single-rank migration on one
host, the criu driver. Batches, multihost, and the benchmark are future work (see the end).

## 1. What this is

FMI has two ways to survive a CRIU checkpoint of a rank on the TCP family. They pay at
different times:

- **Sequenced links** (`framed` + `recover_links` on Direct/DirectTCP): every message is
  framed, copied into retention, and acknowledged. A freeze needs no cooperation from
  anyone — whatever the kernel lost is replayed from retention. Cost: on every message.
- **The neighborhood drain** (this document, backend `DrainTCP`): messages travel as raw
  bytes with no per-message state. When a rank migrates, the protocol moves every in-flight
  byte into user memory *before* the dump, with cooperation from the peers of that rank
  only. Cost: at the migration event.

"Shim" means: the mechanism sits inside the channel, between the collective algorithms and
the sockets. The application is unmodified. Collectives are unmodified too — `PeerToPeer`
decomposes them into `send_object`/`recv_object`, and those two functions are the whole
surface the protocol occupies.

The migrating rank ends up stopped with **zero open sockets**. The CRIU image therefore
contains no socket, needs no `--tcp-close` on either leg, and is host-agnostic. Peers pause
only their link to the migrating rank and keep computing and communicating with everyone
else. Ranks with no link to it do nothing.

## 2. Where it sits in the code

```
Channel → PeerToPeer → DrainTCP          (include/comm/DrainTCP.h, src/comm/DrainTCP.cpp)
```

`DrainTCP` derives from `PeerToPeer` directly, not from `TcpChannelBase`. The reasons:

- `TcpChannelBase::write_all`/`read_all` treat a replaced connection as "restart at a frame
  boundary" (`LinkReplaced`). The drain protocol needs the opposite: keep the byte offset,
  wait, continue on the new connection. The two contracts cannot share a loop.
- The framing, retention, and ack machinery of the sequenced layer is exactly the
  steady-state cost this protocol exists to avoid, and it must not appear in the arm of the
  benchmark that claims to be free of it.

Shared code was extracted rather than duplicated (both extractions are pure moves):

- `PeerRegistry` (from `DirectTCP::Registry`): the deadline-clamped hiredis client. DrainTCP
  added stream and lease verbs: `xadd`, `tail_id`, `xread_after`, `set_nx_px`,
  `del_if_equal`, `hdel`.
- `TcpEndpoint`: byte-order helpers, `fnv1a64`, `random_nonce`, `set_nonblocking`,
  `resolve_ipv4`, `resolve_advertise_ip`, `param_or`.

The other pieces:

- `include/comm/DrainProtocol.h` — the hello record codec and the `DrainParticipant`
  interface the migration runtime calls. Deliberately not part of `Channel`: `Channel` is
  the interface user backends implement.
- `include/utils/MigrationTrigger.h` — the process-wide runtime (signal, epoch, trigger
  thread). In `utils` because it owns process-wide signal policy, like `utils/Signals.h`.
- `include/comm/DrainCoordinator.h` — the control-plane interface plus its Redis
  implementation. An interface so tests can fake orderings Redis will not produce on demand.

## 3. Steady state

### 3.1 Data path

Per peer, one `LinkState`: a mutex, a condition variable, the socket fd, a generation
counter, two cumulative byte counters (`bytes_sent`, `bytes_received`), a buffer of drained
bytes (`inbound` + cursor), the draining flags and their start time, the peer's incarnation
and epoch (both used as fences, section 4.4), `migration_ms_total` (section 4.6), and
`unrecoverable` — an error the control side records and the application thread throws.
Links live in a vector sized once and accessed through a bounds-checked accessor.

`send_object` and `recv_object` are loops with one rule: **the lock is held for exactly one
non-blocking syscall (or one copy out of `inbound`), and every wait happens with the lock
released.** One exception, which still waits nowhere: when drain is armed and the loop sees
a FIN or reset, it drains the already-readable bytes into `inbound` and closes the fd under
the same lock hold (`begin_draining_locked`), so nothing readable is lost between the
observation and the close. The message offset (`moved`) is a stack variable. So:

- A checkpoint can land between chunks, never inside a syscall.
- A drain (which takes the lock) can cut in within one chunk's time.
- After any interruption the loop re-checks the link state and continues from `moved`.
  Eviction, not restart.

`recv_object` consumes `inbound` before it reads the socket, and before it establishes a
connection at all — bytes the drain already moved into user memory satisfy a receive
without any network. The buffer is freed the moment it is spent.

The counters advance only when bytes are actually delivered: `bytes_sent` when the kernel
accepts them, `bytes_received` when they reach the application's buffer or the `inbound`
buffer. They are what a reconnect cross-checks (3.2), so they must describe delivery, not
intent.

With drain off, a connection that dies is an error, thrown with the peer and the
`moved/len` offsets in the message — except EOF before the first byte of an operation,
which is `Utils::Timeout`, matching the raw transports (a peer that finalized and left).
With drain armed, a death is first treated as a possible migration (the tentative drain of
section 4.4) and becomes the loud error only if no leave notice confirms it in time.

### 3.2 The hello record

The only wire structure in the protocol, sent once per connection in each direction at
establish and at every reconnect. 56 bytes, big-endian (`ResumeRecord`,
`include/comm/DrainProtocol.h`):

| field | checks |
|---|---|
| magic `FMID`, version | codec-level refusal |
| sender rank, receiver rank | right process, right direction |
| link-name hash (fnv1a64 of `comm_name\|lo-hi`) | right communicator, right pair |
| nonce | dialer echoes the registry entry's nonce; proves it reached this listener incarnation, not a recycled port |
| sender incarnation | must be monotone per rank; lower = superseded lineage, refused |
| `bytes_sent`, `bytes_received` | each end asserts `peer.bytes_sent == my.bytes_received` and vice versa |

The counter check is the safety story of a headerless stream: if a migration cut lost or
duplicated a single byte, the reconnect fails immediately, naming all four counts. Without
it, the stream would desynchronize silently — the known failure mode of the reference
implementation's bare-int handshake.

Per-message cost of all of this: zero bytes, zero copies. The counters are two integer
additions under a lock already held.

### 3.3 Establishment and the control thread

Discovery mirrors DirectTCP, under a separate namespace so the two backends cannot
cross-talk: registry hash `fmi:drain:<comm_name>`, field = rank, value = `ip:port:nonce`.
A rank dials peers with lower ids and accepts from higher ids. The dialer polls the
registry until the target's entry appears, connects, and exchanges hellos; budget
`max_timeout`, then `Utils::Timeout`.

Accepts run on a per-channel **control thread**. It blocks in `poll()` on the listener and
a self-pipe, with a coarse housekeeping tick (registry TTL refresh). With drain armed there
is one more thread, the runtime's trigger thread (4.1), which reads the coordinator and
applies leave notices. Together they are why a peer that is busy in application code still
answers connection attempts and drain requests; without them, a migration would be
unbounded against a compute-bound peer — the objection that sank earlier coordinated
designs. `accept()` faults that leave the connection queued (EMFILE and friends) back off
instead of spinning.

The dialing half of establishment runs under a shared lock (`establish_gate`), which a
drain takes exclusively; the accepting half runs on the control thread, which a drain
pauses (and it closes the listener) before touching any link. Between the two, no
establishment can overlap a drain — "establishment is not a safe point" is mechanical
instead of promised.

`OperationScope`'s thread-local identity is invisible to the control thread. That is
correct: the control thread moves bytes and counters, never message identity.

### 3.4 What the steady state costs

Per message: 0 wire bytes, 0 copies, 0 allocations, 0 extra syscalls; one uncontended
mutex acquisition per chunk. Per process: one mostly-blocked control thread, plus (armed
only) one trigger thread. This is the arm of the T1 benchmark that must measure ≈ free.

## 4. The migration runtime

### 4.1 Trigger

`MigrationTrigger` is a process singleton. One armed drain channel per process; a second is
`std::logic_error`. Arming happens at `set_incarnation`, the first point where the channel
knows its rank, peer count, and comm name — so the migratable window opens when the
communicator is constructed and closes at finalize.

Two request paths, selected by the `trigger` config key:

- **signal**: `sigqueue(pid, SIGRTMIN + drain_signal_offset, epoch)`. Arming installs a
  handler first (the safety net for threads created before arming: it records the request
  instead of letting the default action kill the job), then blocks the signal; the trigger
  thread takes it with `sigtimedwait` and also checks the handler's recorded request each
  tick. The payload is the requester's view of the epoch: a request below the current epoch
  is dropped (it was aimed at a lineage this process has already left). Pending requests
  are discarded at attach and at final detach — a request that landed while nothing was
  armed belongs to nobody and cannot fire a migration later.
- **control**: a `migrate` event on the coordinator stream.

`pthread_atfork` disarms the runtime in a forked child; the child re-arms through its own
communicator.

### 4.2 Control plane

`RedisDrainCoordinator`, three keys, all with TTL:

- `fmi:drain:<comm>:members` — hash, rank → `epoch:ip:port:nonce`.
- `fmi:drain:<comm>:events` — a Redis **stream** of `leaving` / `sealed` / `restored` /
  `migrate` events (fields: `type`, `rank`, `epoch`, `batch`, plus extras such as the
  leaver's `incarnation`). A stream, not pub/sub: a frozen rank must replay exactly the
  events it missed, resuming from its last-read id. Delivery is at-least-once, so handlers
  are idempotent and epoch-filtered.
- `fmi:drain:<comm>:batch` — the batch lease, `SET NX PX batch_lease_ms` (its own expiry;
  `registry_ttl_s` does not apply to it). One migration batch in flight; a rank that finds
  the lease held fails its migration loudly. Released by compare-and-delete. A
  self-initiated migration (a signal, or a `migrate` event with no batch) takes and
  releases its own lease. A `migrate` event that names a batch means the requester holds
  the lease: the rank skips taking it and never releases it. The shipped driver requests by
  signal, so today ranks always take their own.

### 4.3 The migrator sequence

Runs on the trigger thread. One phase, no acknowledgments; the leave notice and TCP
half-close semantics are the only synchronization.

1. Take `establish_gate` exclusively (budget `establish_yield_ms`; an in-flight dial lets
   go at its next retry boundary).
2. Emit `leaving`. If the control plane is unreachable, fail loudly — the migration is off.
3. Pause the control thread (request/ack over the wake pipe; it parks holding no socket)
   and close the listener.
4. Take **every** link's lock in ascending peer order — established or not — and set
   `draining` on all but this rank's own slot. Every link, because an application thread
   parked on a link that was never established must also be held across the stop and wake
   onto the migration clock. Then `shutdown(SHUT_WR)` on every link with a live fd, then
   one poll loop reading all of those to EOF, appending to each link's `inbound` (counted
   into `bytes_received`). Half-close-all-first matters: two ranks draining each other
   pairwise would deadlock. EOF is `recv() == 0`; a quiet period of `drain_grace_ms`
   (re-armed on progress, capped by `max(max_timeout, drain_grace_ms)`) without EOF is a
   loud `std::runtime_error` ("drain incomplete, peer(s) ..."): no stop, channel broken.
   Timeouts are never treated as EOF. Close each fd.
5. Emit `sealed`, carrying the per-peer counters and the last-read stream id.
6. Disconnect the registry and the coordinator. The process now owns zero sockets.
7. `malloc_trim(0)`, flush stdio.
8. `raise(SIGSTOP)` — still holding every link lock, so no application thread can observe
   the half-restored state that follows.

Why the guarantee holds: the migrator's `SHUT_WR` flushes its own send queue (the kernel
retransmits) and delivers FIN after it; the survivor half-closes its side on the notice (or
on seeing the FIN); the survivor's FIN arrives after everything it sent; the migrator's
read-to-EOF captures all of it. At the stop, for every link: `peer.bytes_sent ==
my.bytes_received` with the bytes in the application's buffer or in `inbound`, and the
socket is gone. That equality is re-asserted by the hello at reconnect.

### 4.4 Survivors

On `leaving{rank, epoch, incarnation}` (their trigger thread): lock that link, half-close,
read to EOF into `inbound`, close. Idempotent. Two orderings needed explicit handling:

- **FIN before the notice.** The data path often sees the migrator's FIN first. With drain
  armed, an unexpected connection death is recorded as a tentative drain
  (`drain_unconfirmed`) instead of an error: readable bytes are taken first, the fd is
  closed, the operation parks. The notice confirms it. If no notice arrives within
  `drain_grace_ms`, it becomes the loud unplanned-death error. With drain off, behavior is
  unchanged from a raw transport.
- **Notice after the restore.** A slow consumer can read a `leaving` after the migrator has
  already restored and re-dialed. Every notice carries the leaver's incarnation and is
  dropped when the link already belongs to a later lineage; the peer's epoch
  (`LinkState::peer_epoch`, updated by `restored` events) fences the same way. Without
  this the late notice tears down the new connection and both ends wait forever (observed
  before the fence existed).

A dialer also refuses to install a connection onto a link that a notice marked draining
(`file_link` re-checks under the lock); the acceptor deliberately does not refuse, because
the one dialer that can legitimately reach a draining link is the peer's restored lineage.

An application thread blocked on a migrating peer parks with its offset intact. Its
`max_timeout` does not run during the migration (4.6); `migration_max_ms` bounds the wait
instead, and expiry throws an error naming the peer — the documented failure surface.
Ranks without a live link to the migrator do no network work: they record the notice (so a
new dial toward the migrator parks until its restore) and continue.

### 4.5 Restore

Execution resumes at the return of `raise(SIGSTOP)` — after `criu restore` plus `SIGCONT`
(criu faithfully restores the stopped state), or immediately in a rehearsal. On the trigger
thread, `noexcept`:

1. Bump the epoch and the incarnation.
2. Bind a fresh listener (new port, new nonce) **before** re-advertising; re-resolve the
   advertise address. This is why cross-host restore works: the restore leg runs code,
   unlike the sequenced protocol's restore, which runs none. (Verified across four
   machines — see §9; the one environmental requirement is monotonic-clock preservation,
   the clock note in §9.)
3. Republish the member record at the new epoch and emit `restored`. The registry and
   coordinator connections come back lazily inside those calls (the client redials when its
   context is gone); the stream cursor is simply the member variable the image preserved,
   used by the next poll.
4. Un-pause the control thread. Clear each link: fd stays −1, `draining` cleared (banking
   the migration window, 4.6), condition variables notified. Re-establishment is lazy and
   belongs to whichever application thread next needs the link. Peers above this rank
   redial it (they see `restored`, or their parked establish loop picks up the new registry
   entry); this rank redials peers below it.
5. Release the link locks, last.

Failures on this path are recorded per link in `LinkState::unrecoverable` and thrown on the
application thread — never on the control thread, which would terminate the process while
holding locks.

`rehearse_migration_in_place(hold_ms)` runs the whole sequence with step 8 replaced by an
optional sleep and an immediate restore. It is the regression net: everything except the
CRIU image itself, in milliseconds, inside a Boost test.

### 4.6 Migration time accounting

An operation's `max_timeout` must not be charged for time a migration took. Crediting the
waiting thread does not work: a thread parked on the link *mutex* (not the condition
variable) never runs during the window and cannot record it — this was a confirmed bug
that killed jobs with a terminal `Timeout` right after a successful migration.

The account therefore lives on the link, not the thread. `LinkState::migration_ms_total`
accumulates every closed migration window; the single place `draining` goes false
(`close_migration_window_locked`) banks the elapsed time. Each operation snapshots the
counter at entry (`OperationClock::migration_baseline`) and, at every deadline check made
under the lock, absorbs the delta into its budget. A thread is credited the same amount
whether it spent the window on the condition variable, on the mutex, or polling.

## 5. Unsafe windows

A migration request that lands in one is deferred or refused, never silently dropped.

| window | behavior |
|---|---|
| before the channel is armed | discarded: arming drains the pending signal set and clears the recorded request — a request that landed while nothing was armed belongs to nobody. Drivers must target an armed rank |
| link establishment | the drain waits up to `establish_yield_ms` for the gate; a dialer checks for a pending drain at each retry boundary and lets go (one in-flight attempt is bounded by the operation budget) |
| a migration already in progress | serialized by the channel's migration mutex |
| finalize / teardown | finalize joins the trigger thread first, so no migration can start during it; afterwards the channel refuses use |
| between `fork()` and the child's re-arm | disarmed by the atfork handler |

Safe, and load-bearing for R1: inside a collective (it is P2P underneath), blocked in a
`recv` whose sender has not sent (the schedule that made boundary-parking designs unsound),
mid-message on either side, holding application mutexes.

## 6. Failure model

Migration is planned; all ranks are alive. There is no abort protocol and no retention:

- The control plane unreachable at step 2, an incomplete drain at step 4, a held batch
  lease: loud failure before the point of no return; the job dies or the channel is
  deliberately broken. No silent path.
- A connection that dies outside a migration: loud, named, terminal for that link.
- A survivor whose patience (`migration_max_ms`) runs out: loud, named.
- Unplanned failures (crash without an image, lost image): out of scope, as for the whole
  protocol family.

## 7. Differences from the GapRunner reference implementation

The mechanism follows GapRunner's `DirectCheckpoint` (per-chunk lock, offset resume,
`SHUT_WR` + read-to-EOF drain, external SIGSTOP/waitpid dumper). Deliberate departures:

| GapRunner | here |
|---|---|
| one process-global checkpoint lock | per-link locks; survivors keep every other link running |
| bare-int handshake, nothing validated | 56-byte hello: identity, nonce, incarnation, byte counters |
| `MAX_PEERS=64`, unchecked indexing | vector sized to the job, bounds-checked accessor |
| `int` byte counts (< 2 GiB) | `size_t`/`uint64_t` throughout |
| drain timeout conflated with EOF | distinct outcomes; timeout is a loud "drain incomplete" error |
| unbounded thread joins (blocking `connect`) | every control/trigger wait bounded; self-pipe shutdown |
| etcd + watch revisions | Redis stream with resume-from-id |
| destructor is not the teardown | `finalize()` is the whole teardown; destructor covers stragglers |
| drained buffers never pruned | `inbound` freed when spent |
| survivor self-checkpoints after 3 s | survivors wait (`migration_max_ms`); no self-stop cascade |
| drains everything, no scoping | drains the migrating rank's links only |

## 8. Compared with the sequenced links

| | sequenced links | neighborhood drain |
|---|---|---|
| per-message cost | header, retention copy, acks | zero |
| coordination at the event | none | leave notice + drain, live peers required |
| message identity checked | every frame | no (counters at reconnect only) |
| unplanned connection death | replayed from retention | loud failure |
| image | contains sockets; `--tcp-close` both legs | socketless; no TCP flags; host-agnostic |
| cross-host restore | unbacked on this branch | measured: 100+ cross-host migrations, sequential and batch (privileged criu — see the clock note in §9) |
| request → dump latency | ≈ 0 | one drain (measured: ~1 ms seal on loopback) |

Neither dominates. The benchmark that decides between them per workload is future work.

## 9. Verification

- `tests/drain_transport.cpp` — the channel without migration: collectives at 2–8 ranks,
  multi-chunk messages, hello refusals (including counters wrong with everything else
  right, in both directions).
- `tests/drain_migration.cpp` — a fake coordinator for orderings Redis will not produce
  (duplicate and stale notices, FIN/notice races, the incarnation fence, a held lease);
  forked-rank rehearsals: mid-message cuts on both sides resume byte-exact, a compute-bound
  peer's drain completes in a fraction of the peer's 3 s sleep, a blocked survivor outlives
  `max_timeout`, patience expiry is a named error, the signal path drops a stale epoch, a
  rank migrates twice, and a sealed rank provably owns zero sockets (`/proc/self/fd`
  scanned during the hold). New tests were checked by mutation: each fails when its check
  is disabled.
- `runbooks/drain-migration/drain_driver.py` — the real thing: sigqueue, wait for `sealed`
  and state `T`, a pre-dump `/proc/<pid>/fd` socket scan, `criu dump` with **no
  `--tcp-close`** (its success is the socketless assertion; the dump log is grepped and the
  image checked for socket image files), restore, `SIGCONT`, checksums against a clean
  baseline. Evidence in that runbook's README: 43 migrations across 2 and 4 ranks, shapes,
  seeds, and a double cut of one rank, 42 of 43 green end to end (the one failure came from
  a concurrently instrumented binary and did not reproduce against a pristine build —
  recorded there as such); a control run against an undrained rank fails the dump exactly
  as predicted. `crit` inspection of an image (no socket entries at all) was done by hand
  and recorded in that README.

- The 4-machine cluster campaign (evidence in the same README): **cross-host restore is
  measured, not asserted** — 47 sequential cross-host migrations (chains of one rank over
  three machines, whole-node sequential evacuation, mid-message cuts, 12 ranks), then
  **single-cut batch evacuations** through the `migrate`-event path with a driver-held
  lease: 28 cuts, 60 ranks moved between machines, including one machine's ranks to one
  survivor and to a spread, k=3 at 12 ranks, and two machines emptied in one k=4 cut. All
  sealed before anything is dumped; 41 pairwise counter cross-checks between co-migrating
  peers, none disagree; every image socket-free; drain time unchanged by k (0–20 ms). The
  `peer_migrating` fence was verified at the network level: zero TCP resets on evacuated
  nodes while restored ranks waited out frozen co-members. The capstone is LULESH (a real
  application, unmodified): 23 cross-host migrations across the same four machines —
  sequential, whole-machine cuts, and two-machine k=4 cuts — every trial ending in the
  bit-identical `Final Origin Energy`, with the drain itself at 1–20 ms regardless of k
  and of tens of MB in flight per link.

**Cross-host restore and the clock.** One environmental requirement the cluster campaign
established: the library's deadlines and migration accounting use `CLOCK_MONOTONIC`, which
differs between machines by their uptime gap. criu preserves it by restoring the process
into a time namespace — which requires privilege (`CAP_SYS_ADMIN`; run criu under sudo or
grant the file capability). `--unprivileged` criu cannot create the namespace and does not
warn: the restored rank lands on the destination's clock, and a forward jump expires every
absolute deadline at once (measured: a 74-hour uptime gap threw `Timeout` immediately
after an otherwise successful migration; the same cut under privileged criu passed with a
756 ms window). Same-host restores are unaffected. Re-basing the library's deadlines
across a restore — which would lift the privilege requirement — is future work.

Not yet verified: TLA model checking of the drain ordering (the existing modules cover the
sequenced protocol only); a dedicated neighborhood-stop rate measurement (survivor logs
and job wall-clocks show no disturbance, but the per-rank round-rate harness is future
work).

## 10. Configuration

Backend block `DrainTCP` (see `config/fmi_drain_tcp.json`):

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
| `drain_grace_ms` | 5000 | drain quiet budget; also the FIN-without-notice grace |
| `migration_max_ms` | 120000 | survivor patience for a migrating peer |
| `establish_yield_ms` | 2000 | how long a drain waits for an in-flight establishment |
| `batch_lease_ms` | 120000 | batch lease PX |
| `control_poll_interval_ms` | 20 | wait slice: data-path polls, cv waits, trigger tick |
| `drain_rehearsal_only` | false | both trigger paths rehearse instead of stopping (tests) |
| `drain_hold_ms` | 0 | hold between seal and restore for an event- or signal-driven rehearsal (tests; `rehearse_migration_in_place`'s argument covers direct calls) |

One deliberate asymmetry, documented rather than fixed: a `migrate` event addressed to
this rank carries no epoch fence (the signal path's payload does). It needs none — the
stream cursor advances past a dispatched migrate and is carried inside the image across
the stop, so a stale migrate cannot be redelivered; the drivers still write the true
epoch for the evidence trail.

Constraints: enable exactly one backend in a config used for sweeps (the cost model would
route around DrainTCP otherwise); one drain-armed channel per process; unique `comm_name`
per run.

## 11. Known limits and future work

- `inbound` has no size cap; a peer that streams gigabytes during a drain grows it without
  bound. Acceptable for planned migrations of cooperating ranks; a cap is future work.
- Overlapping migration requests are refused by the lease, not queued; a driver batches
  its own cuts under one lease (verified: back-to-back cuts, and a refused intruder).
  A queueing policy for correlated spot evictions remains open.
- Stage 3 is done: multi-rank batches, whole-host and two-host evacuation, and the
  multihost driver (`runbooks/drain-migration/{cluster,node_helper,multihost_drain}.py`)
  are built and measured. Still open from it: the dedicated neighborhood-stop rate
  measurement (survivor logs and wall-clocks show no disturbance; a per-rank round-rate
  harness would make it a number).
- Stage 4: the steady-state and migration-frequency benchmark against the sequenced links
  and raw DirectTCP, and a TLA model of the drain ordering (no-loss at the stop,
  close-only-after-FIN, socketless-at-stop, the neighborhood property).
- Deadline re-basing across a restore, so cross-host works under unprivileged criu
  (today: privileged criu for the time namespace — the clock note in §9).
- Restoring into environments without inbound connectivity (Lambda) needs a TCPunch-style
  rendezvous on the reconnect path; the establishment seam is where it would land.
