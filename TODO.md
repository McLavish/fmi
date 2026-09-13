# Open work

This list comes from the August 17, 2026 implementation review and the September
7 migration-correctness review. Completed wire-format cleanup and obsolete
planning notes have been removed. Source references use symbols where possible
because line numbers move as the code changes.

**Keep both migration mechanisms.** Retain-and-Replay and Local Drain have
different costs and failure assumptions. The work below improves their
correctness and reduces maintenance effort.

The September 7 review recorded a passing 189-test suite with Redis enabled and
S3/TCPunch disabled. Separate rehearsals reproduced the two priority defects
below. Those reproductions did not use actual CRIU dump and restore.

## 0. Measurement

Steady-state comparisons now exist in
[fmi-spot-migration's overhead experiments](https://github.com/McLavish/fmi-spot-migration/tree/main/benchmarks/overhead),
including scale results. Migration timings are recorded in its
[migration campaigns](https://github.com/McLavish/fmi-spot-migration/tree/main/benchmarks/migration).
The old claim that neither mechanism had been measured is obsolete.

- [ ] Extend comparisons to migration frequency and larger migration campaigns.
  Interpret small-message replay costs in light of the separate header/payload
  writes described in §4; results describe the implementation measured, not a
  lower bound on every possible replay implementation.

## 1. Correctness bugs

- [ ] **P1: Operations entered during a drain lose migration-time credit.**
  `DrainTCP::send_object` and `recv_object` start their operation clock before
  taking `l.mu`, but snapshot `migration_ms_total` after taking it. If a drain
  holds the mutex, an operation can spend the migration blocked and then record
  the post-migration baseline. It charges that time to `max_timeout` and can time
  out immediately after successful restore. Both send and receive reproduced
  this with a ready loopback peer, a **600 ms rehearsal hold**, and a **200 ms
  transport budget**, failing before dialing. Make the initial timestamp and
  migration baseline consistent without double-counting credit. Test entry
  before, during, and after migration. Raising `max_timeout` above the full
  expected migration duration is a temporary mitigation that delays failure
  detection; it does not fix the accounting.

- [ ] **P1: An unread stale control command can stop a rank again after restore.**
  DrainTCP accepts a local `Migrate` event without checking `event.epoch`, unlike
  the signal path in `MigrationTrigger`. Against real Redis in rehearsal mode,
  an epoch-0 signal migration followed by an epoch-0 command appended while sealed
  caused two restores, ending at epoch 2. Retried commands with new stream IDs
  have the same problem. In a real migration, the second request could issue
  `SIGSTOP` after the driver finishes. The cursor protects consumed entries, not
  unread stale commands; `trigger=both` alone does not cause the bug. Reject
  epochs below the current epoch before changing batch state, while advancing
  the stream cursor. Test stale retries, mixed signal/control requests, and a
  valid request in the new epoch. Increasing `max_timeout` does not help.

- [ ] **ACK servicing can throw while another peer is being serviced.**
  `TcpChannelBase::service_established_links` calls `maybe_send_ack` outside its
  enclosing exception handler. A partial ACK write can throw from `write_all`
  while the application waits for another peer, violating the servicing contract.
  Handle the failure within servicing and preserve its connection state.

- [ ] **Failed establishment-gate acquisition leaves drain flags set.**
  If `take_establish_gate()` throws, cleanup clears `drain_pending` but can leave
  yielded links marked `draining`, bypassing the outer `break_every_link` handler.
  Waiters then escape only when `migration_max_ms` expires, potentially naming a
  peer that never migrated. Make cleanup consistent across both error paths.

- [ ] **Staggered finalize looks like an unplanned death.**
  DrainTCP closes peer sockets without a permanent-leave event or equivalent
  shutdown drain. An armed peer still receiving records a tentative drain,
  waits the default 5-second grace, then fails. Define graceful shutdown behavior
  comparable to `TcpChannelBase::drain_links_for_shutdown`.

- [ ] **The fork-child trigger mutex is not reset.**
  `MigrationTrigger::forget_in_child` disarms the child but does not reconstruct
  a mutex another thread may have held at fork. Later attach, detach, or
  `drain_signal` calls can deadlock on it.

- [ ] **SIGPIPE ownership differs between S3 and the shared helper.**
  `suppress_sigpipe` preserves `SIG_IGN`; S3 treats it as unclaimed and replaces
  it with the SDK handler. Apply one rule for application-owned dispositions.

- [ ] **Permanent store errors use inconsistent exception types.**
  S3 uses `Utils::BackendFailure`; equivalent Redis errors, including a value
  length mismatch, use `std::runtime_error`. Make the caller-visible contract
  consistent.

- [ ] **Oversized frames look like a full retention window.**
  `SequencedLink::admit` returns false for both conditions. An oversized message
  therefore consumes the timeout budget before reporting a retention limit with
  zero outstanding data. Distinguish permanent size rejection from backpressure.

- [ ] **Two transport limits cannot be configured.**
  `parse_tcp_params` omits `link_max_frame_bytes` (default 16 MiB) and
  `max_link_repairs`, although it reads the other link limits.

- [ ] **The stuck-rank diagnostic derives wait time from inconsistent deadlines.**
  Some `pump` callers pass a short slice deadline, so subtracting `max_timeout`
  produces a misleading nearly constant wait. Depending on the timeout and
  modulo condition, this can suppress or repeatedly emit the beacon. Pass the
  actual operation start time. The benchmark method documents the observed
  residue-dependent behavior; the earlier claim that it could never fire was
  too broad.

- [ ] **Orphaned-link redial depends on incompletely initialized state.**
  The rescue path requires `link_suspect_since.size() == num_peers`, but that
  vector is initialized by another path observing a live descriptor. Initialize
  all repair-state vectors consistently in `ensure_link_state`.

- [ ] **`maybe_send_ack` checks the wrong vector before indexing sockets.**
  It guards against `links.size()` but accesses `sockets[partner_id]`. A queued
  receive before socket initialization could expose an out-of-bounds access.
  Review the similar guard in `check_socket` too.

- [ ] **Reconciliation inside a catch handler can escape unexpectedly.**
  `send_object` repeats an unguarded `reconcile_if_needed` pattern already handled
  in `reconcile_or_repair`. Make both retry paths enforce the same error contract.

- [ ] **Registry errors name the wrong backend.**
  `PeerRegistry` prefixes errors with `DirectTCP` even when DrainTCP or its
  coordinator called it. Supply or preserve caller context.

- [ ] **`SOL_TCP` fallback macros appear inside function bodies.**
  Move them to a shared header; their preprocessor scope extends beyond the
  apparent function scope.

- [ ] **Nested OperationScope behavior disagrees with its source comment.**
  The implementation saves and restores the innermost identity. The comment
  promises that the outermost wins. Normal paths do not nest communicator calls,
  but future re-entry could make collective counters count internal work.
  Define and enforce the nesting contract.

## 2. Structural duplication

DirectTCP and DrainTCP duplicate connection establishment. Moving DrainTCP under
`TcpChannelBase` would recover little of it: that base owns neither listeners nor
registry lookup, dialing, and accepting. Share establishment helpers while
preserving the different frame-restart and byte-offset data paths.

- [ ] Extract a registered TCP endpoint for listener binding, publication,
  registry parsing, nonblocking connect, hello handling, and common cost-model
  code. Compare DirectTCP and DrainTCP, including DrainTCP's second listener
  binding block in `rebind_listener`.
- [ ] Consolidate DrainTCP's initial bind and rebind logic, including error and
  socket-option handling.
- [ ] Share hiredis connection setup, timeout conversion, fork checks, redial,
  argv marshalling, and reply ownership between Redis and PeerRegistry. Both
  already use `redisCommandArgv`; their remaining error and pipelining policies
  can remain separate.
- [ ] Consolidate `FMI_LINK_TRACE` handling across TcpChannelBase, DirectTCP, and
  SequencedLink. Include repeated environment checks, throttling, formatting,
  `first_int`, and the unconditional `DRAIN GAP` output.
- [ ] Share compatible configuration accessors while preserving each setting's
  required/default/clamp/error policy.
- [ ] Reuse `SequencedLink::classify` in `accept` instead of duplicating it.
- [ ] Share the common portions of DrainTCP send/receive chunk loops while keeping
  receive-buffer and EOF handling explicit.
- [ ] Consolidate duplicated staging dispatch, suspect marking, and repair-budget
  checks within TcpChannelBase.
- [ ] Share ClientServer reduce/scan folding and store upload retry loops where
  their ordering and failure semantics agree.
- [ ] Share byte-order helper implementations without changing wire order:
  DrainTCP endpoint records are big-endian; sequenced frames are little-endian.
- [ ] Move the duplicated `poll_attempts` calculation into RecoverableClientServer,
  which owns the timeout values.
- [ ] Keep DrainTCP interface contracts in the header and shorten duplicate
  implementation comments to references.
- [ ] Consolidate scalar/vector Python reduction dispatch. Reconsider whether
  `PythonBindingSupport.h` needs to be separate from its sole consumer, while
  preserving the custom-operation flags fix.

## 3. Unused or redundant interfaces and state

The version-4 cleanup already removed redundant frame fields and sequenced-link
incarnations. The remaining candidates need a fresh reference check, including
consumers in fmi-spot-migration.

- [ ] Decide whether to retain the transmitted `next_send_seq` and
  `lowest_retained` handshake fields. They currently support a structural sanity
  check and tracing; reconciliation uses `peer.next_expected_seq`. Their local
  counterparts remain necessary for replay.
- [ ] Resolve `ack_safe_seq` duplicating `next_recv` in production. Either maintain
  an independent watermark when needed or remove the redundant state and correct
  the class documentation.
- [ ] Review the coordinator's members hash as an external interface. Library
  discovery uses the transport registry, but campaign tooling also reads control
  state. No library-local reader is not sufficient evidence to delete it.
- [ ] Remove or use `DrainTCP::LinkState::generation` and correct comments claiming
  it is checked. This is distinct from TcpChannelBase's active generation checks.
- [ ] Review apparently unused helpers such as `SequencedLink::accept_inline`,
  `last_ack_sent`, `configuration`, `Channel::current_operation`,
  `MigrationTrigger::running`, `DrainTCP::drained_peers`,
  `DrainParticipant::peer_is_leaving`, `OperationIdentity::valid`, and
  `S3::delete_object`. Check downstream callers before removing public symbols.
  `DirectTCP::connection_count` is used by benchmarks and is not a deletion
  candidate on the basis of library-local references.
- [ ] Review the selective drain `targets` argument, whose current callers pass
  an empty set.
- [ ] Simplify trigger attachment refcounting if the one-armed-channel invariant
  makes it redundant.
- [ ] Decide which test helpers belong in public headers: `snapshot`, `seed`,
  `ack_safe`, `encode_header_checked`, and `set_coordinator_for_testing`.
- [ ] Either report S3's flag-off transient-failure counters in diagnostics or
  stop maintaining counters that are never used on that path.

## 4. Performance

- [ ] **Coalesce framed header and payload writes.** `write_frame` uses two
  `write_all` calls with `TCP_NODELAY`, increasing small-message packet and syscall
  costs. Evaluate `MSG_MORE` or a vectored write. Preserve generation checks and
  correct partial-write advancement across the header/payload boundary. Re-encode
  the current cumulative ACK on every send and replay; a retained encoded header
  would contain a stale ACK. Avoid adding a payload copy just to join the buffers:
  the initial transmission currently reads directly from the application buffer,
  in addition to the separate retained copy.
- [ ] Pool retained payload allocations where beneficial. Retention must outlive
  the application's send buffer; removing that ownership copy is not generally
  valid.
- [ ] Reduce copies for serviced receives, which read into temporary storage,
  copy into the committed queue, and copy out on delivery. Preserve R1's custody
  and acknowledgment ordering.
- [ ] Discard duplicate payloads through bounded scratch space instead of a
  full-sized allocation.
- [ ] Avoid 20 ms `pump` slicing when a backend has no servicing work to do.
- [ ] Avoid unconditional `accept()` attempts in DirectTCP servicing when they
  add syscalls without useful progress.

## 5. Configuration and build

- [ ] Consolidate test-only JSON configurations under tests or generate them from
  a shared base. Preserve the relative-path behavior expected by test execution.
- [ ] Remove unused model blocks for backends a configuration never enables.
- [ ] Allow builds to omit DrainTCP and its migration helpers while keeping Redis.
  The factory references every compiled-in backend, so JSON flags cannot remove
  their static-link cost. The August review measured about **129 KB of `.text`**
  and **160 KB of allocated sections** for the drain stack at `-O2`; the old
  448 KB figure described growth across the whole branch. A drain build option
  would remove only its own portion.
- [ ] Correct the `FMI_ENABLE_REDIS` CMake help text: it gates hiredis-dependent
  DirectTCP, DrainTCP, and control helpers as well as the Redis data backend.
- [ ] Make S3's ineffective `object_ttl_s` setting harder to mistake for cleanup.
  Recovery requires bucket lifecycle expiration; the existing warning can be
  missed in deployment logs.

## 6. Source documentation

- [ ] Shorten repeated source commentary while retaining API contracts and
  failure explanations needed to maintain correctness.
- [ ] Correct DrainTCP's inheritance rationale: frame-boundary restart differs
  from byte-offset resume, but TcpChannelBase framing and retention are optional.
- [ ] Correct TcpChannelBase comments that mention `MSG_WAITALL` or claim
  `SO_SNDTIMEO` bounds nonblocking ACK writes. The implementation uses
  `MSG_DONTWAIT` with its own deadline.
- [ ] Remove DirectTCP documentation for the nonexistent `expect_sender`
  parameter and duplicate `accept_one` summaries.
- [ ] Describe PeerRegistry's coordinator stream/lease responsibilities as well
  as peer discovery, and remove its stale comparison with Redis command strings.

Independent fixes for pre-existing FMI bugs are already carried by this branch;
see [CLAUDE.md](CLAUDE.md). Do not undo those fixes when isolating migration costs.
