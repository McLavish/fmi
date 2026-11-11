# Open work on the `redesign` branch

The library was redesigned on 2026-11-11 (branch `redesign`; the design record is
fmi-spot-migration's `runtime/DESIGN.md`). There is one migration mechanism,
Local Drain, in a new channel (`LocalDrain`). Retain-and-Replay, the first Local
Drain channel (`DrainTCP`, its Redis coordinator and its trigger thread) and the
relocation logic of DirectTCP were removed. Their last version is the tag
`pre-redesign`; the list of open work they had is `TODO.md`, kept unchanged beside this file.

Source references use symbols where possible because line numbers move as the
code changes.

## 0. Not measured yet

Nothing below has a number. Each needs one before the paper or a record states
it.

- [x] **Plain DirectTCP against its last measured version, Part 1.** Measured
  on the cluster on 2026-11-11 at `7942dce` (fmi-spot-migration,
  `benchmarks/overhead/RESULTS-redesign.md`): over 36 shape, ladder and
  application cells the baseline is 1.50 % faster in the median than in October.
  Since `7942dce` it builds its link name only when the socket is missing
  (818 user instructions fewer per ping-pong iteration), so it is not October's
  baseline.
- [ ] **Part 2 of the evaluation (the migration experiments)** has to be
  measured again on the cluster with this build.
- [x] **Steady-state cost of `LocalDrain` against DirectTCP**, at 2 to 128 ranks.
  Same record: median +0.55 % over the 36 cells (28 within 2 %), +0.08 to
  +0.39 µs on a two-rank round trip of 30 µs up to 1 KiB, 316 user and 304 kernel
  instructions more per ping-pong iteration. Large messages are not resolved:
  from 4 KiB the runs of one arm differ by up to a factor of 2.8 (see §3, the
  4 MiB case).
- [x] **128 ranks and a real checkpoint**, as development runs on 2026-11-11
  (fmi-spot-migration, `benchmarks/REDESIGN-CLUSTER-DEVRUNS-2026-11-11.md`): LULESH
  at 125 ranks and HPCCG at 128 ranks on the cluster's four machines, with up to
  16 moves of a whole host (criu dump, stream, restore on another machine), give
  the result of the run without a move. Five applications and the eight
  communication shapes do the same at 8 ranks on one machine
  (`runtime/tests/test_local_moves.py` there). Not a measurement window.
- [ ] **`Direct` (TCPunch) and S3 were not compiled** after the change to
  `TcpChannelBase` and `Channel.h`: the TCPunch submodule and the AWS SDK were
  not on the machine. (The Python binding is built in the benchmark tree and
  carried four workloads in the window of 2026-11-11.)

## 1. Defects of the first Local Drain channel, and what became of them

- **Operations entered during a drain lost their time credit** (P1, 2026-09-07).
  Closed by construction: the clock of an operation starts under the link lock.
  Test: `an_operation_that_waits_out_a_move_keeps_its_budget`.
- **A stale control command could stop a rank again after its restore** (P1,
  2026-09-07). Closed by construction: `leave` names a generation and is ignored
  unless it is the current one; rows only move forward. Tests:
  `a_leave_for_another_generation_is_ignored`, `a_stale_row_changes_nothing`.
- **A staggered finalize looked like a death.** Closed: a rank says `bye`, peers
  that asked about it get `gone`, and an operation that still waited for it ends
  in a timeout at once. Test: `a_peer_that_finalized_is_a_timeout`.
- **A leave notice that arrived before the acceptor had filed its link was
  lost.** Does not apply: a `leaving` row never acts on a link. The FIN closes a
  link, and the row only says why. Test: `a_peer_closes_on_the_fin_alone`.
- **A failed acquisition of the establishment gate left links marked as
  draining.** Closed: both ways a move can fail (`leave` cannot take the gate,
  or the drain does not reach EOF) break every link and report `failed`.
  No test provokes the first one.

## 2. Correctness

- [x] **`finalize()` during a leave** (`9cf040e`, 2026-11-11). `finalize()` said `bye` once,
  before it waited for the service thread, and swallowed a failure, so a rank that finalized
  while it was leaving ended on its new host without `bye` and was reported lost. Now
  `finalize()` waits for a move that has started and says `bye` on the link the rank has
  then, and a leave that arrives once `finalize()` has begun is not started. Seen with a move
  in the last moments of an 8-rank LULESH (fmi-spot-migration, `runtime/DESIGN.md`); the
  timing that failed 3 times out of 3 before gives 10 jobs out of 10 done.
- [ ] **`LocalDrain` after `fork`.** A child that inherits a started channel has
  its descriptors and none of its service thread. The tests fork before a
  channel starts. Decide whether a started channel must refuse to be used in a
  child, and say so in the header.
- [ ] **A host manager that dies.** The channel marks the control link as lost
  and every lookup then fails with that reason, but links that are up keep
  working. Decide whether the job should end at once (D14 says a failed move
  ends the job; a lost host manager is not a move).
- [ ] **SIGPIPE ownership differs between S3 and the shared helper.**
  `suppress_sigpipe` preserves `SIG_IGN`; S3 treats it as unclaimed and replaces
  it with the SDK handler. Apply one rule for application-owned dispositions.
- [ ] **Permanent store errors use inconsistent exception types.**
  S3 uses `Utils::BackendFailure`; equivalent Redis errors, including a value
  length mismatch, use `std::runtime_error`. Make the caller-visible contract
  consistent.
- [ ] **`SOL_TCP` fallback macros appear inside a function body**
  (`TcpChannelBase::apply_socket_options`). Their preprocessor scope extends
  beyond the function.

## 3. Performance

- [ ] **Large point-to-point messages on the drain data path.** In the
  2026-09-16/17 overhead campaign (fmi-spot-migration,
  `benchmarks/overhead/RESULTS-slimfly27.md`, *Latency and bandwidth*) a 4 MiB
  ping-pong between two otherwise idle ranks over 100 Gbit/s IPoIB took 1.2 to
  1.5 times as long on `DrainTCP` as on DirectTCP, in all three drain
  configurations, the unarmed one included. `LocalDrain` keeps the shape of that
  loop (one non-blocking system call per acquisition of the per-link lock, a
  `poll` between partial transfers), so measure it first. Whatever changes must
  keep what the loop exists for: a drain can interrupt an operation between two
  steps and resume it at a byte offset.
- [ ] **The 20 ms wait slice of `TcpChannelBase::pump`.** A wait on a link wakes
  every 20 ms to accept on the listener. Polling the listener together with the
  link would remove the wake-ups.
- [ ] **DirectTCP calls `accept()` on every slice**, also when nothing is
  waiting.
- [ ] **`LocalDrain` receives into a drained buffer with `vector::insert`.** A
  drain of a large backlog reallocates as it grows. Measure the drain of a full
  socket buffer at 128 ranks before changing it.

## 4. Structure

- [ ] DirectTCP and `LocalDrain` both bind a listener, exchange a hello and dial
  without blocking. Share what is the same (`TcpEndpoint` already holds the byte
  order and address helpers); the discovery differs (Redis registry against the
  host manager) and stays separate.
- [ ] Share hiredis connection setup, timeout conversion, fork checks, redial,
  argv marshalling, and reply ownership between Redis and `PeerRegistry`.
- [ ] Share ClientServer reduce/scan folding and store upload retry loops where
  their ordering and failure semantics agree.
- [ ] Move the duplicated `poll_attempts` calculation into
  `RecoverableClientServer`, which owns the timeout values.
- [ ] Consolidate scalar/vector Python reduction dispatch. Reconsider whether
  `PythonBindingSupport.h` needs to be separate from its sole consumer, while
  preserving the custom-operation flags fix.
- [ ] Review `S3::delete_object` and S3's flag-off transient-failure counters:
  use them or remove them. Check callers in fmi-spot-migration first.
  `DirectTCP::connection_count` is used by the overhead benchmark.

## 5. Configuration and build

- [x] **Leftovers.** The `trim_heap` setting is gone (`6a61e7e`): the heap is always trimmed
  before a stop, as in every run so far. The `s<peer>=` and `r<peer>=` counters in a rank's
  `drained` line stay: the host manager no longer reads them, but three cases of
  `tests/local_drain.cpp` check the counters through them.

- [ ] Consolidate test-only JSON configurations under tests or generate them from
  a shared base. Preserve the relative-path behavior expected by test execution.
- [ ] Remove unused model blocks for backends a configuration never enables.
- [ ] Correct the `FMI_ENABLE_REDIS` CMake help text: it gates DirectTCP as well
  as the Redis data backend. `LocalDrain` is always built and needs no Redis.
- [ ] Make S3's ineffective `object_ttl_s` setting harder to mistake for cleanup.
  Recovery requires bucket lifecycle expiration; the existing warning can be
  missed in deployment logs.

Independent fixes for pre-existing FMI bugs are already carried by this branch;
see [CLAUDE-redesign.md](CLAUDE-redesign.md). Do not undo those fixes when isolating migration costs.
