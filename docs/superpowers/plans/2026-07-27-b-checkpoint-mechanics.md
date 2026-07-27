# Checkpoint Mechanics (Stage B) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Retire the highest-variance CRIU unknowns — pid reclaim, `--tcp-close`, threads frozen under a held lock, `PR_SET_PTRACER` survival, post-restore signal/EINTR hygiene, and freeze cost as a function of RSS — by building the resident-agent freeze/dump/stage/restore harness of the design spec's normative contract 4 and validating it end to end against a toy process that contains **no FMI**, so that Plan C's commitment point (spec staging map, stage **C2**) is taken with measured data instead of assumptions.

**Architecture:** A ~50-line standalone toy target is the dump/restore subject for every experiment. Around it, three new experimental components are added under `src/ft/experimental/`: `ProcessIdentity` (pidfd + start time + registered nonce, verified before any freeze), `AgentDirectory` (an additive `fmi:ft:<comm>:ckpt:` Redis key set with its own hiredis connection, a pause pub/sub channel, CAS state transitions and restore leases), and `CheckpointService` (freeze → dump → stage → verify-digest → CAS `CHECKPOINTED`, and restore-under-lease → commit CAS → `SIGCONT`/`SIGKILL`). `LocalRankAgent` gains a resident `daemon` verb and a batched freeze-all-then-dump evacuation path; `tools/rank_agent.cpp` exposes them. Every experiment writes a recorded result into a companion findings file, and each experiment's conclusion is pinned by a Boost test case in a new `CriuLab` suite so it cannot silently regress.

**Tech Stack:** C++17, criu 4.2 (`/usr/local/sbin/criu`, `cap_sys_ptrace=eip`), Linux `pidfd_open`/`pidfd_send_signal`/`/proc/<pid>/stat`, hiredis (pub/sub + `EVAL`), Redis 7 on `127.0.0.1:6379`, Boost.Test, CMake, POSIX threads/processes/pipes/sockets.

## Global Constraints

- The design spec `docs/superpowers/specs/2026-07-27-sequenced-incarnation-links-design.md` is normative. This plan implements **Axis B / stage B** only and says HOW and IN WHAT ORDER; it never restates WHAT or WHY. Cite it as "contract 4", "contract 3 restore commit discipline", etc.
- **No dependency on ControlPlane v2.** Do not modify `include/ft/ControlPlane.h` or `src/ft/ControlPlane.cpp`. Plan B reuses the existing CRIU surface (`include/ft/ControlPlane.h:112-125`) and adds every new key under a disjoint `fmi:ft:<comm>:ckpt:` namespace owned by `AgentDirectory`. Plan C stage C3 replaces `ControlPlane` and Plan A task 6 makes one additive edit to it (a defaulted `data_comm_name` parameter on `clear_job_state`); a Plan B edit there would collide with both. Plan B consumes only the v1 read/write surface it already uses, so nothing here needs either of those changes to have landed.
- **Disjoint file set.** Plan B may touch only: `tools/`, `src/ft/experimental/`, `include/ft/experimental/`, `tests/criu_lab.cpp`, `tests/CMakeLists.txt`, the `FMI_ENABLE_CRIU` source list in `CMakeLists.txt` (`CMakeLists.txt:72-77`), and `docs/superpowers/findings/2026-07-27-b-checkpoint-mechanics-findings.md`. It must not touch `src/comm/`, `include/comm/`, `include/Communicator.h`, `src/Communicator.cpp`, `src/ft/TransparentMigrationRuntime.cpp`, or the `tests/migration_*counterexample*` files — those belong to Plans A and C.
- **No configuration-schema changes.** `include/utils/Configuration.h` (the `Criu` block is `include/utils/Configuration.h:34-39`) stays untouched — Plan C task 1 adds the sibling `fault_tolerance.link` block there and owns that file; new knobs are environment variables (`FMI_CKPT_*`), defaulting from `config.criu.poll_ms` / `config.criu.quiesce_timeout_ms` / `config.criu.images_dir`.
- **The dump/restore subject is the toy target and it must not link FMI.** Its CMake target links `Threads::Threads` (and `${HIREDIS_LIBRARIES}` only for the hiredis experiment). Any experiment that needs FMI in the image is out of scope for stage B.
- Every task must end with a tree that configures, builds, and passes the whole suite. Every task names its own new test case.
- **Experiments are deliverables.** Each experiment task appends a section to
  `docs/superpowers/findings/2026-07-27-b-checkpoint-mechanics-findings.md` using the fixed template from Task 1 (Question / Method / Raw result / Verdict / Consequence for the spec) **in the same commit** as its code, and pins its verdict with a Boost assertion.
- Real-criu test cases must self-skip loudly (`BOOST_TEST_MESSAGE` + a recorded reason) when `criu` is missing or lacks capabilities, following the skip pattern of `tests/criu_fault_tolerance.cpp:44-53`. Mock-criu cases (`tests/criu_fault_tolerance.cpp:194-243`, which records every invocation's argv into `<dir>/<mode>.args`) carry the flag-composition assertions so they run everywhere.
- Machine facts for every verification block on this host: Redis native at `127.0.0.1:6379`; `tcpunchd` is `./extern/TCPunch/server/build-fresh/tcpunchd 10000` (the `build/` copy is stale); criu 4.2 at `/usr/local/sbin/criu` with `cap_net_admin,cap_sys_ptrace,cap_sys_admin,cap_sys_resource,cap_checkpoint_restore=eip`.
- Build flags: `-DFMI_BUILD_TESTS=ON -DFMI_ENABLE_CRIU=ON`. `FMI_ENABLE_CRIU=ON` forces `FMI_ENABLE_REDIS=ON` (`CMakeLists.txt:13-14`), and without it neither the rank agent (`tools/CMakeLists.txt:1-5`) nor the counterexample binaries are built.
- The `Communicator`/`Channels` suites resolve `../../config/fmi_test.json` (`tests/communicator.cpp:12`), so the full suite is run from `build/tests`. `config/fmi_test.json:19` still holds the stale LAN address `192.168.0.166` for `backends.Direct.host`; override it to `127.0.0.1` locally to get a green Direct run and **do not commit that override**.
- Use `uv`, not bare `pip`, for any Python; this plan requires no Python packages.
- Stage and commit only files belonging to the current task; preserve all pre-existing working-tree changes (the branch already carries modified `runbooks/**/orchestrator.py`, `tests/CMakeLists.txt`, `tests/channels.cpp`, `tests/communicator.cpp` and untracked `tests/forked_rank_guard.h`, `tests/migration_p2p_cut_counterexample.cpp`).

---

### Task 1: Toy target process and the `CriuLab` harness suite

**Files:**
- Create: `tools/criu_lab/toy_target.cpp`
- Create: `tests/criu_lab.cpp`
- Create: `docs/superpowers/findings/2026-07-27-b-checkpoint-mechanics-findings.md`
- Modify: `tools/CMakeLists.txt`
- Modify: `tests/CMakeLists.txt`

**Interfaces:**
- The toy is a standalone `main` with no FMI include and no FMI link. CLI:

```text
fmi-criu-toy --state-file <path> --nonce <str> [--tick-ms 50]
             [--socket none|listen|connect|midconnect] [--peer <host:port>] [--listen-port <p>]
             [--threads 1|2] [--hold-mutex] [--block-in-poll]
             [--ballast-mb <n>] [--hiredis <host:port>] [--exit-after <n>]
```

- Behaviour: on start it truncates `--state-file` and writes one identity header line, then every `--tick-ms` it increments a counter and rewrites the file atomically (write to `<path>.tmp`, `rename`). The line format is stable and is the whole post-restore oracle:

```text
nonce=<str> pid=<n> start_ticks=<n> tick=<n> monotonic_ms=<n> mutex_held=<0|1> \
  sock=<none|listen|connect|midconnect> sock_fd=<n|-1> sock_errno=<n> sock_ok=<0|1> \
  poll_wakeups=<n> tls_probe=<n> rss_kb=<n>
```

- `--threads 2 --hold-mutex` starts a second thread that takes a `std::mutex`, sets `mutex_held=1`, sleeps past the freeze window, then releases; the main tick loop must acquire the same mutex to publish a tick, so a freeze taken while the lock is held is observable as a tick stall. `--block-in-poll` parks the second thread in `poll(2)` on a pipe with a long timeout and counts wakeups (an EINTR-triggered spurious wakeup increments `poll_wakeups`).
- `--socket connect` establishes a TCP connection to `--peer` and, once per tick, does a 1-byte `send`/`recv` recording `sock_errno`/`sock_ok`. `--socket listen` binds and listens without accepting. `--socket midconnect` starts a non-blocking `connect` to a blackholed address and never completes it.
- `--hiredis <host:port>` opens a hiredis connection and issues one `PING` per tick, recording the outcome in `sock_ok`/`sock_errno`.
- `tests/criu_lab.cpp` declares `BOOST_AUTO_TEST_SUITE(CriuLab)` and shared helpers: `spawn_toy(args) -> ToyHandle{pid, state_path}`, `read_toy_state(path) -> map<string,string>`, `wait_for_tick_at_least(path, n, timeout_ms)`, `criu_available()` (checks `FMI_CKPT_CRIU_BIN` or `/usr/local/sbin/criu` is executable), and `toy_binary_path()` (from the `FMI_CRIU_TOY_PATH` compile definition, skipping loudly if absent).

- [ ] **Step 1: Verify the target is absent (RED)**

```bash
cmake --build build --target fmi_criu_toy_target -j2
./build/tests/Boost_Tests_run --run_test=CriuLab --list_content
```

Expected: `No rule to make target 'fmi_criu_toy_target'`, and the suite listing reports no `CriuLab`.

- [ ] **Step 2: Write the toy target**

Implement `tools/criu_lab/toy_target.cpp` to the CLI and state-file contract above. Keep it under ~200 lines including argument parsing; the *process* it models is ~50 lines of behaviour. `start_ticks` is field 22 of `/proc/self/stat` (parse after the last `')'` so a parenthesised comm cannot shift the fields). `rss_kb` is `VmRSS` from `/proc/self/status`. No FMI header may be included.

- [ ] **Step 3: Wire the CMake targets**

In `tools/CMakeLists.txt`, inside the existing `if(FMI_ENABLE_CRIU)` block (`tools/CMakeLists.txt:1-5`):

```cmake
find_package(Threads REQUIRED)
add_executable(fmi_criu_toy_target criu_lab/toy_target.cpp)
set_target_properties(fmi_criu_toy_target PROPERTIES OUTPUT_NAME "fmi-criu-toy")
target_link_libraries(fmi_criu_toy_target Threads::Threads)
if(FMI_ENABLE_REDIS)
    target_include_directories(fmi_criu_toy_target PRIVATE ${HIREDIS_INCLUDE_DIRS})
    target_link_libraries(fmi_criu_toy_target ${HIREDIS_LIBRARIES})
    target_compile_definitions(fmi_criu_toy_target PRIVATE FMI_TOY_HIREDIS=1)
endif()
# tests/ is added before tools/ (CMakeLists.txt:111,115), so the test target already exists here.
if(TARGET Boost_Tests_run)
    add_dependencies(Boost_Tests_run fmi_criu_toy_target)
endif()
```

In `tests/CMakeLists.txt`, append `criu_lab.cpp` to the `FMI_ENABLE_CRIU` branch of `FMI_TEST_SOURCES` (`tests/CMakeLists.txt:6-9`) and add:

```cmake
target_compile_definitions(Boost_Tests_run PRIVATE
    FMI_CRIU_TOY_PATH="${CMAKE_BINARY_DIR}/tools/fmi-criu-toy")
```

- [ ] **Step 4: Add the findings file skeleton**

Create `docs/superpowers/findings/2026-07-27-b-checkpoint-mechanics-findings.md` with a title, a "Machine" block (kernel `uname -srm`, `criu --version`, `getcap /usr/local/sbin/criu`, Redis version), a table of contents listing experiments 1-7 plus the deployment decision, all marked `PENDING`, and this required per-experiment template:

```text
### Experiment <n> — <title>
**Question:** ...
**Method:** exact commands, pasteable
**Raw result:** verbatim output / measured table
**Verdict:** one sentence
**Consequence for the spec:** which contract/open item it settles, and what Plan C must assume
```

- [ ] **Step 5: Add the first `CriuLab` test (GREEN)**

Add `BOOST_AUTO_TEST_CASE(toy_target_publishes_identity_and_advances)`: spawn the toy with `--tick-ms 20 --nonce lab-1`, wait for `tick >= 5`, assert `nonce`, `pid` equals the spawned pid, `start_ticks > 0`, `tick` strictly increases across two reads, then `SIGKILL` + `waitpid`. Run:

```bash
cmake -S . -B build -DFMI_BUILD_TESTS=ON -DFMI_ENABLE_CRIU=ON
cmake --build build -j"$(nproc)"
./build/tests/Boost_Tests_run --run_test=CriuLab
(cd build/tests && ./Boost_Tests_run)
ldd build/tools/fmi-criu-toy | grep -ci fmi || echo "toy does not link FMI: OK"
```

Expected: `CriuLab` passes; the full suite passes; the toy links no FMI.

- [ ] **Step 6: Commit**

```bash
git add tools/criu_lab/toy_target.cpp tools/CMakeLists.txt tests/criu_lab.cpp \
  tests/CMakeLists.txt docs/superpowers/findings/2026-07-27-b-checkpoint-mechanics-findings.md
git commit -m "test: add CRIU lab toy target and harness suite"
```

---

### Task 2: Verified process identity and the freeze primitive

**Files:**
- Create: `include/ft/experimental/ProcessIdentity.h`
- Create: `src/ft/experimental/ProcessIdentity.cpp`
- Modify: `CMakeLists.txt`
- Modify: `tests/criu_lab.cpp`

**Interfaces:**
- Per contract 4, identity is verified **before any freeze**, so a recycled PID is never frozen. Produces:

```cpp
namespace FMI::FT {
    //! Immutable identity of a checkpoint target: the tuple that survives a PID being reused.
    struct ProcessIdentity {
        int pid = 0;
        std::uint64_t start_ticks = 0;   //!< field 22 of /proc/<pid>/stat
        std::string nonce;               //!< registered by the target itself
    };

    //! Read the current identity of @p pid. Throws when the pid is gone.
    [[nodiscard]] ProcessIdentity read_process_identity(int pid, std::string nonce);

    //! An open pidfd bound to one verified identity. Every signal goes through this handle, so
    //! a pid recycled after the check can never receive it.
    class VerifiedProcess {
    public:
        //! Open a pidfd for @p expected.pid and re-verify start_ticks and the nonce AFTER the
        //! pidfd is open (open-then-recheck closes the check/act race). Returns nullopt when the
        //! identity does not match; throws only on unexpected OS errors.
        static std::optional<VerifiedProcess> open(const ProcessIdentity& expected,
                                                   const std::string& nonce_path);
        VerifiedProcess(VerifiedProcess&&) noexcept;
        ~VerifiedProcess();

        void signal(int sig) const;      //!< pidfd_send_signal
        void freeze() const;             //!< SIGSTOP + wait until /proc/<pid>/stat state is 'T'
        void thaw() const;               //!< SIGCONT
        [[nodiscard]] bool alive() const;
        [[nodiscard]] const ProcessIdentity& identity() const;
        [[nodiscard]] std::chrono::microseconds last_freeze_latency() const;
    };
}
```

- `freeze()` records wall-clock microseconds from `SIGSTOP` issue to the first `/proc/<pid>/stat` observation of state `T`; Task 7 reads it. The nonce is read from `<nonce_path>` (the toy's state file) so identity does not depend on any control plane in this task.
- Add `src/ft/experimental/ProcessIdentity.cpp` to the `FMI_ENABLE_CRIU` source list at `CMakeLists.txt:72-77`.

- [ ] **Step 1: Prove the gap (RED)**

Add the new cases first and run:

```bash
cmake --build build -j"$(nproc)" 2>&1 | tail -5
```

Expected: compile error — `ProcessIdentity.h` does not exist.

- [ ] **Step 2: Implement identity capture and the pidfd handle**

Use `syscall(SYS_pidfd_open, pid, 0)` and `syscall(SYS_pidfd_send_signal, fd, sig, nullptr, 0)` (no glibc wrapper is assumed). Order is mandatory: read identity → open pidfd → re-read identity → compare. `freeze()` polls `/proc/<pid>/stat` at 200 µs for up to 2 s and throws `FMI::Utils::Timeout` if the task never reaches `T`.

- [ ] **Step 3: Add the identity tests (GREEN)**

- `identity_matches_live_toy`: spawn the toy, `VerifiedProcess::open` succeeds, `identity().nonce` matches.
- `identity_rejects_recycled_pid`: capture the toy's identity, `SIGKILL` + reap it, then spawn a second toy with a **different** nonce in a loop until it lands on the same pid (bounded attempts; skip loudly with the attempt count if the pid is never reclaimed within the budget) and assert `VerifiedProcess::open(old_identity, ...)` returns `nullopt`. Also assert the cheap half unconditionally: `open()` against a captured identity whose `start_ticks` has been perturbed by +1 returns `nullopt`.
- `freeze_then_thaw_stalls_and_resumes_ticks`: freeze the toy, assert `tick` is unchanged across a 300 ms window, thaw, assert `tick` advances again.

```bash
cmake --build build -j"$(nproc)"
./build/tests/Boost_Tests_run --run_test=CriuLab
(cd build/tests && ./Boost_Tests_run)
```

- [ ] **Step 4: Commit**

```bash
git add include/ft/experimental/ProcessIdentity.h src/ft/experimental/ProcessIdentity.cpp \
  CMakeLists.txt tests/criu_lab.cpp
git commit -m "feat: verify process identity via pidfd before freezing"
```

---

### Task 3: Experiment 1 — same-host pid reclaim and the fate of `migrate`/`migrate-local`

**Files:**
- Create: `tools/criu_lab/exp1_pid_reclaim.sh`
- Modify: `src/ft/experimental/LocalRankAgent.cpp`
- Modify: `include/ft/experimental/LocalRankAgent.h`
- Modify: `tests/criu_lab.cpp`
- Modify: `docs/superpowers/findings/2026-07-27-b-checkpoint-mechanics-findings.md`

**Interfaces:**
- Today's discipline is stated verbatim at `src/ft/experimental/LocalRankAgent.cpp:417` ("No `--leave-stopped`: criu ptrace-seizes, dumps, then kills and reaps the task, freeing the pid so the immediate restore can reclaim it"), and `restore_rank` (`src/ft/experimental/LocalRankAgent.cpp:441-453`) passes no pid-namespace or remap flag. Per contract 4, Plan B must state the fate of the same-host verbs explicitly.
- Produces an explicit, documented split in `LocalRankAgent`:

```cpp
    //! Which dump discipline a call site needs. Same-host in-place restore reclaims the dumped
    //! pid, which is only free after criu has killed and reaped the original — so KillAndReclaim
    //! has NO frozen-original abort path. LeaveStopped keeps the original frozen for abort
    //! recovery and is therefore cross-host only (the restore happens on another host, where the
    //! pid is free). See findings experiment 1.
    enum class DumpDiscipline { KillAndReclaim, LeaveStopped };

    void dump_rank(int pid, const std::string& dir, DumpDiscipline discipline) const;
```

- **Every existing call site keeps `KillAndReclaim`, and therefore keeps its behaviour byte-for-byte**: `migrate_rank` / `migrate_ranks` / `migrate_local` (`src/ft/experimental/LocalRankAgent.cpp:98-221`) *and* `evacuate_local` (`:223-271`). Only the flag becomes explicit; the header documents the guarantee `KillAndReclaim` cannot offer. `LeaveStopped` gets its **first and only** call site in Task 12's new `evacuate_local_batched`, which is a second path added beside `evacuate_local`, not a change to it.
  > **Why not switch `evacuate_local` here.** It is the verified path behind `runbooks/k8s-criu-node-evacuation/`; flipping it to `--leave-stopped` would leave the dumped originals frozen on the evacuated host instead of reaped, changing a cluster-verified runbook's observable behaviour inside a task whose stated contract is "existing call sites keep their behaviour". Task 12 introduces the new discipline where it can be validated against toy targets first.

- [ ] **Step 1: Reproduce both branches (RED, and this is the experiment)**

Write `tools/criu_lab/exp1_pid_reclaim.sh` to run four cases against the toy on a single host, printing the criu exit code and the tail of `restore.log` for each:

1. dump without `--leave-stopped`, then restore → expect success, pid reclaimed.
2. dump with `--leave-stopped`, then restore with the original still frozen → expect failure `Can't fork for <pid>: File exists`.
3. case 2, but `SIGKILL` + reap the original before restoring → expect success.
4. case 2, but restore inside a new pid namespace (`unshare --pid --fork --mount-proc`) → record whether criu 4.2 restores the dumped pid there and whether the restored task remains reachable from the host (`pidfd_open` on the *host* pid).

```bash
CRIU=/usr/local/sbin/criu bash tools/criu_lab/exp1_pid_reclaim.sh 2>&1 | tee /tmp/fmi-exp1.log
```

Expected: case 2 fails with the quoted message; cases 1 and 3 succeed. Cases 2 and 4 are the decision data.

- [ ] **Step 2: Record the finding and the disposition**

Append experiment 1 to the findings file using the Task 1 template, pasting `/tmp/fmi-exp1.log`. The verdict must state, in one sentence, which of the three enumerated dispositions holds for same-host `migrate`/`migrate-local`: (a) keep the kill-and-reclaim discipline and lose the frozen-original abort path, (b) move to a pid namespace, or (c) lose the same-host verbs. The default disposition this plan implements is **(a)**, because the frozen original and the reclaimed pid are mutually exclusive on one host; case 4's result decides whether (b) is recorded as a viable future option or ruled out.

- [ ] **Step 3: Make the discipline explicit in code**

Add `DumpDiscipline` and thread it through `dump_rank`. When `LeaveStopped`, append `--leave-stopped` to the criu argv. Extend the comment block at `src/ft/experimental/LocalRankAgent.cpp:407-419` to name the experiment and the lost abort path rather than repeating the rationale. Leave the three-attempt retry (`:420-438`) unchanged.

- [ ] **Step 4: Pin it with tests (GREEN)**

- In `tests/criu_lab.cpp`: `same_host_restore_requires_the_original_reaped` — real criu, self-skipping. Dump the toy with `--leave-stopped`, assert the restore fails; kill the original, assert the identical restore succeeds and the restored toy's `tick` continues from the dumped value (not from 0).
- In the same suite: `dump_discipline_flags_are_explicit` — mock-criu case reusing the `<dir>/<mode>.args` recording of `tests/criu_fault_tolerance.cpp:194-243`; call `dump_rank` twice against the mock, once per enumerator, and assert `DumpDiscipline::KillAndReclaim` produces a `dump.args` with **no** `--leave-stopped` while `DumpDiscipline::LeaveStopped` produces one with it. Also assert that every *existing* call site (`migrate_local`, `evacuate_local`) still lands on the `KillAndReclaim` argv, which is the regression guard for the "behaviour is unchanged" claim above.

```bash
cmake --build build -j"$(nproc)"
./build/tests/Boost_Tests_run --run_test=CriuLab
./build/tests/Boost_Tests_run --run_test=CriuFaultTolerance
(cd build/tests && ./Boost_Tests_run)
```

- [ ] **Step 5: Commit**

```bash
git add tools/criu_lab/exp1_pid_reclaim.sh src/ft/experimental/LocalRankAgent.cpp \
  include/ft/experimental/LocalRankAgent.h tests/criu_lab.cpp \
  docs/superpowers/findings/2026-07-27-b-checkpoint-mechanics-findings.md
git commit -m "feat: make the dump discipline explicit and record pid-reclaim findings"
```

---

### Task 4: Experiment 2 — `--tcp-close` on dump and restore

**Files:**
- Create: `tools/criu_lab/exp2_tcp_close.sh`
- Modify: `src/ft/experimental/LocalRankAgent.cpp`
- Modify: `tests/criu_lab.cpp`
- Modify: `docs/superpowers/findings/2026-07-27-b-checkpoint-mechanics-findings.md`

**Interfaces:**
- Per contract 3's restore commit discipline, criu 4.2 records `--tcp-close` in the image and requires it again at restore (failure mode `criu/image.c:94: Need to set the --tcp-close options`). Both `dump_rank` and `restore_rank` therefore gain the flag.
- **This contradicts two in-tree comments that must be corrected in the same commit:** `src/ft/experimental/LocalRankAgent.cpp:407-413` ("No TCP flags: a quiesced rank holds no established TCP socket … criu therefore has nothing to repair or close") and `include/ft/ControlPlane.h:86-92` ("which removes the need for criu's `--tcp-close`"). Both are true only for the quiesce-point-dump discipline the spec retires. `include/ft/ControlPlane.h` is outside Plan B's file set, so correct only the `LocalRankAgent` comment and record the header's stale claim in the findings for Plan C.

- [ ] **Step 1: Reproduce the three socket shapes (RED, and this is the experiment)**

`tools/criu_lab/exp2_tcp_close.sh` runs a matrix over `{established, midconnect, listen} × {dump with/without --tcp-close} × {restore with/without --tcp-close}`, using the toy's `--socket` modes against a peer `nc -l` (established), a blackholed RFC 5737 address `192.0.2.1:9` (midconnect), and `--socket listen` (LISTEN). For each cell record: criu dump exit code, restore exit code, the exact error line, and — on success — the restored toy's `sock_ok`/`sock_errno` on its first post-restore socket use.

```bash
CRIU=/usr/local/sbin/criu bash tools/criu_lab/exp2_tcp_close.sh 2>&1 | tee /tmp/fmi-exp2.log
grep -n "Need to set the --tcp-close" /tmp/fmi-exp2.log
```

Expected: dump-with/restore-without reproduces the `criu/image.c:94` message; the established case restores with a closed socket; the LISTEN and midconnect rows are the new data.

- [ ] **Step 2: Record the finding**

Append experiment 2 to the findings file with the full matrix as a table. The consequence section must state, per socket shape, whether `--tcp-close` alone is sufficient or whether the application must reconnect — this is the evidence base for the spec's "`--tcp-close` transparency covers FMI-managed connections only" preflight policy.

- [ ] **Step 3: Add the flag to both legs**

Append `--tcp-close` to the criu argv in `dump_rank` and in `restore_rank` (`src/ft/experimental/LocalRankAgent.cpp:441-453`). Replace the stale "No TCP flags" rationale with the recorded reason and a pointer to experiment 2. Operator flags still arrive through `FMI_CRIU_EXTRA_ARGS` in `run_criu` (`src/ft/experimental/CriuExec.cpp:54-68`), so no call site needs to duplicate them.

- [ ] **Step 4: Pin it with tests (GREEN)**

- `tcp_close_is_passed_to_dump_and_restore` — mock-criu case asserting `--tcp-close` appears in both `dump.args` and `restore.args`.
- `established_socket_survives_dump_restore_as_closed` — real criu, self-skipping: toy with `--socket connect` against a local listener, dump + restore, assert the restored toy keeps ticking and reports `sock_ok=0` with a recorded `sock_errno` on first use (the value goes into the findings).

```bash
cmake --build build -j"$(nproc)"
./build/tests/Boost_Tests_run --run_test=CriuLab
./build/tests/Boost_Tests_run --run_test=CriuFaultTolerance
(cd build/tests && ./Boost_Tests_run)
```

- [ ] **Step 5: Commit**

```bash
git add tools/criu_lab/exp2_tcp_close.sh src/ft/experimental/LocalRankAgent.cpp \
  tests/criu_lab.cpp docs/superpowers/findings/2026-07-27-b-checkpoint-mechanics-findings.md
git commit -m "feat: pass --tcp-close on both criu legs and record socket findings"
```

---

### Task 5: Experiment 3 — two threads with a mutex held across the freeze

**Files:**
- Create: `tools/criu_lab/exp3_threads.sh`
- Modify: `tools/criu_lab/toy_target.cpp`
- Modify: `tests/criu_lab.cpp`
- Modify: `docs/superpowers/findings/2026-07-27-b-checkpoint-mechanics-findings.md`

**Interfaces:**
- Extends the toy with the observables this experiment needs, without changing its existing state-file fields: `cond_waits=<n>` (a `pthread_cond_t` the second thread waits on), `tls_probe=<n>` (a `thread_local` counter, per-thread, written into the state line by the owning thread), and `futex_word=<n>` (the raw value of a `std::atomic<int>` used as a hand-rolled futex the two threads ping-pong on).
- No new component: this experiment is pure measurement plus a pinned regression test.

- [ ] **Step 1: Reproduce (RED, and this is the experiment)**

`tools/criu_lab/exp3_threads.sh` dumps and restores the toy in four configurations and records, for each, whether the restore succeeded, whether both threads resumed, whether the mutex was still owned by the same thread after restore, and the value of every observable across the cut:

1. `--threads 2` idle second thread.
2. `--threads 2 --hold-mutex`, dump taken while `mutex_held=1`.
3. `--threads 2 --block-in-poll`, dump taken while the second thread sits in `poll(2)`.
4. `--threads 2 --hold-mutex --block-in-poll` (both, worst case).

```bash
CRIU=/usr/local/sbin/criu bash tools/criu_lab/exp3_threads.sh 2>&1 | tee /tmp/fmi-exp3.log
grep -nE "mutex_held|poll_wakeups|tls_probe|cond_waits|futex_word" /tmp/fmi-exp3.log
```

Expected: all four restore; case 2 shows the lock still held by the same thread id after restore (criu restores the futex word verbatim); case 3 records whether `poll(2)` returns `EINTR` on restore.

- [ ] **Step 2: Record the finding**

Append experiment 3. The verdict must answer three questions explicitly: (i) is lock-held-at-freeze safe, (ii) does a thread blocked in `poll(2)` observe `EINTR` after restore, (iii) does TLS survive per-thread. The consequence section must state the **restart discipline** stage C's progress engine must adopt — concretely, whether the engine may hold a per-link mutex across a potential freeze point, which is direct input to the spec's open item 3 (inline-send versus full-engine).

- [ ] **Step 3: Pin it with a test (GREEN)**

`threads_and_held_mutex_survive_dump_restore` — real criu, self-skipping: run configuration 4, dump while `mutex_held=1`, restore, assert both `tick` and `cond_waits` advance after restore (i.e. neither thread is deadlocked), and assert `tls_probe` for the main thread is continuous across the cut.

```bash
cmake --build build -j"$(nproc)"
./build/tests/Boost_Tests_run --run_test=CriuLab
(cd build/tests && ./Boost_Tests_run)
```

- [ ] **Step 4: Commit**

```bash
git add tools/criu_lab/exp3_threads.sh tools/criu_lab/toy_target.cpp tests/criu_lab.cpp \
  docs/superpowers/findings/2026-07-27-b-checkpoint-mechanics-findings.md
git commit -m "test: record two-thread and held-mutex checkpoint findings"
```

---

### Task 6: Experiments 4 and 5 — `PR_SET_PTRACER` survival, SIGPIPE and EINTR after restore

**Files:**
- Create: `tools/criu_lab/exp4_ptracer.sh`
- Create: `tools/criu_lab/exp5_signals.sh`
- Modify: `tools/criu_lab/toy_target.cpp`
- Modify: `tests/criu_lab.cpp`
- Modify: `docs/superpowers/findings/2026-07-27-b-checkpoint-mechanics-findings.md`

**Interfaces:**
- Contract 4's process-level restore hygiene lists both. Today `PR_SET_PTRACER` is set once, at quiesce time, in `src/ft/TransparentMigrationRuntime.cpp:155` — a file **outside** Plan B's set, so this task only measures and records; the fix belongs to Plan C. The masking effect is real on this host: `/usr/local/sbin/criu` carries `cap_sys_ptrace=eip`, so the yama check is bypassed.
- Toy additions: `--set-ptracer` (calls `prctl(PR_SET_PTRACER, PR_SET_PTRACER_ANY, ...)` once at start) and `ptracer=<n>` in the state line (from `prctl(PR_GET_DUMPABLE)` plus a direct read-back attempt of the ptracer setting), plus `sigpipe_count=<n>` (a process-local `SIGPIPE` handler installed only under `--count-sigpipe`, never `SIG_IGN`).

- [ ] **Step 1: Reproduce experiment 4 (RED/measurement)**

`tools/criu_lab/exp4_ptracer.sh`: run the toy with `--set-ptracer`, dump+restore it, then attempt a **second** dump/restore of the restored process using a criu binary **stripped of `cap_sys_ptrace`** (copy `/usr/local/sbin/criu` to a temp path, `setcap -r` it or copy without caps) under `sysctl kernel.yama.ptrace_scope=1`, so the capability no longer masks the setting. Record whether the second dump succeeds and, if not, the exact yama error.

```bash
cat /proc/sys/kernel/yama/ptrace_scope
CRIU=/usr/local/sbin/criu bash tools/criu_lab/exp4_ptracer.sh 2>&1 | tee /tmp/fmi-exp4.log
```

Expected: the second dump fails under an uncapped criu if `PR_SET_PTRACER` does not survive — which would mean a rank can be migrated once and never again on the rootless path. Record either way; if `ptrace_scope` is 0 on this host, record that the whole check is inert here and the test must run with `ptrace_scope=1`.

- [ ] **Step 2: Reproduce experiment 5 (RED/measurement)**

`tools/criu_lab/exp5_signals.sh`: two shapes.
1. `--socket connect --count-sigpipe` against a listener that is killed **while the toy is frozen**; on restore the first `send` hits a dead socket. Record whether the process dies of `SIGPIPE`, whether `sigpipe_count` increments, and the `sock_errno`.
2. `--hiredis 127.0.0.1:6379`: dump+restore, then record the first post-restore `PING` outcome and errno, and whether hiredis reports the context as errored and reconnects.

```bash
redis-cli -h 127.0.0.1 -p 6379 ping
CRIU=/usr/local/sbin/criu bash tools/criu_lab/exp5_signals.sh 2>&1 | tee /tmp/fmi-exp5.log
```

- [ ] **Step 3: Record both findings**

Append experiments 4 and 5. Experiment 4's consequence must state whether `prctl` has to be re-armed after every restore (and by whom — the restored process's first poll, or the agent through `/proc`). Experiment 5's consequence must confirm or refute the spec's prescription of `pthread_sigmask(SIG_BLOCK, {SIGPIPE})` on the engine thread rather than a process-global `SIG_IGN`, and must state, with the measured errno, whether the first post-restore hiredis call needs a retry-on-EINTR wrapper. Note that `src/comm/Direct.cpp:36-43` and `:62-69` treat every non-`EAGAIN` errno — `EINTR` included — as fatal, so an EINTR observed here is a Plan C work item and not a Plan B fix.

- [ ] **Step 4: Pin them with tests (GREEN)**

- `restored_process_does_not_die_of_sigpipe` — real criu, self-skipping: shape 1 above; assert the restored toy is still alive and ticking after its first failed send, and that the recorded `sock_errno` matches the findings value.
- `ptracer_setting_after_restore_is_recorded` — asserts the toy's `ptracer=` field after restore equals the value recorded in the findings file (read from a small constant in the test so a change in kernel behaviour fails loudly rather than silently).

```bash
cmake --build build -j"$(nproc)"
./build/tests/Boost_Tests_run --run_test=CriuLab
(cd build/tests && ./Boost_Tests_run)
```

- [ ] **Step 5: Commit**

```bash
git add tools/criu_lab/exp4_ptracer.sh tools/criu_lab/exp5_signals.sh \
  tools/criu_lab/toy_target.cpp tests/criu_lab.cpp \
  docs/superpowers/findings/2026-07-27-b-checkpoint-mechanics-findings.md
git commit -m "test: record ptracer survival and post-restore signal findings"
```

---

### Task 7: Experiment 6 — freeze latency and frozen duration versus RSS

**Files:**
- Create: `tools/criu_lab/exp6_rss_scaling.sh`
- Modify: `tests/criu_lab.cpp`
- Modify: `docs/superpowers/findings/2026-07-27-b-checkpoint-mechanics-findings.md`

**Interfaces:**
- Two distinct numbers per data point, and the plan must not conflate them: **freeze latency** (`VerifiedProcess::last_freeze_latency()`, `SIGSTOP` → observed `T`) and **frozen duration** (`SIGSTOP` → the restored process's first advancing tick). The spec's config-surface note expects frozen duration ≈ `O(RSS)` at ~0.4 s/GB and makes `W` a function of it.
- Measurement uses the toy's `--ballast-mb` to sweep RSS at 64, 256, 512, 1024, 2048, 4096 MB, five repetitions each, reporting median and p95.

- [ ] **Step 1: Run the sweep (RED/measurement)**

```bash
free -g
CRIU=/usr/local/sbin/criu bash tools/criu_lab/exp6_rss_scaling.sh 2>&1 | tee /tmp/fmi-exp6.csv
column -s, -t /tmp/fmi-exp6.csv
```

The script emits CSV `rss_mb,rep,freeze_latency_us,dump_ms,frozen_ms,image_bytes`. Skip any ballast size exceeding half of available RAM and record the skip.

- [ ] **Step 2: Repeat the two largest sizes with `--pre-dump`/`--track-mem`**

For 2048 and 4096 MB, run a second variant: `criu pre-dump --track-mem` followed by `criu dump --prev-images-dir`, and record the frozen duration of the final dump only. This is the entire evidence base for spec open item 4.

- [ ] **Step 3: Record the finding**

Append experiment 6 with the CSV, a fitted slope in s/GB for both the plain and the pre-dump variants, and the p95 numbers. The consequence section must (i) confirm or correct the ~0.4 s/GB figure, (ii) state whether `--pre-dump`/`--track-mem` enters scope — settling spec open item 4 — and (iii) give Plan C a concrete formula for sizing `W` against measured frozen duration, closing the feedback loop the spec's config surface describes.

- [ ] **Step 4: Pin a cheap bound with a test (GREEN)**

`freeze_latency_is_sub_millisecond_at_small_rss` — real criu not required: spawn a toy at default RSS, freeze it via `VerifiedProcess`, assert `last_freeze_latency()` is under a generous 50 ms ceiling and log the actual value with `BOOST_TEST_MESSAGE`. The ceiling exists to catch a pathological regression (e.g. a freeze implementation that starts polling `/proc` at 100 ms), not to encode the measurement.

```bash
cmake --build build -j"$(nproc)"
./build/tests/Boost_Tests_run --run_test=CriuLab
(cd build/tests && ./Boost_Tests_run)
```

- [ ] **Step 5: Commit**

```bash
git add tools/criu_lab/exp6_rss_scaling.sh tests/criu_lab.cpp \
  docs/superpowers/findings/2026-07-27-b-checkpoint-mechanics-findings.md
git commit -m "test: measure freeze latency and frozen duration against RSS"
```

---

### Task 8: Additive agent directory — state, leases, manifests, pause pub/sub

**Files:**
- Create: `include/ft/experimental/AgentDirectory.h`
- Create: `src/ft/experimental/AgentDirectory.cpp`
- Modify: `CMakeLists.txt`
- Modify: `tests/criu_lab.cpp`

**Interfaces:**
- Per the global constraint, this is a **separate** Redis client with its own hiredis context, not a `ControlPlane` change. `ControlPlane`'s single connection is mutex-guarded (`src/ft/ControlPlane.cpp:112-150`) and a blocking `SUBSCRIBE` would monopolise it, so a second connection is required regardless.
- Keys live under `fmi:ft:<comm>:ckpt:` — disjoint from `prefix()` (`src/ft/ControlPlane.cpp:192`) and from `criu_prefix()` (`src/ft/ControlPlane.cpp:225-244`), so `clear_criu_state`'s `criu:*` sweep (`src/ft/ControlPlane.cpp:443-456`) does not touch them and `AgentDirectory::clear()` owns them. Note that `clear_job_state` (`src/ft/ControlPlane.cpp:424-440`) sweeps only `<prefix>epoch:*`, so it does not reach `ckpt:` either — **including after Plan A task 6 extends it with an optional data-namespace sweep**, which is scoped to the caller's `data_comm_name` and never to `fmi:ft:`:

```text
fmi:ft:<comm>:ckpt:state              HASH  rank -> ACTIVE|PAUSING|CHECKPOINTED|RESTORE_RESERVED|ACTIVATING
fmi:ft:<comm>:ckpt:pause              HASH  rank -> migration_id
fmi:ft:<comm>:ckpt:nonce              HASH  rank -> identity nonce registered by the target
fmi:ft:<comm>:ckpt:lease:<rank>       STR   attempt_id, SET NX EX <lease_ttl_s>
fmi:ft:<comm>:ckpt:manifest:<mid>:<r> HASH  uri, sha256, bytes, created_ms, discipline
fmi:ft:<comm>:ckpt:events             PUB/SUB channel; message body "<rank> <migration_id> <verb>"
```

- Produces:

```cpp
namespace FMI::FT {
    enum class CkptState : std::uint8_t { Active, Pausing, Checkpointed, RestoreReserved, Activating };

    struct ImageManifest {
        std::string uri;          //!< file://<path> or redis:<epoch>:<rank>
        std::string sha256;
        std::uint64_t bytes = 0;
        std::uint64_t created_ms = 0;
        std::string discipline;   //!< "kill-and-reclaim" | "leave-stopped"
    };

    struct PauseRequest {
        FMI::Utils::peer_num rank = 0;
        std::string migration_id;
    };

    //! Stage-B checkpoint directory. Additive over the existing control plane: its own hiredis
    //! connection, its own `ckpt:` key namespace, no ControlPlane API surface touched. Every
    //! mutation is a CAS on (rank, expected state) executed as one Lua script, so two agents can
    //! never both believe they own a transition.
    class AgentDirectory {
    public:
        AgentDirectory(const FMI::Utils::FaultToleranceConfig& config, std::string comm_name);
        ~AgentDirectory();

        void register_target(FMI::Utils::peer_num rank, const std::string& nonce) const;
        [[nodiscard]] std::string nonce_for(FMI::Utils::peer_num rank) const;

        //! Orchestrator side: mark ranks PAUSING and publish one event per rank.
        void request_pause(const std::vector<FMI::Utils::peer_num>& ranks,
                           const std::string& migration_id) const;
        [[nodiscard]] std::vector<PauseRequest> pause_set() const;

        //! Agent side, CAS-guarded. Each returns false when the expected state did not hold.
        bool mark_checkpointed(FMI::Utils::peer_num rank, const std::string& migration_id,
                               const ImageManifest& manifest) const;
        bool abort_pause(FMI::Utils::peer_num rank, const std::string& migration_id) const;
        bool acquire_restore_lease(FMI::Utils::peer_num rank, const std::string& attempt_id,
                                   unsigned int ttl_s) const;
        bool commit_restore(FMI::Utils::peer_num rank, const std::string& attempt_id) const;
        bool abort_restore(FMI::Utils::peer_num rank, const std::string& attempt_id) const;
        bool mark_active(FMI::Utils::peer_num rank) const;

        [[nodiscard]] CkptState state_of(FMI::Utils::peer_num rank) const;
        [[nodiscard]] std::optional<ImageManifest> manifest_for(FMI::Utils::peer_num rank,
                                                                const std::string& migration_id) const;
        void clear() const;

        //! Blocking subscription used by the daemon verb; see Task 9.
        class Subscription;
        [[nodiscard]] std::unique_ptr<Subscription> subscribe() const;
    };
}
```

- `acquire_restore_lease` accepts `CHECKPOINTED` **or** `RESTORE_RESERVED`-with-expired-lease, per contract 3. `abort_pause` is idempotent by `migration_id`. This task implements the state machine and its CAS scripts only; the daemon, staging and restore legs arrive in Tasks 9-11. Add `src/ft/experimental/AgentDirectory.cpp` to `CMakeLists.txt:72-77`.

> **This is a deliberate, scheduled duplication — record it as such.** Contract 3 specifies **one**
> directory under `fmi:ft:<comm>:`, and Plan C stage C3 implements it as `ControlPlane` v2. The
> `ckpt:` namespace here is a **shadow implementation of contract 3's five agent-driven edges**
> (`PAUSING → CHECKPOINTED`, `abort_pause`, `acquire_restore_lease`, `commit_restore`,
> `abort_restore`), built in a disjoint namespace for exactly one reason: `ControlPlane` v2 does not
> exist yet and stage B must land without it. It is not a second permanent authority.
>
> **Convergence is C3's job and is listed in its work items.** When C3 lands, the `ckpt:` namespace
> is deleted and `AgentDirectory` becomes a thin client of `ControlPlane` v2's Lua scripts. The five
> state-machine cases below (`ckpt_second_lease_is_refused_while_the_first_holds`,
> `ckpt_commit_restore_requires_the_owning_attempt`, `ckpt_abort_pause_is_idempotent_by_migration_id`,
> and the happy path) are written against the `AgentDirectory` **interface**, not its Redis keys, so
> they survive that swap unchanged and become C3's acceptance tests for the real scripts. Do not
> assert on raw key names inside these cases except in `ckpt_namespace_is_disjoint_from_criu_state`,
> which is the one case that legitimately dies with the shadow namespace.

- [ ] **Step 1: Prove the gap (RED)**

```bash
redis-cli -h 127.0.0.1 -p 6379 keys 'fmi:ft:*:ckpt:*'
```

Expected: empty — no `ckpt:` namespace exists.

- [ ] **Step 2: Implement the directory**

One `EVAL` script per transition, each taking `(rank, expected_state, token)` and returning 1/0. Reuse the existing hiredis reply-ownership pattern (`ReplyPtr` with `freeReplyObject`, `src/ft/ControlPlane.cpp:23`) rather than raw pointers. Every state string is `to_string(CkptState)`-round-trippable so the Redis contents stay human-readable, matching `include/ft/Common.h:19-42`.

- [ ] **Step 3: Add the state-machine tests (GREEN)**

New cases in `CriuLab`, all skipping loudly when Redis is unreachable:
- `ckpt_pause_then_checkpoint_then_lease_then_commit` — the happy path, asserting each intermediate `state_of`.
- `ckpt_second_lease_is_refused_while_the_first_holds` — two `acquire_restore_lease` calls with different `attempt_id`; the second returns false; after the TTL expires it succeeds.
- `ckpt_commit_restore_requires_the_owning_attempt` — `commit_restore` with a foreign `attempt_id` returns false and leaves the state at `RESTORE_RESERVED`.
- `ckpt_abort_pause_is_idempotent_by_migration_id` — calling it twice returns true then false and clears both the pause entry and the lease.
- `ckpt_namespace_is_disjoint_from_criu_state` — write both, call `ControlPlane::clear_criu_state()`, assert the `ckpt:` keys survive and `AgentDirectory::clear()` removes them.

```bash
cmake --build build -j"$(nproc)"
./build/tests/Boost_Tests_run --run_test=CriuLab
./build/tests/Boost_Tests_run --run_test=CriuFaultTolerance
(cd build/tests && ./Boost_Tests_run)
redis-cli -h 127.0.0.1 -p 6379 keys 'fmi:ft:*:ckpt:*'
```

Expected: suite green; the final `keys` returns empty (every test clears its namespace).

- [ ] **Step 4: Commit**

```bash
git add include/ft/experimental/AgentDirectory.h src/ft/experimental/AgentDirectory.cpp \
  CMakeLists.txt tests/criu_lab.cpp
git commit -m "feat: add additive checkpoint directory with CAS states and leases"
```

---

### Task 9: Resident `daemon` verb — pub/sub with poll fallback, and experiment 7

**Files:**
- Create: `tools/criu_lab/exp7_request_latency.sh`
- Modify: `include/ft/experimental/AgentDirectory.h`
- Modify: `src/ft/experimental/AgentDirectory.cpp`
- Modify: `include/ft/experimental/LocalRankAgent.h`
- Modify: `src/ft/experimental/LocalRankAgent.cpp`
- Modify: `tools/rank_agent.cpp`
- Modify: `tests/criu_lab.cpp`
- Modify: `docs/superpowers/findings/2026-07-27-b-checkpoint-mechanics-findings.md`

**Interfaces:**
- Per contract 4, the agent is resident, subscribes to the pause channel, polls the pause set as a fallback, and is itself never checkpointed. Produces:

```cpp
    //! Blocking pub/sub reader on fmi:ft:<comm>:ckpt:events. Owns its own hiredis context and a
    //! self-pipe so stop() interrupts the blocking read from another thread.
    class AgentDirectory::Subscription {
    public:
        //! Wait up to @p timeout_ms for the next event; nullopt on timeout or on stop().
        [[nodiscard]] std::optional<PauseRequest> next(unsigned int timeout_ms);
        void stop();
    };
```

```cpp
    //! Resident host agent loop. Blocks on the pause subscription and, independently, re-reads
    //! the pause set every poll_ms so a dropped subscription (Redis restart, missed message)
    //! degrades latency instead of losing the request. Returns when @p stop is set.
    //! FMI_CKPT_POLL_MS overrides the fallback interval (default: config.criu.poll_ms).
    void LocalRankAgent::run_daemon(const std::atomic<bool>& stop) const;
```

- CLI addition in `tools/rank_agent.cpp` (usage string at `tools/rank_agent.cpp:35-41`): `fmi-rank-agent daemon <comm_name> <num_peers> <config>`, running until `SIGINT`/`SIGTERM`. The verb is documented in the same header comment block as the existing verbs.
- The daemon only *observes and dispatches* in this task; the freeze/dump/stage body is Task 10 and is stubbed behind a `on_pause` callback so this task is independently testable.

- [ ] **Step 1: Prove the verb is absent (RED)**

```bash
./build/tools/fmi-rank-agent daemon lab-comm 2 config/fmi_criu_test.json; echo "exit=$?"
```

Expected: `unknown command: daemon`, exit 1.

- [ ] **Step 2: Implement the subscription**

A dedicated `redisContext` issuing `SUBSCRIBE`, then `redisGetReply` under a `redisSetTimeout`; a self-pipe fd plus a `poll()` on the context fd makes `stop()` prompt. On any subscription error the loop reconnects with backoff and the poll fallback covers the gap. Reconnect attempts and dropped-subscription counts are exposed as counters for the findings.

- [ ] **Step 3: Implement `run_daemon`**

Single thread: `poll()` on the subscription fd with `FMI_CKPT_POLL_MS` as the timeout; on wakeup (event or timeout) re-read `pause_set()` and dispatch each rank not already dispatched. Dispatch is idempotent by `(rank, migration_id)`.

- [ ] **Step 4: Run experiment 7**

`tools/criu_lab/exp7_request_latency.sh` starts the daemon with a callback that timestamps the freeze, then issues 50 `request_pause` calls and reports request→frozen latency (median/p95) in three configurations: pub/sub enabled with the fallback at 1000 ms; pub/sub disabled (subscription forcibly killed) with the fallback at 1000 ms, 250 ms, and 25 ms.

```bash
CRIU=/usr/local/sbin/criu bash tools/criu_lab/exp7_request_latency.sh 2>&1 | tee /tmp/fmi-exp7.csv
column -s, -t /tmp/fmi-exp7.csv
```

Append experiment 7 to the findings with the table; the consequence section states the recommended default fallback interval and the latency penalty a subscription loss costs.

- [ ] **Step 5: Pin it with tests (GREEN)**

- `daemon_dispatches_on_published_event` — start `run_daemon` on a thread with a recording callback, `request_pause` one rank, assert dispatch within 500 ms, then `stop`.
- `daemon_dispatches_without_pubsub_via_poll` — write the pause entry directly into the hash **without** publishing, assert dispatch within `3 × FMI_CKPT_POLL_MS`.
- `daemon_dispatch_is_idempotent_per_migration_id` — publish the same `(rank, migration_id)` three times, assert exactly one dispatch.

```bash
cmake --build build -j"$(nproc)"
./build/tests/Boost_Tests_run --run_test=CriuLab
(cd build/tests && ./Boost_Tests_run)
./build/tools/fmi-rank-agent 2>&1 | grep -c daemon
```

- [ ] **Step 6: Commit**

```bash
git add tools/criu_lab/exp7_request_latency.sh include/ft/experimental/AgentDirectory.h \
  src/ft/experimental/AgentDirectory.cpp include/ft/experimental/LocalRankAgent.h \
  src/ft/experimental/LocalRankAgent.cpp tools/rank_agent.cpp tests/criu_lab.cpp \
  docs/superpowers/findings/2026-07-27-b-checkpoint-mechanics-findings.md
git commit -m "feat: add resident daemon verb with pause subscription and poll fallback"
```

---

### Task 10: Freeze → dump → stage → verify-digest → CAS `CHECKPOINTED`

**Files:**
- Create: `include/ft/experimental/ArtifactStore.h`
- Create: `src/ft/experimental/ArtifactStore.cpp`
- Create: `include/ft/experimental/CheckpointService.h`
- Create: `src/ft/experimental/CheckpointService.cpp`
- Modify: `include/ft/experimental/CriuExec.h`
- Modify: `src/ft/experimental/CriuExec.cpp`
- Modify: `CMakeLists.txt`
- Modify: `tests/criu_lab.cpp`

**Interfaces:**
- Per contract 4, the sequence is: verify identity → freeze → dump → stage to a durable artifact store → verify the digest → **only then** CAS `CHECKPOINTED`, and the **agent** writes the state. Produces:

```cpp
namespace FMI::FT {
    //! Durable staging for checkpoint images. "file://<dir>" writes <dir>/<mid>/<rank>.tar.gz and
    //! fsyncs the file and its directory; "redis" reuses ControlPlane::criu_image_put
    //! (include/ft/ControlPlane.h:123-125) and stays available as the small-image fallback for
    //! local runbooks. Selected by FMI_CKPT_ARTIFACT_URI, defaulting to
    //! file://<config.criu.images_dir>/artifacts.
    class ArtifactStore {
    public:
        static std::unique_ptr<ArtifactStore> from_uri(const std::string& uri,
                                                       std::shared_ptr<ControlPlane> control_plane);
        //! Stage @p blob and return its manifest. Must be durable before returning.
        virtual ImageManifest put(const std::string& migration_id, FMI::Utils::peer_num rank,
                                  const std::string& blob) = 0;
        //! Fetch and verify against @p manifest. Throws on a digest or length mismatch.
        virtual std::string get(const ImageManifest& manifest) = 0;
        virtual void remove(const ImageManifest& manifest) = 0;
        virtual ~ArtifactStore();
    };

    //! One local target's checkpoint, in the contract-4 order. Never writes rank state itself
    //! except through AgentDirectory, and never CASes CHECKPOINTED before the digest verifies.
    class CheckpointService {
    public:
        CheckpointService(AgentDirectory& directory, ArtifactStore& store,
                          FMI::Utils::FaultToleranceConfig config, std::string comm_name);

        struct CheckpointOutcome {
            bool checkpointed = false;
            ImageManifest manifest;
            std::chrono::microseconds freeze_latency{0};
            std::chrono::milliseconds frozen_duration{0};
            std::string failure;   //!< non-empty when the pause was aborted
        };

        //! Verify identity, freeze, dump, pack, stage, verify, CAS CHECKPOINTED. On ANY failure
        //! the target is thawed and abort_pause is CAS'd back to ACTIVE (contract 3's mandatory
        //! PAUSING -> ACTIVE failure edge), so a failed dump can never wedge the job.
        CheckpointOutcome checkpoint(const PauseRequest& request, const ProcessIdentity& expected,
                                     DumpDiscipline discipline);
    };
}
```

- `CriuExec` gains stdout capture, needed for the digest without adding a crypto dependency:

```cpp
    //! Run a command and return its stdout. Same failure semantics as run_process.
    [[nodiscard]] std::string run_process_capture(const std::vector<std::string>& args);
```

  The digest is the first field of `sha256sum <path>`. Packing reuses the existing `/`-relative `tar` discipline of `pack_rank_image` (`src/ft/experimental/LocalRankAgent.cpp:385-405`) including `FMI_CRIU_EXTRA_FILES` (`:25-42`), so a staged archive stays restorable on another host.
- Failure edge: contract 3 requires an external actor to be able to drive every state. Today a failed dump is three attempts then a rethrow (`src/ft/experimental/LocalRankAgent.cpp:420-438`) with no state written; `CheckpointService` keeps the retry and adds the `abort_pause` on exhaustion.

- [ ] **Step 1: Prove the gap (RED)**

```bash
grep -rn "mark_checkpointed" src/ tools/ | grep -v AgentDirectory
```

Expected: no hits — nothing calls the CAS yet.

- [ ] **Step 2: Implement the artifact store**

`file://` backend: write to `<dir>/<mid>/<rank>.tar.gz.tmp`, `fsync` the fd, `rename`, then `fsync` the directory fd — a rename alone is not durable. `get()` re-reads and re-digests before returning. `redis` backend delegates to `criu_image_put`/`criu_image_get` and refuses blobs over `FMI_CKPT_REDIS_MAX_BYTES` (default 64 MiB) with a message naming the `file://` alternative.

- [ ] **Step 3: Implement the checkpoint sequence**

Exact order, with no step reordered: `VerifiedProcess::open` (nonce from `AgentDirectory::nonce_for`) → `freeze()` → `dump_rank(..., discipline)` → `pack_rank_image` → `ArtifactStore::put` → `ArtifactStore::get` + digest compare → `AgentDirectory::mark_checkpointed`. Record `freeze_latency` and `frozen_duration` into the outcome. On any throw: `thaw()` (when the process still exists), `abort_pause`, and return `checkpointed=false` with the failure text.

- [ ] **Step 4: Add the sequencing tests (GREEN)**

- `checkpoint_marks_state_only_after_digest_verifies` — mock criu; between staging and the CAS, corrupt the staged file; assert `mark_checkpointed` was never reached, the state returned to `ACTIVE`, and the failure text names the digest.
- `checkpoint_refuses_a_recycled_pid` — pass a `ProcessIdentity` with a perturbed `start_ticks`; assert no `SIGSTOP` was delivered (the toy keeps ticking) and the pause was aborted.
- `checkpoint_aborts_pause_after_dump_failure` — mock criu with `FMI_MOCK_FAIL_MODE=dump`; assert three attempts, then state `ACTIVE`, and the toy thawed and ticking.
- `checkpoint_happy_path_stages_and_marks` — real criu, self-skipping; assert the manifest's `sha256` matches `sha256sum` of the staged file, the state is `CHECKPOINTED`, and the outcome's `frozen_duration` is recorded.

```bash
cmake --build build -j"$(nproc)"
./build/tests/Boost_Tests_run --run_test=CriuLab
./build/tests/Boost_Tests_run --run_test=CriuFaultTolerance
(cd build/tests && ./Boost_Tests_run)
```

- [ ] **Step 5: Commit**

```bash
git add include/ft/experimental/ArtifactStore.h src/ft/experimental/ArtifactStore.cpp \
  include/ft/experimental/CheckpointService.h src/ft/experimental/CheckpointService.cpp \
  include/ft/experimental/CriuExec.h src/ft/experimental/CriuExec.cpp CMakeLists.txt \
  tests/criu_lab.cpp
git commit -m "feat: stage checkpoints durably and CAS state after digest verification"
```

---

### Task 11: Restore under lease — pidfile, commit CAS, `SIGCONT` on win, `SIGKILL` on loss

**Files:**
- Modify: `include/ft/experimental/CheckpointService.h`
- Modify: `src/ft/experimental/CheckpointService.cpp`
- Modify: `src/ft/experimental/LocalRankAgent.cpp`
- Modify: `tools/rank_agent.cpp`
- Modify: `tests/criu_lab.cpp`

**Interfaces:**
- Per contract 3's restore commit discipline, arbitration happens **before** either duplicate runs. Produces:

```cpp
    struct RestoreOutcome {
        bool committed = false;
        int restored_pid = 0;
        std::string attempt_id;
        std::string failure;
    };

    //! Acquire the lease, fetch + verify the image, then
    //!   criu restore --tcp-close --leave-stopped --restore-detached --pidfile <attempt>
    //! read the pidfile, and only then attempt commit_restore. Winner gets SIGCONT; a loser or a
    //! straggler whose lease expired is SIGKILLed WHILE STILL STOPPED, so a twin can never pair,
    //! ack, or touch the control plane. The image is retained until ACTIVATING -> ACTIVE.
    RestoreOutcome CheckpointService::restore_under_lease(FMI::Utils::peer_num rank,
                                                          const std::string& migration_id,
                                                          const std::string& attempt_id);
```

- CLI: `fmi-rank-agent restore-leased <comm_name> <num_peers> <config> <rank> [attempt_id]`, added beside `restore-remote` in `tools/rank_agent.cpp:94-109` and documented in the header block. `restore-remote` is left untouched so the existing k8s runbook keeps working.
- `--leave-stopped` at restore is what makes the kill-on-loss safe; combined with `--restore-detached`, the restored task is reparented, so the pid comes from `--pidfile` and never from `waitpid`.

- [ ] **Step 1: Prove the gap (RED)**

```bash
./build/tools/fmi-rank-agent restore-leased lab-comm 2 config/fmi_criu_test.json 0; echo "exit=$?"
```

Expected: `unknown command: restore-leased`, exit 1.

- [ ] **Step 2: Implement the restore leg**

Argv exactly: `criu restore -D <dir> -o restore.log --shell-job --manage-cgroups=ignore --tcp-close --leave-stopped --restore-detached --pidfile <attempt>.pid` (plus `FMI_CRIU_EXTRA_ARGS` via `run_criu`). After the pidfile is read, `commit_restore`; on success `SIGCONT` via a `VerifiedProcess` opened on the restored pid; on failure `SIGKILL` **before** any `SIGCONT`, then `abort_restore`. Delete the staged artifact only after observing `ACTIVATING → ACTIVE`.

- [ ] **Step 3: Add the arbitration tests (GREEN)**

- `restore_loser_is_killed_while_still_stopped` — mock criu writing a pidfile for a real stopped toy; run two `restore_under_lease` calls concurrently with different `attempt_id`; assert exactly one commits, the loser's process was killed, and the killed process's `/proc/<pid>/stat` state was `T` (never `R`/`S`) at the moment of the kill — the observable form of "a twin can never pair".
- `restore_without_a_lease_never_commits` — call `commit_restore` with an `attempt_id` that never acquired the lease; assert false and no `SIGCONT`.
- `restore_flags_include_tcp_close_leave_stopped_and_pidfile` — mock-criu argv assertion over `restore.args`.
- `expired_lease_can_be_reacquired_and_committed` — let the TTL lapse, assert a second attempt acquires and commits, per contract 3's `RESTORE_RESERVED`-with-expired-lease rule.
- `restore_under_lease_round_trip` — real criu, self-skipping: full checkpoint (Task 10) then restore-under-lease; assert the restored toy resumes from the dumped `tick`, not from 0, and only after `SIGCONT`.

```bash
cmake --build build -j"$(nproc)"
./build/tests/Boost_Tests_run --run_test=CriuLab
(cd build/tests && ./Boost_Tests_run)
```

- [ ] **Step 4: Commit**

```bash
git add include/ft/experimental/CheckpointService.h src/ft/experimental/CheckpointService.cpp \
  src/ft/experimental/LocalRankAgent.cpp tools/rank_agent.cpp tests/criu_lab.cpp
git commit -m "feat: restore under lease with commit CAS and stopped-loser kill"
```

---

### Task 12: Foreign-fd preflight, batched evacuation, deployment decision, and findings consolidation

**Files:**
- Create: `docs/superpowers/findings/2026-07-27-b-deployment-decision.md`
- Modify: `include/ft/experimental/CheckpointService.h`
- Modify: `src/ft/experimental/CheckpointService.cpp`
- Modify: `include/ft/experimental/LocalRankAgent.h`
- Modify: `src/ft/experimental/LocalRankAgent.cpp`
- Modify: `tools/rank_agent.cpp`
- Modify: `tests/criu_lab.cpp`
- Modify: `docs/superpowers/findings/2026-07-27-b-checkpoint-mechanics-findings.md`

**Interfaces:**
- Per contract 4, batched evacuation freezes **all** local targets first and only then runs the slow dumps, with **no wait-for-quiesce gate**. Today `evacuate_local` (`src/ft/experimental/LocalRankAgent.cpp:223-271`) does the opposite: `wait_for_ready_ranks` (`:352-377`) blocks until every target has published `QUIESCED` for the target epoch before any dump starts. Produces a second path that leaves the existing one intact:

```cpp
    //! Contract-4 batched evacuation: verify identity and FREEZE every local target first (a
    //! cheap SIGSTOP each), then run the slow dumps. No wait-for-quiesce gate: mutual in-flight
    //! traffic is unacked by construction under Plan C's link layer and is replayed, so nothing
    //! needs to drain first. Stage-B scope: validated against toy targets only.
    EvacuationResult evacuate_local_batched(const std::string& migration_id) const;
```

- `evacuate_local_batched` is the **first and only** `DumpDiscipline::LeaveStopped` call site (Task 3). Contract 4 makes frozen-original abort recovery cross-host only, and evacuation is by construction cross-host, so the pid the restore reclaims is free on the destination.
- CLI verb `evacuate-batched`, documented beside the existing verbs in `tools/rank_agent.cpp:35-41`. `evacuate_local`'s freeze/quiesce sequence and its runbook (`runbooks/k8s-criu-node-evacuation/`) are untouched — Task 3 only made its dump discipline (`KillAndReclaim`) explicit.
- **Deployment decision.** `docs/superpowers/findings/2026-07-27-b-deployment-decision.md` evaluates DaemonSet versus per-pod sidecar against the two k8s runbooks that actually exist in this repo, and must cite what they already do: `runbooks/k8s-criu-node-evacuation/k8s/machine-deployment.yaml:1-4` documents that the orchestrator `kubectl exec`s `fmi-rank-agent migrate-local` into the **same container** as the ranks precisely so "the agent shares the ranks' PID + mount namespace — the prerequisite for criu dump/restore", and `runbooks/k8s-criu-node-evacuation/k8s/knative-restore-service.yaml:1-14` plus `runbooks/k8s-criu-node-evacuation/restore_server.py:1-19` put the restore leg **inside** the Knative pod behind a held request. The document must state the consequence for `runbooks/localstack-python311-redis/knative-migration/` as well, and must record the one property a sidecar must additionally satisfy that today's exec-into-the-rank-container arrangement does not: residency (the agent must already be running to receive a pause request, not be exec'd on demand).

- [ ] **Step 1: Prove the gap (RED)**

```bash
./build/tools/fmi-rank-agent evacuate-batched lab-comm 2 config/fmi_criu_test.json; echo "exit=$?"
test -f docs/superpowers/findings/2026-07-27-b-deployment-decision.md; echo "exit=$?"
grep -rn "proc/.*\/fd\|foreign_fd\|FOREIGN_FD" src/ft/experimental/ include/ft/experimental/ || echo "no preflight: expected"
```

Expected: `unknown command: evacuate-batched` (exit 1), the decision document is absent (exit 1), and no fd preflight exists anywhere in the agent sources.

- [ ] **Step 2: Implement the foreign-fd preflight**

Contract 4 closes with: "`--tcp-close` transparency covers FMI-managed connections only. An
application holding its own sockets needs its own reconnect story; the agent preflights
`/proc/<pid>/fd` and warns or rejects per policy." No other plan owns this — Plan A is data-plane
keys and Plan C never inspects a target's fds — so it lands here, in the agent, where the evidence
from experiment 2 already is.

Add to `CheckpointService` a preflight run **before** the freeze and after identity verification:
walk `/proc/<pid>/fd`, `readlink` each entry, and classify every `socket:[...]` inode against
`/proc/<pid>/net/{tcp,tcp6,unix}`. FMI-managed sockets are the ones whose peer address matches a
directory-registered rank or the configured Redis endpoint; everything else is foreign. Policy from
`FMI_CKPT_FOREIGN_FD_POLICY`: `warn` (default — log the count and the peer addresses, proceed) or
`reject` (abort the pause with a failure text naming each foreign fd, so contract 3's
`PAUSING → ACTIVE` edge fires and the job does not wedge). Pin it with
`foreign_socket_is_reported_and_can_reject`: a toy started with `--socket connect` against a
non-rank listener is warned about under `warn` and aborts the pause under `reject`, while the same
toy with `--socket none` preflights clean under both.

- [ ] **Step 3: Implement batched evacuation**

Freeze phase: for every discovered local rank (`discover_local_ranks`, `src/ft/experimental/LocalRankAgent.cpp:187-207`), open a `VerifiedProcess` and `freeze()`. Any identity mismatch aborts the whole batch **before** any freeze takes effect by thawing whatever was already frozen — a partially frozen host must never be left behind. Dump phase: reuse the existing per-rank worker-thread pattern (`:239-265`), which already keeps the control plane on the calling thread only; each worker runs `CheckpointService::checkpoint` with the target already frozen (add a `skip_freeze` overload rather than double-freezing).

- [ ] **Step 4: Write the deployment decision document**

Structure: the property required by contract 4 (never checkpointed, free to hold sockets, resident) → what a DaemonSet can and cannot reach (PID namespace, mount namespace, criu reopening the executable by path, and the Knative restore leg) → what the two in-repo runbooks already do → the recommendation and exactly which manifests would change. Cite files by path and line as above.

- [ ] **Step 5: Add the batched-evacuation tests (GREEN)**

- `batched_evacuation_freezes_every_target_before_any_dump` — three toys; a mock criu that records a timestamp on entry; assert the last freeze timestamp precedes the first dump timestamp.
- `batched_evacuation_thaws_all_on_identity_mismatch` — one of the three has a stale identity; assert none of the three remains stopped (all `tick` values advance) and no dump ran.
- `batched_evacuation_has_no_quiesce_gate` — assert the batched path completes with the toys never publishing any quiesce marker, distinguishing it from `evacuate_local`.

```bash
cmake --build build -j"$(nproc)"
./build/tests/Boost_Tests_run --run_test=CriuLab
(cd build/tests && ./Boost_Tests_run)
```

- [ ] **Step 6: Consolidate the findings and run full verification**

Flip every `PENDING` in the findings table of contents to a verdict, and add a closing "Inputs to Plan C" section that answers, in one line each: spec open item 3 (inline-send versus full-engine — from experiments 3 and 6), spec open item 4 (`--pre-dump`/`--track-mem` — from experiment 6), the `W`-sizing formula, and whether stage C2's commitment point is safe to take. Then:

```bash
git submodule update --init --recursive
cmake -S . -B build -DFMI_BUILD_TESTS=ON -DFMI_ENABLE_CRIU=ON
cmake --build build -j"$(nproc)"
./extern/TCPunch/server/build-fresh/tcpunchd 10000 &
redis-cli -h 127.0.0.1 -p 6379 ping
./build/tests/Boost_Tests_run --run_test=CriuLab
./build/tests/Boost_Tests_run --run_test=CriuFaultTolerance
(cd build/tests && ./Boost_Tests_run)
./build/tools/fmi-rank-agent 2>&1 | head -3
redis-cli -h 127.0.0.1 -p 6379 keys 'fmi:ft:*:ckpt:*'
kill %1
```

Expected: every suite green, the usage line lists `daemon`, `restore-leased`, and `evacuate-batched`, and the `ckpt:` namespace is empty after the run (no test leaks Redis state).

- [ ] **Step 7: Audit repository scope**

```bash
git diff --check
git status --short
git diff --name-only HEAD~12..HEAD | sort -u
```

Confirm: no file outside Plan B's declared set changed **in Plan B's own twelve commits**; `include/ft/ControlPlane.h`, `src/ft/ControlPlane.cpp`, `src/comm/**`, `include/comm/**`, and `include/utils/Configuration.h` are untouched by them; the pre-existing working-tree modifications to `runbooks/**/orchestrator.py`, `tests/channels.cpp`, and `tests/communicator.cpp` are still unstaged; and `config/fmi_test.json` carries no committed host override.

> **Do not read this audit as a claim about the branch.** Plans A and C land in parallel and legitimately touch files Plan B may not. In particular Plan A task 6 modifies `include/ft/ControlPlane.h` and `src/ft/ControlPlane.cpp` (an additive defaulted parameter on `clear_job_state`) and Plan C task 1 modifies `include/utils/Configuration.h` and `src/utils/Configuration.cpp` (the additive `fault_tolerance.link` block). Neither collides with anything Plan B writes — `AgentDirectory` owns a disjoint `ckpt:` namespace and Plan B adds no configuration schema. Scope the audit to `HEAD~12..HEAD` as written, not to `git status` across the whole branch.

- [ ] **Step 8: Commit**

```bash
git add docs/superpowers/findings/2026-07-27-b-deployment-decision.md \
  docs/superpowers/findings/2026-07-27-b-checkpoint-mechanics-findings.md \
  include/ft/experimental/CheckpointService.h src/ft/experimental/CheckpointService.cpp \
  include/ft/experimental/LocalRankAgent.h src/ft/experimental/LocalRankAgent.cpp \
  tools/rank_agent.cpp tests/criu_lab.cpp
git commit -m "feat: add foreign-fd preflight and batched evacuation"
```
