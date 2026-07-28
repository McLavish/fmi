# Checkpoint Mechanics (Stage B) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Retire the highest-variance CRIU unknowns — pid reclaim, `--tcp-close`, threads frozen under a held lock, the post-restore `PR_SET_PTRACER` re-arm obligation, post-restore signal/EINTR hygiene, agent death mid-checkpoint, and freeze cost as a function of RSS — by building the agent freeze/dump/stage/restore harness of the design spec's normative contract 4 and validating it end to end against a toy process that contains **no FMI**, so that Plan C's commitment point (spec staging map, stage **C2**) is taken with measured data instead of assumptions. Two of the experiments now confirm-or-refute a *modelled prediction* rather than exploring blind (`docs/tla/README.md`, `MembershipSameHost` and `MembershipTableLiteral`), and Plan B owes the numbers — measured `criu restore` duration, hence the lease TTL and attempt budget `k` — that `Membership.tla` has no clock to supply.

**Architecture:** A ~50-line standalone toy target is the dump/restore subject for every experiment. Around it, three new experimental components are added under `src/ft/experimental/`: `ProcessIdentity` (pidfd + start time + registered nonce, verified before any freeze), `AgentDirectory` (an additive `fmi:ft:<run_id>:<comm>:ckpt:` Redis key set with its own hiredis connection, a pause pub/sub channel, an agent-liveness key, per-rank incarnations, CAS state transitions and restore leases), and `CheckpointService` (freeze → dump → stage → verify-digest → CAS `CHECKPOINTED`, and restore-under-lease → commit CAS → `SIGCONT`/`SIGKILL`). `LocalRankAgent` gains a resident `daemon` verb, an orchestrator-driven `abort-pause` verb (the edge `MembershipTableLiteral` shows is mandatory), and a batched freeze-all-then-dump evacuation path; `tools/rank_agent.cpp` exposes them. Every experiment writes a recorded result into a companion findings file, and each experiment's conclusion is pinned by a Boost test case in a new `CriuLab` suite so it cannot silently regress.

**Tech Stack:** C++17, criu 4.2 (`/usr/local/sbin/criu`, `cap_sys_ptrace=eip`), Linux `pidfd_open`/`pidfd_send_signal`/`/proc/<pid>/stat`, hiredis (pub/sub + `EVAL`), Redis 7 on `127.0.0.1:6379`, Boost.Test, CMake, POSIX threads/processes/pipes/sockets.

## Global Constraints

- The design spec `docs/superpowers/specs/2026-07-27-sequenced-incarnation-links-design.md` is normative. This plan implements **Axis B / stage B** only and says HOW and IN WHAT ORDER; it never restates WHAT or WHY. Cite it as "contract 4", "contract 3 restore commit discipline", etc.
- **`docs/tla/README.md` is the machine-checked companion** (three TLA+ modules, 36 TLC 2.19 configurations, all exhausted) and is authoritative over any prose summary. Two of its results are direct obligations on this plan, because Plan B owns the agent and the process layer:
  - `MembershipSameHost` violates `ActiveHasProcess` at depth 9 (188 states generated, 84 distinct) — on the same-host shape the directory advertises a rank `ACTIVE` with **no process in existence** after an abort. Task 3 / experiment 1 must confirm that prediction empirically.
  - `MembershipTableLiteral` violates `NoDeadEnd` at depth 15 (354 generated, 179 distinct) with `dumpfail = FALSE` and a **single agent** — an agent dying after freezing its target and before completing or failing the dump leaves `PAUSING` with no actor. Task 10 / experiment 8 must demonstrate the orchestrator-driven `abort_pause` edge that closes it, including who `SIGCONT`s the frozen original.
  - Contract 3's lease/attempt-budget rule is normative but its **numbers are not**, and `Membership.tla` has no clock. The measured `criu restore` durations of Tasks 7 and 11 are the only evidence base for the lease TTL and the attempt budget `k` (spec open item 5).
- **No dependency on ControlPlane v2.** Do not modify `include/ft/ControlPlane.h` or `src/ft/ControlPlane.cpp`. Plan B reuses the existing CRIU surface (`include/ft/ControlPlane.h:112-125`) and adds every new key under a disjoint `fmi:ft:<run_id>:<comm>:ckpt:` namespace owned by `AgentDirectory`. Plan C stage C3 replaces `ControlPlane` and Plan A task 6 makes one additive edit to it (a defaulted `data_comm_name` parameter on `clear_job_state`); a Plan B edit there would collide with both. Plan B consumes only the v1 read/write surface it already uses, so nothing here needs either of those changes to have landed.
- **`run_id` fences every name Plan B mints, and Plan B never writes the index.** Per contract 1's universal name fence, every `AgentDirectory` key, every CRIU image directory, every manifest path and every staged blob key created by this plan carries `run_id` ahead of `comm_name`. Resolution order, implemented once in `AgentDirectory` on its own hiredis connection (reading a key is not a `ControlPlane` change): the `FMI_CKPT_RUN_ID` environment variable, else the `fmi:ft:name:<comm_name> → run_id` index if Plan A/C has already written it, else the literal `local` with a loud `BOOST_TEST_MESSAGE`/log line saying the fence is degraded for a stage-B lab run. **Plan B must not write `fmi:ft:name:<comm_name>`** — minting it at job creation belongs to whoever creates jobs, which is Plan C stage C3.
  - Consequence for the Redis image fallback: `ControlPlane::criu_image_put`/`criu_image_get` (`include/ft/ControlPlane.h:123,125`) key blobs by `(epoch, rank)` under `criu_prefix()` with **no** `run_id`, and Plan B may not change that signature. The stage-B `redis` artifact backend therefore stores blobs under its own `…:ckpt:image:<migration_id>:<rank>` key instead of calling `criu_image_put`, which keeps contract 4's small-image fallback while satisfying the fence. Record the divergence in the findings as a C3 convergence item; leave `criu_image_put` in place and untouched for `restore-remote`.
- **Disjoint file set.** Plan B may touch only: `tools/`, `src/ft/experimental/`, `include/ft/experimental/`, `tests/criu_lab.cpp`, `tests/CMakeLists.txt`, the `FMI_ENABLE_CRIU` source list in `CMakeLists.txt` (`CMakeLists.txt:73-78`), and `docs/superpowers/findings/2026-07-27-b-checkpoint-mechanics-findings.md`. It must not touch `src/comm/`, `include/comm/`, `include/Communicator.h`, `src/Communicator.cpp`, `src/ft/TransparentMigrationRuntime.cpp`, or the `tests/migration_*counterexample*` files — those belong to Plans A and C.
- **No configuration-schema changes.** `include/utils/Configuration.h` (the `Criu` block is `include/utils/Configuration.h:34-39`) stays untouched — Plan C task 1 adds the sibling `fault_tolerance.link` block there and owns that file; new knobs are environment variables (`FMI_CKPT_*`), defaulting from `config.criu.poll_ms` / `config.criu.quiesce_timeout_ms` / `config.criu.images_dir`.
- **The dump/restore subject is the toy target and it must not link FMI.** Its CMake target links `Threads::Threads` (and `${HIREDIS_LIBRARIES}` only for the hiredis experiment). Any experiment that needs FMI in the image is out of scope for stage B.
- Every task must end with a tree that configures, builds, and passes the whole suite. Every task names its own new test case.
- **Experiments are deliverables.** Each experiment task appends a section to
  `docs/superpowers/findings/2026-07-27-b-checkpoint-mechanics-findings.md` using the fixed template from Task 1 (Question / Method / Raw result / Verdict / Consequence for the spec / Modelled prediction where one exists) **in the same commit** as its code, and pins its verdict with a Boost assertion. Experiments 1 and 8 have modelled predictions and must state the expected outcome **before** the raw result, so a null result reads as a red flag rather than as a finding.
- Real-criu test cases must self-skip loudly (`BOOST_TEST_MESSAGE` + a recorded reason) when `criu` is missing or lacks capabilities, following the skip pattern of `tests/criu_fault_tolerance.cpp:44-54`. Mock-criu cases (`tests/criu_fault_tolerance.cpp:194-241`, whose generated script records every invocation's argv into `<dir>/<mode>.args` at `:241` and supports `FMI_MOCK_FAIL_MODE` / `FMI_MOCK_GATE_MODE` injection at `:222-237`) carry the flag-composition assertions so they run everywhere.
- Machine facts for every verification block on this host: Redis native at `127.0.0.1:6379`; `tcpunchd` is `./extern/TCPunch/server/build-fresh/tcpunchd 10000` (the `build/` copy is stale); criu 4.2 at `/usr/local/sbin/criu` with `cap_net_admin,cap_sys_ptrace,cap_sys_admin,cap_sys_resource,cap_checkpoint_restore=eip`.
- Build flags: `-DFMI_BUILD_TESTS=ON -DFMI_ENABLE_CRIU=ON`. `FMI_ENABLE_CRIU=ON` *requires* `FMI_ENABLE_REDIS=ON` and fails configuration otherwise (`CMakeLists.txt:13-15`, a `FATAL_ERROR` — it does not silently turn Redis on), and without it neither the rank agent (`tools/CMakeLists.txt:1-5`) nor the counterexample binaries are built.
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

Create `docs/superpowers/findings/2026-07-27-b-checkpoint-mechanics-findings.md` with a title, a "Machine" block (kernel `uname -srm`, `criu --version`, `getcap /usr/local/sbin/criu`, Redis version, and the `run_id` used for the lab run), a table of contents listing experiments 1-8 plus the deployment decision, all marked `PENDING`, and this required per-experiment template:

```text
### Experiment <n> — <title>
**Question:** ...
**Method:** exact commands, pasteable
**Raw result:** verbatim output / measured table
**Verdict:** one sentence
**Consequence for the spec:** which contract/open item it settles, and what Plan C must assume
**Modelled prediction (where one exists):** the `docs/tla` configuration this experiment confirms or refutes, and what a null result would mean
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
- Add `src/ft/experimental/ProcessIdentity.cpp` to the `FMI_ENABLE_CRIU` source list at `CMakeLists.txt:73-78`.

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
- **This experiment now confirms a modelled prediction rather than exploring blind.** `MembershipSameHost.cfg` violates `ActiveHasProcess` at depth 9 (188 states generated, 84 distinct — `docs/tla/README.md`, result 14): on the same-host shape the directory advertises a rank as `ACTIVE` with **no process in existence** after an abort, because the process the abort would have resumed was killed and reaped by the dump. Cross-host-only frozen-original abort recovery is therefore a *safety* requirement, not a convenience.

  > **Expected result, stated up front so a null result is a red flag.** Case 2 must fail with `Can't fork for <pid>: File exists`; cases 1 and 3 must succeed. That failure is the process-layer fact the model abstracts into "the original is gone": a same-host dump either reaps the original (so `--leave-stopped` is unavailable and there is nothing to `SIGCONT` on abort) or keeps it (so the in-place restore cannot have the pid). **If case 2 succeeds** — i.e. criu 4.2 on this kernel restores over a still-frozen original — then the model's abstraction does not hold here and the experiment has refuted, not confirmed, the scoping. Record that outcome loudly, do **not** proceed to the "(a)" disposition below on it, and raise it as a spec correction rather than absorbing it silently.
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

The "Modelled prediction" field must name `MembershipSameHost.cfg` (`ActiveHasProcess` violated, depth 9) and state whether the run confirmed it, together with the one operational consequence: under disposition (a) the same-host verbs have **no** `PAUSING → ACTIVE` recovery, so contract 3's mandatory abort edge is only realisable on the cross-host path — which is exactly why Task 10's experiment 8 is run in the cross-host shape — the agent's own `SIGSTOP` freeze with the dump still pending, resumed by an actor other than the agent that froze it — and never against `migrate-local`, where criu has already reaped the process the abort would resume.

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

Append `--tcp-close` to the criu argv in `dump_rank` and in `restore_rank` (`src/ft/experimental/LocalRankAgent.cpp:441-453`; the argv itself is `:445-452`, and `run_criu` appends `FMI_CRIU_EXTRA_ARGS` at `src/ft/experimental/CriuExec.cpp:54-68`). Replace the stale "No TCP flags" rationale with the recorded reason and a pointer to experiment 2. Operator flags still arrive through `FMI_CRIU_EXTRA_ARGS` in `run_criu` (`src/ft/experimental/CriuExec.cpp:54-68`), so no call site needs to duplicate them.

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
- Contract 4's process-level restore hygiene lists both. **Experiment 4 measures a forward obligation, not a present bug.** Today's `PR_SET_PTRACER` call site sits *inside the quiesce path*, at `src/ft/TransparentMigrationRuntime.cpp:155` in `checkpoint_and_wait_for_restore()`, so the current code re-arms it on **every** migration and is correct as written — a rank that migrates twice under the v1 protocol arms the prctl twice. v2 deletes that quiesce point, and nothing on the new path re-arms it after a restore, so **a rank would be migratable once and never again on the rootless path**. The question this experiment answers is therefore "does the setting survive `criu restore`, i.e. does deleting the quiesce point actually create an obligation, and where must the re-arm be placed" — not "is today's code broken". `src/ft/TransparentMigrationRuntime.cpp` is **outside** Plan B's file set, so this task measures and records; placing the re-arm on the post-restore path belongs to Plan C. The gap is masked on this host anyway: `/usr/local/sbin/criu` carries `cap_sys_ptrace=eip`, so the yama check is bypassed and running the demos would never catch it.
- Toy additions: `--set-ptracer` (calls `prctl(PR_SET_PTRACER, PR_SET_PTRACER_ANY, ...)` once at start) and `ptracer=<n>` in the state line (from `prctl(PR_GET_DUMPABLE)` plus a direct read-back attempt of the ptracer setting), plus `sigpipe_count=<n>` (a process-local `SIGPIPE` handler installed only under `--count-sigpipe`, never `SIG_IGN`).

- [ ] **Step 1: Reproduce experiment 4 (RED/measurement)**

`tools/criu_lab/exp4_ptracer.sh`: run the toy with `--set-ptracer`, dump+restore it, then attempt a **second** dump/restore of the restored process using a criu binary **stripped of `cap_sys_ptrace`** (copy `/usr/local/sbin/criu` to a temp path, `setcap -r` it or copy without caps) under `sysctl kernel.yama.ptrace_scope=1`, so the capability no longer masks the setting. Record whether the second dump succeeds and, if not, the exact yama error.

```bash
cat /proc/sys/kernel/yama/ptrace_scope
CRIU=/usr/local/sbin/criu bash tools/criu_lab/exp4_ptracer.sh 2>&1 | tee /tmp/fmi-exp4.log
```

Expected: the second dump fails under an uncapped criu if `PR_SET_PTRACER` does not survive — which is what turns "v2 deletes the quiesce-path re-arm" into a real forward obligation rather than a theoretical one. Record either way; if `ptrace_scope` is 0 on this host, record that the whole check is inert here and the test must run with `ptrace_scope=1`. A *surviving* setting is also a publishable result: it retires the obligation and Plan C then needs no re-arm at all.

- [ ] **Step 2: Reproduce experiment 5 (RED/measurement)**

`tools/criu_lab/exp5_signals.sh`: two shapes.
1. `--socket connect --count-sigpipe` against a listener that is killed **while the toy is frozen**; on restore the first `send` hits a dead socket. Record whether the process dies of `SIGPIPE`, whether `sigpipe_count` increments, and the `sock_errno`.
2. `--hiredis 127.0.0.1:6379`: dump+restore, then record the first post-restore `PING` outcome and errno, and whether hiredis reports the context as errored and reconnects.

```bash
redis-cli -h 127.0.0.1 -p 6379 ping
CRIU=/usr/local/sbin/criu bash tools/criu_lab/exp5_signals.sh 2>&1 | tee /tmp/fmi-exp5.log
```

- [ ] **Step 3: Record both findings**

Append experiments 4 and 5. Experiment 4's consequence must state whether deleting the quiesce-path call at `src/ft/TransparentMigrationRuntime.cpp:155` creates a re-arm obligation at all and, if it does, where the re-arm lands on the v2 path (the restored process's first directory poll, or the agent through `/proc`) — phrased as an instruction to Plan C, never as a defect report against today's code. Experiment 5's consequence must confirm or refute the spec's prescription of `pthread_sigmask(SIG_BLOCK, {SIGPIPE})` on the engine thread rather than a process-global `SIG_IGN`, and must state, with the measured errno, whether the first post-restore hiredis call needs a retry-on-EINTR wrapper. Note that `src/comm/Direct.cpp:36-43` and `:62-69` treat every non-`EAGAIN` errno — `EINTR` included — as fatal, so an EINTR observed here is a Plan C work item and not a Plan B fix.

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
- Per the global constraint, this is a **separate** Redis client with its own hiredis context, not a `ControlPlane` change. `ControlPlane`'s single connection is mutex-guarded (the member at `src/ft/ControlPlane.cpp:76`, taken at `:112` and `:124`) and a blocking `SUBSCRIBE` would monopolise it, so a second connection is required regardless.
- Keys live under `fmi:ft:<run_id>:<comm>:ckpt:`, per the `run_id` fence in Global Constraints — `run_id` is resolved once in the constructor (`FMI_CKPT_RUN_ID` → the `fmi:ft:name:<comm_name>` index → the literal `local`, logged when degraded) and is a member, so no call site can forget it. The namespace is disjoint from `prefix()` (`src/ft/ControlPlane.cpp:199-201`, which is `fmi:ft:<comm>:` with no `run_id`) and from `criu_prefix()` (`src/ft/ControlPlane.cpp:232-256`), so `clear_criu_state`'s `criu:*` sweep (`src/ft/ControlPlane.cpp:450-464`) does not touch them and `AgentDirectory::clear()` owns them. Note that `clear_job_state` (`src/ft/ControlPlane.cpp:431-447`) sweeps only `<prefix>epoch:*`, so it does not reach `ckpt:` either — **including after Plan A task 6 extends it with an optional data-namespace sweep**, which is scoped to the caller's `data_comm_name` and never to `fmi:ft:`:

```text
fmi:ft:<run_id>:<comm>:ckpt:state              HASH  rank -> ACTIVE|PAUSING|CHECKPOINTED|RESTORE_RESERVED|ACTIVATING
fmi:ft:<run_id>:<comm>:ckpt:inc                HASH  rank -> incarnation (monotonic, never reset)
fmi:ft:<run_id>:<comm>:ckpt:pause              HASH  rank -> migration_id
fmi:ft:<run_id>:<comm>:ckpt:nonce              HASH  rank -> identity nonce registered by the target
fmi:ft:<run_id>:<comm>:ckpt:lease:<rank>       STR   attempt_id, SET NX EX <lease_ttl_s>
fmi:ft:<run_id>:<comm>:ckpt:manifest:<mid>:<r> HASH  uri, sha256, bytes, created_ms, discipline
fmi:ft:<run_id>:<comm>:ckpt:image:<mid>:<r>    STR   packed image blob (the `redis` artifact backend only)
fmi:ft:<run_id>:<comm>:ckpt:agent:<agent_id>   STR   agent liveness, SET EX <2 x heartbeat>; Task 9 renews it
fmi:ft:<run_id>:<comm>:ckpt:events             PUB/SUB channel; message body "<rank> <migration_id> <verb>"
```

- **Incarnations, not just states.** Contract 3 bumps the incarnation on `commit_restore` **and on both `abort_pause` rows**; without the bump on abort, teardown is asymmetric — the survivor saw the target leave `ACTIVE` and closed its link, while the resumed target returns at incarnation `i` believing its `ESTABLISHED` socket is healthy and never re-registers. Plan B has no links, so it cannot observe that hang, but it owns the directory writes that make it possible, so the `inc` hash and the bump are implemented and pinned here rather than deferred to C3.

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
    //! connection, its own run_id-fenced `ckpt:` key namespace, no ControlPlane API surface
    //! touched. Every mutation is a CAS on (rank, expected state, expected incarnation) executed
    //! as one Lua script, so two agents can never both believe they own a transition.
    class AgentDirectory {
    public:
        AgentDirectory(const FMI::Utils::FaultToleranceConfig& config, std::string comm_name);
        ~AgentDirectory();

        //! The resolved fence for every key this object writes. Never empty; "local" means the
        //! index was absent and the run is a lab run (logged loudly at construction).
        [[nodiscard]] const std::string& run_id() const;

        void register_target(FMI::Utils::peer_num rank, const std::string& nonce) const;
        [[nodiscard]] std::string nonce_for(FMI::Utils::peer_num rank) const;
        [[nodiscard]] std::uint64_t incarnation_of(FMI::Utils::peer_num rank) const;

        //! Orchestrator side: mark ranks PAUSING and publish one event per rank.
        void request_pause(const std::vector<FMI::Utils::peer_num>& ranks,
                           const std::string& migration_id) const;
        [[nodiscard]] std::vector<PauseRequest> pause_set() const;

        //! Agent side, CAS-guarded. Each returns false when the expected state did not hold.
        bool mark_checkpointed(FMI::Utils::peer_num rank, const std::string& migration_id,
                               const ImageManifest& manifest) const;
        //! Contract 3's mandatory failure edge, accepted from PAUSING *and* CHECKPOINTED, and
        //! drivable by EITHER actor: the agent on dump failure, or the orchestrator on
        //! agent-liveness loss (MembershipTableLiteral: with the orchestrator's edge withheld
        //! from PAUSING and one agent per pod, NoDeadEnd is violated at depth 15). CASes
        //! incarnation+1 on both rows, clears the pause entry and any lease, and is idempotent
        //! by migration_id. It does NOT resume the frozen original — see Task 10 for who does.
        bool abort_pause(FMI::Utils::peer_num rank, const std::string& migration_id) const;
        bool acquire_restore_lease(FMI::Utils::peer_num rank, const std::string& attempt_id,
                                   unsigned int ttl_s) const;
        //! CASes incarnation+1. Returns false for a foreign or lease-less attempt_id.
        bool commit_restore(FMI::Utils::peer_num rank, const std::string& attempt_id) const;
        bool abort_restore(FMI::Utils::peer_num rank, const std::string& attempt_id) const;
        bool mark_active(FMI::Utils::peer_num rank) const;

        //! Agent liveness, the trigger for the orchestrator-driven abort_pause above. renew()
        //! is called from the daemon loop (Task 9); live_agents() is what an orchestrator polls.
        void renew_agent_lease(const std::string& agent_id, unsigned int ttl_s) const;
        [[nodiscard]] bool agent_is_live(const std::string& agent_id) const;

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

- `acquire_restore_lease` accepts `CHECKPOINTED` **or** `RESTORE_RESERVED`-with-expired-lease, per contract 3 — the lazy realisation of `RESTORE_RESERVED → CHECKPOINTED`, since a Redis TTL cannot mutate a `state` field and no sweeper is specified. `abort_pause` is idempotent by `migration_id`. This task implements the state machine and its CAS scripts only; the daemon, staging and restore legs arrive in Tasks 9-11. Add `src/ft/experimental/AgentDirectory.cpp` to `CMakeLists.txt:73-78`.
- **Out of scope for stage B, and recorded as such in the header comment:** `LOST`, the bounded attempt budget `k`, and idempotent operation tokens. `LOST` and `k` exist because contract 3's restore phase is otherwise a non-terminating SCC (`Membership.cfg`: temporal properties violated on `AcquireRestoreLease ↔ LeaseExpirySpurious` and `AcquireRestoreLease ↔ AbortRestore`, both cycling with the pause entry set), and tokens exist because `ControlPlane::command` re-issues a mutating `EVAL` unconditionally on a `nullptr` reply (`src/ft/ControlPlane.cpp:143-147`). Both belong to the single directory C3 builds, not to a shadow one — but Plan B must not build a shadow that *contradicts* them, so `AgentDirectory` never re-enters `RESTORE_RESERVED` without a caller-supplied attempt id and Task 11 records the measured restore duration that sizes the TTL.

> **This is a deliberate, scheduled duplication — record it as such.** Contract 3 specifies **one**
> directory under `fmi:ft:<run_id>:<comm>:`, and Plan C stage C3 implements it as `ControlPlane` v2 (which is also where the `fmi:ft:name:<comm_name> → run_id` index is minted). The
> `ckpt:` namespace here is a **shadow implementation of contract 3's five checkpoint-phase edges**
> (`PAUSING → CHECKPOINTED`, `abort_pause` — agent- *or* orchestrator-driven, from both rows —
> `acquire_restore_lease`, `commit_restore`, `abort_restore`), built in a disjoint namespace for
> exactly one reason: `ControlPlane` v2 does not exist yet and stage B must land without it. It is
> not a second permanent authority.
>
> **Convergence is C3's job and is listed in its work items.** When C3 lands, the `ckpt:` namespace
> is deleted and `AgentDirectory` becomes a thin client of `ControlPlane` v2's Lua scripts. The five
> state-machine cases below (`ckpt_second_lease_is_refused_while_the_first_holds`,
> `ckpt_commit_restore_requires_the_owning_attempt`, `ckpt_abort_pause_is_idempotent_by_migration_id`,
> and the happy path) are written against the `AgentDirectory` **interface**, not its Redis keys, so
> they survive that swap unchanged and become C3's acceptance tests for the real scripts. Do not
> assert on raw key names inside these cases except in `ckpt_namespace_is_disjoint_from_criu_state`
> and `ckpt_keys_are_run_id_fenced`, which are the two cases that legitimately die with the shadow
> namespace.

- [ ] **Step 1: Prove the gap (RED)**

```bash
redis-cli -h 127.0.0.1 -p 6379 keys 'fmi:ft:*:ckpt:*'
redis-cli -h 127.0.0.1 -p 6379 keys 'fmi:ft:name:*'
```

Expected: both empty — no `ckpt:` namespace exists, and no `run_id` index has been minted by any plan yet (so the lab run will take the `local` fallback and say so).

- [ ] **Step 2: Implement the directory**

One `EVAL` script per transition, each taking `(rank, expected_state, expected_incarnation, token)` and returning `{applied, incarnation}`. Reuse the existing hiredis reply-ownership pattern (`ReplyPtr` with `freeReplyObject`, `src/ft/ControlPlane.cpp:22`) rather than raw pointers. Every state string is `to_string(CkptState)`-round-trippable so the Redis contents stay human-readable, matching `include/ft/Common.h:20-42`. Resolve `run_id` once in the constructor and build every key through one private `key(suffix)` helper, so a forgotten fence is a compile-time impossibility rather than a review item.

- [ ] **Step 3: Add the state-machine tests (GREEN)**

New cases in `CriuLab`, all skipping loudly when Redis is unreachable:
- `ckpt_pause_then_checkpoint_then_lease_then_commit` — the happy path, asserting each intermediate `state_of` and that `incarnation_of` advances exactly once, at `commit_restore`.
- `ckpt_second_lease_is_refused_while_the_first_holds` — two `acquire_restore_lease` calls with different `attempt_id`; the second returns false; after the TTL expires it succeeds.
- `ckpt_commit_restore_requires_the_owning_attempt` — `commit_restore` with a foreign `attempt_id` returns false and leaves the state at `RESTORE_RESERVED`.
- `ckpt_abort_pause_is_idempotent_by_migration_id` — calling it twice returns true then false and clears both the pause entry and the lease.
- `ckpt_abort_pause_bumps_the_incarnation_from_both_rows` — from `PAUSING` and again from `CHECKPOINTED`, assert `incarnation_of` is `i+1` after each. Contract 3 requires the bump on both rows; without it a survivor that already closed its link waits forever against a target that never re-registers.
- `ckpt_abort_pause_from_pausing_needs_no_agent` — drive `abort_pause` from `PAUSING` with no agent lease present at all (the orchestrator's edge), assert it applies. This is the `MembershipTableLiteral` dead end closed at the directory level; Task 10's experiment 8 closes it at the process level.
- `ckpt_namespace_is_disjoint_from_criu_state` — write both, call `ControlPlane::clear_criu_state()`, assert the `ckpt:` keys survive and `AgentDirectory::clear()` removes them.
- `ckpt_keys_are_run_id_fenced` — construct two `AgentDirectory` objects for the same `comm_name` under different `FMI_CKPT_RUN_ID` values, drive both to `PAUSING`, and assert neither observes the other's state and `clear()` on one leaves the other intact. This is the whole point of the fence: two runs of one job name must not share a directory.

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
    //! Blocking pub/sub reader on fmi:ft:<run_id>:<comm>:ckpt:events. Owns its own hiredis context and a
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
    //! degrades latency instead of losing the request. Renews its own liveness key on every
    //! wakeup, so an orchestrator can detect agent death and drive abort_pause (contract 3's
    //! PAUSING row; MembershipTableLiteral shows the job wedges without it). Returns when
    //! @p stop is set. FMI_CKPT_POLL_MS overrides the fallback interval (default:
    //! config.criu.poll_ms); FMI_CKPT_AGENT_ID names this agent (default: hostname + pid).
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

Single thread: `poll()` on the subscription fd with `FMI_CKPT_POLL_MS` as the timeout; on wakeup (event or timeout) renew the agent-liveness key (`renew_agent_lease`, TTL = `3 × FMI_CKPT_POLL_MS` with a 2 s floor), then re-read `pause_set()` and dispatch each rank not already dispatched. Dispatch is idempotent by `(rank, migration_id)`. The renewal is deliberately on the *same* thread as dispatch, not on a helper: a thread wedged inside a dump must stop renewing, because "the agent is still renewing" is exactly the signal the orchestrator uses to decide it may **not** abort a pause.

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
- `daemon_liveness_key_expires_when_the_loop_stops` — assert `agent_is_live` is true while the daemon runs, then `stop()` it and assert the key expires within its TTL. This is the orchestrator's only trigger for the `PAUSING` abort edge, so it must fail loudly if the renewal ever moves off the dispatch thread.

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

### Task 10: Freeze → dump → stage → verify-digest → CAS `CHECKPOINTED`, and experiment 8 — agent death mid-checkpoint

**Files:**
- Create: `include/ft/experimental/ArtifactStore.h`
- Create: `src/ft/experimental/ArtifactStore.cpp`
- Create: `include/ft/experimental/CheckpointService.h`
- Create: `src/ft/experimental/CheckpointService.cpp`
- Create: `tools/criu_lab/exp8_agent_death.sh`
- Modify: `include/ft/experimental/CriuExec.h`
- Modify: `src/ft/experimental/CriuExec.cpp`
- Modify: `tools/rank_agent.cpp`
- Modify: `CMakeLists.txt`
- Modify: `tests/criu_lab.cpp`
- Modify: `docs/superpowers/findings/2026-07-27-b-checkpoint-mechanics-findings.md`

**Interfaces:**
- Per contract 4, the sequence is: verify identity → freeze → dump → stage to a durable artifact store → verify the digest → **only then** CAS `CHECKPOINTED`, and the **agent** writes the state. Produces:

```cpp
namespace FMI::FT {
    //! Durable staging for checkpoint images, run_id-fenced on every path and key.
    //! "file://<dir>" writes <dir>/<run_id>/<mid>/<rank>.tar.gz and fsyncs the file and its
    //! directory; "redis" writes AgentDirectory's own
    //! fmi:ft:<run_id>:<comm>:ckpt:image:<mid>:<rank> and stays available as contract 4's
    //! small-image fallback for local runbooks. It deliberately does NOT call
    //! ControlPlane::criu_image_put (include/ft/ControlPlane.h:123,125), whose key is
    //! (epoch, rank) under criu_prefix() with no run_id and whose signature Plan B may not
    //! change; criu_image_put stays in place, untouched, for restore-remote. Selected by
    //! FMI_CKPT_ARTIFACT_URI, defaulting to file://<config.criu.images_dir>/artifacts.
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
        //! the target is thawed and abort_pause is CAS'd back to ACTIVE at incarnation i+1
        //! (contract 3's mandatory PAUSING -> ACTIVE failure edge), so a failed dump can never
        //! wedge the job.
        CheckpointOutcome checkpoint(const PauseRequest& request, const ProcessIdentity& expected,
                                     DumpDiscipline discipline);

        //! The orchestrator's half of the same edge, for the case no agent can cover: the agent
        //! that froze this rank died before completing OR failing its dump, so nothing will ever
        //! call checkpoint()'s failure path. CASes abort_pause (incarnation i+1) and then
        //! RESUMES the frozen original — assigning the resume responsibility contract 3 leaves
        //! open. Refuses when the owning agent's liveness key is still present, so it can never
        //! race a dump that is merely slow. Cross-host shape: `resume_here=false` performs only
        //! the CAS and names, in the returned text, the host whose agent must SIGCONT.
        struct AbortOutcome { bool aborted = false; bool resumed = false; std::string detail; };
        AbortOutcome abort_pause_and_resume(FMI::Utils::peer_num rank,
                                            const std::string& migration_id,
                                            const std::string& owning_agent_id,
                                            bool resume_here);
    };
}
```

- CLI: `fmi-rank-agent abort-pause <comm_name> <num_peers> <config> <rank> <migration_id> [agent_id]`, added beside the existing verbs in `tools/rank_agent.cpp:36-41` and documented in the header comment block. This is the only verb an *orchestrator* (not an agent) drives, and it exists because `MembershipTableLiteral` shows the job has no other way out of `PAUSING`.

- `CriuExec` gains stdout capture, needed for the digest without adding a crypto dependency:

```cpp
    //! Run a command and return its stdout. Same failure semantics as run_process.
    [[nodiscard]] std::string run_process_capture(const std::vector<std::string>& args);
```

  The digest is the first field of `sha256sum <path>`. Packing reuses the existing `/`-relative `tar` discipline of `pack_rank_image` (`src/ft/experimental/LocalRankAgent.cpp:385-405`) including `FMI_CRIU_EXTRA_FILES` (`:25-42`), so a staged archive stays restorable on another host.
- Failure edge: contract 3 requires that **every reachable strongly-connected component** of the state graph have an outbound edge some external actor can drive — the per-state form is provably too weak (`Membership.cfg`: `CHECKPOINTED ⇄ RESTORE_RESERVED` is a cycle in which every state has an outbound edge and the job still never escapes). Today a failed dump is three attempts then a rethrow (`src/ft/experimental/LocalRankAgent.cpp:420-438`) with no state written; `CheckpointService` keeps the retry and adds the `abort_pause` on exhaustion (the agent's half of the edge) **and** `abort_pause_and_resume` for the case where the agent itself is gone (the orchestrator's half). Both halves are required: with only the first, `MembershipTableLiteral` wedges at `PAUSING` with `dumpfail = FALSE`.

- [ ] **Step 1: Prove the gap (RED)**

```bash
grep -rn "mark_checkpointed" src/ tools/ | grep -v AgentDirectory
```

Expected: no hits — nothing calls the CAS yet.

- [ ] **Step 2: Implement the artifact store**

`file://` backend: write to `<dir>/<run_id>/<mid>/<rank>.tar.gz.tmp`, `fsync` the fd, `rename`, then `fsync` the directory fd — a rename alone is not durable. `get()` re-reads and re-digests before returning. `redis` backend writes `AgentDirectory`'s `…:ckpt:image:<mid>:<rank>` through the directory's own connection (not `criu_image_put`, per the interface note above) and refuses blobs over `FMI_CKPT_REDIS_MAX_BYTES` (default 64 MiB) with a message naming the `file://` alternative.

The CRIU **image directory** is re-keyed the same way. `LocalRankAgent::rank_image_dir` builds `<images_dir>/<comm_name>/epoch-<n>/rank-<r>` today (`src/ft/experimental/LocalRankAgent.cpp:379-382`) and `cleanup()` removes `<images_dir>/<comm_name>` (`:91`); stage-B paths produced by `CheckpointService` are `<images_dir>/<run_id>/<comm_name>/<migration_id>/rank-<r>` instead, so a second run of the same job name cannot restore from the first run's images. **Do not re-key `rank_image_dir` itself** — it is the verified `migrate-local` / `evacuate-local` path and its images are addressed by the epoch surface C3 deletes; note the divergence in the findings as a C3 convergence item alongside the `criu_image_put` one.

- [ ] **Step 3: Implement the checkpoint sequence**

Exact order, with no step reordered: `VerifiedProcess::open` (nonce from `AgentDirectory::nonce_for`) → `freeze()` → `dump_rank(..., discipline)` → `pack_rank_image` → `ArtifactStore::put` → `ArtifactStore::get` + digest compare → `AgentDirectory::mark_checkpointed`. Record `freeze_latency` and `frozen_duration` into the outcome. On any throw: `thaw()` (when the process still exists), `abort_pause`, and return `checkpointed=false` with the failure text.

- [ ] **Step 4: Run experiment 8 — the agent dies between freeze and dump completion**

This is the process-level counterpart of `MembershipTableLiteral.cfg` (`NoDeadEnd` violated at depth 15, 354 states generated, 179 distinct, **`dumpfail = FALSE`** and a **single agent**): with one agent per pod and `abort_pause` available only to the agent — whose own precondition is a *failed* dump that never happened — nothing has an edge out of `PAUSING`, the rank stays frozen, and its pause entry suspends deadlines job-wide. Plan B owns the agent, so Plan B owes the demonstration that the orchestrator-driven edge actually recovers a real frozen process.

`tools/criu_lab/exp8_agent_death.sh` runs three shapes against a toy target, each with the daemon of Task 9 as the only agent:

1. **Between freeze and dump.** `FMI_MOCK_GATE_MODE=dump` (`tests/criu_fault_tolerance.cpp:231-238`) parks mock criu at the dump; `SIGKILL` the daemon while its target is frozen. Record: the toy's `/proc/<pid>/stat` state (must be `T`), the directory state (must be `PAUSING`), the pause entry (must be present), and the agent-liveness key's expiry instant.
2. **Recovery.** Once the liveness key has expired, run `fmi-rank-agent abort-pause … <rank> <migration_id>` and record: the CAS result, the incarnation before and after (must be `i` → `i+1`), the toy's state afterwards (must be `R`/`S`), and its `tick` resuming from the pre-freeze value.
3. **The refusal.** Repeat shape 1 but leave the daemon **alive and merely slow** (gate held, no kill). `abort-pause` must refuse while the liveness key is present, and the held dump must then complete normally when the gate is released. A slow agent is not a dead one, and the model's own `MembershipUnfencedSigCont` result — a twin produced with `crashed = {}` — is the standing warning that "slow but alive" is a real, non-crash schedule.

```bash
CRIU=/usr/local/sbin/criu bash tools/criu_lab/exp8_agent_death.sh 2>&1 | tee /tmp/fmi-exp8.log
redis-cli -h 127.0.0.1 -p 6379 keys 'fmi:ft:*:ckpt:*'
```

Expected: shape 1 leaves a frozen toy with no actor (the wedge, reproduced); shape 2 recovers it and bumps the incarnation; shape 3 refuses. **A null result — shape 1 recovering on its own — means either the daemon did not really die or the toy was not really frozen; re-run rather than record it as "no wedge exists".**

Append experiment 8 using the Task 1 template. The consequence section must state (i) the liveness TTL an orchestrator should wait before aborting, measured, not guessed, (ii) that on the **cross-host** path the `SIGCONT` is a *different node's* agent, so `abort-pause` there is a two-step (CAS on the orchestrator, resume on the surviving host's agent) — the resume responsibility spec open item 5 asks Plan B to assign — and (iii) that the same-host verbs cannot offer this edge at all, per experiment 1.

- [ ] **Step 5: Add the sequencing tests (GREEN)**

- `checkpoint_marks_state_only_after_digest_verifies` — mock criu; between staging and the CAS, corrupt the staged file; assert `mark_checkpointed` was never reached, the state returned to `ACTIVE`, and the failure text names the digest.
- `checkpoint_refuses_a_recycled_pid` — pass a `ProcessIdentity` with a perturbed `start_ticks`; assert no `SIGSTOP` was delivered (the toy keeps ticking) and the pause was aborted.
- `checkpoint_aborts_pause_after_dump_failure` — mock criu with `FMI_MOCK_FAIL_MODE=dump`; assert three attempts, then state `ACTIVE`, and the toy thawed and ticking.
- `checkpoint_happy_path_stages_and_marks` — real criu, self-skipping; assert the manifest's `sha256` matches `sha256sum` of the staged file, the state is `CHECKPOINTED`, and the outcome's `frozen_duration` is recorded.
- `staged_paths_and_keys_carry_the_run_id` — stage the same `(migration_id, rank)` under two different `FMI_CKPT_RUN_ID` values against both artifact backends; assert the two `file://` paths differ by exactly the `run_id` component, the two Redis keys differ likewise, and neither `get()` can reach the other's blob.
- `abort_pause_and_resume_recovers_a_frozen_target` — the experiment-8 shape as a test, mock criu, no real criu required: gate the dump, kill the dispatching daemon, let the liveness key expire, call `abort_pause_and_resume`, and assert the toy resumes ticking **from its pre-freeze tick**, the state is `ACTIVE`, and `incarnation_of` advanced by exactly one.
- `abort_pause_refuses_while_the_agent_is_live` — same setup without the kill; assert the call returns `aborted=false` with a detail naming the live agent, and that the target is still `T` and still `PAUSING` afterwards (the refusal must not half-apply).

```bash
cmake --build build -j"$(nproc)"
./build/tests/Boost_Tests_run --run_test=CriuLab
./build/tests/Boost_Tests_run --run_test=CriuFaultTolerance
(cd build/tests && ./Boost_Tests_run)
./build/tools/fmi-rank-agent 2>&1 | grep -c abort-pause
```

- [ ] **Step 6: Commit**

```bash
git add include/ft/experimental/ArtifactStore.h src/ft/experimental/ArtifactStore.cpp \
  include/ft/experimental/CheckpointService.h src/ft/experimental/CheckpointService.cpp \
  include/ft/experimental/CriuExec.h src/ft/experimental/CriuExec.cpp \
  tools/criu_lab/exp8_agent_death.sh tools/rank_agent.cpp CMakeLists.txt \
  tests/criu_lab.cpp docs/superpowers/findings/2026-07-27-b-checkpoint-mechanics-findings.md
git commit -m "feat: stage checkpoints durably and recover a frozen target after agent death"
```

---

### Task 11: Restore under lease — pidfile, commit CAS, `SIGCONT` on win, `SIGKILL` on loss

**Files:**
- Modify: `include/ft/experimental/CheckpointService.h`
- Modify: `src/ft/experimental/CheckpointService.cpp`
- Modify: `src/ft/experimental/LocalRankAgent.cpp`
- Modify: `tools/rank_agent.cpp`
- Modify: `tests/criu_lab.cpp`
- Modify: `docs/superpowers/findings/2026-07-27-b-checkpoint-mechanics-findings.md`

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
- **Lease exclusion is not the arbitrator; the `commit_restore` CAS is.** `LeaseExclusion` holds in `Membership.tla` but is near-vacuous — Redis `SET NX` makes it true by construction — and its useful content is negative: a straggler keeps its restored process after its lease expires. The kill-on-loss must therefore be driven by the CAS result, never by "my lease looks valid".
- **This task owes two numbers that no model can supply.** `Membership.tla` has no clock, and `Membership.cfg`'s liveness failure isolates to the single unstated assumption "the lease outlives the `criu restore`" (`MembershipLivenessSF.cfg` is the identical 9,869 / 2,919 / depth-25 state space with strong instead of weak fairness on the restore steps, and it holds). So record, from the real-criu round trip below at each RSS point of experiment 6: **measured `criu restore` duration (median and p95)**, and the resulting recommended lease TTL and attempt budget `k`. These are spec open item 5's inputs.

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
- `restore_under_lease_round_trip` — real criu, self-skipping: full checkpoint (Task 10) then restore-under-lease; assert the restored toy resumes from the dumped `tick`, not from 0, and only after `SIGCONT`. Log the measured restore duration with `BOOST_TEST_MESSAGE`.
- `commit_restore_bumps_the_incarnation_exactly_once` — two concurrent attempts; assert the incarnation advances by one, not two, and that the loser's failure is the CAS result and not a lease check.

```bash
cmake --build build -j"$(nproc)"
./build/tests/Boost_Tests_run --run_test=CriuLab
(cd build/tests && ./Boost_Tests_run)
```

- [ ] **Step 4: Record the restore-duration numbers**

Append a short "restore duration" subsection to experiment 6's finding (the RSS sweep is where the ballast sizes already are): median and p95 `criu restore` wall time per RSS point, the recommended lease TTL as a multiple of p95, and the attempt budget `k`. State plainly that these are the numbers contract 3's lease rule requires and that `Membership.tla` cannot produce them.

- [ ] **Step 5: Commit**

```bash
git add include/ft/experimental/CheckpointService.h src/ft/experimental/CheckpointService.cpp \
  src/ft/experimental/LocalRankAgent.cpp tools/rank_agent.cpp tests/criu_lab.cpp \
  docs/superpowers/findings/2026-07-27-b-checkpoint-mechanics-findings.md
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
- **Deployment decision — reconcile "resident per-node agent" with "per-pod sidecar" against what is actually deployed.** `docs/superpowers/findings/2026-07-27-b-deployment-decision.md` must open with the evidence, not the recommendation:

  ```bash
  grep -rn -iE "daemonset|sidecar|shareProcessNamespace|hostPID" runbooks/
  ```

  returns **nothing**. Neither shipped k8s runbook uses any of those shapes: `runbooks/k8s-criu-node-evacuation/k8s/machine-deployment.yaml:1-4` documents that the orchestrator `kubectl exec`s `fmi-rank-agent migrate-local` into the **same container** as the ranks precisely so "the agent shares the ranks' PID + mount namespace — the prerequisite for criu dump/restore", and `runbooks/k8s-criu-node-evacuation/k8s/knative-restore-service.yaml:1-14` plus `runbooks/k8s-criu-node-evacuation/restore_server.py:1-19` put the restore leg **inside** the Knative pod behind a held request. The **in-container agent is therefore the verified baseline**, and the document must either make that shape normative for the v2 runbooks or state the sidecar's prerequisites normatively — it may not silently adopt an unverified shape.

  Enumerate exactly three shapes, with a verdict each:
  1. **In-container agent (verified baseline).** What actually runs today. Its one gap against contract 4 is **residency**: the agent is `kubectl exec`'d on demand rather than already running to receive a pause request, so Task 9's `daemon` verb is what closes it. Its one property the sidecar lacks: it dies *with* its rank, so "the agent is gone" and "the rank is gone" cannot diverge.
  2. **Per-pod sidecar.** Permitted only with all three prerequisites, and a deployment claiming this shape without them is ill-formed: (a) `shareProcessNamespace: true` on the pod so the sidecar can see and `ptrace`-seize the rank; (b) an **identical mount layout** in both containers for every path criu reopens — the executable, every mapped library, the images directory, and any file the rank holds open, because criu reopens by path and a mismatch fails at *restore*, not at dump; (c) the additional Knative feature gate needed to set `shareProcessNamespace` on a Knative Service, on top of the three gates `runbooks/k8s-criu-node-evacuation/k8s/knative-restore-service.yaml:6-11` already enables (`podspec-securitycontext`, `containerspec-addcapabilities`, `podspec-volumes-emptydir`). None of the three is verified on a cluster by this plan, and the document must say so.
  3. **DaemonSet.** Cannot dump a rank in another pod (different PID and mount namespaces) and cannot perform the restore leg on Knative at all. Ruled out.

  The document must also state that contract 3's `PAUSING` dead end (`MembershipTableLiteral`, `NoDeadEnd` violated at depth 15) is **the sidecar's failure mode specifically** — an agent that can die while its rank lives — which is why Task 10's orchestrator-driven `abort-pause` verb and experiment 8 are prerequisites for adopting shape 2 at all, and finally state the consequence for `runbooks/localstack-python311-redis/knative-migration/`.

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

Structure: the property required by contract 4 (never checkpointed, free to hold sockets, resident) → the grep result showing no `daemonset`/`sidecar`/`shareProcessNamespace`/`hostPID` anywhere in `runbooks/` → what the two in-repo runbooks actually do → the three shapes and their verdicts → the recommendation, stated as *which shape is normative for the v2 runbooks*, and exactly which manifests would change if the sidecar is chosen. Cite files by path and line as above, and mark every sidecar prerequisite `UNVERIFIED` unless it was actually exercised on a cluster during this plan — a prerequisite listed without a verification status is how an unverified shape becomes the default by accident.

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

Flip every `PENDING` in the findings table of contents to a verdict, and add a closing "Inputs to Plan C" section that answers, in one line each: spec open item 3 (inline-send versus full-engine — from experiments 3 and 6), spec open item 4 (`--pre-dump`/`--track-mem` — from experiment 6), spec open item 5 (the lease TTL and attempt budget `k`, plus the assigned resume responsibility on the orchestrator-driven `abort_pause` edge — from experiments 6 and 8 and Task 11), spec open item 6 (the agent deployment shape — from the decision document), the `W`-sizing formula, and whether stage C2's commitment point is safe to take. Add a short "Confirmed / refuted modelled predictions" table listing `MembershipSameHost` (experiment 1) and `MembershipTableLiteral` (experiment 8) with the observed outcome, and a "C3 convergence items" list naming the two deliberate divergences this plan records: the `redis` artifact backend bypassing `ControlPlane::criu_image_put` for the `run_id` fence, and `LocalRankAgent::rank_image_dir` staying epoch-keyed. Then:

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

Expected: every suite green, the usage line lists `daemon`, `abort-pause`, `restore-leased`, and `evacuate-batched`, and the `ckpt:` namespace is empty after the run (no test leaks Redis state, under every `run_id` the tests used).

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
