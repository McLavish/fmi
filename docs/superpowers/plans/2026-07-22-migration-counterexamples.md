# Migration Protocol Counterexamples Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build an opt-in C++17 tool that discovers and replays many deterministic safety and liveness counterexamples against FMI's transparent-migration protocol.

**Architecture:** A shared test-only scenario DSL feeds both a bounded abstract explorer and a subprocess-isolated real-FMI runner. The runner first validates every program without migration, then inserts one orchestrated migration cut and classifies any semantic difference; Redis provides the deterministic complete corpus, while Direct cases run when `tcpunchd` is available.

**Tech Stack:** C++17, FMI `Communicator` and `FT::ControlPlane`, Redis/hiredis, TCPunch Direct transport, CMake, POSIX threads/processes/pipes.

## Global Constraints

- Exactly one orchestrator serializes migration requests and epoch promotion attempts.
- The orchestrator, ranks, Redis, TCPunch, hosts, and network do not crash or partition.
- Rank programs may use any valid mix and ordering of point-to-point and collective operations.
- A completed `send` creates a delivery obligation even when its matching `recv` has not started.
- Per-source FIFO, exactly-once delivery, collective synchronization, and eventual progress must survive migration.
- Use `state_transfer="criu"` only to park and resume a target in tests; never invoke the `criu` binary.
- Do not modify production headers or sources and do not fix the protocol in this task.
- Preserve `tests/migration_p2p_cut_counterexample.cpp` as the narrative minimal proof.
- Every real scenario must pass an identical no-migration baseline before its migrated result is evidence.
- Known-bad cases remain opt-in and outside `Boost_Tests_run`/CTest.
- Exit `0` when no selected counterexample reproduces, `2` when at least one reproduces, `1` for setup/harness failure, and reserve `124` for an uncontained top-level timeout.
- Use `uv`, not bare `pip`, for any Python package management; this implementation requires no Python packages.
- Stage and commit only files belonging to the current task; preserve all pre-existing working-tree changes.

---

### Task 1: Scenario DSL, stable corpus, and CLI foundation

**Files:**
- Create: `tests/migration_counterexample_scenarios.h`
- Create: `tests/migration_counterexample_scenarios.cpp`
- Create: `tests/migration_counterexample_runner.cpp`
- Create: `tests/migration_cut_model.cpp`
- Modify: `tests/CMakeLists.txt`

**Interfaces:**
- Produces:

```cpp
namespace FMI::Tests::MigrationCounterexamples {
enum class Backend { Redis, Direct };
enum class ActionKind { Send, Receive, Barrier, Synchronize };
enum class ScenarioKind {
    Program,
    FutureSendAfterCut,
    StaleBarrierObject,
    PendingSetExpansion,
    FailedOperationBoundary,
    IncompleteMembership
};
enum class Classification {
    Preserved,
    WrongPayload,
    LostMessage,
    ReorderedOrDuplicated,
    PartialPayload,
    PrematureCollective,
    PromotionStuck,
    OperationStuck,
    SetupError,
    InfrastructureSkip
};

struct Action {
    ActionKind kind;
    FMI::Utils::peer_num peer = 0;
    std::vector<int> payload;
    std::size_t receive_elements = 0;
    std::string tag;
};

struct Phase {
    std::vector<std::vector<Action>> rank_actions;
};

struct Scenario {
    std::string id;
    std::string property;
    Backend backend;
    ScenarioKind kind;
    FMI::Utils::peer_num world;
    std::vector<FMI::Utils::peer_num> targets;
    std::size_t request_before_phase;
    std::vector<Phase> phases;
    Classification expected_current_failure;
    std::chrono::milliseconds deadline;
};

struct Observation {
    Classification classification = Classification::SetupError;
    std::string detail;
    std::vector<std::vector<int>> received_payloads;
    bool baseline_valid = false;
    bool promoted = false;
};

const std::vector<Scenario>& scenarios();
const Scenario* find_scenario(const std::string& id);
std::string to_string(Backend value);
std::string to_string(Classification value);
}
```

- The catalog contains these stable IDs exactly once: `queued_message_lost`,
  `queued_message_substituted`, `partially_drained_fifo_suffix`,
  `multi_message_backlog_shift`, `variable_size_partial_overwrite`,
  `bidirectional_backlog`, `three_rank_ring_backlog`, `unrelated_rank_migration`,
  `future_matching_send_after_cut`, `direct_moved_link_backlog`,
  `direct_survivor_link_crosses_epoch`, `stale_barrier_object`,
  `pending_set_expands_after_park`, `failed_operation_advances_boundary`,
  `promotion_before_full_membership`, and `single_rank_self_send`.

- The CLI supports `--list`, `--case <id>`, `--backend redis|direct`, and `--explore`.
  Execution and exploration may initially return a clear `not implemented` setup error; argument
  parsing, filtering, stable listing, duplicate-ID rejection, and exit-code aggregation are real.

- [ ] **Step 1: Verify the target is absent (RED)**

Run:

```bash
cmake --build build --target fmi_migration_counterexamples -j2
```

Expected: non-zero with `No rule to make target 'fmi_migration_counterexamples'`.

- [ ] **Step 2: Add the scenario types and exact stable catalog**

Implement the interfaces above. Each `Scenario` supplies complete per-rank phase vectors for
program-shaped cases; specialized cases use an empty phase list and their `ScenarioKind`.
`find_scenario` returns `nullptr` for an unknown ID. Duplicate catalog IDs are detected during
startup and reported as `SetupError` rather than silently shadowed.

- [ ] **Step 3: Add CLI parsing and listing**

Implement a small `main` that parses the exact options above, rejects incompatible/repeated options,
prints one line per selected scenario as `<id>\t<backend>\t<property>`, and emits usage for invalid
input. Unknown IDs/backends exit `1`; `--list` exits `0`.

- [ ] **Step 4: Wire the opt-in CMake target**

Inside the existing `if(FMI_ENABLE_CRIU)` block, add:

```cmake
add_executable(
    fmi_migration_counterexamples
    migration_counterexample_runner.cpp
    migration_counterexample_scenarios.cpp
    migration_cut_model.cpp
)
target_link_libraries(fmi_migration_counterexamples FMI Threads::Threads)
target_compile_definitions(
    fmi_migration_counterexamples
    PRIVATE FMI_SOURCE_DIR="${CMAKE_SOURCE_DIR}"
)
```

Create `tests/migration_cut_model.cpp` in this task with only the declaration-compatible stub:

```cpp
std::vector<ModelCounterexample> explore_bounded_model() { return {}; }
```

Add `ModelCounterexample` and this function declaration to the shared header so Task 2 can replace
the stub without changing callers.

- [ ] **Step 5: Build and verify listing (GREEN)**

Run:

```bash
cmake -S . -B build -DFMI_BUILD_TESTS=ON -DFMI_ENABLE_CRIU=ON
cmake --build build --target fmi_migration_counterexamples -j2
./build/tests/fmi_migration_counterexamples --list
./build/tests/fmi_migration_counterexamples --case not-a-case
```

Expected: build succeeds; `--list` prints 16 unique lines and exits `0`; unknown case exits `1`.

- [ ] **Step 6: Commit**

```bash
git add tests/CMakeLists.txt tests/migration_counterexample_scenarios.h \
  tests/migration_counterexample_scenarios.cpp tests/migration_counterexample_runner.cpp \
  tests/migration_cut_model.cpp
git commit -m "test: scaffold migration counterexample corpus"
```

### Task 2: Deterministic bounded protocol explorer

**Files:**
- Modify: `tests/migration_cut_model.cpp`
- Modify: `tests/migration_counterexample_runner.cpp`
- Modify: `tests/migration_counterexample_scenarios.h`

**Interfaces:**
- Consumes: `Action`, `Backend`, `Classification`, `ModelCounterexample`, and
  `explore_bounded_model()` from Task 1.
- Produces:

```cpp
struct ModelCounterexample {
    std::string normalized_trace;
    Backend backend;
    Classification classification;
    std::string matching_scenario_id;
};

std::vector<ModelCounterexample> explore_bounded_model();
```

- The model state contains rank PCs, completed-operation boundaries, active epoch, fixed cut,
  pending/parked/quiesced sets, and tagged FIFO queues keyed by `(sender, receiver)`.
- Eager send enqueues and completes; receive blocks on an empty matching queue; migration uses the
  current first-observer/max-published cut rule and promotion gates.
- Redis promotion discards all old queues; Direct promotion discards only moved-rank queues.
- Enumerate worlds 2 and 3, one target, programs up to 4 actions per rank, and request placements
  after at least one completed send. Deduplicate visited states and normalized failure traces.
- Deterministic output is sorted by `(classification, backend, normalized_trace)`.

- [ ] **Step 1: Run the unsupported explorer (RED)**

Run:

```bash
./build/tests/fmi_migration_counterexamples --explore
```

Expected: non-zero with the Task 1 `not implemented` diagnostic or no counterexamples, proving the
new explorer behavior is absent.

- [ ] **Step 2: Implement state transitions and semantic comparison**

Implement a reference transition that preserves channel queues at promotion and a current-protocol
transition that applies backend reconfiguration. Classify the first difference as
`WrongPayload`, `LostMessage`, `ReorderedOrDuplicated`, `PromotionStuck`, or `OperationStuck`.
Normalize payload IDs by send order so isomorphic traces deduplicate.

- [ ] **Step 3: Implement minimization and stable mapping**

For each failure, repeatedly remove an action or move the request later when the same classification
still holds. Map representative shapes to curated IDs:

```text
lost unmatched queue                 -> queued_message_lost
later send consumes old receive slot -> queued_message_substituted
multiple queued messages shift       -> multi_message_backlog_shift
moved Direct edge drops bytes         -> direct_moved_link_backlog
kept Direct edge crosses epoch        -> direct_survivor_link_crosses_epoch
receive needs future send             -> future_matching_send_after_cut
```

- [ ] **Step 4: Render and verify deterministic output (GREEN)**

`--explore` prints one block per minimized trace containing classification, backend, matching corpus
ID, and per-rank schedule. Run:

```bash
./build/tests/fmi_migration_counterexamples --explore > /tmp/fmi-explore-1.txt
./build/tests/fmi_migration_counterexamples --explore > /tmp/fmi-explore-2.txt
diff -u /tmp/fmi-explore-1.txt /tmp/fmi-explore-2.txt
rg -n "queued_message_lost|queued_message_substituted|future_matching_send_after_cut" \
  /tmp/fmi-explore-1.txt
```

Expected: `diff` exits `0`; all three IDs occur; explorer exits `2` because violations exist.

- [ ] **Step 5: Commit**

```bash
git add tests/migration_cut_model.cpp tests/migration_counterexample_runner.cpp \
  tests/migration_counterexample_scenarios.h
git commit -m "test: explore bounded migration cut traces"
```

### Task 3: Subprocess-isolated Redis program runner and P2P corpus

**Files:**
- Modify: `tests/migration_counterexample_runner.cpp`
- Modify: `tests/migration_counterexample_scenarios.cpp`

**Interfaces:**
- Consumes: program-shaped `Scenario` phase data from Tasks 1-2.
- Produces:

```cpp
Observation run_program_case(const Scenario& scenario, bool migrate);
Observation run_isolated(const Scenario& scenario, bool migrate);
```

- `run_program_case` constructs one communicator per rank using
  `config/fmi_ft_stress_redis_test.json`, executes phases, and records tagged receives.
- A cancelable phase gate coordinates test phases without adding FMI operations.
- In migrated mode, the main thread waits until every rank completes phases before
  `request_before_phase`, requests the exact target set, releases the next phase, and retries
  promotion until success or the scenario deadline.
- `run_isolated` forks one child, transfers an `Observation` through a pipe, applies the deadline,
  kills/reaps only the child on expiry, and never lets an unbounded protocol wait hang the corpus.
- Baseline and migrated observations are compared before classification is accepted.

- [ ] **Step 1: Run one unsupported real scenario (RED)**

Run:

```bash
./build/tests/fmi_migration_counterexamples --case queued_message_substituted
```

Expected: exit `1` with the Task 1 unsupported-execution diagnostic.

- [ ] **Step 2: Implement the phase gate and real rank executor**

For `Send`, construct `FMI::Comm::Data<std::vector<int>>` from `payload` and call `send`.
For `Receive`, allocate `receive_elements`, call `recv`, and append the returned vector.
For `Barrier`, call `comm.barrier()`. `Synchronize` only advances the cancelable test phase.
Catch `FMI::Utils::Timeout` distinctly from other exceptions and preserve the rank/action in
`detail`.

- [ ] **Step 3: Implement migration orchestration and isolation**

Use a unique communicator name containing scenario ID, baseline/migrated label, PID, and monotonic
timestamp. Clear job/CRIU state before and after. A pipe message contains classification, promoted
flag, receive payloads, and detail using a length-prefixed binary encoding; truncated/invalid pipe
data is `SetupError`. Use `waitpid(WNOHANG)` polling and `SIGKILL` on the scenario deadline.

- [ ] **Step 4: Implement and run the Redis P2P cases (GREEN)**

Populate and execute:

```text
queued_message_lost
queued_message_substituted
partially_drained_fifo_suffix
multi_message_backlog_shift
variable_size_partial_overwrite
bidirectional_backlog
three_rank_ring_backlog
unrelated_rank_migration
single_rank_self_send
```

Each case must print `baseline=preserved` before its migrated classification. Run each with
`--case`; then run `--backend redis`. Expected: valid baselines, all selected cases execute even
after a timeout-classified child, and aggregate exit `2`.

- [ ] **Step 5: Commit**

```bash
git add tests/migration_counterexample_runner.cpp tests/migration_counterexample_scenarios.cpp
git commit -m "test: replay Redis migration counterexamples"
```

### Task 4: Specialized liveness, collective, and control-plane cases

**Files:**
- Modify: `tests/migration_counterexample_runner.cpp`
- Modify: `tests/migration_counterexample_scenarios.cpp`

**Interfaces:**
- Consumes: scenario registry, isolation, timeout, and aggregation from Task 3.
- Produces:

```cpp
Observation run_specialized_case(const Scenario& scenario, bool migrate);
```

- Dispatch by `ScenarioKind`; do not encode control-plane mutations as fake rank actions.

- [ ] **Step 1: Verify specialized cases are unsupported (RED)**

Run each of these and require exit `1`/`SetupError`:

```bash
./build/tests/fmi_migration_counterexamples --case stale_barrier_object
./build/tests/fmi_migration_counterexamples --case pending_set_expands_after_park
./build/tests/fmi_migration_counterexamples --case failed_operation_advances_boundary
./build/tests/fmi_migration_counterexamples --case promotion_before_full_membership
./build/tests/fmi_migration_counterexamples --case future_matching_send_after_cut
```

- [ ] **Step 2: Implement liveness and pending-set cases**

`future_matching_send_after_cut` forces the receiver into an early blocking receive and holds the
matching sender before its future send; classify the bounded promotion failure as `OperationStuck`.
`pending_set_expands_after_park` requests one target, waits until the other rank is parked at the
published cut, expands the pending set to include that survivor, and proves promotion remains gated
because it can no longer observe `self_pending`.

- [ ] **Step 3: Implement collective and boundary-accounting cases**

`stale_barrier_object` uses process ranks and a `state_transfer="none"` Redis configuration derived
at test runtime in `/tmp`; the target exits without finalization, the survivor's epoch-1 barrier 0
must not return before the replacement arrives, and an early return is `PrematureCollective`.
`failed_operation_advances_boundary` catches a deterministic timed-out receive, observes the
published next boundary on the following guarded operation, and classifies acceptance of the failed
attempt as `WrongPayload` or `PromotionStuck` according to the resulting schedule.

- [ ] **Step 4: Implement incomplete-membership promotion**

Register fewer than `world` ranks, request and quiesce a registered target, publish the necessary
boundary through a real guarded call, and show `promote_epoch` succeeds before the healthy delayed
rank joins. Classify this as `PrematureCollective` and report member/world counts.

- [ ] **Step 5: Verify all specialized cases (GREEN)**

Run the five commands from Step 1. Expected: every baseline is valid or the control-only case emits
an explicit reference precondition; every migrated run produces its named bounded classification;
aggregate Redis execution still exits `2` and reaps every child.

- [ ] **Step 6: Commit**

```bash
git add tests/migration_counterexample_runner.cpp tests/migration_counterexample_scenarios.cpp
git commit -m "test: add migration control-plane counterexamples"
```

### Task 5: Direct transport counterexamples

**Files:**
- Modify: `tests/migration_counterexample_runner.cpp`
- Modify: `tests/migration_counterexample_scenarios.cpp`

**Interfaces:**
- Consumes: scenario isolation and observations from Task 3.
- Produces real process-per-rank execution for the two `Backend::Direct` scenarios.

- [ ] **Step 1: Run Direct cases without implementation (RED)**

With `tcpunchd` listening on port 10000, run:

```bash
./build/tests/fmi_migration_counterexamples --case direct_moved_link_backlog
./build/tests/fmi_migration_counterexamples --case direct_survivor_link_crosses_epoch
```

Expected: setup/unsupported failure, not a semantic classification.

- [ ] **Step 2: Add the TCPunch probe and process rank harness**

Probe `127.0.0.1:10000` before forking. If unavailable, return `InfrastructureSkip` without
counting it as a protocol pass or failure. When available, fork one process per rank, use shared
anonymous memory for phase/cancel/result state, `_Exit` from every child, and bounded `waitpid`
cleanup. Never run concurrent TCPunch ranks as threads in one process.

- [ ] **Step 3: Implement moved-link and survivor-link schedules**

Warm the Direct connection with one matched exchange. For `direct_moved_link_backlog`, queue a new
tagged message on a link involving the target, cut, send a later tag on the re-paired link, and
classify loss/substitution. For `direct_survivor_link_crosses_epoch`, queue the old tag between two
survivors, migrate rank 2, then receive after promotion and report whether epoch-0 bytes cross the
epoch on the retained socket.

- [ ] **Step 4: Verify Direct behavior (GREEN)**

Start the checked-in `tcpunchd`, run both commands from Step 1, then stop only that server process.
Expected: baselines preserve FIFO; migrated outcomes are semantic classifications, never a
rendezvous setup error; the aggregate runner exits `2` when either documented invariant is violated.

- [ ] **Step 5: Commit**

```bash
git add tests/migration_counterexample_runner.cpp tests/migration_counterexample_scenarios.cpp
git commit -m "test: replay Direct migration counterexamples"
```

### Task 6: Full verification and usage documentation

**Files:**
- Create: `tests/MIGRATION_COUNTEREXAMPLES.md`
- Modify: `tests/migration_counterexample_runner.cpp` only if verification exposes a harness defect

**Interfaces:**
- Documents build prerequisites, exit codes, CLI examples, scenario classifications, why a baseline
  is mandatory, and how to promote a fixed case into an ordinary regression test.

- [ ] **Step 1: Write the documentation check (RED)**

Run:

```bash
test -f tests/MIGRATION_COUNTEREXAMPLES.md
```

Expected: exit `1` because the usage document does not exist.

- [ ] **Step 2: Add concise usage and interpretation documentation**

Include exact build/run commands, Redis/tcpunchd prerequisites, all four top-level exit codes, and
a table of stable scenario IDs with current classifications. State explicitly that exit `2` is the
expected diagnostic result while bugs reproduce.

- [ ] **Step 3: Run fresh verification**

Run:

```bash
cmake -S . -B build -DFMI_BUILD_TESTS=ON -DFMI_ENABLE_CRIU=ON
cmake --build build --target fmi_migration_counterexamples \
  fmi_migration_p2p_cut_counterexample Boost_Tests_run -j2
./build/tests/fmi_migration_counterexamples --list
./build/tests/fmi_migration_counterexamples --explore
./build/tests/fmi_migration_counterexamples --backend redis
./build/tests/fmi_migration_p2p_cut_counterexample
./build/tests/Boost_Tests_run \
  --run_test=FaultTolerance/transparent_migration_cut_timing_stress_redis
```

Expected: all builds succeed; listing is unique; explorer and known-bad runners exit `2` with
diagnostic classifications; original counterexample exits `2`; focused ordinary Boost test exits
`0`. Run Direct cases separately with `tcpunchd` and record skips distinctly.

- [ ] **Step 4: Audit repository scope and output**

Run:

```bash
git diff --check
git status --short
git diff --name-only HEAD~5..HEAD
```

Confirm no production header/source changed, no unrelated user change was staged, every child is
reaped, and output contains no unexplained warning/error noise. Cross-check every explorer
classification against at least one real-FMI scenario with the same stable identifier; when the
bounded model finds a class that the live DSL cannot faithfully replay, name that gap explicitly in
`tests/MIGRATION_COUNTEREXAMPLES.md` rather than presenting the model result as implementation
evidence.

- [ ] **Step 5: Commit**

```bash
git add tests/MIGRATION_COUNTEREXAMPLES.md
git commit -m "docs: explain migration counterexample runner"
```
