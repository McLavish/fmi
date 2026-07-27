#pragma once

#include "utils/Common.h"

#include <chrono>
#include <cstddef>
#include <string>
#include <vector>

namespace FMI::Tests::MigrationCounterexamples {

enum class Backend { Redis, Direct, S3 };

// The nine primitives a scenario program may issue.  Send/Receive/Barrier/Synchronize are the
// original four; the six collectives were added so that the behavioural corpus can express the
// operations whose algorithm selection depends on the reduction function's flags.
enum class ActionKind {
    Send,
    Receive,
    Barrier,
    Synchronize,
    Bcast,
    Gather,
    Scatter,
    Reduce,
    Allreduce,
    Scan
};

// Element-wise reduction kernels available to Reduce/Allreduce/Scan.  The kernel is deliberately
// decoupled from the declared commutative/associative flags: a scenario may declare a
// mathematically commutative kernel as non-commutative in order to force the left-to-right
// algorithm (include/Communicator.h:103 and :128).
enum class ReduceOp { Sum, Product, Max, Min, ConcatLeftToRight };

struct ReduceFunction {
    ReduceOp op = ReduceOp::Sum;
    // left_to_right = !(commutative && associative) selects an entirely different algorithm, so
    // these two flags are part of scenario identity, not an implementation detail.
    bool commutative = true;
    bool associative = true;
};

enum class ScenarioKind {
    Program,
    FutureSendAfterCut,
    StaleBarrierObject,
    PendingSetExpansion,
    FailedOperationBoundary,
    IncompleteMembership
};

// fault_tolerance.state_transfer selects the config the executor loads.
enum class StateTransfer { Criu, None };

// Where in the target rank's execution the migration request is staged.
enum class TriggerKind {
    // Between two phases: every rank is at a guard boundary.  This is the only trigger the
    // current protocol can drive.
    BeforePhase,
    // Inside a logical FMI operation, after the operation guard was entered.
    MidOperation,
    // Inside application compute strictly between two FMI operations of the same phase.
    MidCompute,
    // While the target blocks in recv() whose matching send has not been issued.
    MidBlockingRecv,
    // While the target is inside Direct/TCPunch rendezvous pairing.
    MidPairing,
    // Immediately after a send() returned on the target, before its next operation.
    AfterCompletedSend
};

struct Trigger {
    TriggerKind kind = TriggerKind::BeforePhase;
    // BeforePhase: the phase index the request is issued before.  All other kinds: the phase that
    // contains the anchor action.
    std::size_t phase = 0;
    // Index of the anchor action inside phase, ignored by BeforePhase.
    std::size_t action = 0;
    // Rank whose execution the anchor action belongs to, ignored by BeforePhase.
    FMI::Utils::peer_num rank = 0;
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
    InfrastructureSkip,
    // The job aborted with a clear diagnostic.  This is the CORRECT outcome for genuinely
    // ill-formed or unsupported programs.
    LoudFail,
    // The config/backend combination is legitimately out of scope (for example S3 under CRIU).
    Unsupported,
    // The harness cannot yet stage this trigger against the current protocol.
    NotYetDrivable
};

struct Action {
    ActionKind kind = ActionKind::Synchronize;
    // Send/Receive: the p2p peer.  Ignored by every collective.
    FMI::Utils::peer_num peer = 0;
    // Send: the payload.  Bcast/Scatter: the root's contribution.  Gather/Reduce/Allreduce/Scan:
    // this rank's contribution.
    std::vector<int> payload;
    // Receive: element count.  Bcast: element count on non-root ranks.  Scatter: per-rank element
    // count.  Ignored elsewhere (the result size follows from payload and world).
    std::size_t receive_elements = 0;
    // Stable name of the value this action moves.  Used to label receive-history mismatches.
    std::string tag;
    // Collective root, for Bcast/Gather/Scatter/Reduce.
    FMI::Utils::peer_num root = 0;
    // The result this action is expected to observe on the rank that records it.  Purely
    // documentary for the auto-classifier (which compares against the no-migration baseline), but
    // it is the contract an author states and the harness reports on mismatch.
    std::vector<int> expected;
    // Reduction function identity, for Reduce/Allreduce/Scan.
    ReduceFunction function;
};

struct Phase {
    std::vector<std::vector<Action>> rank_actions;
};

struct Scenario {
    // Stable, unique identifier.  Never renamed: it is the regression key.
    std::string id;
    // One sentence stating the property the scenario asserts.
    std::string property;
    Backend backend;
    ScenarioKind kind;
    FMI::Utils::peer_num world;
    // Ranks the migration request targets.
    std::vector<FMI::Utils::peer_num> targets;
    // Where the migration request is staged.
    Trigger trigger;
    // The per-phase, per-rank program.  Empty for the specialized ScenarioKinds.
    std::vector<Phase> phases;
    // What today's protocol does.  This value drives the reported classification when the
    // migrated receive history differs from the baseline.
    Classification expected_v1;
    // The contracted end state per docs/superpowers/specs/2026-07-27-sequenced-incarnation-links-design.md.
    Classification expected_v2;
    std::chrono::milliseconds deadline;
    // Which fault_tolerance.state_transfer config the executor loads.
    StateTransfer state_transfer = StateTransfer::Criu;
    // Corpus axis.  Filled in automatically by the registry; "core" for the built-in catalog.
    std::string axis = "core";
};

struct Observation {
    Classification classification = Classification::SetupError;
    std::string detail;
    std::vector<std::vector<int>> received_payloads;
    bool baseline_valid = false;
    bool promoted = false;
};

struct ModelCounterexample {
    // A payload-normalized, deterministic per-rank schedule.  The explorer's
    // ordering key is (classification, backend, normalized_trace).
    std::string normalized_trace;
    Backend backend;
    Classification classification;
    std::string matching_scenario_id;
};

const std::vector<Scenario>& scenarios();
const Scenario* find_scenario(const std::string& id);
std::string to_string(Backend value);
std::string to_string(Classification value);
std::string to_string(ActionKind value);
std::string to_string(TriggerKind value);
std::string to_string(StateTransfer value);
std::vector<ModelCounterexample> explore_bounded_model();

// True when the current protocol can stage this trigger.  Today only TriggerKind::BeforePhase
// qualifies; every other trigger is reported as Classification::NotYetDrivable instead of being
// silently downgraded to a phase boundary.
bool is_drivable(const Trigger& trigger);

// True when the rank that executes this action records a value into the receive history.  The
// auto-classifier compares those histories between the baseline and the migrated run.
bool records_result(const Action& action, FMI::Utils::peer_num rank);

// Element-wise application of a reduction kernel.  Shared by the executor and by corpus authors
// computing their expected results.
std::vector<int> apply_reduce_op(ReduceOp op, const std::vector<int>& left,
                                 const std::vector<int>& right);

// Empty means the catalog has unique, non-empty identifiers.  The runner maps a
// non-empty error to Classification::SetupError before running or listing cases.
const std::string& catalog_error();

} // namespace FMI::Tests::MigrationCounterexamples
