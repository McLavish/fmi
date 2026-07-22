#pragma once

#include "utils/Common.h"

#include <chrono>
#include <cstddef>
#include <string>
#include <vector>

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

struct ModelCounterexample {
    std::string normalized_trace;
    Backend backend;
    Classification classification;
    std::string matching_scenario_id;
};

const std::vector<Scenario>& scenarios();
const Scenario* find_scenario(const std::string& id);
std::string to_string(Backend value);
std::string to_string(Classification value);
std::vector<ModelCounterexample> explore_bounded_model();

// Empty means the catalog has unique, non-empty identifiers.  The runner maps a
// non-empty error to Classification::SetupError before running or listing cases.
const std::string& catalog_error();

} // namespace FMI::Tests::MigrationCounterexamples
