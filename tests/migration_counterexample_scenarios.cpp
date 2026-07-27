#include "migration_counterexample_registry.h"
#include "migration_counterexample_scenarios.h"

#include <algorithm>
#include <numeric>
#include <set>
#include <utility>

namespace FMI::Tests::MigrationCounterexamples {
namespace {

// ---------------------------------------------------------------------------
// The built-in catalog.  These sixteen scenarios are the regression baseline: their identifiers
// and their expected_v1 classifications are frozen.
// ---------------------------------------------------------------------------

std::vector<Scenario> make_scenarios() {
    return {
        {
            "queued_message_lost",
            "an unmatched completed send remains receivable across the migration cut",
            Backend::Redis, ScenarioKind::Program, 2, {1}, before_phase(1),
            {phase({{send(1, {101}, "old")}, {send(0, {901}, "cut-align")}}),
             phase({{barrier()}, {barrier(), receive(0, 1, "old")}})},
            Classification::LostMessage, Classification::Preserved, redis_deadline
        },
        {
            "queued_message_substituted",
            "a post-cut send must not occupy the receive slot of an old queued message",
            Backend::Redis, ScenarioKind::Program, 2, {1}, before_phase(1),
            {phase({{send(1, {101}, "old")}, {send(0, {901}, "cut-align")}}),
             phase({{send(1, {303}, "new")}, {receive(0, 1, "old")}})},
            Classification::WrongPayload, Classification::Preserved, redis_deadline
        },
        {
            "partially_drained_fifo_suffix",
            "draining a FIFO prefix must preserve the old undrained suffix",
            Backend::Redis, ScenarioKind::Program, 2, {1}, before_phase(1),
            {phase({{send(1, {101}, "first"), send(1, {202}, "second")},
                    {receive(0, 1, "first"), send(0, {901}, "cut-align")}}),
             phase({{barrier()}, {barrier(), receive(0, 1, "second")}})},
            Classification::LostMessage, Classification::Preserved, redis_deadline
        },
        {
            "multi_message_backlog_shift",
            "a queued FIFO stream must retain every old message in order after migration",
            Backend::Redis, ScenarioKind::Program, 2, {1}, before_phase(1),
            {phase({{send(1, {101}, "first"), send(1, {202}, "second"),
                     send(1, {303}, "third")},
                    {send(0, {801}, "cut-align-1"), send(0, {802}, "cut-align-2"),
                     send(0, {803}, "cut-align-3")}}),
             phase({{send(1, {404}, "fourth"), send(1, {505}, "fifth"),
                     send(1, {606}, "sixth")},
                    {receive(0, 1, "first"), receive(0, 1, "second"),
                     receive(0, 1, "third")}})},
            Classification::ReorderedOrDuplicated, Classification::Preserved, redis_deadline
        },
        {
            "variable_size_partial_overwrite",
            "a successful receive must never partially overwrite an old variable-size payload",
            Backend::Redis, ScenarioKind::Program, 2, {1}, before_phase(1),
            {phase({{send(1, {101, 102, 103}, "wide-old")},
                    {send(0, {901}, "cut-align")}}),
             phase({{send(1, {404}, "narrow-new")}, {receive(0, 3, "wide-old")}})},
            Classification::PartialPayload, Classification::Preserved, redis_deadline
        },
        {
            "bidirectional_backlog",
            "simultaneous queued sends in both directions survive the same cut",
            Backend::Redis, ScenarioKind::Program, 2, {1}, before_phase(1),
            {phase({{send(1, {101}, "zero-to-one")}, {send(0, {202}, "one-to-zero")}}),
             phase({{receive(1, 1, "one-to-zero")}, {receive(0, 1, "zero-to-one")}})},
            Classification::LostMessage, Classification::Preserved, redis_deadline
        },
        {
            "three_rank_ring_backlog",
            "equal rank progress does not permit dropping a three-rank ring backlog",
            Backend::Redis, ScenarioKind::Program, 3, {1}, before_phase(1),
            {phase({{send(1, {101}, "zero-to-one")}, {send(2, {202}, "one-to-two")},
                    {send(0, {303}, "two-to-zero")}}),
             phase({{receive(2, 1, "two-to-zero")}, {receive(0, 1, "zero-to-one")},
                    {receive(1, 1, "one-to-two")}})},
            Classification::LostMessage, Classification::Preserved, redis_deadline
        },
        {
            "unrelated_rank_migration",
            "migration of an unrelated rank must preserve survivor-to-survivor queued data",
            Backend::Redis, ScenarioKind::Program, 3, {2}, before_phase(1),
            {phase({{send(1, {101}, "survivor-link")},
                    {send(0, {901}, "cut-align-1")},
                    {send(0, {902}, "cut-align-2")}}),
             phase({{barrier()}, {barrier(), receive(0, 1, "survivor-link")},
                    {barrier()}})},
            Classification::LostMessage, Classification::Preserved, redis_deadline
        },
        {
            "future_matching_send_after_cut",
            "a receive below the cut cannot rely on a matching send beyond the cut",
            Backend::Redis, ScenarioKind::FutureSendAfterCut, 2, {1}, before_phase(0), {},
            Classification::OperationStuck, Classification::Preserved, redis_deadline
        },
        {
            "direct_moved_link_backlog",
            "moving a Direct endpoint must not discard unread bytes on its established link",
            Backend::Direct, ScenarioKind::Program, 2, {1}, before_phase(2),
            {phase({{send(1, {1}, "warmup")}, {receive(0, 1, "warmup")}}),
             phase({{send(1, {101}, "old")}, {send(0, {901}, "cut-align")}}),
             phase({{send(1, {303}, "new")}, {receive(0, 1, "old")}})},
            Classification::WrongPayload, Classification::Preserved, direct_deadline
        },
        {
            "direct_survivor_link_crosses_epoch",
            "an old Direct survivor link must not deliver epoch-zero bytes after promotion",
            Backend::Direct, ScenarioKind::Program, 3, {2}, before_phase(2),
            {phase({{send(1, {1}, "warmup-0-1"), receive(2, 1, "warmup-2-0")},
                    {receive(0, 1, "warmup-0-1"), send(2, {2}, "warmup-1-2")},
                    {receive(1, 1, "warmup-1-2"), send(0, {3}, "warmup-2-0")}}),
             phase({{send(1, {101}, "old-survivor-link")},
                    {send(2, {201}, "queued-1-2")},
                    {send(0, {301}, "queued-2-0")}}),
             phase({{send(1, {303}, "new-survivor-link"),
                     send(2, {404}, "current-moved-link")},
                    {receive(0, 1, "old-survivor-link")},
                    {receive(0, 1, "current-moved-link")}})},
            Classification::ReorderedOrDuplicated, Classification::Preserved, direct_deadline
        },
        {
            "stale_barrier_object",
            "an old barrier object must not satisfy a collective in the new epoch",
            Backend::Redis, ScenarioKind::StaleBarrierObject, 2, {1}, before_phase(0), {},
            Classification::PrematureCollective, Classification::Preserved, redis_deadline,
            StateTransfer::None
        },
        {
            "pending_set_expands_after_park",
            "a rank added to a pending set after parking must still be able to quiesce",
            Backend::Redis, ScenarioKind::PendingSetExpansion, 2, {0}, before_phase(0), {},
            Classification::PromotionStuck, Classification::Preserved, redis_deadline
        },
        {
            "failed_operation_advances_boundary",
            "a failed operation must not advance the migration operation boundary",
            Backend::Redis, ScenarioKind::FailedOperationBoundary, 2, {1}, before_phase(0), {},
            Classification::OperationStuck, Classification::Preserved, redis_deadline
        },
        {
            "promotion_before_full_membership",
            "promotion requires the configured world to be present in the new epoch",
            Backend::Redis, ScenarioKind::IncompleteMembership, 3, {1}, before_phase(0), {},
            Classification::PrematureCollective, Classification::Preserved, redis_deadline
        },
        {
            "single_rank_self_send",
            "a self-send queued before the cut remains receivable after promotion",
            Backend::Redis, ScenarioKind::Program, 1, {0}, before_phase(1),
            {phase({{send(0, {101}, "self-old")}}),
             phase({{receive(0, 1, "self-old")}})},
            Classification::LostMessage, Classification::Preserved, redis_deadline
        },
    };
}

// ---------------------------------------------------------------------------
// Registry
// ---------------------------------------------------------------------------

struct RegistryEntry {
    std::string axis;
    ScenarioFactory factory = nullptr;
    std::size_t order = 0;
};

std::vector<RegistryEntry>& registry() {
    static std::vector<RegistryEntry> entries;
    return entries;
}

bool& registry_frozen() {
    static bool frozen = false;
    return frozen;
}

std::string& registry_error() {
    static std::string error;
    return error;
}

std::vector<Scenario> build_catalog() {
    registry_frozen() = true;
    std::vector<Scenario> catalog = make_scenarios();
    for (auto& scenario : catalog) {
        scenario.axis = "core";
    }

    // Deterministic ordering independent of link order: axis name, then registration order
    // within an axis, then the factory's own order.
    auto entries = registry();
    std::stable_sort(entries.begin(), entries.end(),
                     [](const RegistryEntry& left, const RegistryEntry& right) {
                         if (left.axis != right.axis) return left.axis < right.axis;
                         return left.order < right.order;
                     });
    for (const auto& entry : entries) {
        if (entry.factory == nullptr) continue;
        auto batch = entry.factory();
        for (auto& scenario : batch) {
            scenario.axis = entry.axis;
            catalog.push_back(std::move(scenario));
        }
    }
    return catalog;
}

const std::string& validate_catalog(const std::vector<Scenario>& catalog) {
    static const std::string error = [&catalog] {
        if (!registry_error().empty()) return registry_error();
        std::set<std::string> ids;
        for (const auto& scenario : catalog) {
            if (scenario.id.empty()) {
                return std::string{"scenario catalog contains an empty identifier"};
            }
            if (!ids.insert(scenario.id).second) {
                return std::string{"duplicate scenario identifier: "} + scenario.id;
            }
            if (scenario.axis.empty()) {
                return std::string{"scenario has an empty axis: "} + scenario.id;
            }
        }
        return std::string{};
    }();
    return error;
}

} // namespace

ScenarioRegistrar::ScenarioRegistrar(const char* axis, ScenarioFactory factory) {
    if (axis == nullptr || *axis == '\0' || factory == nullptr) {
        registry_error() = "a corpus translation unit registered with an empty axis or factory";
        return;
    }
    if (registry_frozen()) {
        registry_error() = std::string("corpus axis registered after the catalog was built: ") +
                           axis;
        return;
    }
    registry().push_back({axis, factory, registry().size()});
}

const std::vector<Scenario>& scenarios() {
    static const std::vector<Scenario> catalog = build_catalog();
    return catalog;
}

const Scenario* find_scenario(const std::string& id) {
    for (const auto& scenario : scenarios()) {
        if (scenario.id == id) {
            return &scenario;
        }
    }
    return nullptr;
}

const std::string& catalog_error() {
    return validate_catalog(scenarios());
}

bool is_drivable(const Trigger& trigger) {
    // The current protocol only accepts a migration request that is observed at an operation
    // guard boundary, which the executor can only stage between two phases.
    return trigger.kind == TriggerKind::BeforePhase;
}

bool records_result(const Action& action, FMI::Utils::peer_num rank) {
    switch (action.kind) {
    case ActionKind::Receive:
    case ActionKind::Bcast:
    case ActionKind::Scatter:
    case ActionKind::Allreduce:
    case ActionKind::Scan:
        return true;
    case ActionKind::Gather:
    case ActionKind::Reduce:
        return rank == action.root;
    case ActionKind::Send:
    case ActionKind::Barrier:
    case ActionKind::Synchronize:
        return false;
    }
    return false;
}

std::vector<int> apply_reduce_op(ReduceOp op, const std::vector<int>& left,
                                 const std::vector<int>& right) {
    std::vector<int> result = left;
    if (op == ReduceOp::ConcatLeftToRight) {
        result.insert(result.end(), right.begin(), right.end());
        return result;
    }
    const auto count = std::min(result.size(), right.size());
    for (std::size_t index = 0; index < count; ++index) {
        switch (op) {
        case ReduceOp::Sum:
            result[index] += right[index];
            break;
        case ReduceOp::Product:
            result[index] *= right[index];
            break;
        case ReduceOp::Max:
            result[index] = std::max(result[index], right[index]);
            break;
        case ReduceOp::Min:
            result[index] = std::min(result[index], right[index]);
            break;
        case ReduceOp::ConcatLeftToRight:
            break;
        }
    }
    return result;
}

std::string to_string(Backend value) {
    switch (value) {
    case Backend::Redis:
        return "redis";
    case Backend::Direct:
        return "direct";
    case Backend::S3:
        return "s3";
    }
    return "unknown";
}

std::string to_string(Classification value) {
    switch (value) {
    case Classification::Preserved:
        return "preserved";
    case Classification::WrongPayload:
        return "wrong_payload";
    case Classification::LostMessage:
        return "lost_message";
    case Classification::ReorderedOrDuplicated:
        return "reordered_or_duplicated";
    case Classification::PartialPayload:
        return "partial_payload";
    case Classification::PrematureCollective:
        return "premature_collective";
    case Classification::PromotionStuck:
        return "promotion_stuck";
    case Classification::OperationStuck:
        return "operation_stuck";
    case Classification::SetupError:
        return "setup_error";
    case Classification::InfrastructureSkip:
        return "infrastructure_skip";
    case Classification::LoudFail:
        return "loud_fail";
    case Classification::Unsupported:
        return "unsupported";
    case Classification::NotYetDrivable:
        return "not_yet_drivable";
    }
    return "unknown";
}

std::string to_string(ActionKind value) {
    switch (value) {
    case ActionKind::Send:
        return "send";
    case ActionKind::Receive:
        return "receive";
    case ActionKind::Barrier:
        return "barrier";
    case ActionKind::Synchronize:
        return "synchronize";
    case ActionKind::Bcast:
        return "bcast";
    case ActionKind::Gather:
        return "gather";
    case ActionKind::Scatter:
        return "scatter";
    case ActionKind::Reduce:
        return "reduce";
    case ActionKind::Allreduce:
        return "allreduce";
    case ActionKind::Scan:
        return "scan";
    }
    return "unknown";
}

std::string to_string(TriggerKind value) {
    switch (value) {
    case TriggerKind::BeforePhase:
        return "before_phase";
    case TriggerKind::MidOperation:
        return "mid_operation";
    case TriggerKind::MidCompute:
        return "mid_compute";
    case TriggerKind::MidBlockingRecv:
        return "mid_blocking_recv";
    case TriggerKind::MidPairing:
        return "mid_pairing";
    case TriggerKind::AfterCompletedSend:
        return "after_completed_send";
    }
    return "unknown";
}

std::string to_string(StateTransfer value) {
    switch (value) {
    case StateTransfer::Criu:
        return "criu";
    case StateTransfer::None:
        return "none";
    }
    return "unknown";
}

} // namespace FMI::Tests::MigrationCounterexamples
