#include "migration_counterexample_scenarios.h"

#include <set>
#include <utility>

namespace FMI::Tests::MigrationCounterexamples {
namespace {

Action send(FMI::Utils::peer_num peer, std::vector<int> payload, std::string tag) {
    return {ActionKind::Send, peer, std::move(payload), 0, std::move(tag)};
}

Action receive(FMI::Utils::peer_num peer, std::size_t elements, std::string tag) {
    return {ActionKind::Receive, peer, {}, elements, std::move(tag)};
}

Action synchronize() {
    return {ActionKind::Synchronize};
}

Action barrier() {
    return {ActionKind::Barrier};
}

Phase phase(std::initializer_list<std::vector<Action>> rank_actions) {
    return {rank_actions};
}

// Redis operations have a configured five-second backend timeout.  The subprocess
// deadline must leave enough room for that timeout to be reported and serialized.
constexpr auto redis_deadline = std::chrono::seconds(8);
constexpr auto direct_deadline = std::chrono::seconds(10);

std::vector<Scenario> make_scenarios() {
    return {
        {
            "queued_message_lost",
            "an unmatched completed send remains receivable across the migration cut",
            Backend::Redis, ScenarioKind::Program, 2, {1}, 1,
            {phase({{send(1, {101}, "old")}, {send(0, {901}, "cut-align")}}),
             phase({{barrier()}, {barrier(), receive(0, 1, "old")}})},
            Classification::LostMessage, redis_deadline
        },
        {
            "queued_message_substituted",
            "a post-cut send must not occupy the receive slot of an old queued message",
            Backend::Redis, ScenarioKind::Program, 2, {1}, 1,
            {phase({{send(1, {101}, "old")}, {send(0, {901}, "cut-align")}}),
             phase({{send(1, {303}, "new")}, {receive(0, 1, "old")}})},
            Classification::WrongPayload, redis_deadline
        },
        {
            "partially_drained_fifo_suffix",
            "draining a FIFO prefix must preserve the old undrained suffix",
            Backend::Redis, ScenarioKind::Program, 2, {1}, 1,
            {phase({{send(1, {101}, "first"), send(1, {202}, "second")},
                    {receive(0, 1, "first"), send(0, {901}, "cut-align")}}),
             phase({{barrier()}, {barrier(), receive(0, 1, "second")}})},
            Classification::LostMessage, redis_deadline
        },
        {
            "multi_message_backlog_shift",
            "a queued FIFO stream must retain every old message in order after migration",
            Backend::Redis, ScenarioKind::Program, 2, {1}, 1,
            {phase({{send(1, {101}, "first"), send(1, {202}, "second"),
                     send(1, {303}, "third")},
                    {send(0, {801}, "cut-align-1"), send(0, {802}, "cut-align-2"),
                     send(0, {803}, "cut-align-3")}}),
             phase({{send(1, {404}, "fourth"), send(1, {505}, "fifth"),
                     send(1, {606}, "sixth")},
                    {receive(0, 1, "first"), receive(0, 1, "second"),
                     receive(0, 1, "third")}})},
            Classification::ReorderedOrDuplicated, redis_deadline
        },
        {
            "variable_size_partial_overwrite",
            "a successful receive must never partially overwrite an old variable-size payload",
            Backend::Redis, ScenarioKind::Program, 2, {1}, 1,
            {phase({{send(1, {101, 102, 103}, "wide-old")},
                    {send(0, {901}, "cut-align")}}),
             phase({{send(1, {404}, "narrow-new")}, {receive(0, 3, "wide-old")}})},
            Classification::PartialPayload, redis_deadline
        },
        {
            "bidirectional_backlog",
            "simultaneous queued sends in both directions survive the same cut",
            Backend::Redis, ScenarioKind::Program, 2, {1}, 1,
            {phase({{send(1, {101}, "zero-to-one")}, {send(0, {202}, "one-to-zero")}}),
             phase({{receive(1, 1, "one-to-zero")}, {receive(0, 1, "zero-to-one")}})},
            Classification::LostMessage, redis_deadline
        },
        {
            "three_rank_ring_backlog",
            "equal rank progress does not permit dropping a three-rank ring backlog",
            Backend::Redis, ScenarioKind::Program, 3, {1}, 1,
            {phase({{send(1, {101}, "zero-to-one")}, {send(2, {202}, "one-to-two")},
                    {send(0, {303}, "two-to-zero")}}),
             phase({{receive(2, 1, "two-to-zero")}, {receive(0, 1, "zero-to-one")},
                    {receive(1, 1, "one-to-two")}})},
            Classification::LostMessage, redis_deadline
        },
        {
            "unrelated_rank_migration",
            "migration of an unrelated rank must preserve survivor-to-survivor queued data",
            Backend::Redis, ScenarioKind::Program, 3, {2}, 1,
            {phase({{send(1, {101}, "survivor-link")},
                    {send(0, {901}, "cut-align-1")},
                    {send(0, {902}, "cut-align-2")}}),
             phase({{barrier()}, {barrier(), receive(0, 1, "survivor-link")},
                    {barrier()}})},
            Classification::LostMessage, redis_deadline
        },
        {
            "future_matching_send_after_cut",
            "a receive below the cut cannot rely on a matching send beyond the cut",
            Backend::Redis, ScenarioKind::FutureSendAfterCut, 2, {1}, 0, {},
            Classification::OperationStuck, redis_deadline
        },
        {
            "direct_moved_link_backlog",
            "moving a Direct endpoint must not discard unread bytes on its established link",
            Backend::Direct, ScenarioKind::Program, 2, {1}, 1,
            {phase({{send(1, {1}, "warmup")}, {receive(0, 1, "warmup")}}),
             phase({{send(1, {101}, "old")}, {synchronize()}}),
             phase({{send(1, {303}, "new")}, {receive(0, 1, "old")}})},
            Classification::LostMessage, direct_deadline
        },
        {
            "direct_survivor_link_crosses_epoch",
            "an old Direct survivor link must not deliver epoch-zero bytes after promotion",
            Backend::Direct, ScenarioKind::Program, 3, {2}, 1,
            {phase({{send(1, {1}, "warmup")}, {receive(0, 1, "warmup")}, {synchronize()}}),
             phase({{send(1, {101}, "old-survivor-link")}, {synchronize()}, {synchronize()}}),
             phase({{synchronize()}, {receive(0, 1, "old-survivor-link")}, {synchronize()}})},
            Classification::ReorderedOrDuplicated, direct_deadline
        },
        {
            "stale_barrier_object",
            "an old barrier object must not satisfy a collective in the new epoch",
            Backend::Redis, ScenarioKind::StaleBarrierObject, 2, {1}, 0, {},
            Classification::PrematureCollective, redis_deadline
        },
        {
            "pending_set_expands_after_park",
            "a rank added to a pending set after parking must still be able to quiesce",
            Backend::Redis, ScenarioKind::PendingSetExpansion, 2, {0}, 0, {},
            Classification::PromotionStuck, redis_deadline
        },
        {
            "failed_operation_advances_boundary",
            "a failed operation must not advance the migration operation boundary",
            Backend::Redis, ScenarioKind::FailedOperationBoundary, 2, {1}, 0, {},
            Classification::WrongPayload, redis_deadline
        },
        {
            "promotion_before_full_membership",
            "promotion requires the configured world to be present in the new epoch",
            Backend::Redis, ScenarioKind::IncompleteMembership, 3, {1}, 0, {},
            Classification::PrematureCollective, redis_deadline
        },
        {
            "single_rank_self_send",
            "a self-send queued before the cut remains receivable after promotion",
            Backend::Redis, ScenarioKind::Program, 1, {0}, 1,
            {phase({{send(0, {101}, "self-old")}}),
             phase({{receive(0, 1, "self-old")}})},
            Classification::LostMessage, redis_deadline
        },
    };
}

const std::string& validate_catalog(const std::vector<Scenario>& catalog) {
    static const std::string error = [&catalog] {
        std::set<std::string> ids;
        for (const auto& scenario : catalog) {
            if (scenario.id.empty()) {
                return std::string{"scenario catalog contains an empty identifier"};
            }
            if (!ids.insert(scenario.id).second) {
                return std::string{"duplicate scenario identifier: "} + scenario.id;
            }
        }
        return std::string{};
    }();
    return error;
}

} // namespace

const std::vector<Scenario>& scenarios() {
    static const std::vector<Scenario> catalog = make_scenarios();
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

std::string to_string(Backend value) {
    switch (value) {
    case Backend::Redis:
        return "redis";
    case Backend::Direct:
        return "direct";
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
    }
    return "unknown";
}

} // namespace FMI::Tests::MigrationCounterexamples
