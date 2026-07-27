#include "migration_counterexample_scenarios.h"

#include <algorithm>
#include <cstdint>
#include <deque>
#include <map>
#include <optional>
#include <set>
#include <sstream>
#include <tuple>
#include <utility>

namespace FMI::Tests::MigrationCounterexamples {
namespace {

using Rank = FMI::Utils::peer_num;
using Edge = std::pair<Rank, Rank>;

struct Message {
    std::string tag;
    std::uint64_t epoch = 0;
};

struct Delivery {
    std::string expected;
    std::string actual;
    std::uint64_t send_epoch = 0;

    bool operator==(const Delivery& other) const {
        return std::tie(expected, actual, send_epoch) ==
               std::tie(other.expected, other.actual, other.send_epoch);
    }
};

struct Program {
    Backend backend = Backend::Redis;
    Rank world = 0;
    Rank target = 0;
    Rank first_observer = 0;
    std::vector<std::vector<Action>> ranks;
    // Rank scheduling attempts made before the orchestrator requests migration.
    // A blocked receive remains in this sequence but does not advance its rank PC.
    std::vector<Rank> request_prefix;
};

struct ModelState {
    std::vector<std::size_t> pcs;
    std::vector<std::size_t> completed_boundaries;
    std::vector<int> published_boundaries;
    std::uint64_t active_epoch = 0;
    std::optional<std::size_t> fixed_cut;
    std::set<Rank> pending;
    std::set<Rank> parked;
    std::set<Rank> quiesced;
    std::map<Edge, std::deque<Message>> queues;
    std::vector<std::vector<Delivery>> deliveries;
    std::size_t completed_sends = 0;
    std::size_t queued_at_promotion = 0;
    std::size_t dropped_at_promotion = 0;
    bool promoted = false;
    bool crossed_epoch = false;
};

enum class StepResult { Progress, Blocked, Parked };

struct RunResult {
    ModelState state;
    Classification failure = Classification::Preserved;
    bool valid_request = false;
    bool finished = false;
};

struct Evaluation {
    Classification classification = Classification::Preserved;
    bool valid = false;
    std::size_t queued_at_promotion = 0;
    std::size_t dropped_at_promotion = 0;
    bool crossed_epoch = false;
    bool wrong_payload = false;
};

Action send(Rank peer, std::string tag) {
    return {ActionKind::Send, peer, {1}, 0, std::move(tag)};
}

Action receive(Rank peer, std::string tag) {
    return {ActionKind::Receive, peer, {}, 1, std::move(tag)};
}

Action synchronize() {
    return {ActionKind::Synchronize};
}

ModelState initial_state(Rank world) {
    ModelState state;
    state.pcs.resize(world);
    state.completed_boundaries.resize(world);
    state.published_boundaries.assign(world, -1);
    state.deliveries.resize(world);
    return state;
}

std::string state_key(const ModelState& state) {
    std::ostringstream output;
    output << state.active_epoch << ':';
    if (state.fixed_cut) {
        output << *state.fixed_cut;
    } else {
        output << '-';
    }
    output << '|';
    for (std::size_t rank = 0; rank < state.pcs.size(); ++rank) {
        output << state.pcs[rank] << ',' << state.completed_boundaries[rank] << ','
               << state.published_boundaries[rank] << ';';
    }
    output << '|';
    for (const auto rank : state.pending) output << 'n' << rank;
    for (const auto rank : state.parked) output << 'p' << rank;
    for (const auto rank : state.quiesced) output << 'q' << rank;
    output << '|';
    for (const auto& [edge, queue] : state.queues) {
        output << edge.first << '>' << edge.second << ':';
        for (const auto& message : queue) {
            output << message.tag << '@' << message.epoch << ',';
        }
        output << ';';
    }
    return output.str();
}

void observe_boundary(ModelState& state, Rank rank) {
    state.published_boundaries[rank] =
        static_cast<int>(state.completed_boundaries[rank]);
    if (state.pending.empty() || state.fixed_cut) {
        return;
    }

    const auto proposal = state.completed_boundaries[rank];
    auto cut = proposal;
    for (Rank other = 0; other < state.published_boundaries.size(); ++other) {
        if (other == rank || state.published_boundaries[other] < 0) {
            continue;
        }
        const auto published = static_cast<std::size_t>(state.published_boundaries[other]);
        if (published >= proposal) {
            cut = std::max(cut, published + 1);
        }
    }
    state.fixed_cut = cut;
}

StepResult step_rank(const Program& program, ModelState& state, Rank rank) {
    if (state.parked.count(rank) != 0 || state.quiesced.count(rank) != 0) {
        return StepResult::Parked;
    }

    observe_boundary(state, rank);
    if (state.fixed_cut && state.completed_boundaries[rank] >= *state.fixed_cut) {
        if (state.pending.count(rank) != 0) {
            state.quiesced.insert(rank);
        } else {
            state.parked.insert(rank);
        }
        return StepResult::Parked;
    }
    if (state.pcs[rank] >= program.ranks[rank].size()) {
        return StepResult::Blocked;
    }

    const auto& action = program.ranks[rank][state.pcs[rank]];
    if (action.kind == ActionKind::Receive) {
        auto& queue = state.queues[{action.peer, rank}];
        if (queue.empty()) {
            return StepResult::Blocked;
        }
        const auto message = queue.front();
        queue.pop_front();
        state.deliveries[rank].push_back({action.tag, message.tag, message.epoch});
        if (state.active_epoch != message.epoch) {
            state.crossed_epoch = true;
        }
    } else if (action.kind == ActionKind::Send) {
        state.queues[{rank, action.peer}].push_back({action.tag, state.active_epoch});
        ++state.completed_sends;
    }

    ++state.pcs[rank];
    ++state.completed_boundaries[rank];
    return StepResult::Progress;
}

bool promotion_gate_open(const Program& program, const ModelState& state) {
    if (!state.fixed_cut || state.quiesced.count(program.target) == 0) {
        return false;
    }
    return std::all_of(state.completed_boundaries.begin(),
                       state.completed_boundaries.end(), [&](std::size_t boundary) {
                           return boundary >= *state.fixed_cut;
                       });
}

void promote(const Program& program, ModelState& state, bool preserve_queues) {
    for (const auto& [edge, queue] : state.queues) {
        (void)edge;
        state.queued_at_promotion += queue.size();
    }

    if (!preserve_queues) {
        if (program.backend == Backend::Redis) {
            state.dropped_at_promotion = state.queued_at_promotion;
            state.queues.clear();
        } else {
            for (auto iterator = state.queues.begin(); iterator != state.queues.end();) {
                if (iterator->first.first == program.target ||
                    iterator->first.second == program.target) {
                    state.dropped_at_promotion += iterator->second.size();
                    iterator = state.queues.erase(iterator);
                } else {
                    ++iterator;
                }
            }
        }
    }

    ++state.active_epoch;
    state.fixed_cut.reset();
    state.pending.clear();
    state.parked.clear();
    state.quiesced.clear();
    state.promoted = true;
}

bool all_finished(const Program& program, const ModelState& state) {
    for (Rank rank = 0; rank < program.world; ++rank) {
        if (state.pcs[rank] != program.ranks[rank].size()) {
            return false;
        }
    }
    return true;
}

ModelState state_before_request(const Program& program) {
    auto state = initial_state(program.world);
    for (const auto rank : program.request_prefix) {
        if (rank < program.world) step_rank(program, state, rank);
    }
    return state;
}

RunResult run_program(const Program& program, bool preserve_queues) {
    RunResult result;
    if (std::any_of(program.request_prefix.begin(), program.request_prefix.end(),
                    [&](Rank rank) { return rank >= program.world; })) return result;
    result.state = state_before_request(program);
    if (result.state.completed_sends == 0) {
        return result;
    }
    result.valid_request = true;
    result.state.pending.insert(program.target);

    // Explore every first-observer choice by putting that rank first in each round.
    std::vector<Rank> order;
    order.reserve(program.world);
    order.push_back(program.first_observer);
    for (Rank rank = 0; rank < program.world; ++rank) {
        if (rank != program.first_observer) order.push_back(rank);
    }

    std::set<std::string> visited;
    while (!result.state.promoted) {
        if (!visited.insert(state_key(result.state)).second) {
            bool blocked_receive = false;
            if (result.state.fixed_cut) {
                for (Rank rank = 0; rank < program.world; ++rank) {
                    if (result.state.completed_boundaries[rank] >= *result.state.fixed_cut ||
                        result.state.pcs[rank] >= program.ranks[rank].size()) {
                        continue;
                    }
                    const auto& action = program.ranks[rank][result.state.pcs[rank]];
                    if (action.kind == ActionKind::Receive &&
                        result.state.queues[{action.peer, rank}].empty()) {
                        blocked_receive = true;
                    }
                }
            }
            result.failure = blocked_receive ? Classification::OperationStuck
                                             : Classification::PromotionStuck;
            return result;
        }

        bool progressed = false;
        for (const auto rank : order) {
            const auto before = state_key(result.state);
            step_rank(program, result.state, rank);
            progressed = progressed || before != state_key(result.state);
        }
        if (promotion_gate_open(program, result.state)) {
            promote(program, result.state, preserve_queues);
            break;
        }
        if (!progressed) {
            continue; // The repeated-state check classifies the precise stuck condition.
        }
    }

    visited.clear();
    while (!all_finished(program, result.state)) {
        if (!visited.insert(state_key(result.state)).second) {
            return result;
        }
        for (Rank rank = 0; rank < program.world; ++rank) {
            step_rank(program, result.state, rank);
        }
    }
    result.finished = true;
    return result;
}

bool baseline_finishes(const Program& program) {
    auto state = initial_state(program.world);
    std::set<std::string> visited;
    while (!all_finished(program, state)) {
        if (!visited.insert(state_key(state)).second) return false;
        for (Rank rank = 0; rank < program.world; ++rank) {
            step_rank(program, state, rank);
        }
    }
    return true;
}

Evaluation evaluate(const Program& program) {
    const auto reference = run_program(program, true);
    const auto current = run_program(program, false);
    Evaluation result;
    result.valid = baseline_finishes(program) && reference.valid_request && current.valid_request;
    if (!result.valid) return result;

    result.queued_at_promotion = current.state.queued_at_promotion;
    result.dropped_at_promotion = current.state.dropped_at_promotion;
    result.crossed_epoch = current.state.crossed_epoch;
    if (current.failure != Classification::Preserved) {
        result.classification = current.failure;
        return result;
    }
    if (program.backend == Backend::Direct && current.state.crossed_epoch) {
        result.classification = Classification::ReorderedOrDuplicated;
        return result;
    }

    bool delivery_mismatch = false;
    std::size_t reference_deliveries = 0;
    for (Rank rank = 0; rank < program.world; ++rank) {
        reference_deliveries += reference.state.deliveries[rank].size();
        delivery_mismatch = delivery_mismatch ||
                            reference.state.deliveries[rank] != current.state.deliveries[rank];
    }
    if (!delivery_mismatch && reference.finished == current.finished) {
        return result;
    }

    bool wrong_payload = false;
    for (const auto& deliveries : current.state.deliveries) {
        for (const auto& delivery : deliveries) {
            wrong_payload = wrong_payload || delivery.expected != delivery.actual;
        }
    }
    result.wrong_payload = wrong_payload;
    if (current.state.queued_at_promotion > 1 && reference_deliveries > 1 &&
        wrong_payload) {
        result.classification = Classification::ReorderedOrDuplicated;
    } else if (wrong_payload) {
        result.classification = Classification::WrongPayload;
    } else {
        result.classification = Classification::LostMessage;
    }
    return result;
}

std::string matching_id(const Program& program, const Evaluation& result) {
    if (result.classification == Classification::OperationStuck) {
        return "future_matching_send_after_cut";
    }
    if (program.backend == Backend::Direct) {
        if (result.crossed_epoch) return "direct_survivor_link_crosses_epoch";
        if (result.dropped_at_promotion != 0) return "direct_moved_link_backlog";
    }
    if (result.classification == Classification::ReorderedOrDuplicated &&
        result.queued_at_promotion > 1 && result.wrong_payload) {
        return "multi_message_backlog_shift";
    }
    if (result.classification == Classification::WrongPayload) {
        return "queued_message_substituted";
    }
    if (result.classification == Classification::LostMessage) {
        return "queued_message_lost";
    }
    return {};
}

Program minimize(Program program, Classification classification, const std::string& id) {
    bool changed = true;
    while (changed) {
        changed = false;
        for (Rank rank = 0; rank < program.world && !changed; ++rank) {
            for (std::size_t index = program.ranks[rank].size(); index-- > 0;) {
                auto candidate = program;
                candidate.ranks[rank].erase(candidate.ranks[rank].begin() + index);
                const auto result = evaluate(candidate);
                if (result.valid && result.classification == classification &&
                    matching_id(candidate, result) == id) {
                    program = std::move(candidate);
                    changed = true;
                    break;
                }
            }
        }
    }

    // Moving a request later narrows its failure window.  Keep doing so while it
    // retains the same semantic class; the bound prevents retrying one blocked PC forever.
    const auto action_bound = [&] {
        std::size_t count = 0;
        for (const auto& actions : program.ranks) count += actions.size();
        return count + program.world;
    }();
    for (std::size_t attempt = 0; attempt < action_bound; ++attempt) {
        bool moved = false;
        const auto before = state_key(state_before_request(program));
        for (Rank rank = 0; rank < program.world; ++rank) {
            auto candidate = program;
            candidate.request_prefix.push_back(rank);
            if (state_key(state_before_request(candidate)) == before) continue;
            const auto result = evaluate(candidate);
            if (result.valid && result.classification == classification &&
                matching_id(candidate, result) == id) {
                program = std::move(candidate);
                moved = true;
                break;
            }
        }
        if (!moved) break;
    }
    return program;
}

std::string render_action(const Action& action,
                          const std::map<std::string, std::string>& payload_names) {
    std::ostringstream output;
    switch (action.kind) {
    case ActionKind::Send:
        output << "send(r" << action.peer << ',' << payload_names.at(action.tag) << ')';
        break;
    case ActionKind::Receive: {
        const auto found = payload_names.find(action.tag);
        output << "recv(r" << action.peer << ','
               << (found == payload_names.end() ? "unmatched" : found->second) << ')';
        break;
    }
    case ActionKind::Barrier:
        output << "barrier";
        break;
    case ActionKind::Synchronize:
        output << "sync";
        break;
    case ActionKind::Bcast:
    case ActionKind::Gather:
    case ActionKind::Scatter:
    case ActionKind::Reduce:
    case ActionKind::Allreduce:
    case ActionKind::Scan:
        // The bounded explorer's state space is p2p-only; collectives are catalog-only actions.
        output << to_string(action.kind) << "(root=" << action.root << ')';
        break;
    }
    return output.str();
}

std::string normalize_trace(const Program& program) {
    std::map<std::string, std::string> payload_names;
    std::size_t next_payload = 0;
    // Payload identity is deliberately assigned from send order, not from the
    // arbitrary integer bytes/tags used to construct the candidate.
    for (const auto& actions : program.ranks) {
        for (const auto& action : actions) {
            if (action.kind == ActionKind::Send && payload_names.count(action.tag) == 0) {
                payload_names[action.tag] = "p" + std::to_string(next_payload++);
            }
        }
    }

    std::ostringstream output;
    output << "  target: r" << program.target << '\n';
    output << "  request after:";
    for (const auto rank : program.request_prefix) output << " r" << rank;
    output << '\n';
    output << "  first observer: r" << program.first_observer << '\n';
    for (Rank rank = 0; rank < program.world; ++rank) {
        output << "  rank " << rank << ':';
        if (program.ranks[rank].empty()) output << " <empty>";
        for (const auto& action : program.ranks[rank]) {
            output << ' ' << render_action(action, payload_names);
        }
        output << '\n';
    }
    return output.str();
}

Program queue_program(Backend backend, Rank world, Rank target, int shape) {
    Program program;
    program.backend = backend;
    program.world = world;
    program.target = target;
    program.ranks.resize(world);
    if (shape == 0) {
        program.ranks[0] = {send(1, "old")};
        program.ranks[1] = {synchronize(), receive(0, "old")};
        program.request_prefix = {0, 1};
    } else if (shape == 1) {
        program.ranks[0] = {send(1, "old"), send(1, "new")};
        program.ranks[1] = {synchronize(), receive(0, "old")};
        program.request_prefix = {0, 1};
    } else {
        program.ranks[0] = {send(1, "old-0"), send(1, "old-1"), send(1, "new")};
        program.ranks[1] = {synchronize(), synchronize(), receive(0, "old-0"),
                            receive(0, "old-1")};
        program.request_prefix = {0, 0, 1, 1};
    }
    for (Rank rank = 2; rank < world; ++rank) {
        program.ranks[rank] = {synchronize(), synchronize()};
        program.request_prefix.push_back(rank);
        if (shape == 2) program.request_prefix.push_back(rank);
    }
    return program;
}

Program future_send_program(Backend backend, Rank world, Rank target) {
    Program program;
    program.backend = backend;
    program.world = world;
    program.target = target;
    program.ranks.resize(world);
    program.ranks[0] = {send(1, "warmup"), synchronize(), send(1, "future")};
    program.ranks[1] = {receive(0, "warmup"), receive(0, "future")};
    program.request_prefix = {0, 1, 1}; // the final attempt blocks and publishes boundary 1
    for (Rank rank = 2; rank < world; ++rank) {
        program.ranks[rank] = {synchronize(), synchronize()};
        program.request_prefix.push_back(rank);
    }
    return program;
}

std::vector<Program> candidate_families() {
    std::vector<Program> programs;
    for (Rank world : {Rank{2}, Rank{3}}) {
        for (Rank target = 0; target < world; ++target) {
            for (int shape = 0; shape != 3; ++shape) {
                programs.push_back(queue_program(Backend::Redis, world, target, shape));
            }
            programs.push_back(future_send_program(Backend::Redis, world, target));
            programs.push_back(future_send_program(Backend::Direct, world, target));
        }
    }

    // Direct drops only links incident to the moved rank.
    for (Rank target : {Rank{0}, Rank{1}}) {
        programs.push_back(queue_program(Backend::Direct, 2, target, 0));
        programs.push_back(queue_program(Backend::Direct, 2, target, 1));
    }
    // In a three-rank world target 2 leaves the survivor 0->1 link alive.
    programs.push_back(queue_program(Backend::Direct, 3, 2, 0));
    return programs;
}

} // namespace

std::vector<ModelCounterexample> explore_bounded_model() {
    using OutputKey = std::tuple<Classification, Backend, std::string>;
    using RepresentativeScore =
        std::tuple<Rank, std::size_t, std::size_t, Backend, Rank, Rank, std::string>;
    std::set<OutputKey> normalized_failures;
    std::map<std::string, std::pair<RepresentativeScore, ModelCounterexample>> representatives;

    for (auto family : candidate_families()) {
        const auto full_prefix = family.request_prefix;
        for (std::size_t prefix_size = 1; prefix_size <= full_prefix.size(); ++prefix_size) {
            family.request_prefix.assign(full_prefix.begin(), full_prefix.begin() + prefix_size);
            for (Rank observer = 0; observer < family.world; ++observer) {
                family.first_observer = observer;
                const auto initial = evaluate(family);
                if (!initial.valid || initial.classification == Classification::Preserved) {
                    continue;
                }
                const auto id = matching_id(family, initial);
                if (id.empty()) continue;

                auto minimized = minimize(family, initial.classification, id);
                const auto minimized_result = evaluate(minimized);
                const auto trace = normalize_trace(minimized);
                ModelCounterexample counterexample{
                    trace, minimized.backend, minimized_result.classification, id};
                const auto output_key = std::make_tuple(counterexample.classification,
                                                        counterexample.backend,
                                                        counterexample.normalized_trace);
                if (!normalized_failures.insert(output_key).second) continue;

                std::size_t action_count = 0;
                for (const auto& actions : minimized.ranks) action_count += actions.size();
                const auto score = std::make_tuple(
                    minimized.world, action_count, minimized.request_prefix.size(),
                    minimized.backend, minimized.target, minimized.first_observer, trace);
                const auto found = representatives.find(id);
                if (found == representatives.end() || score < found->second.first) {
                    representatives[id] = {score, std::move(counterexample)};
                }
            }
        }
    }

    std::vector<ModelCounterexample> result;
    result.reserve(representatives.size());
    for (auto& [id, representative] : representatives) {
        (void)id;
        result.push_back(std::move(representative.second));
    }
    std::sort(result.begin(), result.end(), [](const auto& left, const auto& right) {
        return std::tie(left.classification, left.backend, left.normalized_trace) <
               std::tie(right.classification, right.backend, right.normalized_trace);
    });
    return result;
}

} // namespace FMI::Tests::MigrationCounterexamples
