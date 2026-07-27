#pragma once

// Registration surface for the behavioural acceptance corpus.
//
// Every corpus translation unit under tests/corpus/ includes this header, defines one factory
// function returning its scenarios, and registers it with FMI_REGISTER_SCENARIOS.  No corpus file
// ever edits a shared list, so corpus files never conflict with each other.

#include "migration_counterexample_scenarios.h"

#include <cstddef>
#include <initializer_list>
#include <string>
#include <utility>
#include <vector>

namespace FMI::Tests::MigrationCounterexamples {

// Signature every corpus factory must have.
using ScenarioFactory = std::vector<Scenario> (*)();

// Self-registering entry point.  Construct one per translation unit at namespace scope; the
// FMI_REGISTER_SCENARIOS macro does exactly that.  `axis` is copied into Scenario::axis of every
// scenario the factory returns.
struct ScenarioRegistrar {
    ScenarioRegistrar(const char* axis, ScenarioFactory factory);
};

// ---------------------------------------------------------------------------
// Deadlines
// ---------------------------------------------------------------------------

// Redis operations have a configured five-second backend timeout.  The subprocess deadline must
// leave enough room for that timeout to be reported and serialized.
inline constexpr std::chrono::milliseconds redis_deadline{8000};
inline constexpr std::chrono::milliseconds direct_deadline{10000};

// ---------------------------------------------------------------------------
// Trigger helpers
// ---------------------------------------------------------------------------

inline Trigger before_phase(std::size_t phase) {
    return {TriggerKind::BeforePhase, phase, 0, 0};
}

inline Trigger mid_operation(std::size_t phase, std::size_t action,
                             FMI::Utils::peer_num rank) {
    return {TriggerKind::MidOperation, phase, action, rank};
}

inline Trigger mid_compute(std::size_t phase, std::size_t action, FMI::Utils::peer_num rank) {
    return {TriggerKind::MidCompute, phase, action, rank};
}

inline Trigger mid_blocking_recv(std::size_t phase, std::size_t action,
                                 FMI::Utils::peer_num rank) {
    return {TriggerKind::MidBlockingRecv, phase, action, rank};
}

inline Trigger mid_pairing(std::size_t phase, std::size_t action, FMI::Utils::peer_num rank) {
    return {TriggerKind::MidPairing, phase, action, rank};
}

inline Trigger after_completed_send(std::size_t phase, std::size_t action,
                                    FMI::Utils::peer_num rank) {
    return {TriggerKind::AfterCompletedSend, phase, action, rank};
}

// ---------------------------------------------------------------------------
// Reduction function helpers
// ---------------------------------------------------------------------------

inline ReduceFunction reduce_function(ReduceOp op, bool commutative, bool associative) {
    return {op, commutative, associative};
}

// Commutative and associative: selects the binomial / recursive-doubling algorithms.
inline ReduceFunction sum_no_order() { return {ReduceOp::Sum, true, true}; }

// Non-commutative and non-associative: forces the fixed left-to-right algorithms.
inline ReduceFunction sum_left_to_right() { return {ReduceOp::Sum, false, false}; }

// ---------------------------------------------------------------------------
// Action helpers, one per primitive
// ---------------------------------------------------------------------------

//! send(payload) to `peer`.
inline Action send(FMI::Utils::peer_num peer, std::vector<int> payload, std::string tag) {
    Action action;
    action.kind = ActionKind::Send;
    action.peer = peer;
    action.payload = std::move(payload);
    action.tag = std::move(tag);
    return action;
}

//! recv(elements) from `peer`.  Records the received vector into the receive history.
inline Action receive(FMI::Utils::peer_num peer, std::size_t elements, std::string tag) {
    Action action;
    action.kind = ActionKind::Receive;
    action.peer = peer;
    action.receive_elements = elements;
    action.tag = std::move(tag);
    return action;
}

//! barrier().  Records nothing.
inline Action barrier() {
    Action action;
    action.kind = ActionKind::Barrier;
    return action;
}

//! No-op placeholder that still consumes a program slot.  Records nothing, issues no operation.
inline Action synchronize() {
    Action action;
    action.kind = ActionKind::Synchronize;
    return action;
}

//! bcast(root).  `payload` is the root's buffer; `elements` is the buffer length on every rank
//! (it must equal payload.size()).  Every rank records the resulting buffer.
inline Action bcast(FMI::Utils::peer_num root, std::vector<int> payload, std::size_t elements,
                    std::vector<int> expected, std::string tag) {
    Action action;
    action.kind = ActionKind::Bcast;
    action.root = root;
    action.payload = std::move(payload);
    action.receive_elements = elements;
    action.expected = std::move(expected);
    action.tag = std::move(tag);
    return action;
}

//! gather(root).  `contribution` is this rank's send buffer; every rank allocates a
//! world * contribution.size() receive buffer but only the root records the result.
inline Action gather(FMI::Utils::peer_num root, std::vector<int> contribution,
                     std::vector<int> expected, std::string tag) {
    Action action;
    action.kind = ActionKind::Gather;
    action.root = root;
    action.payload = std::move(contribution);
    action.expected = std::move(expected);
    action.tag = std::move(tag);
    return action;
}

//! scatter(root).  `payload` is the root's world * elements send buffer (non-root ranks allocate
//! an equally sized zero buffer); `elements` is the per-rank receive count.  Every rank records
//! its slice.
inline Action scatter(FMI::Utils::peer_num root, std::vector<int> payload, std::size_t elements,
                      std::vector<int> expected, std::string tag) {
    Action action;
    action.kind = ActionKind::Scatter;
    action.root = root;
    action.payload = std::move(payload);
    action.receive_elements = elements;
    action.expected = std::move(expected);
    action.tag = std::move(tag);
    return action;
}

//! reduce(root, f).  `contribution` is this rank's send buffer; only the root records the result.
//! `result_elements` defaults to contribution.size(); set it to a different value to express an
//! ill-formed program whose result buffer does not match its send buffer (FMI rejects that with a
//! diagnostic, so such a scenario contracts Classification::LoudFail).
inline Action reduce(FMI::Utils::peer_num root, std::vector<int> contribution,
                     std::vector<int> expected, ReduceFunction function, std::string tag,
                     std::size_t result_elements = 0) {
    Action action;
    action.kind = ActionKind::Reduce;
    action.root = root;
    action.payload = std::move(contribution);
    action.expected = std::move(expected);
    action.function = function;
    action.tag = std::move(tag);
    action.receive_elements = result_elements;
    return action;
}

//! allreduce(f).  Every rank records the result.  See reduce() for `result_elements`.
inline Action allreduce(std::vector<int> contribution, std::vector<int> expected,
                        ReduceFunction function, std::string tag,
                        std::size_t result_elements = 0) {
    Action action;
    action.kind = ActionKind::Allreduce;
    action.payload = std::move(contribution);
    action.expected = std::move(expected);
    action.function = function;
    action.tag = std::move(tag);
    action.receive_elements = result_elements;
    return action;
}

//! scan(f), inclusive prefix scan.  Every rank records its own prefix result.  See reduce() for
//! `result_elements`.
inline Action scan(std::vector<int> contribution, std::vector<int> expected,
                   ReduceFunction function, std::string tag,
                   std::size_t result_elements = 0) {
    Action action;
    action.kind = ActionKind::Scan;
    action.payload = std::move(contribution);
    action.expected = std::move(expected);
    action.function = function;
    action.tag = std::move(tag);
    action.receive_elements = result_elements;
    return action;
}

//! One phase: exactly one action list per rank, in rank order.
inline Phase phase(std::initializer_list<std::vector<Action>> rank_actions) {
    return {rank_actions};
}

} // namespace FMI::Tests::MigrationCounterexamples

// Registers `factory_function` (a `std::vector<Scenario>()`) under the corpus axis
// `axis_name`.  Place one invocation at namespace scope in each corpus translation unit.
#define FMI_REGISTER_SCENARIOS(axis_name, factory_function)                                   \
    namespace {                                                                               \
    const ::FMI::Tests::MigrationCounterexamples::ScenarioRegistrar                           \
            fmi_scenario_registrar(axis_name, &factory_function);                             \
    }
