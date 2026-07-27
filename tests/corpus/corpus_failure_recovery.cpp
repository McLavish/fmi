// Behavioural acceptance corpus — axis: failure-recovery
//
// This translation unit owns the "failure-recovery" axis and nothing else.  Append scenarios to the
// vector below; never edit a shared list.  Every scenario identifier must be globally unique
// across the whole corpus, so prefix yours with the axis (for example
// "failure_recovery_<short_name>").
//
// See tests/migration_counterexample_registry.h for the action helpers, the trigger helpers and
// the Scenario field documentation.

#include "../migration_counterexample_registry.h"

#include <vector>

namespace FMI::Tests::MigrationCounterexamples {
namespace {

std::vector<Scenario> corpus_failure_recovery_scenarios() {
    std::vector<Scenario> batch;
    // Author this axis's scenarios here, one push_back per scenario.
    return batch;
}

FMI_REGISTER_SCENARIOS("failure-recovery", corpus_failure_recovery_scenarios)

} // namespace
} // namespace FMI::Tests::MigrationCounterexamples
