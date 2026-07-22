#include "migration_counterexample_scenarios.h"

#include <iostream>
#include <optional>
#include <string>
#include <vector>

namespace FMI::Tests::MigrationCounterexamples {
namespace {

struct Options {
    bool list = false;
    bool explore = false;
    std::optional<std::string> case_id;
    std::optional<Backend> backend;
};

void usage(std::ostream& output) {
    output << "usage: fmi_migration_counterexamples [--list] [--case <id>] "
              "[--backend redis|direct] [--explore]\n";
}

bool parse_backend(const std::string& value, Backend& backend) {
    if (value == "redis") {
        backend = Backend::Redis;
        return true;
    }
    if (value == "direct") {
        backend = Backend::Direct;
        return true;
    }
    return false;
}

std::optional<Options> parse_options(int argc, char* argv[]) {
    Options options;
    for (int index = 1; index < argc; ++index) {
        const std::string argument = argv[index];
        if (argument == "--list") {
            if (options.list) {
                std::cerr << "--list may only be specified once\n";
                return std::nullopt;
            }
            options.list = true;
        } else if (argument == "--explore") {
            if (options.explore) {
                std::cerr << "--explore may only be specified once\n";
                return std::nullopt;
            }
            options.explore = true;
        } else if (argument == "--case") {
            if (options.case_id || ++index == argc) {
                std::cerr << "--case requires one identifier and may only be specified once\n";
                return std::nullopt;
            }
            if (std::string(argv[index]).rfind("--", 0) == 0) {
                std::cerr << "--case requires one identifier and may only be specified once\n";
                return std::nullopt;
            }
            options.case_id = argv[index];
        } else if (argument == "--backend") {
            if (options.backend || ++index == argc) {
                std::cerr << "--backend requires redis or direct and may only be specified once\n";
                return std::nullopt;
            }
            if (std::string(argv[index]).rfind("--", 0) == 0) {
                std::cerr << "--backend requires redis or direct and may only be specified once\n";
                return std::nullopt;
            }
            Backend backend;
            if (!parse_backend(argv[index], backend)) {
                std::cerr << "unknown backend: " << argv[index] << '\n';
                return std::nullopt;
            }
            options.backend = backend;
        } else {
            std::cerr << "unknown option: " << argument << '\n';
            return std::nullopt;
        }
    }

    if (options.explore && (options.list || options.case_id || options.backend)) {
        std::cerr << "--explore cannot be combined with scenario selection options\n";
        return std::nullopt;
    }
    return options;
}

std::vector<const Scenario*> select_scenarios(const Options& options) {
    std::vector<const Scenario*> selected;
    if (options.case_id) {
        const auto* scenario = find_scenario(*options.case_id);
        if (scenario == nullptr) {
            return selected;
        }
        if (!options.backend || scenario->backend == *options.backend) {
            selected.push_back(scenario);
        }
        return selected;
    }
    for (const auto& scenario : scenarios()) {
        if (!options.backend || scenario.backend == *options.backend) {
            selected.push_back(&scenario);
        }
    }
    return selected;
}

int aggregate_exit(const std::vector<Classification>& classifications) {
    bool semantic_counterexample = false;
    for (const auto classification : classifications) {
        if (classification == Classification::SetupError) {
            return 1;
        }
        if (classification != Classification::Preserved &&
            classification != Classification::InfrastructureSkip) {
            semantic_counterexample = true;
        }
    }
    return semantic_counterexample ? 2 : 0;
}

int run_selected(const std::vector<const Scenario*>& selected) {
    std::vector<Classification> classifications;
    classifications.reserve(selected.size());
    for (const auto* scenario : selected) {
        std::cerr << scenario->id << ": " << to_string(Classification::SetupError)
                  << ": execution not implemented\n";
        classifications.push_back(Classification::SetupError);
    }
    return aggregate_exit(classifications);
}

int run_explorer() {
    const auto counterexamples = explore_bounded_model();
    if (counterexamples.empty()) {
        std::cerr << "explore: setup_error: no bounded counterexamples found\n";
        return aggregate_exit({Classification::SetupError});
    }

    std::vector<Classification> classifications;
    classifications.reserve(counterexamples.size());
    for (std::size_t index = 0; index < counterexamples.size(); ++index) {
        const auto& counterexample = counterexamples[index];
        std::cout << "counterexample " << (index + 1) << '\n'
                  << "  classification: " << to_string(counterexample.classification) << '\n'
                  << "  backend: " << to_string(counterexample.backend) << '\n'
                  << "  matching corpus ID: " << counterexample.matching_scenario_id << '\n'
                  << counterexample.normalized_trace;
        if (index + 1 != counterexamples.size()) std::cout << '\n';
        classifications.push_back(counterexample.classification);
    }
    return aggregate_exit(classifications);
}

} // namespace
} // namespace FMI::Tests::MigrationCounterexamples

int main(int argc, char* argv[]) {
    using namespace FMI::Tests::MigrationCounterexamples;

    const auto options = parse_options(argc, argv);
    if (!options) {
        usage(std::cerr);
        return 1;
    }
    if (!catalog_error().empty()) {
        std::cerr << "scenario catalog setup_error: " << catalog_error() << '\n';
        return 1;
    }
    if (options->explore) {
        return run_explorer();
    }

    const auto selected = select_scenarios(*options);
    if (options->case_id && selected.empty()) {
        if (find_scenario(*options->case_id) == nullptr) {
            std::cerr << "unknown scenario: " << *options->case_id << '\n';
        } else {
            std::cerr << "scenario is not available for the selected backend: "
                      << *options->case_id << '\n';
        }
        return 1;
    }

    if (options->list) {
        for (const auto* scenario : selected) {
            std::cout << scenario->id << '\t' << to_string(scenario->backend) << '\t'
                      << scenario->property << '\n';
        }
        return 0;
    }
    return run_selected(selected);
}
