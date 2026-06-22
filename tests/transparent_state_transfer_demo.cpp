#include "../include/fmi.h"

#include <chrono>
#include <cstdlib>
#include <iostream>
#include <thread>

// CRIU transparent state-transfer demo (same-host v1).
//
// This exercises single-rank transparent
// migration with fault_tolerance.state_transfer="criu": a plain FMI::Communicator application,
// one rank migrated mid-run by an external fmi-migration-supervisor. The proof of state
// transfer is the post-migration collective: rank r mutates a local `state` BEFORE the
// migration boundary, and the phase-2 allreduce can only produce the expected total if every
// rank's in-memory `state` survived the criu dump/restore — with no application checkpoint code
// and no recomputation (the restored rank resumes inside the barrier, after phase 1).
//
//   transparent_state_transfer_demo <rank> <num_peers> <config> <comm_name> [migration_window_ms]
//
// The migration window is a sleep after phase 1 that gives the orchestrator time to observe all
// ranks ACTIVE and request_migration before the ranks reach the barrier (the quiesce point).
namespace {
    FMI::Utils::Function<int> sum_fn() {
        return FMI::Utils::Function<int>([](int a, int b) { return a + b; }, true, true);
    }
}

int main(int argc, char** argv) {
    if (argc < 5) {
        std::cerr << "usage: transparent_state_transfer_demo <rank> <num_peers> <config> <comm_name> "
                     "[migration_window_ms]" << std::endl;
        return 1;
    }

    FMI::Utils::peer_num rank = static_cast<FMI::Utils::peer_num>(std::stoul(argv[1]));
    FMI::Utils::peer_num num_peers = static_cast<FMI::Utils::peer_num>(std::stoul(argv[2]));
    std::string config_path = argv[3];
    std::string comm_name = argv[4];
    unsigned int window_ms = argc >= 6 ? static_cast<unsigned int>(std::stoul(argv[5])) : 5000;

    FMI::Communicator comm(rank, num_peers, config_path, comm_name);
    comm.hint(FMI::Utils::Hint::fast);

    // Phase 1: a collective that runs before any migration.
    int state = static_cast<int>(rank) + 1;
    FMI::Comm::Data<int> phase1_value(state);
    FMI::Comm::Data<int> phase1_sum;
    comm.allreduce(phase1_value, phase1_sum, sum_fn());
    std::cout << "rank=" << rank << " phase1_sum=" << phase1_sum.get() << " state=" << state << std::endl;

    // Mutate application state that lives only in process memory. If the rank is migrated, this
    // value must survive transparently for phase 2 to be correct.
    state += 100;
    std::cout << "rank=" << rank << " pre_migration_state=" << state
              << " (sleeping " << window_ms << "ms for the migration window)" << std::endl;
    std::this_thread::sleep_for(std::chrono::milliseconds(window_ms));

    // Migration boundary: a targeted rank is checkpointed here and resumes (restored) at this
    // same point under epoch N+1; survivors reconfigure here too.
    comm.barrier();

    // Phase 2: post-migration collective over the preserved state.
    FMI::Comm::Data<int> phase2_value(state);
    FMI::Comm::Data<int> phase2_sum;
    comm.allreduce(phase2_value, phase2_sum, sum_fn());

    int base = static_cast<int>(num_peers) * (static_cast<int>(num_peers) + 1) / 2; // sum of (p+1)
    int expected = base + 100 * static_cast<int>(num_peers);
    std::cout << "rank=" << rank << " post_migration_state=" << state
              << " phase2_sum=" << phase2_sum.get() << " expected=" << expected << std::endl;

    if (phase2_sum.get() != expected) {
        std::cerr << "rank=" << rank << " FAIL: phase2_sum=" << phase2_sum.get()
                  << " != expected " << expected << " — application state did NOT survive migration"
                  << std::endl;
        return 1;
    }

    std::cout << "rank=" << rank << " OK: application state survived transparent migration" << std::endl;
    return 0;
}
