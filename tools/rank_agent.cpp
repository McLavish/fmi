#include "../include/fmi.h"

#include <iostream>
#include <string>

// Host-local driver for CRIU-backed single-rank transparent migration (same-host v1).
//
//   fmi-rank-agent migrate <comm_name> <num_peers> <config> <rank>
//       Migrate one explicit logical rank: wait for it to reach its checkpoint-ready quiesce
//       point, criu dump + restore its process image, then promote the epoch.
//
//   fmi-rank-agent migrate-local <comm_name> <num_peers> <config>
//       Migrate every rank running on this host in one epoch cut: discover the host-local ranks
//       from the CRIU registry, request their migration, wait for all to quiesce, then criu
//       dump + restore them in parallel and promote the epoch once.
//
//   fmi-rank-agent watch <comm_name> <num_peers> <config>
//       Wait for any rank to be marked for migration (request_migration) and migrate it.
//
//   fmi-rank-agent cleanup <comm_name> <num_peers> <config>
//       Remove this communicator's CRIU image tree and clear its CRIU control-plane state.
int main(int argc, char** argv) {
    if (argc < 5) {
        std::cerr << "usage: fmi-rank-agent <migrate|migrate-local|watch|cleanup> <comm_name> "
                     "<num_peers> <config> [rank]"
                  << std::endl;
        return 1;
    }

    std::string command = argv[1];
    std::string comm_name = argv[2];
    std::string config_path = argv[4];

    try {
        // Parse inside the try so a non-numeric <num_peers> exits through the same
        // "fatal: ..." path as every other error instead of std::terminate.
        auto num_peers = static_cast<FMI::Utils::peer_num>(std::stoul(argv[3]));
        FMI::FT::LocalRankAgent agent(config_path, comm_name, num_peers);

        if (command == "migrate") {
            if (argc < 6) {
                std::cerr << "migrate requires a <rank> argument" << std::endl;
                return 1;
            }
            auto rank = static_cast<FMI::Utils::peer_num>(std::stoul(argv[5]));
            if (rank >= num_peers) {
                std::cerr << "rank " << rank << " is out of range for num_peers=" << num_peers
                          << std::endl;
                return 1;
            }
            std::cout << "migrated_rank=" << rank << " promoted_epoch=" << agent.migrate_rank(rank)
                      << std::endl;
            return 0;
        }
        if (command == "migrate-local") {
            auto epoch = agent.migrate_local();
            if (epoch == 0) {
                // Distinct non-zero exit so an orchestrator can tell "no host-local ranks were
                // discovered" (a placement/registration problem) apart from a successful migration
                // (exit 0) and from a fatal error (exit 1).
                std::cerr << "no host-local ranks found to migrate" << std::endl;
                return 2;
            }
            std::cout << "promoted_epoch=" << epoch << std::endl;
            return 0;
        }
        if (command == "watch") {
            auto epoch = agent.watch_once();
            if (epoch == 0) {
                std::cerr << "no migration request observed within the quiesce timeout" << std::endl;
                return 1;
            }
            std::cout << "promoted_epoch=" << epoch << std::endl;
            return 0;
        }
        if (command == "cleanup") {
            agent.cleanup();
            std::cout << "cleaned_up=" << comm_name << std::endl;
            return 0;
        }

        std::cerr << "unknown command: " << command << std::endl;
        return 1;
    } catch (const std::exception& e) {
        std::cerr << "fatal: " << e.what() << std::endl;
        return 1;
    }
}
