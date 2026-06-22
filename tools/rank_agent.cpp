#include "../include/fmi.h"

#include <iostream>
#include <string>

// Host-local driver for CRIU-backed single-rank transparent migration (same-host v1).
//
//   fmi-rank-agent migrate <comm_name> <num_peers> <config> <rank>
//       Migrate one explicit logical rank: wait for it to reach its checkpoint-ready quiesce
//       point, criu dump + restore its process image, then promote the epoch.
//
//   fmi-rank-agent watch <comm_name> <num_peers> <config>
//       Wait for any rank to be marked for migration (request_migration) and migrate it.
//
//   fmi-rank-agent cleanup <comm_name> <num_peers> <config>
//       Remove this communicator's CRIU image tree and clear its CRIU control-plane state.
int main(int argc, char** argv) {
    if (argc < 5) {
        std::cerr << "usage: fmi-rank-agent <migrate|watch|cleanup> <comm_name> <num_peers> <config> [rank]"
                  << std::endl;
        return 1;
    }

    std::string command = argv[1];
    std::string comm_name = argv[2];
    FMI::Utils::peer_num num_peers = static_cast<FMI::Utils::peer_num>(std::stoul(argv[3]));
    std::string config_path = argv[4];

    try {
        FMI::FT::LocalRankAgent agent(config_path, comm_name, num_peers);

        if (command == "migrate") {
            if (argc < 6) {
                std::cerr << "migrate requires a <rank> argument" << std::endl;
                return 1;
            }
            auto rank = static_cast<FMI::Utils::peer_num>(std::stoul(argv[5]));
            std::cout << "migrated_rank=" << rank << " promoted_epoch=" << agent.migrate_rank(rank)
                      << std::endl;
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
