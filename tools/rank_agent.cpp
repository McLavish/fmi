#include "../include/fmi.h"

#include <iostream>
#include <string>

// Host-local driver for CRIU-backed transparent migration.
//
//   fmi-rank-agent migrate <comm_name> <num_peers> <config> <rank>
//       Migrate one explicit logical rank: wait for it to reach its checkpoint-ready quiesce
//       point, criu dump + restore its process image, then promote the epoch.
//
//   fmi-rank-agent migrate-local <comm_name> <num_peers> <config>
//       Migrate every rank running on this host in one epoch cut: discover the host-local ranks
//       from the CRIU registry, request their migration, wait for all to quiesce, then criu
//       dump + restore them in parallel and promote the epoch once. (Same-host, in-place.)
//
//   fmi-rank-agent evacuate-local <comm_name> <num_peers> <config>
//       Cross-host dump half: request + dump every host-local rank in one cut, pack each image
//       (plus FMI_CRIU_EXTRA_FILES) and stage the archives in the control plane. No restore, no
//       promotion — restore-remote agents on other hosts consume the images.
//
//   fmi-rank-agent restore-remote <comm_name> <num_peers> <config> <rank>
//       Cross-host restore half: fetch the staged image for <rank>, unpack at /, criu restore
//       it (parked in its promotion wait), and re-advertise the rank as Running on this host.
//
//   fmi-rank-agent promote <comm_name> <num_peers> <config>
//       Promote the epoch to current+1, releasing every parked rank. Run by the orchestrator
//       once every restore-remote of the cut has succeeded.
//
//   fmi-rank-agent watch <comm_name> <num_peers> <config>
//       Wait for any rank to be marked for migration (request_migration) and migrate it.
//
//   fmi-rank-agent cleanup <comm_name> <num_peers> <config>
//       Remove this communicator's CRIU image tree and clear its CRIU control-plane state.
int main(int argc, char** argv) {
    if (argc < 5) {
        std::cerr << "usage: fmi-rank-agent <migrate|migrate-local|evacuate-local|restore-remote|"
                     "promote|watch|cleanup> <comm_name> <num_peers> <config> [rank]"
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
        if (command == "evacuate-local") {
            auto result = agent.evacuate_local();
            if (result.staged_epoch == 0) {
                // Same distinct exit code as migrate-local's empty-host case.
                std::cerr << "no host-local ranks found to evacuate" << std::endl;
                return 2;
            }
            std::cout << "staged_epoch=" << result.staged_epoch << " ranks=";
            for (std::size_t i = 0; i < result.ranks.size(); i++) {
                std::cout << (i == 0 ? "" : ",") << result.ranks[i];
            }
            std::cout << std::endl;
            return 0;
        }
        if (command == "restore-remote") {
            if (argc < 6) {
                std::cerr << "restore-remote requires a <rank> argument" << std::endl;
                return 1;
            }
            auto rank = static_cast<FMI::Utils::peer_num>(std::stoul(argv[5]));
            if (rank >= num_peers) {
                std::cerr << "rank " << rank << " is out of range for num_peers=" << num_peers
                          << std::endl;
                return 1;
            }
            auto result = agent.restore_remote(rank);
            std::cout << "restored_rank=" << rank << " pid=" << result.pid
                      << " host=" << result.host_id << std::endl;
            return 0;
        }
        if (command == "promote") {
            std::cout << "promoted_epoch=" << agent.promote_next() << std::endl;
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
