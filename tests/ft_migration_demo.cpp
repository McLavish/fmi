#include "../include/fmi.h"

#include <chrono>
#include <iostream>
#include <string>
#include <thread>

namespace {
    FMI::Utils::Function<int> sum_fn() {
        return FMI::Utils::Function<int>([](int a, int b) { return a + b; }, true, true);
    }

    int run_normal_peer(FMI::Utils::peer_num rank, FMI::Utils::peer_num num_peers, const std::string& config_path,
                        const std::string& comm_name, const std::string& worker_id, unsigned int wait_ms) {
        FMI::FT::Session session(rank, num_peers, config_path, comm_name, worker_id);

        FMI::Comm::Data<int> first = static_cast<int>(rank + 1);
        FMI::Comm::Data<int> first_sum;
        session.comm().allreduce(first, first_sum, sum_fn());
        std::cout << "phase1 rank=" << rank << " epoch=" << session.epoch() << " sum=" << first_sum.get() << std::endl;

        std::this_thread::sleep_for(std::chrono::milliseconds(wait_ms));

        auto event = session.safe_point();
        if (event == FMI::FT::Event::MigrateSelf) {
            std::cout << "migrate_self rank=" << rank << " epoch=" << session.epoch() << std::endl;
            return 100;
        }
        if (event == FMI::FT::Event::Reconfigured) {
            std::cout << "reconfigured rank=" << rank << " epoch=" << session.epoch() << std::endl;
        }

        FMI::Comm::Data<int> second = static_cast<int>(100 + rank);
        FMI::Comm::Data<int> second_sum;
        session.comm().allreduce(second, second_sum, sum_fn());
        std::cout << "phase2 rank=" << rank << " epoch=" << session.epoch() << " sum=" << second_sum.get() << std::endl;
        return 0;
    }

    int run_replacement_peer(FMI::Utils::peer_num rank, FMI::Utils::peer_num num_peers, const std::string& config_path,
                             const std::string& comm_name, const std::string& worker_id) {
        FMI::FT::Session session(rank, num_peers, config_path, comm_name, worker_id);
        auto event = session.safe_point();
        if (event != FMI::FT::Event::Reconfigured) {
            std::cerr << "replacement rank=" << rank << " did not reconfigure" << std::endl;
            return 2;
        }

        FMI::Comm::Data<int> second = static_cast<int>(100 + rank);
        FMI::Comm::Data<int> second_sum;
        session.comm().allreduce(second, second_sum, sum_fn());
        std::cout << "phase2 rank=" << rank << " epoch=" << session.epoch() << " sum=" << second_sum.get() << std::endl;
        return 0;
    }
}

int main(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "usage: ft_migration_demo <mode> ..." << std::endl;
        return 1;
    }

    std::string mode = argv[1];
    try {
        if (mode == "migrate") {
            if (argc != 6) {
                std::cerr << "usage: ft_migration_demo migrate <rank> <num_peers> <config> <comm_name>" << std::endl;
                return 1;
            }
            FMI::Utils::peer_num rank = static_cast<FMI::Utils::peer_num>(std::stoul(argv[2]));
            FMI::Utils::peer_num num_peers = static_cast<FMI::Utils::peer_num>(std::stoul(argv[3]));
            FMI::FT::Coordinator coordinator(argv[4], argv[5], num_peers);
            coordinator.request_migration(rank);
            std::cout << "migration_requested rank=" << rank << " epoch=" << coordinator.epoch() << std::endl;
            return 0;
        }

        if (mode == "cleanup") {
            if (argc != 5) {
                std::cerr << "usage: ft_migration_demo cleanup <num_peers> <config> <comm_name>" << std::endl;
                return 1;
            }
            FMI::Utils::peer_num num_peers = static_cast<FMI::Utils::peer_num>(std::stoul(argv[2]));
            FMI::FT::Coordinator coordinator(argv[3], argv[4], num_peers);
            coordinator.clear_job_state();
            std::cout << "cleanup_done" << std::endl;
            return 0;
        }

        if (mode == "peer") {
            if (argc != 9) {
                std::cerr << "usage: ft_migration_demo peer <rank> <num_peers> <config> <comm_name> <worker_id> <normal|replacement> <wait_ms>" << std::endl;
                return 1;
            }
            FMI::Utils::peer_num rank = static_cast<FMI::Utils::peer_num>(std::stoul(argv[2]));
            FMI::Utils::peer_num num_peers = static_cast<FMI::Utils::peer_num>(std::stoul(argv[3]));
            std::string config_path = argv[4];
            std::string comm_name = argv[5];
            std::string worker_id = argv[6];
            std::string role = argv[7];
            unsigned int wait_ms = static_cast<unsigned int>(std::stoul(argv[8]));

            if (role == "normal") {
                return run_normal_peer(rank, num_peers, config_path, comm_name, worker_id, wait_ms);
            }
            if (role == "replacement") {
                return run_replacement_peer(rank, num_peers, config_path, comm_name, worker_id);
            }
            std::cerr << "unknown peer role: " << role << std::endl;
            return 1;
        }

        std::cerr << "unknown mode: " << mode << std::endl;
        return 1;
    } catch (const std::exception& e) {
        std::cerr << "fatal: " << e.what() << std::endl;
        return 1;
    }
}
