#include "../include/fmi.h"

#include <iostream>
#include <string>

namespace {
    std::string state_to_string(FMI::FT::CriuJobState state) {
        switch (state) {
            case FMI::FT::CriuJobState::Running:
                return "RUNNING";
            case FMI::FT::CriuJobState::CheckpointRequested:
                return "CHECKPOINT_REQUESTED";
            case FMI::FT::CriuJobState::Quiesced:
                return "QUIESCED";
            case FMI::FT::CriuJobState::CheckpointComplete:
                return "CHECKPOINT_COMPLETE";
            case FMI::FT::CriuJobState::RestoreRequested:
                return "RESTORE_REQUESTED";
            case FMI::FT::CriuJobState::Restored:
                return "RESTORED";
        }
        return "UNKNOWN";
    }

    std::string state_to_string(FMI::FT::CriuRankState state) {
        switch (state) {
            case FMI::FT::CriuRankState::Running:
                return "RUNNING";
            case FMI::FT::CriuRankState::Quiesced:
                return "QUIESCED";
        }
        return "UNKNOWN";
    }
}

int main(int argc, char** argv) {
    if (argc < 5) {
        std::cerr << "usage: fmi-criu-supervisor <checkpoint|restore|status|cleanup> <comm_name> <num_peers> <config> [generation]" << std::endl;
        return 1;
    }

    std::string command = argv[1];
    std::string comm_name = argv[2];
    FMI::Utils::peer_num num_peers = static_cast<FMI::Utils::peer_num>(std::stoul(argv[3]));
    std::string config_path = argv[4];

    try {
        FMI::FT::CriuSupervisor supervisor(config_path, comm_name, num_peers);
        if (command == "checkpoint") {
            std::cout << "checkpoint_generation=" << supervisor.checkpoint() << std::endl;
            return 0;
        }
        if (command == "restore") {
            std::uint64_t generation = 0;
            if (argc >= 6) {
                generation = std::stoull(argv[5]);
            }
            std::cout << "restore_generation=" << supervisor.restore(generation) << std::endl;
            return 0;
        }
        if (command == "status") {
            auto status = supervisor.status();
            std::cout << "state=" << state_to_string(status.job.state)
                      << " requested_generation=" << status.job.requested_generation
                      << " completed_generation=" << status.job.completed_generation
                      << " restore_generation=" << status.job.restore_generation
                      << " supervisor=" << status.job.supervisor << std::endl;
            for (const auto& rank : status.ranks) {
                std::cout << "rank=" << rank.rank
                          << " pid=" << rank.pid
                          << " host_id=" << rank.host_id
                          << " backend=" << rank.backend
                          << " state=" << state_to_string(rank.state)
                          << " quiesced_generation=" << rank.quiesced_generation
                          << " last_heartbeat_ms=" << rank.last_heartbeat_ms
                          << std::endl;
            }
            return 0;
        }
        if (command == "cleanup") {
            supervisor.cleanup();
            std::cout << "cleanup_done" << std::endl;
            return 0;
        }

        std::cerr << "unknown command: " << command << std::endl;
        return 1;
    } catch (const std::exception& e) {
        std::cerr << "fatal: " << e.what() << std::endl;
        return 1;
    }
}
