#include "../include/fmi.h"

#include <chrono>
#include <cstdlib>
#include <iostream>
#include <thread>

namespace {
    FMI::Utils::Function<int> sum_fn() {
        return FMI::Utils::Function<int>([](int a, int b) { return a + b; }, true, true);
    }
}

int main(int argc, char** argv) {
    if (argc < 5) {
        std::cerr << "usage: criu_checkpoint_demo <rank> <num_peers> <config> <comm_name> [wait_ms]" << std::endl;
        return 1;
    }

    FMI::Utils::peer_num rank = static_cast<FMI::Utils::peer_num>(std::stoul(argv[1]));
    FMI::Utils::peer_num num_peers = static_cast<FMI::Utils::peer_num>(std::stoul(argv[2]));
    std::string config_path = argv[3];
    std::string comm_name = argv[4];
    unsigned int wait_ms = argc >= 6 ? static_cast<unsigned int>(std::stoul(argv[5])) : 5000;

    FMI::Communicator comm(rank, num_peers, config_path, comm_name);

    int state = static_cast<int>(rank) + 1;
    FMI::Comm::Data<int> phase1_value(state);
    FMI::Comm::Data<int> phase1_sum;
    comm.allreduce(phase1_value, phase1_sum, sum_fn());
    std::cout << "phase1 rank=" << rank << " sum=" << phase1_sum.get() << " state=" << state << std::endl;

    state += 100;
    std::cout << "rank=" << rank << " sleeping_before_phase2_ms=" << wait_ms << " state=" << state << std::endl;
    std::this_thread::sleep_for(std::chrono::milliseconds(wait_ms));

    comm.barrier();

    FMI::Comm::Data<int> phase2_value(state);
    FMI::Comm::Data<int> phase2_sum;
    comm.allreduce(phase2_value, phase2_sum, sum_fn());
    std::cout << "phase2 rank=" << rank << " sum=" << phase2_sum.get() << " state=" << state << std::endl;
    return 0;
}
