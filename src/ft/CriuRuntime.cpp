#include "../../include/ft/CriuRuntime.h"

#include <chrono>
#include <stdexcept>
#include <thread>

#include <unistd.h>

FMI::FT::CriuRuntime::CriuRuntime(FMI::Utils::peer_num peer_id, FMI::Utils::peer_num num_peers, std::string config_path,
                                  std::string comm_name, std::string backend_name, std::function<void()> prepare_for_checkpoint) :
        peer_id(peer_id),
        num_peers(num_peers),
        config_path(std::move(config_path)),
        comm_name(std::move(comm_name)),
        backend_name(std::move(backend_name)),
        prepare_for_checkpoint(std::move(prepare_for_checkpoint)) {
    FMI::Utils::Configuration configuration(this->config_path);
    config = configuration.get_fault_tolerance_config();
    if (!config.enabled || config.mode != FMI::FT::Mode::CriuCoordinated) {
        throw std::runtime_error("CRIU runtime requires fault_tolerance.mode = criu_coordinated");
    }
    host_id = resolve_host_id();
    coordinator = std::make_shared<FMI::FT::Coordinator>(this->config_path, this->comm_name, num_peers);
    coordinator->criu_register_rank(peer_id, current_pid(), host_id, this->backend_name);
}

void FMI::FT::CriuRuntime::enter_operation() {
    while (true) {
        auto generation = coordinator->criu_requested_generation();
        bool should_quiesce = false;
        {
            std::unique_lock<std::mutex> lock(state_mutex);
            if (!quiescing && (generation == 0 || generation <= last_completed_generation)) {
                active_operations++;
                break;
            }

            if (!quiescing && active_operations == 0 && generation > last_completed_generation) {
                quiescing = true;
                should_quiesce = true;
            } else {
                state_cv.wait_for(lock, std::chrono::milliseconds(config.poll_ms), [this]() { return !quiescing; });
            }
        }

        if (should_quiesce) {
            quiesce(generation);
        }
    }

    coordinator->criu_mark_rank_running(peer_id, current_pid(), host_id, this->backend_name);
}

void FMI::FT::CriuRuntime::exit_operation() {
    std::uint64_t generation = 0;
    bool should_quiesce = false;
    {
        std::unique_lock<std::mutex> lock(state_mutex);
        if (active_operations == 0) {
            throw std::runtime_error("CRIU runtime operation accounting underflow");
        }
        active_operations--;
        generation = coordinator->criu_requested_generation();
        if (!quiescing && active_operations == 0 && generation > last_completed_generation) {
            quiescing = true;
            should_quiesce = true;
        } else if (active_operations == 0) {
            state_cv.notify_all();
        }
    }

    coordinator->criu_mark_rank_running(peer_id, current_pid(), host_id, this->backend_name);
    if (should_quiesce) {
        quiesce(generation);
    }
}

void FMI::FT::CriuRuntime::shutdown() {
    coordinator->criu_mark_rank_running(peer_id, current_pid(), host_id, this->backend_name);
}

void FMI::FT::CriuRuntime::quiesce(std::uint64_t generation) {
    prepare_for_checkpoint();
    coordinator->criu_mark_rank_quiesced(peer_id, current_pid(), host_id, this->backend_name, generation);

    while (coordinator->criu_restore_generation() < generation) {
        std::this_thread::sleep_for(std::chrono::milliseconds(config.poll_ms));
    }

    coordinator->criu_mark_rank_running(peer_id, current_pid(), host_id, this->backend_name);

    {
        std::lock_guard<std::mutex> lock(state_mutex);
        last_completed_generation = std::max(last_completed_generation, generation);
        quiescing = false;
    }
    state_cv.notify_all();
}

std::string FMI::FT::CriuRuntime::resolve_host_id() const {
    if (!config.host_id.empty()) {
        return config.host_id;
    }

    char hostname[256] = {0};
    if (gethostname(hostname, sizeof(hostname)) != 0) {
        throw std::runtime_error("Could not determine host_id for CRIU runtime");
    }
    return hostname;
}

int FMI::FT::CriuRuntime::current_pid() const {
    return static_cast<int>(getpid());
}
