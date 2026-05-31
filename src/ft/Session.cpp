#include "../../include/ft/Session.h"
#include "../../include/utils/Configuration.h"

#include <chrono>
#include <stdexcept>
#include <thread>

#include <unistd.h>

namespace {
    std::string make_worker_id(FMI::Utils::peer_num peer_id) {
        auto now = std::chrono::system_clock::now().time_since_epoch();
        auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(now).count();
        return "rank-" + std::to_string(peer_id) + "-pid-" + std::to_string(getpid()) + "-ts-" + std::to_string(millis);
    }
}

FMI::FT::Session::Session(FMI::Utils::peer_num peer_id, FMI::Utils::peer_num num_peers, std::string config_path, std::string comm_name,
                          std::string worker_id, unsigned int faas_memory, std::string placement) :
        peer_id(peer_id),
        num_peers(num_peers),
        config_path(std::move(config_path)),
        base_comm_name(std::move(comm_name)),
        worker_id(worker_id.empty() ? make_worker_id(peer_id) : std::move(worker_id)),
        faas_memory(faas_memory),
        placement(std::move(placement)) {
    Utils::Configuration config(this->config_path);
    auto ft_config = config.get_fault_tolerance_config();
    ft_enabled = ft_config.enabled;
    if (!ft_enabled) {
        communicator = std::make_shared<FMI::Communicator>(peer_id, num_peers, this->config_path, base_comm_name, faas_memory);
        return;
    }
    if (ft_config.mode == FMI::FT::Mode::CriuCoordinated) {
        throw std::runtime_error("FMI::FT::Session does not support fault_tolerance.mode = criu_coordinated");
    }
    if (!ft_config.safe_point_only) {
        throw std::runtime_error("Only safe_point_only fault tolerance is supported");
    }

    coordinator = std::make_unique<FMI::FT::Coordinator>(this->config_path, base_comm_name, num_peers);
    active_epoch = coordinator->epoch();

    std::string current_worker = coordinator->worker_for_rank(active_epoch, peer_id);
    replacement_candidate = coordinator->is_rank_pending(peer_id) &&
                            !current_worker.empty() &&
                            current_worker != this->worker_id;

    if (!replacement_candidate) {
        coordinator->register_rank(active_epoch, peer_id, this->worker_id, RankState::Active);
        if (!placement.empty()) {
            coordinator->set_placement(active_epoch, peer_id, placement);
        }
        coordinator->refresh_lease(active_epoch, peer_id, this->worker_id);
    }

    build_communicator(active_epoch);
}

FMI::Communicator& FMI::FT::Session::comm() {
    if (replacement_candidate) {
        throw std::runtime_error("Replacement workers must call safe_point() before using the communicator");
    }
    return *communicator;
}

FMI::FT::Event FMI::FT::Session::safe_point() {
    if (!ft_enabled) {
        return Event::None;
    }

    auto observed_epoch = coordinator->epoch();
    if (observed_epoch > active_epoch) {
        active_epoch = observed_epoch;
        replacement_candidate = false;
        coordinator->register_rank(active_epoch, peer_id, worker_id, RankState::Active);
        if (!placement.empty()) {
            coordinator->set_placement(active_epoch, peer_id, placement);
        }
        coordinator->refresh_lease(active_epoch, peer_id, worker_id);
        build_communicator(active_epoch);
        return Event::Reconfigured;
    }

    if (!replacement_candidate) {
        coordinator->register_rank(active_epoch, peer_id, worker_id, RankState::Active);
        if (!placement.empty()) {
            coordinator->set_placement(active_epoch, peer_id, placement);
        }
        coordinator->refresh_lease(active_epoch, peer_id, worker_id);
    }

    if (!coordinator->has_pending_migration()) {
        return Event::None;
    }

    if (!replacement_candidate) {
        coordinator->set_rank_state(active_epoch, peer_id, RankState::Quiesced);
        if (coordinator->is_rank_pending(peer_id)) {
            return Event::MigrateSelf;
        }
    }

    std::uint64_t next_epoch = active_epoch + 1;
    RankState join_state = replacement_candidate ? RankState::Replaced : RankState::Active;
    coordinator->register_rank(next_epoch, peer_id, worker_id, join_state);
    if (!placement.empty()) {
        coordinator->set_placement(next_epoch, peer_id, placement);
    }
    coordinator->refresh_lease(next_epoch, peer_id, worker_id);

    auto start = std::chrono::steady_clock::now();
    while (true) {
        observed_epoch = coordinator->epoch();
        if (observed_epoch > active_epoch) {
            active_epoch = observed_epoch;
            replacement_candidate = false;
            coordinator->register_rank(active_epoch, peer_id, worker_id, RankState::Active);
            if (!placement.empty()) {
                coordinator->set_placement(active_epoch, peer_id, placement);
            }
            coordinator->refresh_lease(active_epoch, peer_id, worker_id);
            build_communicator(active_epoch);
            return Event::Reconfigured;
        }

        coordinator->register_rank(next_epoch, peer_id, worker_id, join_state);
        if (!placement.empty()) {
            coordinator->set_placement(next_epoch, peer_id, placement);
        }
        coordinator->refresh_lease(next_epoch, peer_id, worker_id);
        if (coordinator->live_member_count(next_epoch) >= num_peers) {
            coordinator->promote_epoch(next_epoch);
            continue;
        }

        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count();
        if (elapsed >= coordinator->lease_ms()) {
            throw FMI::Utils::Timeout();
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(coordinator->heartbeat_ms()));
    }
}

void FMI::FT::Session::build_communicator(std::uint64_t epoch) {
    std::string comm_name = base_comm_name;
    if (ft_enabled) {
        comm_name = epoch_comm_name(epoch);
    }
    communicator = std::make_shared<FMI::Communicator>(peer_id, num_peers, config_path, comm_name, faas_memory);
}

std::string FMI::FT::Session::epoch_comm_name(std::uint64_t epoch) const {
    return base_comm_name + "@epoch=" + std::to_string(epoch);
}

std::string FMI::FT::Session::default_worker_id(FMI::Utils::peer_num peer_id) {
    return make_worker_id(peer_id);
}
