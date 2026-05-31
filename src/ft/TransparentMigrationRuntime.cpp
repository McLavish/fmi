#include "../../include/ft/TransparentMigrationRuntime.h"

#include <chrono>
#include <cstdlib>
#include <stdexcept>
#include <thread>

FMI::FT::TransparentMigrationRuntime::TransparentMigrationRuntime(
        FMI::Utils::peer_num peer_id,
        FMI::Utils::peer_num num_peers,
        std::string worker_id,
        std::string placement,
        std::uint64_t active_epoch,
        const FMI::Utils::FaultToleranceConfig& config,
        std::shared_ptr<FMI::FT::Coordinator> coordinator,
        std::string base_comm_name,
        std::function<void(const std::string&)> reconfigure_callback) :
        peer_id(peer_id),
        num_peers(num_peers),
        worker_id(std::move(worker_id)),
        placement(std::move(placement)),
        active_epoch(active_epoch),
        config(config),
        coordinator(std::move(coordinator)),
        base_comm_name(std::move(base_comm_name)),
        reconfigure_callback(std::move(reconfigure_callback)) {}

void FMI::FT::TransparentMigrationRuntime::enter_operation() {
    // Refresh membership liveness
    coordinator->refresh_lease(active_epoch, peer_id, worker_id);

    if (!coordinator->has_pending_migration()) {
        return;
    }

    if (coordinator->is_rank_pending(peer_id)) {
        // This rank is the migration target: quiesce and let the orchestrator relaunch
        coordinator->set_rank_state(active_epoch, peer_id, FMI::FT::RankState::Quiesced);
        std::exit(0);
    }

    // Survivor: participate in epoch promotion then reconfigure channels in place
    promote_and_reconfigure();
}

void FMI::FT::TransparentMigrationRuntime::exit_operation() {
    // No in-flight counter needed: transparent migration only quiesces between operations
}

void FMI::FT::TransparentMigrationRuntime::promote_and_reconfigure() {
    std::uint64_t next_epoch = active_epoch + 1;
    auto start = std::chrono::steady_clock::now();

    while (true) {
        auto observed = coordinator->epoch();
        if (observed > active_epoch) {
            // Epoch promoted — update state and reconfigure
            active_epoch = observed;
            coordinator->register_rank(active_epoch, peer_id, worker_id, FMI::FT::RankState::Active);
            if (!placement.empty()) {
                coordinator->set_placement(active_epoch, peer_id, placement);
            }
            coordinator->refresh_lease(active_epoch, peer_id, worker_id);
            reconfigure_callback(base_comm_name + "@epoch=" + std::to_string(active_epoch));
            return;
        }

        coordinator->register_rank(next_epoch, peer_id, worker_id, FMI::FT::RankState::Active);
        if (!placement.empty()) {
            coordinator->set_placement(next_epoch, peer_id, placement);
        }
        coordinator->refresh_lease(next_epoch, peer_id, worker_id);

        if (coordinator->live_member_count(next_epoch) >= num_peers) {
            coordinator->promote_epoch(next_epoch);
        }

        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - start).count();
        if (static_cast<unsigned int>(elapsed) >= coordinator->lease_ms()) {
            throw FMI::Utils::Timeout();
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(coordinator->heartbeat_ms()));
    }
}
