#include "../../include/ft/TransparentMigrationRuntime.h"

#include <cstdlib>
#include <stdexcept>

#include <unistd.h>
#ifdef FMI_ENABLE_CRIU
#include "../../include/ft/experimental/HostId.h"
#include <sys/prctl.h>
#endif

FMI::FT::TransparentMigrationRuntime::TransparentMigrationRuntime(
        FMI::Utils::peer_num peer_id,
        std::string worker_id,
        std::string placement,
        std::uint64_t active_epoch,
        const FMI::Utils::FaultToleranceConfig& config,
        std::shared_ptr<FMI::FT::Coordinator> coordinator,
        std::string base_comm_name,
        std::function<void(const std::string&)> reconfigure_callback,
        std::function<void()> prepare_for_checkpoint) :
        peer_id(peer_id),
        worker_id(std::move(worker_id)),
        placement(std::move(placement)),
        active_epoch(active_epoch),
        config(config),
        coordinator(std::move(coordinator)),
        base_comm_name(std::move(base_comm_name)),
        reconfigure_callback(std::move(reconfigure_callback)),
        prepare_for_checkpoint(std::move(prepare_for_checkpoint)) {
    if (config.state_transfer == "criu") {
#ifndef FMI_ENABLE_CRIU
        throw std::runtime_error(
                "fault_tolerance.state_transfer=\"criu\" requires a build with FMI_ENABLE_CRIU=ON");
#endif
    } else if (config.state_transfer != "none") {
        throw std::runtime_error("Unknown fault_tolerance.state_transfer: " + config.state_transfer);
    }
}

void FMI::FT::TransparentMigrationRuntime::enter_operation() {
    auto observed = coordinator->epoch();
    if (observed > active_epoch) {
        wait_for_promotion_and_reconfigure(config.reconfigure_timeout_ms);
        return;
    }

    if (!coordinator->has_pending_migration()) {
        return;
    }

    if (coordinator->is_rank_pending(peer_id)) {
        // This rank is the migration target.
        if (config.state_transfer == "criu") {
            // Preserve application state: checkpoint this process and let the supervisor
            // restore it. Control resumes (in the restored image) at the epoch-N+1 rebuild.
            checkpoint_and_wait_for_restore();
            return;
        }
        // Default (state_transfer == "none"): quiesce and exit; a fresh replacement recomputes.
        coordinator->set_rank_state(active_epoch, peer_id, FMI::FT::RankState::Quiesced);
        std::exit(0);
    }

    // Survivor: park until the orchestrator promotes the epoch, then rebuild channels in place.
    wait_for_promotion_and_reconfigure(config.reconfigure_timeout_ms);
}

void FMI::FT::TransparentMigrationRuntime::exit_operation() {
    // No in-flight counter needed: transparent migration only quiesces between operations
}

void FMI::FT::TransparentMigrationRuntime::checkpoint_and_wait_for_restore() {
#ifdef FMI_ENABLE_CRIU
    std::uint64_t target_epoch = active_epoch + 1;
    // Data backend name recorded in the CRIU image registry, and this host's identity for
    // same-host scoping. Resolved here so non-CRIU migration pays for neither.
    std::string backend_name = config.preferred_data_backend.empty() ? "Direct" : config.preferred_data_backend;
    std::string host_id = FMI::FT::resolve_host_id(config);

    // Permit the host-local supervisor's (non-parent) criu to ptrace-seize this process under
    // yama ptrace_scope=1. Best-effort: harmlessly fails where YAMA is not present.
    prctl(PR_SET_PTRACER, PR_SET_PTRACER_ANY, 0, 0, 0);

    // Release data-plane transport so the captured image holds no live sockets; the restored
    // process re-pairs lazily under the epoch-N+1 communicator name.
    if (prepare_for_checkpoint) {
        prepare_for_checkpoint();
    }

    // Publish a restorable image entry the supervisor can find: pid + host + QUIESCED for the
    // target epoch. Reuses the CRIU rank registry (single-rank scope).
    int pid = static_cast<int>(getpid());
    coordinator->criu_register_rank(peer_id, pid, host_id, backend_name);
    coordinator->criu_mark_rank_quiesced(peer_id, pid, host_id, backend_name, target_epoch);
    coordinator->set_rank_state(active_epoch, peer_id, FMI::FT::RankState::Quiesced);

    // Block until restored and promoted. The supervisor dumps this process here and restores
    // it; the restored image resumes in this same loop, observes epoch N+1, and reconfigures
    // exactly like a survivor — carrying preserved application memory. Unbounded: the dump /
    // restore span must not race a wall-clock deadline.
    wait_for_promotion_and_reconfigure(0);
#endif
}

void FMI::FT::TransparentMigrationRuntime::wait_for_promotion_and_reconfigure(unsigned int timeout_ms) {
    std::uint64_t observed = active_epoch;
    bool promoted = FMI::Utils::poll_until(
            [this, &observed]() { observed = coordinator->epoch(); return observed > active_epoch; },
            timeout_ms, config.poll_interval_ms);
    if (!promoted) {
        throw FMI::Utils::Timeout();
    }

    active_epoch = observed;
    coordinator->register_rank(active_epoch, peer_id, worker_id, FMI::FT::RankState::Active);
    if (!placement.empty()) {
        coordinator->set_placement(active_epoch, peer_id, placement);
    }
    // After a CRIU restore the rebuilt channels must be ready before control returns to user
    // code; reconfigure installs the epoch-N+1 channel set in place.
    reconfigure_callback(base_comm_name + "@epoch=" + std::to_string(active_epoch));
}
