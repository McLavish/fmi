#include "../../include/ft/TransparentMigrationRuntime.h"

#include <cstdlib>
#include <stdexcept>

#include <unistd.h>
#ifdef FMI_ENABLE_CRIU
#include "../../include/ft/experimental/CriuRequirements.h"
#include "../../include/ft/experimental/HostId.h"
#include <csignal>
#include <sys/prctl.h>
#endif

FMI::FT::TransparentMigrationRuntime::TransparentMigrationRuntime(
        FMI::Utils::peer_num peer_id,
        std::string worker_id,
        std::string placement,
        std::uint64_t active_epoch,
        const FMI::Utils::FaultToleranceConfig& config,
        std::shared_ptr<FMI::FT::ControlPlane> control_plane,
        std::string base_comm_name,
        std::function<void(const std::string&)> reconfigure_callback,
        std::function<void()> prepare_for_checkpoint,
        std::function<void()> finalize_channels) :
        peer_id(peer_id),
        worker_id(std::move(worker_id)),
        placement(std::move(placement)),
        active_epoch(active_epoch),
        config(config),
        control_plane(std::move(control_plane)),
        base_comm_name(std::move(base_comm_name)),
        reconfigure_callback(std::move(reconfigure_callback)),
        prepare_for_checkpoint(std::move(prepare_for_checkpoint)),
        finalize_channels(std::move(finalize_channels)) {
    if (config.state_transfer == "criu") {
#ifdef FMI_ENABLE_CRIU
        // CRIU freezes the whole process image, so every data-plane channel the policy might
        // select must release its sockets before the dump. Pin the data plane to Direct (the only
        // backend that releases sockets) so channel selection can never pick an unsupported one;
        // the rank agent enforces the same rule via this shared helper.
        require_checkpoint_safe_data_plane(config);

        // criu's --tcp-close drops the Redis control socket across checkpoint/restore, so the
        // restored rank's next write would raise SIGPIPE and — under the default disposition — be
        // killed before the ControlPlane can reconnect. Ignore SIGPIPE (process-global) so the
        // write fails with EPIPE and command() reconnects transparently. Armed here, on the rank
        // that is actually checkpointed, so it is captured in the image; never overwrite a handler
        // the application already installed; intentionally not restored (the rank keeps this
        // disposition for the rest of the FT session). Only this rank-side path needs it — the
        // rank agent / orchestrator processes are not --tcp-closed, so they no longer over-arm it.
        auto previous = std::signal(SIGPIPE, SIG_IGN);
        if (previous != SIG_DFL && previous != SIG_ERR) {
            std::signal(SIGPIPE, previous);
        }

        // Advertise this rank's host in the CRIU registry now, at construction, so the
        // host-local rank agent can discover which ranks run on its host BEFORE they reach
        // their quiesce point — the prerequisite for driving "migrate all local". The quiesce
        // path (checkpoint_and_wait_for_restore) later re-marks the same entry QUIESCED for the
        // target epoch. Guarded on a live control plane: some unit tests construct the runtime
        // with a null control plane to exercise the construction-time checks above.
        if (this->control_plane != nullptr) {
            this->control_plane->criu_register_rank(
                    this->peer_id, static_cast<int>(getpid()),
                    resolve_host_id(config), config.preferred_data_backend);
        }
#else
        throw std::runtime_error(
                "fault_tolerance.state_transfer=\"criu\" requires a build with FMI_ENABLE_CRIU=ON");
#endif
    } else if (config.state_transfer != "none") {
        throw std::runtime_error("Unknown fault_tolerance.state_transfer: " + config.state_transfer);
    }
}

void FMI::FT::TransparentMigrationRuntime::enter_operation() {
    if (control_plane == nullptr) {
        // A null control plane is only ever used by construction-time unit tests, which never run
        // an operation. Reaching here means a runtime was wired without coordination — fail loudly
        // instead of dereferencing null.
        throw std::logic_error("TransparentMigrationRuntime::enter_operation requires a control plane");
    }
    // One atomic control-plane snapshot per operation boundary: the epoch, the pending set and
    // the consensus cut boundary are read in a single script, so a concurrent promotion (which
    // bumps the epoch and clears all of them in one script on its side) can never slip between
    // the reads — and the steady state costs exactly one Redis round-trip instead of three.
    // The same round-trip publishes this rank's boundary index (progress telemetry).
    auto snapshot = control_plane->observe_operation(peer_id, boundary_index);
    if (snapshot.epoch > active_epoch) {
        // A cut completed since our last boundary: rejoin at the new epoch first. If this rank
        // is also pending for a NEWER cut, the next boundary's snapshot handles it.
        wait_for_promotion_and_reconfigure();
        return;
    }

    if (!snapshot.any_pending) {
        return;
    }

    // Consensus cut: the request only takes effect at cut_index, fixed atomically by the first
    // rank that observed it (to that rank's boundary + 1). A rank below the cut keeps executing
    // operations — including the migration target — because a slower peer may already be inside
    // one of those operations and would otherwise block forever on a target that quiesced early
    // (or read EOF mid-collective from its closed sockets). Every rank therefore parks at the
    // same boundary, which is what makes closing the data-plane sockets at the quiesce point
    // safe: at a consensus boundary no operation is in flight anywhere.
    if (snapshot.cut_index > 0 && boundary_index < snapshot.cut_index) {
        return;
    }

    if (snapshot.self_pending) {
        // This rank is the migration target.
        if (config.state_transfer == "criu") {
            // Preserve application state: checkpoint this process and let the rank agent
            // restore it. Control resumes (in the restored image) at the epoch-N+1 rebuild.
            checkpoint_and_wait_for_restore();
            return;
        }
        // Default (state_transfer == "none"): quiesce and exit; a fresh replacement recomputes.
        // The QUIESCED marker is what promote_epoch's gate waits for, so the orchestrator can
        // only promote after this write — the exiting process can never race its replacement.
        control_plane->mark_rank_quiesced(active_epoch, peer_id);
        // std::exit does not unwind, so ~Communicator (which finalizes channels) never runs.
        // Finalize here so a Redis/S3 data plane deletes this rank's epoch-N objects instead of
        // leaking them on every migration. Best-effort: we are terminating regardless.
        if (finalize_channels) {
            try {
                finalize_channels();
            } catch (...) {
                // ignore — the rank is exiting
            }
        }
        std::exit(0);
    }

    // Survivor: park until the orchestrator promotes the epoch, then rebuild channels in place.
    wait_for_promotion_and_reconfigure();
}

void FMI::FT::TransparentMigrationRuntime::exit_operation() {
    // Count completed operations: the boundary index published at enter_operation and compared
    // against the consensus cut_index. Incremented in the guard's exit so a parked-and-resumed
    // operation is counted exactly once, after it actually ran.
    boundary_index++;
}

void FMI::FT::TransparentMigrationRuntime::checkpoint_and_wait_for_restore() {
#ifdef FMI_ENABLE_CRIU
    std::uint64_t target_epoch = active_epoch + 1;
    // Data backend name recorded in the CRIU image registry, and this host's identity for
    // same-host scoping. Resolved here so non-CRIU migration pays for neither. The constructor
    // guarantees preferred_data_backend == "Direct" on this path, so it is recorded truthfully.
    std::string backend_name = config.preferred_data_backend;
    std::string host_id = FMI::FT::resolve_host_id(config);

    // Permit the host-local rank agent's (non-parent) criu to ptrace-seize this process under
    // yama ptrace_scope=1. Best-effort: harmlessly fails where YAMA is not present.
    prctl(PR_SET_PTRACER, PR_SET_PTRACER_ANY, 0, 0, 0);

    // Release data-plane transport so the captured image holds no live sockets; the restored
    // process re-pairs lazily under the epoch-N+1 communicator name.
    if (prepare_for_checkpoint) {
        prepare_for_checkpoint();
    }

    // Mark this rank QUIESCED in the epoch states hash FIRST: promote_epoch's gate reads it, and
    // the CRIU registry entry below is the agent's trigger to dump and later promote. Writing the
    // registry entry first would open a window where the agent dumps (killing this process
    // mid-sequence) and then promotes against a states hash that still says MIGRATION_PENDING.
    control_plane->mark_rank_quiesced(active_epoch, peer_id);

    // Then mark the registry entry (already advertised at construction) QUIESCED for the target
    // epoch, so the rank agent can find a restorable image: pid + host + QUIESCED. No re-register
    // here — mark_quiesced would immediately overwrite it.
    int pid = static_cast<int>(getpid());
    control_plane->criu_mark_rank_quiesced(peer_id, pid, host_id, backend_name, target_epoch);

    // Block until restored and promoted. The rank agent dumps this process here and restores
    // it; the restored image resumes in this same loop, observes epoch N+1, and reconfigures
    // exactly like a survivor — carrying preserved application memory.
    wait_for_promotion_and_reconfigure();
#endif
}

void FMI::FT::TransparentMigrationRuntime::wait_for_promotion_and_reconfigure() {
    // Unbounded wait: the library never promotes its own epoch, so a waiting rank can only
    // wait for the external actor (orchestrator or rank agent) to do it. A migration that
    // never completes is the orchestrator's responsibility to detect and resolve, not a
    // condition the library times out on. (Redis connectivity failures still surface as
    // exceptions from the control-plane client; only the "promotion never arrives" case waits.)
    std::uint64_t observed = active_epoch;
    FMI::Utils::poll_until(
            [this, &observed]() { observed = control_plane->epoch(); return observed > active_epoch; },
            0, config.poll_interval_ms);

    active_epoch = observed;
    control_plane->join_epoch(active_epoch, peer_id, worker_id, placement);
    // After a CRIU restore the rebuilt channels must be ready before control returns to user
    // code; reconfigure installs the epoch-N+1 channel set in place.
    reconfigure_callback(epoch_comm_name(base_comm_name, active_epoch));
    // Every rank parked at the same consensus boundary and resumes the new epoch with the same
    // next operation, so resetting here keeps boundary indices cohort-aligned — including for a
    // fresh replacement rank, whose counter starts at 0 by construction.
    boundary_index = 0;
}
