#include "../../include/ft/TransparentMigrationRuntime.h"

#include <algorithm>
#include <cstdlib>
#include <stdexcept>

#include <unistd.h>
#ifdef FMI_ENABLE_CRIU
#include "../../include/ft/experimental/CriuRequirements.h"
#include "../../include/ft/experimental/HostId.h"
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
        ReconfigureCallback reconfigure_callback,
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
        // select must release its sockets before the dump. Pin the data plane to a checkpoint-
        // safe backend (Direct, DirectTCP or Redis — the ones whose prepare_for_checkpoint
        // releases every socket) so channel selection can never pick an unsupported one; the
        // rank agent enforces the same rule via this shared helper.
        require_checkpoint_safe_data_plane(config);

        // No SIGPIPE guard needed here anymore: the quiesce path holds the control-plane
        // connection closed while waiting to be dumped (socket-free wait), so the captured
        // image contains no established TCP socket and the restored rank's first write goes
        // to a freshly opened connection, never to a dead one. The data plane is covered by
        // the TCP transports' MSG_NOSIGNAL sends (Redis by hiredis reconnect-on-error).

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
    // The same round-trip publishes this rank's boundary index (progress telemetry), guarded
    // by active_epoch so a rank observing a stale epoch cannot pollute the new epoch's hash.
    auto snapshot = control_plane->observe_operation(peer_id, boundary_index, active_epoch);
    if (snapshot.epoch > active_epoch) {
        // A cut completed since our last boundary: rejoin at the new epoch. Safe even though
        // this rank never saw the cut itself — promote_epoch's cut gate guarantees the epoch
        // only advances once every member (this rank included) published a boundary >= the
        // cut, so a rank taking this branch was parked at the cut, not below it. If this rank
        // is also pending for a NEWER cut, the next boundary's snapshot handles it.
        wait_for_promotion_and_reconfigure();
        return;
    }

    if (!snapshot.any_pending) {
        return;
    }

    // Consensus cut: the request only takes effect at cut_index, fixed atomically by the first
    // rank that observed it. A rank below the cut keeps executing operations — including the
    // migration target — because a slower peer may already be inside one of those operations
    // and would otherwise block forever on a target that quiesced early (or read EOF
    // mid-collective from its closed sockets). Every rank therefore parks at the same
    // boundary, which is what makes closing the data-plane sockets at the quiesce point safe:
    // at a consensus boundary no operation is in flight anywhere. (cut_proposed distinguishes
    // a legitimate cut at boundary 0 from "no cut fixed yet".)
    if (snapshot.cut_proposed && boundary_index < snapshot.cut_index) {
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
        // Deliberately NO finalize_channels() here: this rank quiesces at the consensus cut
        // having completed operations its slower peers may still be running — on a
        // ClientServer data plane (Redis/S3) they still need to download this rank's uploaded
        // objects for those operations, and finalize would delete them out from under the
        // download loop (which then times out). The objects only become garbage once every
        // rank passes the cut, and by then this process is gone — so its epoch-N objects are
        // left behind by design (epoch-qualified names make them inert; job-level cleanup
        // reclaims the store). Process exit releases sockets and connections on its own.
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
    // guarantees a checkpoint-safe preferred_data_backend (Direct, DirectTCP or Redis) on this
    // path, so it is recorded truthfully.
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

    // Block until restored and promoted, holding the control-plane socket closed between
    // polls: with the data plane already released above, the dumped image then contains no
    // established TCP socket at all, so criu needs neither --tcp-established nor --tcp-close,
    // and the restored image's first control-plane call opens a fresh connection instead of
    // writing to a dead socket (which is what used to require the SIGPIPE guard). The rank
    // agent dumps this process here and restores it; the restored image resumes in this same
    // loop, observes epoch N+1, and reconfigures exactly like a survivor — carrying preserved
    // application memory.
    wait_for_promotion_and_reconfigure(true);
#endif
}

void FMI::FT::TransparentMigrationRuntime::wait_for_promotion_and_reconfigure(bool socket_free_wait) {
    // Unbounded wait: the library never promotes its own epoch, so a waiting rank can only
    // wait for the external actor (orchestrator or rank agent) to do it. A migration that
    // never completes is the orchestrator's responsibility to detect and resolve, not a
    // condition the library times out on. (Redis connectivity failures still surface as
    // exceptions from the control-plane client; only the "promotion never arrives" case waits.)
    //
    // The outer loop re-checks the epoch after reconfiguring: if another promotion landed
    // while this rank was joining/rebuilding, it rejoins again immediately instead of
    // returning to user code configured for an already-stale epoch. Each pass unions the
    // moved sets of every epoch crossed — a rank that skips epochs (e.g. it was slow to poll
    // while two back-to-back cuts completed) must drop its links to EVERY rank migrated in
    // between, not only the last cut's, or it would keep an established-but-dead socket to an
    // earlier cut's target and read EOF from it mid-collective later.
    while (true) {
        std::uint64_t observed = active_epoch;
        FMI::Utils::poll_until(
                [this, &observed, socket_free_wait]() {
                    observed = control_plane->epoch();
                    if (socket_free_wait && observed <= active_epoch) {
                        // Not promoted yet: close the just-used connection before sleeping, so
                        // a criu dump landing anywhere in the sleep captures a socket-free
                        // image.
                        control_plane->disconnect();
                    }
                    return observed > active_epoch;
                },
                0, config.poll_interval_ms);

        std::vector<FMI::Utils::peer_num> moved;
        for (std::uint64_t epoch = active_epoch + 1; epoch <= observed; epoch++) {
            auto epoch_moved = control_plane->moved_ranks(epoch);
            moved.insert(moved.end(), epoch_moved.begin(), epoch_moved.end());
        }
        std::sort(moved.begin(), moved.end());
        moved.erase(std::unique(moved.begin(), moved.end()), moved.end());

        active_epoch = observed;
        control_plane->join_epoch(active_epoch, peer_id, worker_id, placement);
        // After a CRIU restore the rebuilt channels must be ready before control returns to
        // user code; reconfigure installs the new epoch's channel set in place, dropping only
        // the links in the unioned moved set (selective re-pair).
        reconfigure_callback(epoch_comm_name(base_comm_name, active_epoch), moved);
        // Every rank parked at the same consensus boundary and resumes the new epoch with the
        // same next operation, so resetting here keeps boundary indices cohort-aligned —
        // including for a fresh replacement rank, whose counter starts at 0 by construction.
        boundary_index = 0;

        if (control_plane->epoch() == active_epoch) {
            return;
        }
        // Another promotion landed mid-rejoin: loop and catch up before touching user data.
    }
}
