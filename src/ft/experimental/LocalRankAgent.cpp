#include "../../../include/ft/experimental/LocalRankAgent.h"
#include "../../../include/ft/experimental/CriuExec.h"
#include "../../../include/ft/experimental/CriuRequirements.h"
#include "../../../include/ft/experimental/HostId.h"
#include "../../../include/utils/Configuration.h"

#include <algorithm>
#include <exception>
#include <filesystem>
#include <memory>
#include <stdexcept>
#include <thread>
#include <unordered_set>
#include <vector>

namespace fs = std::filesystem;

FMI::FT::LocalRankAgent::LocalRankAgent(std::string config_path, std::string comm_name,
                                                 FMI::Utils::peer_num num_peers) :
        config_path(std::move(config_path)),
        comm_name(std::move(comm_name)),
        num_peers(num_peers) {
    FMI::Utils::Configuration configuration(this->config_path);
    config = configuration.get_fault_tolerance_config();
    // Validate the full configuration before opening the Redis control-plane connection.
    ensure_migration_mode();
    // The whole enabled channel set must be checkpoint-safe, not just the preferred backend
    // (mirrors the Communicator-side check; see require_checkpoint_safe_channels).
    FMI::FT::require_checkpoint_safe_channels(configuration);
    host_id = FMI::FT::resolve_host_id(config);
    control_plane = std::make_shared<FMI::FT::ControlPlane>(this->config_path, this->comm_name, num_peers);
}

void FMI::FT::LocalRankAgent::ensure_migration_mode() const {
    if (!config.enabled) {
        throw std::runtime_error("Rank agent requires fault tolerance to be enabled");
    }
    if (config.control_backend != "Redis") {
        throw std::runtime_error("Rank agent requires Redis as the control backend");
    }
    if (config.state_transfer != "criu") {
        throw std::runtime_error("Rank agent requires fault_tolerance.state_transfer=\"criu\"");
    }
    // Same Direct-only data-plane rule the rank-side runtime enforces (shared helper).
    require_checkpoint_safe_data_plane(config);
}

void FMI::FT::LocalRankAgent::cleanup() {
    auto images_path = fs::path(config.criu.images_dir) / comm_name;
    if (fs::exists(images_path)) {
        fs::remove_all(images_path);
    }
    control_plane->clear_criu_state();
}

std::uint64_t FMI::FT::LocalRankAgent::migrate_rank(FMI::Utils::peer_num rank) const {
    // A single-rank migration is just the degenerate batch — one code path for both.
    return migrate_ranks({rank});
}

std::uint64_t FMI::FT::LocalRankAgent::migrate_ranks(const std::vector<FMI::Utils::peer_num>& ranks) const {
    if (ranks.empty()) {
        return control_plane->epoch();
    }

    std::uint64_t current_epoch = control_plane->epoch();
    std::uint64_t target_epoch = current_epoch + 1;

    // Consistent cut: wait for EVERY target to reach its quiesce point on this host before
    // dumping any of them, so the single epoch promotion below releases them all together.
    auto ready = wait_for_ready_ranks(ranks, target_epoch);

    std::vector<std::string> dirs(ready.size());
    for (std::size_t i = 0; i < ready.size(); i++) {
        dirs[i] = rank_image_dir(target_epoch, ready[i].rank);
        fs::create_directories(dirs[i]);
    }

    // Parallel checkpoint: one thread per rank running ONLY criu dump+restore + filesystem work.
    // The control plane (one mutex-guarded Redis connection) is never touched here — all of
    // that stays on this thread (the waits above, the mark-running + promote below) — so the
    // workers share no mutable state beyond their own slot in `errors`.
    std::vector<std::exception_ptr> errors(ready.size());
    std::vector<std::thread> workers;
    workers.reserve(ready.size());
    for (std::size_t i = 0; i < ready.size(); i++) {
        workers.emplace_back([this, i, &ready, &dirs, &errors]() {
            try {
                // Dump terminates and reaps the (criu-ptraced) target, freeing its pid for restore;
                // restore recreates the process at the same pid with its memory, resuming inside
                // the rank's promotion-wait loop.
                dump_rank(ready[i].pid, dirs[i]);
                restore_rank(dirs[i]);
            } catch (...) {
                errors[i] = std::current_exception();
            }
        });
    }
    for (auto& worker : workers) {
        worker.join();
    }
    // If any rank failed to checkpoint, abort the whole cut WITHOUT promoting — a partial
    // checkpoint must never advance the epoch (the orchestrator owns failure handling).
    for (const auto& error : errors) {
        if (error) {
            std::rethrow_exception(error);
        }
    }

    // Clear each restored rank's checkpoint-ready marker so a watch loop does not re-trigger.
    for (const auto& info : ready) {
        control_plane->criu_mark_rank_running(info.rank, info.pid, host_id, info.backend);
    }

    // Promote the epoch once for the whole set: survivors and every restored rank observe N+1
    // and reconfigure their channels under the new epoch-qualified communicator name. A false
    // return means another actor already advanced the epoch to (at least) the target; the
    // restored ranks are released by that promotion just the same.
    control_plane->promote_epoch(target_epoch);
    return target_epoch;
}

std::uint64_t FMI::FT::LocalRankAgent::migrate_local() const {
    // Discover which ranks run on this host from the CRIU registry (each rank advertises its
    // host_id when its migration runtime is constructed). criu_rank_info() returns one entry per
    // registered rank, so no de-duplication is needed.
    auto current_epoch = control_plane->epoch();
    std::vector<FMI::Utils::peer_num> locals;
    for (const auto& info : control_plane->criu_rank_info()) {
        if (info.host_id != host_id) {
            continue;
        }
        // Skip a rank parked Quiesced for an epoch that has already passed (<= current): a leftover
        // from a prior migration that did not complete. It will never re-quiesce at the new target
        // (current+1), so including it would time out the whole batch in wait_for_ready_ranks. A
        // rank Quiesced at the upcoming target is a legitimate, already-ready candidate and is kept.
        if (info.state == CriuRankState::Quiesced && info.quiesced_generation <= current_epoch) {
            continue;
        }
        locals.push_back(info.rank);
    }
    if (locals.empty()) {
        return 0;
    }

    // Mark the whole local set for migration atomically, then batch-migrate it. request_migrations
    // is idempotent for ranks the orchestrator already marked pending, but throws if a DIFFERENT
    // cut is in flight (a pending rank outside this host's set) — evacuating two hosts must be
    // serialized by the orchestrator, one epoch cut at a time.
    control_plane->request_migrations(locals);
    return migrate_ranks(locals);
}

std::uint64_t FMI::FT::LocalRankAgent::watch_once() {
    // request_migration marks the targeted rank MIGRATION_PENDING (and it stays so until it
    // reaches its quiesce point and flips to QUIESCED). Either state means a migration of this
    // rank is in progress at the current epoch. Only ranks advertised on THIS host in the CRIU
    // registry are candidates: the agent can only dump a local process, and selecting a foreign
    // host's target would time out in wait_for_ready_ranks while a local request starves behind
    // it (directory_snapshot is rank-sorted, so a lower-numbered foreign rank would always win).
    //
    // This relies on a criu-configured comm only ever writing QUIESCED for a migration target:
    // survivors block in wait_for_promotion_and_reconfigure (no state write) and the exit-based
    // "none" path is unreachable under state_transfer="criu". If a future feature marks a rank
    // QUIESCED for another reason, this scan would need to disambiguate.
    FMI::Utils::peer_num target = 0;
    bool found = FMI::Utils::poll_until([this, &target]() {
        std::unordered_set<FMI::Utils::peer_num> local_ranks;
        for (const auto& info : control_plane->criu_rank_info()) {
            if (info.host_id == host_id) {
                local_ranks.insert(info.rank);
            }
        }
        for (const auto& entry : control_plane->directory_snapshot(control_plane->epoch())) {
            if ((entry.state == RankState::MigrationPending || entry.state == RankState::Quiesced) &&
                local_ranks.count(entry.rank) != 0) {
                target = entry.rank;
                return true;
            }
        }
        return false;
    }, config.criu.quiesce_timeout_ms, config.criu.poll_ms);

    return found ? migrate_rank(target) : 0;
}

std::vector<FMI::FT::CriuRankInfo> FMI::FT::LocalRankAgent::wait_for_ready_ranks(
        const std::vector<FMI::Utils::peer_num>& ranks, std::uint64_t target_epoch) const {
    std::vector<CriuRankInfo> ready;
    bool found = FMI::Utils::poll_until([this, &ranks, target_epoch, &ready]() {
        auto infos = control_plane->criu_rank_info();
        std::vector<CriuRankInfo> matched;
        for (auto rank : ranks) {
            auto it = std::find_if(infos.begin(), infos.end(), [this, rank, target_epoch](const CriuRankInfo& info) {
                return info.rank == rank && info.host_id == host_id &&
                       info.state == CriuRankState::Quiesced && info.quiesced_generation == target_epoch &&
                       info.pid > 0;
            });
            if (it == infos.end()) {
                return false;  // not all ranks ready yet
            }
            matched.push_back(*it);
        }
        ready = std::move(matched);
        return true;
    }, config.criu.quiesce_timeout_ms, config.criu.poll_ms);

    if (!found) {
        throw FMI::Utils::Timeout();
    }
    return ready;
}

std::string FMI::FT::LocalRankAgent::rank_image_dir(std::uint64_t target_epoch,
                                                        FMI::Utils::peer_num rank) const {
    return (fs::path(config.criu.images_dir) / comm_name / ("epoch-" + std::to_string(target_epoch)) /
            ("rank-" + std::to_string(rank))).string();
}

void FMI::FT::LocalRankAgent::dump_rank(int pid, const std::string& dir) const {
    // --tcp-close: the rank still holds its Redis control-plane connection; tell criu to close
    //   it on restore (the ControlPlane reconnects lazily) instead of trying to repair it.
    // No --leave-stopped: criu ptrace-seizes, dumps, then kills and reaps the task, freeing the
    //   pid so the immediate restore can reclaim it.
    // Operator flags (FMI_CRIU_EXTRA_ARGS) are appended inside run_criu.
    FMI::FT::run_criu({
            "criu", "dump",
            "-t", std::to_string(pid),
            "-D", dir,
            "-o", "dump.log",
            "--shell-job",
            "--tcp-close"
    });
}

void FMI::FT::LocalRankAgent::restore_rank(const std::string& dir) const {
    if (!fs::exists(dir)) {
        throw std::runtime_error("Missing CRIU image directory: " + dir);
    }
    FMI::FT::run_criu({
            "criu", "restore",
            "-D", dir,
            "-o", "restore.log",
            "--shell-job",
            "--tcp-close",
            "--restore-detached"
    });
}
