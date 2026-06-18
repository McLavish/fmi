#include "../../../include/ft/experimental/MigrationSupervisor.h"
#include "../../../include/ft/experimental/CriuExec.h"

#include <cstdlib>
#include <filesystem>
#include <sstream>
#include <stdexcept>

namespace fs = std::filesystem;

namespace {
    // Append any operator-supplied criu flags (space-separated) from FMI_CRIU_EXTRA_ARGS.
    // Lets a deployment pass environment-specific flags (e.g. --unprivileged for rootless
    // criu, or --tcp-established) without recompiling. Empty/unset by default.
    void append_extra_criu_args(std::vector<std::string>& args) {
        const char* extra = std::getenv("FMI_CRIU_EXTRA_ARGS");
        if (extra == nullptr) {
            return;
        }
        std::istringstream stream(extra);
        std::string token;
        while (stream >> token) {
            args.push_back(token);
        }
    }
}

FMI::FT::MigrationSupervisor::MigrationSupervisor(std::string config_path, std::string comm_name,
                                                 FMI::Utils::peer_num num_peers) :
        FmiFtSupervisor(std::move(config_path), std::move(comm_name), num_peers) {
    ensure_migration_mode();
    connect();
}

void FMI::FT::MigrationSupervisor::ensure_migration_mode() const {
    validate_base();
    if (config.state_transfer != "criu") {
        throw std::runtime_error("Migration supervisor requires fault_tolerance.state_transfer=\"criu\"");
    }
    // Same invariant the rank enforces: CRIU can only dump/restore a process whose data plane
    // is Direct (the sole backend that releases sockets before checkpoint). Refuse to drive a
    // migration against a comm configured for any other data backend.
    if (config.preferred_data_backend != "Direct") {
        throw std::runtime_error(
                "Migration supervisor requires preferred_data_backend=\"Direct\" for CRIU state transfer");
    }
}

std::uint64_t FMI::FT::MigrationSupervisor::migrate_rank(FMI::Utils::peer_num rank) {
    std::uint64_t current_epoch = coordinator->epoch();
    std::uint64_t target_epoch = current_epoch + 1;

    // Wait until the targeted rank has reached its quiesce point and published a restorable
    // image entry (pid + QUIESCED) on this host.
    auto rank_info = wait_for_ready_rank(rank, target_epoch);

    auto dir = rank_image_dir(target_epoch, rank);
    fs::create_directories(dir);

    // Dump terminates and reaps the (criu-ptraced) target, freeing its pid for restore.
    dump_rank(rank_info.pid, dir);
    // Recreate the process from its image: same pid, same memory, control flow resumes inside
    // the rank's promotion-wait loop.
    restore_rank(dir);

    // The restored rank no longer needs its checkpoint-ready marker; clear it so a watch loop
    // does not treat the entry as a fresh request.
    coordinator->criu_mark_rank_running(rank, rank_info.pid, host_id, rank_info.backend);

    // Promote the epoch last: survivors and the restored rank both observe N+1 and reconfigure
    // their channels under the new epoch-qualified communicator name.
    coordinator->promote_epoch(target_epoch);
    return target_epoch;
}

std::uint64_t FMI::FT::MigrationSupervisor::watch_once() {
    // request_migration marks the targeted rank MIGRATION_PENDING (and it stays so until it
    // reaches its quiesce point and flips to QUIESCED). Either state means a migration of this
    // rank is in progress at the current epoch.
    FMI::Utils::peer_num target = 0;
    bool found = FMI::Utils::poll_until([this, &target]() {
        for (const auto& entry : coordinator->directory_snapshot(coordinator->epoch())) {
            if (entry.state == RankState::MigrationPending || entry.state == RankState::Quiesced) {
                target = entry.rank;
                return true;
            }
        }
        return false;
    }, config.quiesce_timeout_ms, config.poll_ms);

    return found ? migrate_rank(target) : 0;
}

FMI::FT::CriuRankInfo FMI::FT::MigrationSupervisor::wait_for_ready_rank(FMI::Utils::peer_num rank,
                                                                       std::uint64_t target_epoch) const {
    CriuRankInfo ready;
    bool found = FMI::Utils::poll_until([this, rank, target_epoch, &ready]() {
        for (const auto& info : coordinator->criu_rank_info()) {
            if (info.rank == rank && info.host_id == host_id &&
                info.state == CriuRankState::Quiesced && info.quiesced_generation == target_epoch &&
                info.pid > 0) {
                ready = info;
                return true;
            }
        }
        return false;
    }, config.quiesce_timeout_ms, config.poll_ms);

    if (!found) {
        throw FMI::Utils::Timeout();
    }
    return ready;
}

std::string FMI::FT::MigrationSupervisor::rank_image_dir(std::uint64_t target_epoch,
                                                        FMI::Utils::peer_num rank) const {
    return (fs::path(config.images_dir) / comm_name / ("epoch-" + std::to_string(target_epoch)) /
            ("rank-" + std::to_string(rank))).string();
}

void FMI::FT::MigrationSupervisor::dump_rank(int pid, const std::string& dir) const {
    // --tcp-close: the rank still holds its Redis control-plane connection; tell criu to close
    //   it on restore (the Coordinator reconnects lazily) instead of trying to repair it.
    // No --leave-stopped: criu ptrace-seizes, dumps, then kills and reaps the task, freeing the
    //   pid so the immediate restore can reclaim it.
    std::vector<std::string> args = {
            "criu", "dump",
            "-t", std::to_string(pid),
            "-D", dir,
            "-o", "dump.log",
            "--shell-job",
            "--tcp-close"
    };
    append_extra_criu_args(args);
    FMI::FT::run_criu(args);
}

void FMI::FT::MigrationSupervisor::restore_rank(const std::string& dir) const {
    if (!fs::exists(dir)) {
        throw std::runtime_error("Missing CRIU image directory: " + dir);
    }
    std::vector<std::string> args = {
            "criu", "restore",
            "-D", dir,
            "-o", "restore.log",
            "--shell-job",
            "--tcp-close",
            "--restore-detached"
    };
    append_extra_criu_args(args);
    FMI::FT::run_criu(args);
}
