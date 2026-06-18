#include "../../../include/ft/experimental/MigrationSupervisor.h"
#include "../../../include/ft/experimental/CriuExec.h"
#include "../../../include/ft/experimental/HostId.h"

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <thread>

#include <unistd.h>

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
        config_path(std::move(config_path)),
        comm_name(std::move(comm_name)),
        num_peers(num_peers) {
    FMI::Utils::Configuration configuration(this->config_path);
    config = configuration.get_fault_tolerance_config();
    ensure_migration_mode();
    host_id = FMI::FT::resolve_host_id(config);
    supervisor_id = host_id + ":" + std::to_string(getpid());
    coordinator = std::make_shared<FMI::FT::Coordinator>(this->config_path, this->comm_name, num_peers);
}

void FMI::FT::MigrationSupervisor::ensure_migration_mode() const {
    if (!config.enabled) {
        throw std::runtime_error("Migration supervisor requires fault tolerance to be enabled");
    }
    if (config.control_backend != "Redis") {
        throw std::runtime_error("Migration supervisor requires Redis as the control backend");
    }
    if (config.state_transfer != "criu") {
        throw std::runtime_error("Migration supervisor requires fault_tolerance.state_transfer=\"criu\"");
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
    auto start = std::chrono::steady_clock::now();
    while (true) {
        auto epoch = coordinator->epoch();
        for (const auto& entry : coordinator->directory_snapshot(epoch)) {
            // request_migration marks the targeted rank MIGRATION_PENDING (and it stays so
            // until it reaches its quiesce point and flips to QUIESCED). Either state means a
            // migration of this rank is in progress at the current epoch.
            if (entry.state == RankState::MigrationPending || entry.state == RankState::Quiesced) {
                return migrate_rank(entry.rank);
            }
        }
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - start).count();
        if (static_cast<unsigned int>(elapsed) >= config.quiesce_timeout_ms) {
            return 0;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(config.poll_ms));
    }
}

FMI::FT::CriuRankInfo FMI::FT::MigrationSupervisor::wait_for_ready_rank(FMI::Utils::peer_num rank,
                                                                       std::uint64_t target_epoch) const {
    auto start = std::chrono::steady_clock::now();
    while (true) {
        for (const auto& info : coordinator->criu_rank_info()) {
            if (info.rank == rank && info.host_id == host_id &&
                info.state == CriuRankState::Quiesced && info.quiesced_generation == target_epoch &&
                info.pid > 0) {
                return info;
            }
        }
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - start).count();
        if (static_cast<unsigned int>(elapsed) >= config.quiesce_timeout_ms) {
            throw FMI::Utils::Timeout();
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(config.poll_ms));
    }
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
