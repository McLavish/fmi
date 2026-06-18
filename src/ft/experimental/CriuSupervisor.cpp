#include "../../../include/ft/experimental/CriuSupervisor.h"
#include "../../../include/ft/experimental/CriuExec.h"

#include <algorithm>
#include <csignal>
#include <filesystem>
#include <stdexcept>

namespace fs = std::filesystem;

FMI::FT::CriuSupervisor::CriuSupervisor(std::string config_path, std::string comm_name, FMI::Utils::peer_num num_peers) :
        FmiFtSupervisor(std::move(config_path), std::move(comm_name), num_peers) {
    validate_base();
    connect();
}

std::uint64_t FMI::FT::CriuSupervisor::checkpoint() {
    auto generation = coordinator->criu_request_checkpoint(supervisor_id);
    wait_for_quiesce(generation);
    coordinator->criu_mark_job_quiesced(generation, supervisor_id);

    auto ranks = local_ranks();
    ensure_same_host_scope(ranks);
    for (const auto& rank : ranks) {
        dump_rank(rank, generation);
    }

    coordinator->criu_mark_checkpoint_complete(generation, supervisor_id);
    return generation;
}

std::uint64_t FMI::FT::CriuSupervisor::restore(std::uint64_t generation) {
    auto job = coordinator->criu_job_info();
    if (generation == 0) {
        generation = job.completed_generation;
    }
    if (generation == 0) {
        throw std::runtime_error("No completed CRIU checkpoint generation is available");
    }

    auto ranks = local_ranks();
    ensure_same_host_scope(ranks);
    (void) coordinator->criu_request_restore(generation, supervisor_id);

    for (const auto& rank : ranks) {
        if (rank.pid > 0) {
            kill(rank.pid, SIGKILL);
        }
    }

    for (const auto& rank : ranks) {
        restore_rank(rank, generation);
    }

    coordinator->criu_mark_job_restored(generation, supervisor_id);
    return generation;
}

FMI::FT::CriuStatus FMI::FT::CriuSupervisor::status() const {
    return {coordinator->criu_job_info(), coordinator->criu_rank_info()};
}

std::string FMI::FT::CriuSupervisor::generation_dir(std::uint64_t generation) const {
    return (fs::path(config.images_dir) / comm_name / ("generation-" + std::to_string(generation))).string();
}

std::vector<FMI::FT::CriuRankInfo> FMI::FT::CriuSupervisor::local_ranks() const {
    auto ranks = coordinator->criu_rank_info();
    std::vector<CriuRankInfo> local;
    for (const auto& rank : ranks) {
        if (rank.host_id == host_id) {
            local.push_back(rank);
        }
    }
    std::sort(local.begin(), local.end(), [](const CriuRankInfo& left, const CriuRankInfo& right) {
        return left.rank < right.rank;
    });
    return local;
}

void FMI::FT::CriuSupervisor::ensure_same_host_scope(const std::vector<CriuRankInfo>& ranks) const {
    if (ranks.size() != num_peers) {
        throw std::runtime_error("CRIU v1 only supports same-host jobs with all ranks registered on the supervisor host");
    }
    for (const auto& rank : ranks) {
        if (rank.host_id != host_id) {
            throw std::runtime_error("CRIU v1 only supports same-host restore");
        }
        if (rank.backend != "Direct") {
            throw std::runtime_error("CRIU v1 only supports the Direct backend");
        }
    }
}

void FMI::FT::CriuSupervisor::wait_for_quiesce(std::uint64_t generation) const {
    bool quiesced = FMI::Utils::poll_until(
            [&]() { return coordinator->criu_all_ranks_quiesced(generation, host_id); },
            config.quiesce_timeout_ms, config.poll_ms);
    if (!quiesced) {
        throw FMI::Utils::Timeout();
    }
}

void FMI::FT::CriuSupervisor::dump_rank(const CriuRankInfo& rank, std::uint64_t generation) const {
    auto rank_dir = fs::path(generation_dir(generation)) / ("rank-" + std::to_string(rank.rank));
    fs::create_directories(rank_dir);
    FMI::FT::run_criu({
            "criu",
            "dump",
            "-t", std::to_string(rank.pid),
            "-D", rank_dir.string(),
            "-o", "dump.log",
            "--shell-job",
            "--leave-stopped"
    });
}

void FMI::FT::CriuSupervisor::restore_rank(const CriuRankInfo& rank, std::uint64_t generation) const {
    auto rank_dir = fs::path(generation_dir(generation)) / ("rank-" + std::to_string(rank.rank));
    if (!fs::exists(rank_dir)) {
        throw std::runtime_error("Missing CRIU image directory: " + rank_dir.string());
    }
    FMI::FT::run_criu({
            "criu",
            "restore",
            "-D", rank_dir.string(),
            "-o", "restore.log",
            "--shell-job",
            "--restore-detached"
    });
}
