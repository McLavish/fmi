#include "../../../include/ft/experimental/CriuSupervisor.h"
#include "../../../include/ft/experimental/HostId.h"

#include <algorithm>
#include <chrono>
#include <csignal>
#include <filesystem>
#include <stdexcept>
#include <thread>

#include <sys/wait.h>
#include <unistd.h>

namespace fs = std::filesystem;

FMI::FT::CriuSupervisor::CriuSupervisor(std::string config_path, std::string comm_name, FMI::Utils::peer_num num_peers) :
        config_path(std::move(config_path)),
        comm_name(std::move(comm_name)),
        num_peers(num_peers) {
    FMI::Utils::Configuration configuration(this->config_path);
    config = configuration.get_fault_tolerance_config();
    ensure_criu_mode();
    host_id = FMI::FT::resolve_host_id(config);
    supervisor_id = host_id + ":" + std::to_string(getpid());
    coordinator = std::make_shared<FMI::FT::Coordinator>(this->config_path, this->comm_name, num_peers);
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

void FMI::FT::CriuSupervisor::cleanup() {
    auto images_path = fs::path(config.images_dir) / comm_name;
    if (fs::exists(images_path)) {
        fs::remove_all(images_path);
    }
    coordinator->clear_criu_job_state();
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

void FMI::FT::CriuSupervisor::ensure_criu_mode() const {
    if (!config.enabled) {
        throw std::runtime_error("CRIU supervisor requires fault tolerance to be enabled");
    }
    if (config.control_backend != "Redis") {
        throw std::runtime_error("CRIU supervisor requires Redis as the control backend");
    }
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
    auto start = std::chrono::steady_clock::now();
    while (true) {
        if (coordinator->criu_all_ranks_quiesced(generation, host_id)) {
            return;
        }

        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - start).count();
        if (elapsed >= config.quiesce_timeout_ms) {
            throw FMI::Utils::Timeout();
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(config.poll_ms));
    }
}

void FMI::FT::CriuSupervisor::dump_rank(const CriuRankInfo& rank, std::uint64_t generation) const {
    auto rank_dir = fs::path(generation_dir(generation)) / ("rank-" + std::to_string(rank.rank));
    fs::create_directories(rank_dir);
    run_criu({
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
    run_criu({
            "criu",
            "restore",
            "-D", rank_dir.string(),
            "-o", "restore.log",
            "--shell-job",
            "--restore-detached"
    });
}

void FMI::FT::CriuSupervisor::run_criu(const std::vector<std::string>& args) {
    std::vector<char*> argv;
    argv.reserve(args.size() + 1);
    for (const auto& arg : args) {
        argv.push_back(const_cast<char*>(arg.c_str()));
    }
    argv.push_back(nullptr);

    pid_t pid = fork();
    if (pid < 0) {
        throw std::runtime_error("fork() failed while launching criu");
    }
    if (pid == 0) {
        execvp(argv[0], argv.data());
        _exit(127);
    }

    int status = 0;
    if (waitpid(pid, &status, 0) < 0) {
        throw std::runtime_error("waitpid() failed while waiting for criu");
    }
    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        throw std::runtime_error("criu command failed with exit code " +
                                 std::to_string(WIFEXITED(status) ? WEXITSTATUS(status) : -1));
    }
}
