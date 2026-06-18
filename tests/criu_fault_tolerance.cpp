#include <boost/test/unit_test.hpp>

#include "../include/fmi.h"
#include "../include/ft/experimental/CriuRuntime.h"
#include "../include/ft/experimental/MigrationSupervisor.h"

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <future>
#include <stdexcept>
#include <string>
#include <thread>

#include <unistd.h>

namespace fs = std::filesystem;

namespace {
    std::string repo_config_path(const std::string& name) {
        return (fs::path(__FILE__).parent_path().parent_path() / "config" / name).lexically_normal().string();
    }

    const std::string criu_config_path = repo_config_path("fmi_criu_test.json");

    std::string unique_comm_name(const std::string& prefix = "criu-ft-tests") {
        auto now = std::chrono::system_clock::now().time_since_epoch();
        auto nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(now).count();
        return prefix + "-" + std::to_string(nanos);
    }

    std::string current_host_id() {
        char hostname[256] = {0};
        if (gethostname(hostname, sizeof(hostname)) != 0) {
            throw std::runtime_error("Could not determine hostname for test");
        }
        return hostname;
    }

    bool redis_available(const std::string& config_path, const std::string& comm_name, FMI::Utils::peer_num num_peers = 2) {
        try {
            FMI::FT::Coordinator coordinator(config_path, comm_name, num_peers);
            coordinator.clear_criu_job_state();
            coordinator.clear_job_state();
            return true;
        } catch (const std::exception&) {
            return false;
        }
    }

    template<typename Predicate>
    void wait_until(Predicate&& predicate,
                    std::chrono::milliseconds timeout = std::chrono::milliseconds(2000),
                    std::chrono::milliseconds poll = std::chrono::milliseconds(25)) {
        auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            if (predicate()) {
                return;
            }
            std::this_thread::sleep_for(poll);
        }
        BOOST_FAIL("Timed out waiting for asynchronous condition");
    }

    std::string bool_string(bool value) {
        return value ? "true" : "false";
    }

    fs::path write_criu_config(const fs::path& config_path,
                               const fs::path& images_dir,
                               const std::string& host_id,
                               bool enable_direct,
                               bool enable_data_redis) {
        std::ofstream out(config_path);
        out << "{\n"
               "  \"backends\": {\n"
               "    \"S3\": {\n"
               "      \"enabled\": false,\n"
               "      \"bucket_name\": \"romanboe-uploadtest\",\n"
               "      \"s3_region\": \"eu-central-1\",\n"
               "      \"timeout\": 100,\n"
               "      \"max_timeout\": 1000\n"
               "    },\n"
               "    \"Redis\": {\n"
            << "      \"enabled\": " << bool_string(enable_data_redis) << ",\n"
               "      \"host\": \"127.0.0.1\",\n"
               "      \"port\": 6379,\n"
               "      \"timeout\": 1,\n"
               "      \"max_timeout\": 1000\n"
               "    },\n"
               "    \"Direct\": {\n"
            << "      \"enabled\": " << bool_string(enable_direct) << ",\n"
               "      \"host\": \"127.0.0.1\",\n"
               "      \"port\": 10000,\n"
               "      \"max_timeout\": 1000\n"
               "    }\n"
               "  },\n"
               "  \"model\": {\n"
               "    \"FaaS\": {\n"
               "      \"gib_second_price\": 0.0000166667\n"
               "    },\n"
               "    \"Redis\": {\n"
               "      \"bandwidth_single\": 100.0,\n"
               "      \"bandwidth_multiple\": 400.0,\n"
               "      \"overhead\": 5.2,\n"
               "      \"transfer_price\": 0.0,\n"
               "      \"instance_price\": 0.0038,\n"
               "      \"requests_per_hour\": 1000,\n"
               "      \"include_infrastructure_costs\": true\n"
               "    },\n"
               "    \"Direct\": {\n"
               "      \"bandwidth\": 400.0,\n"
               "      \"overhead\": 0.34,\n"
               "      \"transfer_price\": 0.0,\n"
               "      \"vm_price\": 0.0134,\n"
               "      \"requests_per_hour\": 1000,\n"
               "      \"include_infrastructure_costs\": true\n"
               "    }\n"
               "  },\n"
               "  \"fault_tolerance\": {\n"
               "    \"enabled\": true,\n"
               "    \"control_backend\": \"Redis\",\n"
               "    \"control_host\": \"127.0.0.1\",\n"
               "    \"control_port\": 6379,\n"
               "    \"poll_interval_ms\": 25,\n"
               "    \"reconfigure_timeout_ms\": 250,\n"
               "    \"preferred_data_backend\": \"Direct\",\n"
               "    \"state_transfer\": \"criu\",\n"
            << "    \"images_dir\": \"" << images_dir.string() << "\",\n"
               "    \"poll_ms\": 25,\n"
               "    \"quiesce_timeout_ms\": 2000,\n"
            << "    \"host_id\": \"" << host_id << "\"\n"
               "  }\n"
               "}\n";
        out.close();
        return config_path;
    }

    struct ScopedPathPrefix {
        explicit ScopedPathPrefix(const fs::path& bin_dir) {
            const char* current = std::getenv("PATH");
            old_value = current == nullptr ? "" : current;
            std::string next = bin_dir.string();
            if (!old_value.empty()) {
                next += ":" + old_value;
            }
            setenv("PATH", next.c_str(), 1);
        }

        ~ScopedPathPrefix() {
            if (old_value.empty()) {
                unsetenv("PATH");
            } else {
                setenv("PATH", old_value.c_str(), 1);
            }
        }

        std::string old_value;
    };

    fs::path write_mock_criu(const fs::path& bin_dir) {
        fs::create_directories(bin_dir);
        auto script_path = bin_dir / "criu";
        std::ofstream out(script_path);
        out << "#!/usr/bin/env bash\n"
               "set -euo pipefail\n"
               "mode=\"$1\"\n"
               "shift\n"
               "dir=\"\"\n"
               "log=\"\"\n"
               "while [ \"$#\" -gt 0 ]; do\n"
               "  case \"$1\" in\n"
               "    -D)\n"
               "      dir=\"$2\"\n"
               "      shift 2\n"
               "      ;;\n"
               "    -o)\n"
               "      log=\"$2\"\n"
               "      shift 2\n"
               "      ;;\n"
               "    *)\n"
               "      shift\n"
               "      ;;\n"
               "  esac\n"
               "done\n"
               "mkdir -p \"$dir\"\n"
               "echo \"$mode\" > \"$dir/$mode.marker\"\n"
               "if [ -n \"$log\" ]; then\n"
               "  echo \"$mode\" > \"$dir/$log\"\n"
               "fi\n";
        out.close();
        fs::permissions(script_path,
                        fs::perms::owner_exec | fs::perms::owner_read | fs::perms::owner_write |
                                fs::perms::group_exec | fs::perms::group_read |
                                fs::perms::others_exec | fs::perms::others_read,
                        fs::perm_options::replace);
        return script_path;
    }
}

BOOST_AUTO_TEST_SUITE(CriuFaultTolerance);

BOOST_AUTO_TEST_CASE(coordinator_tracks_criu_checkpoint_and_restore_state) {
    std::string comm_name = unique_comm_name();
    if (!redis_available(criu_config_path, comm_name, 2)) {
        BOOST_TEST_MESSAGE("Skipping CRIU coordinator state test because Redis is unavailable");
        return;
    }

    FMI::FT::Coordinator coordinator(criu_config_path, comm_name, 2);
    coordinator.clear_criu_job_state();
    coordinator.clear_job_state();

    auto host_id = current_host_id();
    coordinator.criu_register_rank(0, 111, host_id, "Direct");
    coordinator.criu_register_rank(1, 222, host_id, "Direct");

    auto info = coordinator.criu_job_info();
    BOOST_CHECK(info.state == FMI::FT::CriuJobState::Running);
    BOOST_CHECK_EQUAL(info.requested_generation, 0U);
    BOOST_CHECK_EQUAL(info.completed_generation, 0U);
    BOOST_CHECK_EQUAL(info.restore_generation, 0U);

    auto generation = coordinator.criu_request_checkpoint("supervisor-a");
    BOOST_CHECK_EQUAL(generation, 1U);
    BOOST_CHECK_EQUAL(coordinator.criu_requested_generation(), 1U);
    BOOST_CHECK(!coordinator.criu_all_ranks_quiesced(generation, host_id));

    coordinator.criu_mark_rank_quiesced(0, 111, host_id, "Direct", generation);
    BOOST_CHECK(!coordinator.criu_all_ranks_quiesced(generation, host_id));

    coordinator.criu_mark_rank_quiesced(1, 222, host_id, "Direct", generation);
    BOOST_CHECK(coordinator.criu_all_ranks_quiesced(generation, host_id));

    coordinator.criu_mark_job_quiesced(generation, "supervisor-a");
    BOOST_CHECK(coordinator.criu_job_info().state == FMI::FT::CriuJobState::Quiesced);

    coordinator.criu_mark_checkpoint_complete(generation, "supervisor-a");
    info = coordinator.criu_job_info();
    BOOST_CHECK(info.state == FMI::FT::CriuJobState::CheckpointComplete);
    BOOST_CHECK_EQUAL(info.completed_generation, generation);

    BOOST_CHECK_EQUAL(coordinator.criu_request_restore(generation, "supervisor-a"), generation);
    info = coordinator.criu_job_info();
    BOOST_CHECK(info.state == FMI::FT::CriuJobState::RestoreRequested);
    BOOST_CHECK_EQUAL(info.restore_generation, generation);

    coordinator.criu_mark_job_restored(generation, "supervisor-a");
    info = coordinator.criu_job_info();
    BOOST_CHECK(info.state == FMI::FT::CriuJobState::Restored);
    BOOST_CHECK_EQUAL(info.restore_generation, generation);

    auto ranks = coordinator.criu_rank_info();
    BOOST_REQUIRE_EQUAL(ranks.size(), 2U);
    BOOST_CHECK_EQUAL(ranks[0].rank, 0U);
    BOOST_CHECK_EQUAL(ranks[1].rank, 1U);
    BOOST_CHECK(ranks[0].state == FMI::FT::CriuRankState::Quiesced);
    BOOST_CHECK(ranks[1].state == FMI::FT::CriuRankState::Quiesced);
    BOOST_CHECK_EQUAL(ranks[0].quiesced_generation, generation);
    BOOST_CHECK_EQUAL(ranks[1].quiesced_generation, generation);

    coordinator.clear_criu_job_state();
    coordinator.clear_job_state();
}

BOOST_AUTO_TEST_CASE(runtime_quiesces_only_after_active_ops_drain) {
    std::string comm_name = unique_comm_name();
    if (!redis_available(criu_config_path, comm_name, 1)) {
        BOOST_TEST_MESSAGE("Skipping CRIU runtime test because Redis is unavailable");
        return;
    }

    FMI::FT::Coordinator coordinator(criu_config_path, comm_name, 1);
    coordinator.clear_criu_job_state();
    coordinator.clear_job_state();

    std::atomic<int> prepare_calls = 0;
    auto host_id = current_host_id();

    {
        FMI::FT::CriuRuntime runtime(0, 1, criu_config_path, comm_name, "Direct", [&prepare_calls]() {
            prepare_calls.fetch_add(1);
        });

        runtime.enter_operation();
        auto generation = coordinator.criu_request_checkpoint("supervisor-b");

        auto blocked_enter = std::async(std::launch::async, [&runtime]() {
            runtime.enter_operation();
            return true;
        });

        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        BOOST_CHECK(blocked_enter.wait_for(std::chrono::milliseconds(0)) == std::future_status::timeout);
        BOOST_CHECK_EQUAL(prepare_calls.load(), 0);

        auto exit_future = std::async(std::launch::async, [&runtime]() {
            runtime.exit_operation();
            return true;
        });

        wait_until([&prepare_calls]() { return prepare_calls.load() == 1; });
        wait_until([&coordinator, generation, &host_id]() {
            return coordinator.criu_all_ranks_quiesced(generation, host_id);
        });
        BOOST_CHECK(blocked_enter.wait_for(std::chrono::milliseconds(0)) == std::future_status::timeout);
        BOOST_CHECK(exit_future.wait_for(std::chrono::milliseconds(0)) == std::future_status::timeout);

        BOOST_CHECK_EQUAL(coordinator.criu_request_restore(generation, "supervisor-b"), generation);
        BOOST_CHECK(exit_future.wait_for(std::chrono::seconds(2)) == std::future_status::ready);
        BOOST_CHECK(blocked_enter.wait_for(std::chrono::seconds(2)) == std::future_status::ready);
        BOOST_CHECK(exit_future.get());
        BOOST_CHECK(blocked_enter.get());

        auto ranks = coordinator.criu_rank_info();
        BOOST_REQUIRE_EQUAL(ranks.size(), 1U);
        BOOST_CHECK(ranks[0].state == FMI::FT::CriuRankState::Running);
        BOOST_CHECK_EQUAL(ranks[0].quiesced_generation, generation);

        runtime.exit_operation();
        runtime.shutdown();
    }

    coordinator.clear_criu_job_state();
    coordinator.clear_job_state();
}

BOOST_AUTO_TEST_CASE(supervisor_uses_mock_criu_for_checkpoint_and_restore) {
    auto temp_dir = fs::temp_directory_path() / unique_comm_name("criu-supervisor");
    auto images_dir = temp_dir / "images";
    auto config_path = write_criu_config(temp_dir / "fmi-criu.json", images_dir, current_host_id(), true, false);
    auto mock_bin_dir = temp_dir / "bin";
    write_mock_criu(mock_bin_dir);

    std::string comm_name = unique_comm_name("criu-supervisor-job");
    if (!redis_available(config_path.string(), comm_name, 2)) {
        BOOST_TEST_MESSAGE("Skipping CRIU supervisor test because Redis is unavailable");
        fs::remove_all(temp_dir);
        return;
    }

    ScopedPathPrefix path_guard(mock_bin_dir);
    FMI::FT::Coordinator coordinator(config_path.string(), comm_name, 2);
    coordinator.clear_criu_job_state();
    coordinator.clear_job_state();

    auto host_id = current_host_id();
    coordinator.criu_register_rank(0, 0, host_id, "Direct");
    coordinator.criu_register_rank(1, 0, host_id, "Direct");

    FMI::FT::CriuSupervisor supervisor(config_path.string(), comm_name, 2);
    auto checkpoint_future = std::async(std::launch::async, [&supervisor]() {
        return supervisor.checkpoint();
    });

    wait_until([&coordinator]() { return coordinator.criu_requested_generation() == 1; });
    coordinator.criu_mark_rank_quiesced(0, 0, host_id, "Direct", 1);
    coordinator.criu_mark_rank_quiesced(1, 0, host_id, "Direct", 1);

    BOOST_REQUIRE(checkpoint_future.wait_for(std::chrono::seconds(2)) == std::future_status::ready);
    auto checkpoint_generation = checkpoint_future.get();
    BOOST_CHECK_EQUAL(checkpoint_generation, 1U);

    auto rank0_dir = images_dir / comm_name / "generation-1" / "rank-0";
    auto rank1_dir = images_dir / comm_name / "generation-1" / "rank-1";
    BOOST_CHECK(fs::exists(rank0_dir / "dump.marker"));
    BOOST_CHECK(fs::exists(rank1_dir / "dump.marker"));
    BOOST_CHECK(coordinator.criu_job_info().state == FMI::FT::CriuJobState::CheckpointComplete);

    auto restore_generation = supervisor.restore(checkpoint_generation);
    BOOST_CHECK_EQUAL(restore_generation, checkpoint_generation);
    BOOST_CHECK(fs::exists(rank0_dir / "restore.marker"));
    BOOST_CHECK(fs::exists(rank1_dir / "restore.marker"));
    BOOST_CHECK(coordinator.criu_job_info().state == FMI::FT::CriuJobState::Restored);

    supervisor.cleanup();
    BOOST_CHECK(!fs::exists(images_dir / comm_name));
    BOOST_CHECK(coordinator.criu_rank_info().empty());

    coordinator.clear_job_state();
    fs::remove_all(temp_dir);
}

BOOST_AUTO_TEST_CASE(migration_supervisor_dumps_restores_and_promotes_single_rank) {
    auto temp_dir = fs::temp_directory_path() / unique_comm_name("migration-supervisor");
    auto images_dir = temp_dir / "images";
    auto config_path = write_criu_config(temp_dir / "fmi-criu.json", images_dir, current_host_id(), true, false);
    auto mock_bin_dir = temp_dir / "bin";
    write_mock_criu(mock_bin_dir);

    std::string comm_name = unique_comm_name("migration-supervisor-job");
    if (!redis_available(config_path.string(), comm_name, 2)) {
        BOOST_TEST_MESSAGE("Skipping migration supervisor test because Redis is unavailable");
        fs::remove_all(temp_dir);
        return;
    }

    ScopedPathPrefix path_guard(mock_bin_dir);
    FMI::FT::Coordinator coordinator(config_path.string(), comm_name, 2);
    coordinator.clear_criu_job_state();
    coordinator.clear_job_state();

    auto host_id = current_host_id();
    BOOST_CHECK_EQUAL(coordinator.epoch(), 0U);

    // Simulate the targeted rank reaching its CRIU quiesce point: publish a restorable image
    // entry (pid + host + QUIESCED) for the target epoch (current epoch + 1 = 1).
    const int target_pid = 4242;
    coordinator.criu_register_rank(0, target_pid, host_id, "Direct");
    coordinator.criu_mark_rank_quiesced(0, target_pid, host_id, "Direct", 1);

    FMI::FT::MigrationSupervisor supervisor(config_path.string(), comm_name, 2);
    auto promoted_epoch = supervisor.migrate_rank(0);

    BOOST_CHECK_EQUAL(promoted_epoch, 1U);
    BOOST_CHECK_EQUAL(coordinator.epoch(), 1U);

    // The supervisor invoked mock criu dump + restore against the single-rank image dir.
    auto rank0_dir = images_dir / comm_name / "epoch-1" / "rank-0";
    BOOST_CHECK(fs::exists(rank0_dir / "dump.marker"));
    BOOST_CHECK(fs::exists(rank0_dir / "restore.marker"));

    // The checkpoint-ready marker is cleared so a watch loop does not re-trigger.
    bool rank0_running = false;
    for (const auto& info : coordinator.criu_rank_info()) {
        if (info.rank == 0) {
            rank0_running = info.state == FMI::FT::CriuRankState::Running;
        }
    }
    BOOST_CHECK(rank0_running);

    coordinator.clear_criu_job_state();
    coordinator.clear_job_state();
    fs::remove_all(temp_dir);
}

BOOST_AUTO_TEST_SUITE_END();
