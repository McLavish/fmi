#include <boost/test/unit_test.hpp>

#include "../include/fmi.h"
#include "../include/ft/TransparentMigrationRuntime.h"
#include "../include/ft/experimental/LocalRankAgent.h"

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <fstream>
#include <future>
#include <iterator>
#include <mutex>
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
            FMI::FT::ControlPlane control_plane(config_path, comm_name, num_peers);
            control_plane.clear_criu_state();
            control_plane.clear_job_state();
            return true;
        } catch (const std::exception& e) {
            BOOST_TEST_MESSAGE(std::string("redis_available exception: ") + e.what());
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

    // A distinguishable application SIGPIPE handler used to assert the ControlPlane does not
    // overwrite a disposition the application already installed.
    void sigpipe_probe_handler(int) {}

    // Read the current SIGPIPE disposition without leaving it changed (set, then restore).
    void (*current_sigpipe_handler())(int) {
        auto handler = std::signal(SIGPIPE, SIG_IGN);
        std::signal(SIGPIPE, handler);
        return handler;
    }

    fs::path write_criu_config(const fs::path& config_path,
                               const fs::path& images_dir,
                               const std::string& host_id,
                               bool enable_direct,
                               bool enable_data_redis) {
        // ofstream does not create missing parent directories; without this the config file is
        // never written, redis_available() throws "cannot open file", and the test silently
        // skips (skip == no assertions == green) — masking real coverage gaps.
        fs::create_directories(config_path.parent_path());
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
               "    \"preferred_data_backend\": \"Direct\",\n"
               "    \"state_transfer\": \"criu\",\n"
               "    \"criu\": {\n"
            << "      \"images_dir\": \"" << images_dir.string() << "\",\n"
               "      \"poll_ms\": 25,\n"
               "      \"quiesce_timeout_ms\": 2000,\n"
            << "      \"host_id\": \"" << host_id << "\"\n"
               "    }\n"
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

    // RAII setter for an environment variable, restored (or unset) on scope exit so a failing
    // assertion cannot leak the value into later test cases in the shared process.
    struct ScopedEnv {
        ScopedEnv(std::string name, const char* value) : name(std::move(name)) {
            const char* current = std::getenv(this->name.c_str());
            had_old = current != nullptr;
            if (had_old) {
                old_value = current;
            }
            setenv(this->name.c_str(), value, 1);
        }

        ~ScopedEnv() {
            if (had_old) {
                setenv(name.c_str(), old_value.c_str(), 1);
            } else {
                unsetenv(name.c_str());
            }
        }

        std::string name;
        std::string old_value;
        bool had_old;
    };

    fs::path write_mock_criu(const fs::path& bin_dir) {
        fs::create_directories(bin_dir);
        auto script_path = bin_dir / "criu";
        std::ofstream out(script_path);
        out << "#!/usr/bin/env bash\n"
               "set -euo pipefail\n"
               "all_args=\"$*\"\n"
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
               // Optional failure injection: when FMI_MOCK_FAIL_MODE matches this invocation, exit
               // non-zero before writing any marker, so a test can assert that a failed dump/restore
               // aborts the migration without promoting the epoch.
               "fail_mode=\"${FMI_MOCK_FAIL_MODE:-}\"\n"
               "if [ -n \"$fail_mode\" ] && [ \"$mode\" = \"$fail_mode\" ]; then\n"
               "  echo \"mock criu forced failure for $mode\" >&2\n"
               "  exit 1\n"
               "fi\n"
               // Optional ordering gate: when FMI_MOCK_GATE_MODE matches this invocation, signal
               // "entered" and block (bounded) until the test drops a "proceed" file. Lets a test
               // freeze the agent mid-step to observe the dump<restore<promote ordering.
               "gate_mode=\"${FMI_MOCK_GATE_MODE:-}\"\n"
               "gate_dir=\"${FMI_MOCK_GATE_DIR:-}\"\n"
               "if [ -n \"$gate_mode\" ] && [ \"$mode\" = \"$gate_mode\" ] && [ -n \"$gate_dir\" ]; then\n"
               "  mkdir -p \"$gate_dir\"\n"
               "  touch \"$gate_dir/entered\"\n"
               "  n=0\n"
               "  while [ ! -e \"$gate_dir/proceed\" ] && [ \"$n\" -lt 1000 ]; do sleep 0.05; n=$((n+1)); done\n"
               "fi\n"
               "mkdir -p \"$dir\"\n"
               "echo \"$mode\" > \"$dir/$mode.marker\"\n"
               "printf '%s\\n' \"$all_args\" > \"$dir/$mode.args\"\n"
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

BOOST_AUTO_TEST_CASE(rank_agent_dumps_restores_and_promotes_single_rank) {
    auto temp_dir = fs::temp_directory_path() / unique_comm_name("rank-agent");
    auto images_dir = temp_dir / "images";
    auto config_path = write_criu_config(temp_dir / "fmi-criu.json", images_dir, current_host_id(), true, false);
    auto mock_bin_dir = temp_dir / "bin";
    write_mock_criu(mock_bin_dir);

    std::string comm_name = unique_comm_name("rank-agent-job");
    if (!redis_available(config_path.string(), comm_name, 2)) {
        BOOST_TEST_MESSAGE("Skipping rank agent test because Redis is unavailable");
        fs::remove_all(temp_dir);
        return;
    }

    ScopedPathPrefix path_guard(mock_bin_dir);
    FMI::FT::ControlPlane control_plane(config_path.string(), comm_name, 2);
    control_plane.clear_criu_state();
    control_plane.clear_job_state();

    auto host_id = current_host_id();
    BOOST_CHECK_EQUAL(control_plane.epoch(), 0U);

    // Simulate the targeted rank reaching its CRIU quiesce point: publish a restorable image
    // entry (pid + host + QUIESCED) for the target epoch (current epoch + 1 = 1).
    const int target_pid = 4242;
    control_plane.criu_register_rank(0, target_pid, host_id, "Direct");
    control_plane.criu_mark_rank_quiesced(0, target_pid, host_id, "Direct", 1);

    FMI::FT::LocalRankAgent agent(config_path.string(), comm_name, 2);
    auto promoted_epoch = agent.migrate_rank(0);

    BOOST_CHECK_EQUAL(promoted_epoch, 1U);
    BOOST_CHECK_EQUAL(control_plane.epoch(), 1U);

    // The agent invoked mock criu dump + restore against the single-rank image dir.
    auto rank0_dir = images_dir / comm_name / "epoch-1" / "rank-0";
    BOOST_CHECK(fs::exists(rank0_dir / "dump.marker"));
    BOOST_CHECK(fs::exists(rank0_dir / "restore.marker"));

    // The checkpoint-ready marker is cleared so a watch loop does not re-trigger.
    bool rank0_running = false;
    for (const auto& info : control_plane.criu_rank_info()) {
        if (info.rank == 0) {
            rank0_running = info.state == FMI::FT::CriuRankState::Running;
        }
    }
    BOOST_CHECK(rank0_running);

    control_plane.clear_criu_state();
    control_plane.clear_job_state();
    fs::remove_all(temp_dir);
}

BOOST_AUTO_TEST_CASE(criu_runtime_scopes_sigpipe_to_default_disposition) {
    // The criu state-transfer path needs SIGPIPE ignored so a --tcp-close'd Redis socket
    // surfaces EPIPE instead of killing the restored rank. That disposition is process-global,
    // so the runtime (the rank-side criu owner) must only take it over when the application left
    // SIGPIPE at its default — never clobbering a handler the application installed itself.
    // No Redis needed: the runtime ctor arms SIGPIPE before it touches the control plane, so a
    // null control plane is fine (mirrors criu_state_transfer_rejects_non_direct_data_backend).
    FMI::Utils::FaultToleranceConfig config;
    config.enabled = true;
    config.control_backend = "Redis";
    config.state_transfer = "criu";
    config.preferred_data_backend = "Direct";

    auto original = current_sigpipe_handler();

    // From the default disposition, a criu-mode runtime arms SIG_IGN.
    std::signal(SIGPIPE, SIG_DFL);
    {
        FMI::FT::TransparentMigrationRuntime runtime(0, "worker", "", 0, config, nullptr, "comm",
                                                     [](const std::string&) {}, []() {});
        BOOST_CHECK(current_sigpipe_handler() == SIG_IGN);
    }

    // An application-installed handler is left untouched.
    std::signal(SIGPIPE, sigpipe_probe_handler);
    {
        FMI::FT::TransparentMigrationRuntime runtime(0, "worker", "", 0, config, nullptr, "comm",
                                                     [](const std::string&) {}, []() {});
        BOOST_CHECK(current_sigpipe_handler() == sigpipe_probe_handler);
    }

    std::signal(SIGPIPE, original);
}

BOOST_AUTO_TEST_CASE(rank_agent_promotes_epoch_only_after_dump_and_restore) {
    // "Promote last" is the core epoch-fencing invariant: survivors must not reconfigure to
    // N+1 while the migrated rank is still at N. Asserting only the final state cannot catch a
    // promote->dump->restore reordering. Freeze the mock criu mid-restore and assert the
    // ordering directly: dump already done, restore not yet, epoch still N — promotion only
    // lands after restore completes.
    auto temp_dir = fs::temp_directory_path() / unique_comm_name("ordering");
    auto images_dir = temp_dir / "images";
    auto config_path = write_criu_config(temp_dir / "fmi-criu.json", images_dir, current_host_id(), true, false);
    auto mock_bin_dir = temp_dir / "bin";
    write_mock_criu(mock_bin_dir);

    std::string comm_name = unique_comm_name("ordering-job");
    if (!redis_available(config_path.string(), comm_name, 2)) {
        BOOST_TEST_MESSAGE("Skipping ordering test because Redis is unavailable");
        fs::remove_all(temp_dir);
        return;
    }

    ScopedPathPrefix path_guard(mock_bin_dir);
    FMI::FT::ControlPlane control_plane(config_path.string(), comm_name, 2);
    control_plane.clear_criu_state();
    control_plane.clear_job_state();

    auto host_id = current_host_id();
    const int target_pid = 4242;
    control_plane.criu_register_rank(0, target_pid, host_id, "Direct");
    control_plane.criu_mark_rank_quiesced(0, target_pid, host_id, "Direct", 1);

    // Freeze the mock criu when it reaches the restore step.
    auto gate_dir = temp_dir / "gate";
    setenv("FMI_MOCK_GATE_MODE", "restore", 1);
    setenv("FMI_MOCK_GATE_DIR", gate_dir.string().c_str(), 1);

    FMI::FT::LocalRankAgent agent(config_path.string(), comm_name, 2);
    std::exception_ptr worker_error;
    std::uint64_t promoted = 0;
    std::thread worker([&]() {
        try {
            promoted = agent.migrate_rank(0);
        } catch (...) {
            worker_error = std::current_exception();
        }
    });

    auto poll = [](auto&& predicate) {
        for (int i = 0; i < 1000; ++i) {
            if (predicate()) {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        return false;
    };

    bool restore_in_progress = poll([&]() { return fs::exists(gate_dir / "entered"); });
    BOOST_CHECK(restore_in_progress);

    auto rank0_dir = images_dir / comm_name / "epoch-1" / "rank-0";
    // At this instant: dump completed, restore mid-flight, epoch not yet promoted.
    BOOST_CHECK(fs::exists(rank0_dir / "dump.marker"));
    BOOST_CHECK(!fs::exists(rank0_dir / "restore.marker"));
    BOOST_CHECK_EQUAL(control_plane.epoch(), 0U);

    // Let restore finish; promotion must come strictly after it.
    std::ofstream(gate_dir / "proceed").put('x');
    worker.join();
    if (worker_error) {
        std::rethrow_exception(worker_error);
    }

    BOOST_CHECK(fs::exists(rank0_dir / "restore.marker"));
    BOOST_CHECK_EQUAL(promoted, 1U);
    BOOST_CHECK_EQUAL(control_plane.epoch(), 1U);

    unsetenv("FMI_MOCK_GATE_MODE");
    unsetenv("FMI_MOCK_GATE_DIR");
    control_plane.clear_criu_state();
    control_plane.clear_job_state();
    fs::remove_all(temp_dir);
}

BOOST_AUTO_TEST_CASE(runtime_checkpoint_quiesce_publishes_image_then_reconfigures_on_promote) {
    // Cover the rank-side checkpoint_and_wait_for_restore sequence end to end, with no real
    // criu: drive a real TransparentMigrationRuntime on a pending rank and assert it releases
    // transport, publishes a restorable image entry (registered + QUIESCED for epoch N+1),
    // marks its epoch state QUIESCED, and — once the agent would promote — re-registers as
    // ACTIVE at N+1 and reconfigures its channels. The runtime's wait is unbounded, so it runs
    // on a worker thread while the test plays the agent's role (promote_epoch).
    auto temp_dir = fs::temp_directory_path() / unique_comm_name("rankside");
    auto images_dir = temp_dir / "images";
    auto config_path = write_criu_config(temp_dir / "fmi-criu.json", images_dir, current_host_id(), true, false);
    std::string comm_name = unique_comm_name("rankside-job");
    if (!redis_available(config_path.string(), comm_name, 2)) {
        BOOST_TEST_MESSAGE("Skipping rank-side checkpoint test because Redis is unavailable");
        fs::remove_all(temp_dir);
        return;
    }

    auto config = FMI::Utils::Configuration(config_path.string()).get_fault_tolerance_config();
    auto control_plane = std::make_shared<FMI::FT::ControlPlane>(config_path.string(), comm_name, 2);
    control_plane->clear_criu_state();
    control_plane->clear_job_state();
    control_plane->request_migration(0);  // rank 0 becomes the migration target at epoch 0

    std::atomic<bool> prepared{false};
    std::atomic<bool> reconfigured{false};
    std::mutex name_mutex;
    std::string reconfigured_name;

    FMI::FT::TransparentMigrationRuntime runtime(
            0, "worker-0", "", 0, config, control_plane, comm_name,
            [&](const std::string& new_name) {
                {
                    std::lock_guard<std::mutex> lock(name_mutex);
                    reconfigured_name = new_name;
                }
                reconfigured = true;
            },
            [&]() { prepared = true; });

    std::exception_ptr worker_error;
    std::thread worker([&]() {
        try {
            runtime.enter_operation();
        } catch (...) {
            worker_error = std::current_exception();
        }
    });

    auto poll = [](auto&& predicate) {
        for (int i = 0; i < 300; ++i) {
            if (predicate()) {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        return false;
    };

    // The rank publishes a checkpoint-ready image entry for epoch N+1 (= 1).
    bool image_ready = poll([&]() {
        for (const auto& info : control_plane->criu_rank_info()) {
            if (info.rank == 0 && info.state == FMI::FT::CriuRankState::Quiesced &&
                info.quiesced_generation == 1 && info.pid > 0) {
                return true;
            }
        }
        return false;
    });
    BOOST_CHECK(image_ready);
    // Transport was released before the image entry was published.
    BOOST_CHECK(prepared.load());
    // The rank does not advance the epoch itself; it blocks until the agent promotes.
    // (The epoch-N QUIESCED state write is not asserted here: directory_snapshot only returns
    // registered members, and this rank joins membership at N+1 — the rank agent's quiesce
    // signal is the CRIU registry entry checked above, not the old epoch's state hash.)
    BOOST_CHECK_EQUAL(control_plane->epoch(), 0U);
    BOOST_CHECK(!reconfigured.load());

    // Play the agent: promote to epoch 1. The restored rank resumes in its wait loop.
    control_plane->promote_epoch(1);
    bool did_reconfigure = poll([&]() { return reconfigured.load(); });
    worker.join();

    if (worker_error) {
        std::rethrow_exception(worker_error);
    }
    BOOST_CHECK(did_reconfigure);
    {
        std::lock_guard<std::mutex> lock(name_mutex);
        BOOST_CHECK_EQUAL(reconfigured_name, comm_name + "@epoch=1");
    }
    // The rank rejoined as ACTIVE under the new epoch.
    bool active_at_epoch1 = false;
    for (const auto& entry : control_plane->directory_snapshot(1)) {
        if (entry.rank == 0 && entry.state == FMI::FT::RankState::Active) {
            active_at_epoch1 = true;
        }
    }
    BOOST_CHECK(active_at_epoch1);

    control_plane->clear_criu_state();
    control_plane->clear_job_state();
    fs::remove_all(temp_dir);
}

BOOST_AUTO_TEST_CASE(criu_state_transfer_rejects_non_direct_data_backend) {
    // CRIU freezes the whole process image, so the data plane must be Direct — the only backend
    // that releases its sockets in prepare_for_checkpoint(). The runtime must fail fast at
    // construction if the data plane is pinned to anything else, rather than later dumping a
    // process with live Redis/S3 sockets. No Redis needed: the check runs before the control_plane
    // is touched, so a null control_plane is fine.
    FMI::Utils::FaultToleranceConfig config;
    config.enabled = true;
    config.control_backend = "Redis";
    config.state_transfer = "criu";

    config.preferred_data_backend = "Redis";
    BOOST_CHECK_THROW(
            FMI::FT::TransparentMigrationRuntime(
                    0, "worker", "", 0, config, nullptr, "comm",
                    [](const std::string&) {}, []() {}),
            std::runtime_error);

    config.preferred_data_backend = "Direct";
    BOOST_CHECK_NO_THROW(
            FMI::FT::TransparentMigrationRuntime(
                    0, "worker", "", 0, config, nullptr, "comm",
                    [](const std::string&) {}, []() {}));
}

BOOST_AUTO_TEST_CASE(watch_once_migrates_the_pending_member_rank) {
    // watch_once's unique job is candidate selection: scan the rank directory for a rank in
    // MigrationPending/Quiesced and migrate the first one. directory_snapshot only returns
    // registered members, so register a rank the supported way (construct a Communicator — no
    // collective is issued, so no rendezvous is needed), mark it for migration, and assert
    // watch_once discovers and migrates exactly that rank.
    auto temp_dir = fs::temp_directory_path() / unique_comm_name("watch");
    auto images_dir = temp_dir / "images";
    auto config_path = write_criu_config(temp_dir / "fmi-criu.json", images_dir, current_host_id(), true, false);
    auto mock_bin_dir = temp_dir / "bin";
    write_mock_criu(mock_bin_dir);

    std::string comm_name = unique_comm_name("watch-job");
    if (!redis_available(config_path.string(), comm_name, 2)) {
        BOOST_TEST_MESSAGE("Skipping watch_once test because Redis is unavailable");
        fs::remove_all(temp_dir);
        return;
    }

    ScopedPathPrefix path_guard(mock_bin_dir);
    FMI::FT::ControlPlane control_plane(config_path.string(), comm_name, 2);
    control_plane.clear_criu_state();
    control_plane.clear_job_state();

    // Rank 0 joins the directory as an ACTIVE member at epoch 0.
    FMI::Communicator rank0(0, 2, config_path.string(), comm_name, 128, "worker-0");

    // Mark it for migration and publish its checkpoint-ready image for epoch 1.
    control_plane.request_migration(0);
    auto host_id = current_host_id();
    const int target_pid = 4242;
    control_plane.criu_register_rank(0, target_pid, host_id, "Direct");
    control_plane.criu_mark_rank_quiesced(0, target_pid, host_id, "Direct", 1);

    FMI::FT::LocalRankAgent agent(config_path.string(), comm_name, 2);
    auto promoted = agent.watch_once();

    BOOST_CHECK_EQUAL(promoted, 1U);
    BOOST_CHECK_EQUAL(control_plane.epoch(), 1U);
    // The migrated rank was rank 0 specifically: its single-rank image dir was produced.
    auto rank0_dir = images_dir / comm_name / "epoch-1" / "rank-0";
    BOOST_CHECK(fs::exists(rank0_dir / "dump.marker"));
    BOOST_CHECK(fs::exists(rank0_dir / "restore.marker"));

    control_plane.clear_criu_state();
    control_plane.clear_job_state();
    fs::remove_all(temp_dir);
}

BOOST_AUTO_TEST_CASE(rank_agent_cleanup_removes_images_and_state) {
    // Repeated migrations accumulate per-epoch image trees; cleanup() must reclaim them and clear
    // the CRIU control-plane state (the agent previously had no cleanup path at all).
    auto temp_dir = fs::temp_directory_path() / unique_comm_name("cleanup");
    auto images_dir = temp_dir / "images";
    auto config_path = write_criu_config(temp_dir / "fmi-criu.json", images_dir, current_host_id(), true, false);
    std::string comm_name = unique_comm_name("cleanup-job");
    if (!redis_available(config_path.string(), comm_name, 2)) {
        BOOST_TEST_MESSAGE("Skipping cleanup test because Redis is unavailable");
        fs::remove_all(temp_dir);
        return;
    }

    FMI::FT::ControlPlane control_plane(config_path.string(), comm_name, 2);
    control_plane.clear_criu_state();
    control_plane.clear_job_state();

    // Residue from a prior migration: an image tree plus a CRIU registry entry.
    auto comm_images = images_dir / comm_name;
    fs::create_directories(comm_images / "epoch-1" / "rank-0");
    std::ofstream(comm_images / "epoch-1" / "rank-0" / "dump.marker").put('x');
    control_plane.criu_register_rank(0, 4242, current_host_id(), "Direct");
    BOOST_CHECK(fs::exists(comm_images));
    BOOST_CHECK(!control_plane.criu_rank_info().empty());

    FMI::FT::LocalRankAgent agent(config_path.string(), comm_name, 2);
    agent.cleanup();

    BOOST_CHECK(!fs::exists(comm_images));
    BOOST_CHECK(control_plane.criu_rank_info().empty());

    control_plane.clear_criu_state();
    control_plane.clear_job_state();
    fs::remove_all(temp_dir);
}

// Guards the config nesting: the rank agent's images_dir/poll_ms/quiesce_timeout_ms/host_id come
// from the nested fault_tolerance.criu.* block. A regression here would silently fall back to
// defaults (e.g. quiesce_timeout_ms 10000 instead of the configured value), which the agent tests
// would not catch (a larger-than-needed timeout still passes). No Redis needed — parse only.
BOOST_AUTO_TEST_CASE(criu_config_knobs_parse_from_nested_block) {
    auto temp_dir = fs::temp_directory_path() / unique_comm_name("criu-cfg");
    auto images_dir = temp_dir / "imgs";
    auto config_path = write_criu_config(temp_dir / "fmi.json", images_dir, "stable-host", true, false);

    FMI::Utils::Configuration cfg(config_path.string());
    auto ft = cfg.get_fault_tolerance_config();
    BOOST_CHECK_EQUAL(ft.criu.images_dir, images_dir.string());
    BOOST_CHECK_EQUAL(ft.criu.poll_ms, 25u);
    BOOST_CHECK_EQUAL(ft.criu.quiesce_timeout_ms, 2000u);
    BOOST_CHECK_EQUAL(ft.criu.host_id, "stable-host");

    fs::remove_all(temp_dir);
}

BOOST_AUTO_TEST_CASE(runtime_advertises_host_in_criu_registry_on_construction) {
    // For "migrate all local", the host-local agent must learn which ranks run on its host
    // BEFORE they quiesce. So a criu-mode runtime publishes a Running CRIU registry entry
    // carrying its host_id at construction (not only at the later quiesce point, which is too
    // late for discovery).
    auto temp_dir = fs::temp_directory_path() / unique_comm_name("advertise");
    auto images_dir = temp_dir / "images";
    auto host_id = current_host_id();
    auto config_path = write_criu_config(temp_dir / "fmi-criu.json", images_dir, host_id, true, false);
    std::string comm_name = unique_comm_name("advertise-job");
    if (!redis_available(config_path.string(), comm_name, 2)) {
        BOOST_TEST_MESSAGE("Skipping host-advertise test because Redis is unavailable");
        fs::remove_all(temp_dir);
        return;
    }

    auto config = FMI::Utils::Configuration(config_path.string()).get_fault_tolerance_config();
    auto control_plane = std::make_shared<FMI::FT::ControlPlane>(config_path.string(), comm_name, 2);
    control_plane->clear_criu_state();
    control_plane->clear_job_state();

    FMI::FT::TransparentMigrationRuntime runtime(
            0, "worker-0", "", 0, config, control_plane, comm_name,
            [](const std::string&) {}, []() {});

    bool advertised = false;
    for (const auto& info : control_plane->criu_rank_info()) {
        if (info.rank == 0 && info.state == FMI::FT::CriuRankState::Running &&
            info.host_id == host_id && info.pid > 0) {
            advertised = true;
        }
    }
    BOOST_CHECK(advertised);

    control_plane->clear_criu_state();
    control_plane->clear_job_state();
    fs::remove_all(temp_dir);
}

BOOST_AUTO_TEST_CASE(request_migrations_marks_every_rank_pending) {
    // Batch trigger: the orchestrator marks a whole set of ranks for migration in one call.
    // Asserted via the directory state (MIGRATION_PENDING) for each registered member; the
    // pending-set membership the rank side consumes is exercised by the end-to-end demo.
    auto temp_dir = fs::temp_directory_path() / unique_comm_name("req-migrations");
    auto images_dir = temp_dir / "images";
    auto config_path = write_criu_config(temp_dir / "fmi-criu.json", images_dir, current_host_id(), true, false);
    std::string comm_name = unique_comm_name("req-migrations-job");
    if (!redis_available(config_path.string(), comm_name, 3)) {
        BOOST_TEST_MESSAGE("Skipping request_migrations test because Redis is unavailable");
        fs::remove_all(temp_dir);
        return;
    }

    FMI::FT::ControlPlane control_plane(config_path.string(), comm_name, 3);
    control_plane.clear_criu_state();
    control_plane.clear_job_state();

    // Register three ACTIVE members at epoch 0 (no collective issued -> no rendezvous needed).
    FMI::Communicator rank0(0, 3, config_path.string(), comm_name, 128, "worker-0");
    FMI::Communicator rank1(1, 3, config_path.string(), comm_name, 128, "worker-1");
    FMI::Communicator rank2(2, 3, config_path.string(), comm_name, 128, "worker-2");

    control_plane.request_migrations({0, 1, 2});

    int pending_count = 0;
    for (const auto& entry : control_plane.directory_snapshot(0)) {
        if (entry.state == FMI::FT::RankState::MigrationPending) {
            pending_count++;
        }
    }
    BOOST_CHECK_EQUAL(pending_count, 3);

    control_plane.clear_criu_state();
    control_plane.clear_job_state();
    fs::remove_all(temp_dir);
}

BOOST_AUTO_TEST_CASE(migrate_ranks_dumps_restores_all_and_promotes_once) {
    // The batch executor checkpoints a whole set in one epoch cut: every rank gets a dump +
    // restore, and the epoch advances exactly once for the whole set.
    auto temp_dir = fs::temp_directory_path() / unique_comm_name("batch");
    auto images_dir = temp_dir / "images";
    auto host_id = current_host_id();
    auto config_path = write_criu_config(temp_dir / "fmi-criu.json", images_dir, host_id, true, false);
    auto mock_bin_dir = temp_dir / "bin";
    write_mock_criu(mock_bin_dir);

    std::string comm_name = unique_comm_name("batch-job");
    if (!redis_available(config_path.string(), comm_name, 3)) {
        BOOST_TEST_MESSAGE("Skipping batch migrate test because Redis is unavailable");
        fs::remove_all(temp_dir);
        return;
    }

    ScopedPathPrefix path_guard(mock_bin_dir);
    FMI::FT::ControlPlane control_plane(config_path.string(), comm_name, 3);
    control_plane.clear_criu_state();
    control_plane.clear_job_state();

    // All three ranks reach their quiesce point on this host for epoch 1.
    for (FMI::Utils::peer_num rank = 0; rank < 3; rank++) {
        control_plane.criu_register_rank(rank, 4000 + rank, host_id, "Direct");
        control_plane.criu_mark_rank_quiesced(rank, 4000 + rank, host_id, "Direct", 1);
    }

    FMI::FT::LocalRankAgent agent(config_path.string(), comm_name, 3);
    auto promoted = agent.migrate_ranks({0, 1, 2});

    BOOST_CHECK_EQUAL(promoted, 1U);
    BOOST_CHECK_EQUAL(control_plane.epoch(), 1U);
    for (FMI::Utils::peer_num rank = 0; rank < 3; rank++) {
        auto dir = images_dir / comm_name / "epoch-1" / ("rank-" + std::to_string(rank));
        BOOST_CHECK(fs::exists(dir / "dump.marker"));
        BOOST_CHECK(fs::exists(dir / "restore.marker"));
    }

    control_plane.clear_criu_state();
    control_plane.clear_job_state();
    fs::remove_all(temp_dir);
}

BOOST_AUTO_TEST_CASE(migrate_ranks_does_not_promote_when_a_rank_never_quiesces) {
    // Consistent cut: the batch must wait for EVERY target to quiesce before any dump. If one
    // never becomes ready, the whole batch times out and the epoch is NOT promoted (the
    // orchestrator detects the non-zero exit; survivors are never reconfigured to a half-cut).
    auto temp_dir = fs::temp_directory_path() / unique_comm_name("batch-timeout");
    auto images_dir = temp_dir / "images";
    auto host_id = current_host_id();
    auto config_path = write_criu_config(temp_dir / "fmi-criu.json", images_dir, host_id, true, false);
    auto mock_bin_dir = temp_dir / "bin";
    write_mock_criu(mock_bin_dir);

    std::string comm_name = unique_comm_name("batch-timeout-job");
    if (!redis_available(config_path.string(), comm_name, 2)) {
        BOOST_TEST_MESSAGE("Skipping batch timeout test because Redis is unavailable");
        fs::remove_all(temp_dir);
        return;
    }

    ScopedPathPrefix path_guard(mock_bin_dir);
    FMI::FT::ControlPlane control_plane(config_path.string(), comm_name, 2);
    control_plane.clear_criu_state();
    control_plane.clear_job_state();

    // Only rank 0 quiesces; rank 1 never does.
    control_plane.criu_register_rank(0, 4000, host_id, "Direct");
    control_plane.criu_mark_rank_quiesced(0, 4000, host_id, "Direct", 1);

    FMI::FT::LocalRankAgent agent(config_path.string(), comm_name, 2);
    BOOST_CHECK_THROW(agent.migrate_ranks({0, 1}), FMI::Utils::Timeout);

    BOOST_CHECK_EQUAL(control_plane.epoch(), 0U);
    BOOST_CHECK(!fs::exists(images_dir / comm_name / "epoch-1"));

    control_plane.clear_criu_state();
    control_plane.clear_job_state();
    fs::remove_all(temp_dir);
}

BOOST_AUTO_TEST_CASE(migrate_ranks_does_not_promote_when_dump_or_restore_fails) {
    // If any rank's criu dump/restore fails, the batch must abort without promoting — a partial
    // checkpoint must never advance the epoch.
    auto temp_dir = fs::temp_directory_path() / unique_comm_name("batch-fail");
    auto images_dir = temp_dir / "images";
    auto host_id = current_host_id();
    auto config_path = write_criu_config(temp_dir / "fmi-criu.json", images_dir, host_id, true, false);
    auto mock_bin_dir = temp_dir / "bin";
    write_mock_criu(mock_bin_dir);

    std::string comm_name = unique_comm_name("batch-fail-job");
    if (!redis_available(config_path.string(), comm_name, 2)) {
        BOOST_TEST_MESSAGE("Skipping batch failure test because Redis is unavailable");
        fs::remove_all(temp_dir);
        return;
    }

    ScopedPathPrefix path_guard(mock_bin_dir);
    FMI::FT::ControlPlane control_plane(config_path.string(), comm_name, 2);
    control_plane.clear_criu_state();
    control_plane.clear_job_state();

    for (FMI::Utils::peer_num rank = 0; rank < 2; rank++) {
        control_plane.criu_register_rank(rank, 4000 + rank, host_id, "Direct");
        control_plane.criu_mark_rank_quiesced(rank, 4000 + rank, host_id, "Direct", 1);
    }

    ScopedEnv fail("FMI_MOCK_FAIL_MODE", "restore");  // every restore fails
    FMI::FT::LocalRankAgent agent(config_path.string(), comm_name, 2);
    BOOST_CHECK_THROW(agent.migrate_ranks({0, 1}), std::exception);

    BOOST_CHECK_EQUAL(control_plane.epoch(), 0U);

    control_plane.clear_criu_state();
    control_plane.clear_job_state();
    fs::remove_all(temp_dir);
}

BOOST_AUTO_TEST_CASE(migrate_local_targets_only_host_local_ranks) {
    // "migrate all local": the agent discovers from the registry which ranks run on its host and
    // migrates exactly those, leaving ranks advertised on other hosts untouched.
    auto temp_dir = fs::temp_directory_path() / unique_comm_name("local");
    auto images_dir = temp_dir / "images";
    auto host_id = current_host_id();
    auto config_path = write_criu_config(temp_dir / "fmi-criu.json", images_dir, host_id, true, false);
    auto mock_bin_dir = temp_dir / "bin";
    write_mock_criu(mock_bin_dir);

    std::string comm_name = unique_comm_name("local-job");
    if (!redis_available(config_path.string(), comm_name, 3)) {
        BOOST_TEST_MESSAGE("Skipping migrate_local test because Redis is unavailable");
        fs::remove_all(temp_dir);
        return;
    }

    ScopedPathPrefix path_guard(mock_bin_dir);
    FMI::FT::ControlPlane control_plane(config_path.string(), comm_name, 3);
    control_plane.clear_criu_state();
    control_plane.clear_job_state();

    // Ranks 0,1 are local (this host); rank 2 is advertised on another host.
    control_plane.criu_register_rank(0, 4000, host_id, "Direct");
    control_plane.criu_mark_rank_quiesced(0, 4000, host_id, "Direct", 1);
    control_plane.criu_register_rank(1, 4001, host_id, "Direct");
    control_plane.criu_mark_rank_quiesced(1, 4001, host_id, "Direct", 1);
    control_plane.criu_register_rank(2, 4002, "some-other-host", "Direct");
    control_plane.criu_mark_rank_quiesced(2, 4002, "some-other-host", "Direct", 1);

    FMI::FT::LocalRankAgent agent(config_path.string(), comm_name, 3);
    auto promoted = agent.migrate_local();

    BOOST_CHECK_EQUAL(promoted, 1U);
    BOOST_CHECK_EQUAL(control_plane.epoch(), 1U);
    // Locals migrated, remote rank left alone.
    BOOST_CHECK(fs::exists(images_dir / comm_name / "epoch-1" / "rank-0" / "restore.marker"));
    BOOST_CHECK(fs::exists(images_dir / comm_name / "epoch-1" / "rank-1" / "restore.marker"));
    BOOST_CHECK(!fs::exists(images_dir / comm_name / "epoch-1" / "rank-2"));

    control_plane.clear_criu_state();
    control_plane.clear_job_state();
    fs::remove_all(temp_dir);
}

BOOST_AUTO_TEST_SUITE_END();
