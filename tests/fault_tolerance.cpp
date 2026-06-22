#include <boost/test/unit_test.hpp>

#include "../include/fmi.h"

#include <chrono>
#include <filesystem>
#include <future>
#include <thread>

namespace {
    namespace fs = std::filesystem;

    std::string repo_config_path(const std::string& name) {
        return (fs::path(__FILE__).parent_path().parent_path() / "config" / name).lexically_normal().string();
    }

    const std::string ft_config_path = repo_config_path("fmi_ft_test.json");
    const std::string missing_direct_config_path = repo_config_path("fmi_ft_direct_missing.json");

    std::string unique_comm_name() {
        auto now = std::chrono::system_clock::now().time_since_epoch();
        auto nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(now).count();
        return "ft-tests-" + std::to_string(nanos);
    }

    bool redis_available(const std::string& comm_name) {
        try {
            FMI::FT::ControlPlane control_plane(ft_config_path, comm_name, 2);
            control_plane.clear_job_state();
            return true;
        } catch (const std::exception&) {
            return false;
        }
    }
}

BOOST_AUTO_TEST_SUITE(FaultTolerance);

// Preferred data backend listed in FT config but not in active backends → throw
BOOST_AUTO_TEST_CASE(preferred_backend_missing) {
    BOOST_CHECK_THROW(FMI::Communicator(0, 2, missing_direct_config_path, "ft-missing"), std::runtime_error);
}

// In transparent_migration mode the Communicator adopts an epoch-fenced comm_name
BOOST_AUTO_TEST_CASE(transparent_migration_epoch_fenced_comm_name) {
    std::string comm_name = unique_comm_name();
    if (!redis_available(comm_name)) {
        BOOST_TEST_MESSAGE("Skipping: Redis unavailable");
        return;
    }

    FMI::FT::ControlPlane control_plane(ft_config_path, comm_name, 2);
    control_plane.clear_job_state();

    FMI::Communicator rank0(0, 2, ft_config_path, comm_name, 128, "worker-a");
    FMI::Communicator rank1(1, 2, ft_config_path, comm_name, 128, "worker-b");

    BOOST_CHECK_EQUAL(rank0.get_comm_name(), comm_name + "@epoch=0");
    BOOST_CHECK_EQUAL(rank1.get_comm_name(), comm_name + "@epoch=0");

    control_plane.clear_job_state();
}

// Placement is stored in the rank directory and visible via directory_snapshot
BOOST_AUTO_TEST_CASE(transparent_migration_placement_in_directory) {
    std::string comm_name = unique_comm_name();
    if (!redis_available(comm_name)) {
        BOOST_TEST_MESSAGE("Skipping: Redis unavailable");
        return;
    }

    FMI::FT::ControlPlane control_plane(ft_config_path, comm_name, 2);
    control_plane.clear_job_state();

    FMI::Communicator rank0(0, 2, ft_config_path, comm_name, 128, "worker-a", "vm");
    FMI::Communicator rank1(1, 2, ft_config_path, comm_name, 128, "worker-b", "vm");

    auto dir = control_plane.directory_snapshot(0);
    BOOST_REQUIRE_EQUAL(dir.size(), 2u);
    auto entry0 = *std::find_if(dir.begin(), dir.end(), [](const FMI::FT::RankDirectoryEntry& e){ return e.rank == 0; });
    auto entry1 = *std::find_if(dir.begin(), dir.end(), [](const FMI::FT::RankDirectoryEntry& e){ return e.rank == 1; });
    BOOST_CHECK_EQUAL(entry0.placement, "vm");
    BOOST_CHECK_EQUAL(entry1.placement, "vm");
    BOOST_CHECK_EQUAL(entry0.worker_id, "worker-a");
    BOOST_CHECK_EQUAL(entry1.worker_id, "worker-b");

    control_plane.clear_job_state();
}

// Replacement rank constructor waits while its rank is pending, then joins the externally promoted epoch.
BOOST_AUTO_TEST_CASE(transparent_migration_replacement_joins_next_epoch) {
    std::string comm_name = unique_comm_name();
    if (!redis_available(comm_name)) {
        BOOST_TEST_MESSAGE("Skipping: Redis unavailable");
        return;
    }

    FMI::FT::ControlPlane control_plane(ft_config_path, comm_name, 2);
    control_plane.clear_job_state();

    // Rank 0 and rank 1 register into epoch 0
    FMI::Communicator rank0(0, 2, ft_config_path, comm_name, 128, "worker-a");
    FMI::Communicator rank1(1, 2, ft_config_path, comm_name, 128, "worker-b");

    // Trigger migration for rank 1
    control_plane.request_migration(1);

    // Launch replacement in a thread; if it observes pending, its constructor blocks until promotion clears it.
    std::future<std::string> replacement_comm_name = std::async(std::launch::async, [&]() {
        FMI::Communicator replacement(1, 2, ft_config_path, comm_name, 128, "worker-c", "serverless");
        return replacement.get_comm_name();
    });

    // Give the replacement thread a moment to enter its constructor spin loop,
    // then promote epoch 1 externally as the centralized orchestrator.
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    control_plane.promote_epoch(1);

    std::string rep_name = replacement_comm_name.get();
    BOOST_CHECK_EQUAL(rep_name, comm_name + "@epoch=1");

    auto dir1 = control_plane.directory_snapshot(1);
    auto rep = std::find_if(dir1.begin(), dir1.end(), [](const FMI::FT::RankDirectoryEntry& e){ return e.rank == 1; });
    BOOST_REQUIRE(rep != dir1.end());
    BOOST_CHECK_EQUAL(rep->worker_id, "worker-c");
    BOOST_CHECK_EQUAL(rep->placement, "serverless");

    control_plane.clear_job_state();
}

BOOST_AUTO_TEST_SUITE_END();
