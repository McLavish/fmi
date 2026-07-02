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

    // Give the replacement thread a moment to enter its constructor spin loop. The orchestrator
    // may only promote once the target has quiesced (promote_epoch enforces this), so mark the
    // target QUIESCED — in production the target rank writes this itself at its quiesce point.
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    control_plane.mark_rank_quiesced(0, 1);
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

// The wait for epoch promotion is unbounded: there is no library-side timeout. A replacement that
// is promoted long after it began waiting (here, 600 ms) would have thrown FMI::Utils::Timeout
// under the old bounded wait; now it must keep waiting and join.
BOOST_AUTO_TEST_CASE(transparent_migration_wait_for_promotion_is_unbounded) {
    std::string comm_name = unique_comm_name();
    if (!redis_available(comm_name)) {
        BOOST_TEST_MESSAGE("Skipping: Redis unavailable");
        return;
    }

    FMI::FT::ControlPlane control_plane(ft_config_path, comm_name, 2);
    control_plane.clear_job_state();

    FMI::Communicator rank0(0, 2, ft_config_path, comm_name, 128, "worker-a");
    FMI::Communicator rank1(1, 2, ft_config_path, comm_name, 128, "worker-b");

    control_plane.request_migration(1);

    std::future<std::string> replacement_comm_name = std::async(std::launch::async, [&]() {
        FMI::Communicator replacement(1, 2, ft_config_path, comm_name, 128, "worker-c");
        return replacement.get_comm_name();
    });

    // Delay promotion past the former bounded deadline; the replacement must still be waiting,
    // not failed. (Under the old wall-clock wait this 600 ms gap would have thrown Timeout.)
    std::this_thread::sleep_for(std::chrono::milliseconds(600));
    BOOST_CHECK(replacement_comm_name.wait_for(std::chrono::milliseconds(0)) == std::future_status::timeout);
    control_plane.mark_rank_quiesced(0, 1);
    control_plane.promote_epoch(1);

    BOOST_CHECK_EQUAL(replacement_comm_name.get(), comm_name + "@epoch=1");
    control_plane.clear_job_state();
}

// Promoting the epoch reclaims the per-epoch hashes of the epoch being left, so a long-running
// job with many migrations does not accumulate member/state/placement hashes in Redis.
BOOST_AUTO_TEST_CASE(transparent_migration_promotion_reclaims_old_epoch) {
    std::string comm_name = unique_comm_name();
    if (!redis_available(comm_name)) {
        BOOST_TEST_MESSAGE("Skipping: Redis unavailable");
        return;
    }

    FMI::FT::ControlPlane control_plane(ft_config_path, comm_name, 2);
    control_plane.clear_job_state();

    FMI::Communicator rank0(0, 2, ft_config_path, comm_name, 128, "worker-a", "vm");
    FMI::Communicator rank1(1, 2, ft_config_path, comm_name, 128, "worker-b", "vm");

    // Epoch 0's directory (members/states/placement) is populated before promotion.
    BOOST_REQUIRE_EQUAL(control_plane.directory_snapshot(0).size(), 2u);

    control_plane.promote_epoch(1);

    // Advancing to epoch 1 deletes the epoch-0 hashes, so its directory is now empty.
    BOOST_CHECK(control_plane.directory_snapshot(0).empty());

    control_plane.clear_job_state();
}

// Promotion is gated on quiescence: while a pending rank has not marked itself QUIESCED,
// promote_epoch must refuse (throw) and change nothing. Promoting past an un-quiesced target
// clears the pending set — the only signal telling that rank it is a migration target — so its
// next operation would rejoin the new epoch as a survivor next to its replacement.
BOOST_AUTO_TEST_CASE(transparent_migration_promotion_gated_on_quiescence) {
    std::string comm_name = unique_comm_name();
    if (!redis_available(comm_name)) {
        BOOST_TEST_MESSAGE("Skipping: Redis unavailable");
        return;
    }

    FMI::FT::ControlPlane control_plane(ft_config_path, comm_name, 2);
    control_plane.clear_job_state();

    control_plane.request_migration(1);

    // Target still MIGRATION_PENDING: promotion refuses and the epoch stays put.
    BOOST_CHECK_THROW(control_plane.promote_epoch(1), std::runtime_error);
    BOOST_CHECK_EQUAL(control_plane.epoch(), 0U);

    // Once the target quiesces the same promotion succeeds...
    control_plane.mark_rank_quiesced(0, 1);
    BOOST_CHECK(control_plane.promote_epoch(1));
    BOOST_CHECK_EQUAL(control_plane.epoch(), 1U);

    // ...and re-promoting to an already-reached epoch is a no-op (false), not an error.
    BOOST_CHECK(!control_plane.promote_epoch(1));

    control_plane.clear_job_state();
}

// One migration cut at a time: while ranks of one cut are pending, requesting a rank outside
// that set must be rejected loudly. Epoch promotion releases one global cut, so two overlapping
// cuts (e.g. two hosts evacuating concurrently) would silently drop each other's migrations:
// the first promotion clears the whole pending set, including the second cut's not-yet-quiesced
// ranks. Idempotent re-requests (same ranks, or a superset) stay allowed.
BOOST_AUTO_TEST_CASE(transparent_migration_rejects_overlapping_cut) {
    std::string comm_name = unique_comm_name();
    if (!redis_available(comm_name)) {
        BOOST_TEST_MESSAGE("Skipping: Redis unavailable");
        return;
    }

    FMI::FT::ControlPlane control_plane(ft_config_path, comm_name, 2);
    control_plane.clear_job_state();

    control_plane.request_migration(0);
    // Re-requesting the pending rank, or a superset containing it, is the same cut: allowed.
    BOOST_CHECK_NO_THROW(control_plane.request_migrations({0, 1}));
    // A disjoint request while the cut is in flight is a different cut: rejected.
    BOOST_CHECK_THROW(control_plane.request_migrations({0}), std::runtime_error);

    // Completing the cut (quiesce + promote) clears the pending set; new cuts are accepted again.
    control_plane.mark_rank_quiesced(0, 0);
    control_plane.mark_rank_quiesced(0, 1);
    BOOST_CHECK(control_plane.promote_epoch(1));
    BOOST_CHECK_NO_THROW(control_plane.request_migration(0));

    control_plane.clear_job_state();
}

BOOST_AUTO_TEST_SUITE_END();
