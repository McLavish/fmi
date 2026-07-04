#include <boost/test/unit_test.hpp>

#include "../include/fmi.h"

#include <array>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <future>
#include <mutex>
#include <random>
#include <sstream>
#include <thread>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

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

    // The cut-timing stress test moves real data over Direct, so it additionally needs a
    // tcpunchd rendezvous server; probe for it so the suite still passes on infra-less hosts.
    bool tcp_port_open(const char* host, int port) {
        int fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) {
            return false;
        }
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(static_cast<uint16_t>(port));
        inet_pton(AF_INET, host, &addr.sin_addr);
        bool ok = ::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0;
        ::close(fd);
        return ok;
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

#ifdef FMI_ENABLE_CRIU
// Cut-consistency stress: real allreduce traffic over Direct while migration cuts land at
// random offsets relative to the ranks' operation boundaries.
//
// The transparent-migration protocol assumes every rank observes a pending migration at the
// SAME operation index. observe_operation() is a per-rank read at that rank's own boundary, so
// a request that becomes visible between survivor B's enter_operation(op j) and target A's
// enter_operation(op j) leaves B inside op j blocked on A while A parks at its j boundary and
// closes its sockets — B then sees EOF mid-collective (or times out). This test tries to land
// requests inside that window: the target rank staggers its entry into every operation, and the
// orchestrator fires request_migration at a random offset once all ranks pass an iteration
// threshold. state_transfer="criu" is used because the targeted rank then PARKS at its quiesce
// point instead of exiting, which lets all ranks live as threads of the test process (no
// criu binary is involved: promotion is what un-parks the target, exactly as after a restore).
//
// A clean cut keeps the test green (all ranks park at the same boundary, rejoin at epoch N+1,
// and the loop values stay in lockstep). Only an inconsistent cut turns it red.
// Parameterized over the data plane: the Direct variant needs a tcpunchd rendezvous server,
// the Redis variant only needs the same Redis that serves as control plane.
static void run_cut_timing_stress(const std::string& stress_config_file, bool needs_tcpunchd) {
    // Two ranks keep the TCPunch rendezvous load minimal (one pairing per epoch): rank 0 is the
    // reduce root / survivor, rank 1 the staggered migration target. The race needs exactly one
    // survivor already inside an operation the target has not entered, so two ranks suffice.
    constexpr unsigned int world = 2;
    constexpr FMI::Utils::peer_num target_rank = 1;    // the staggered rank is always the target
    constexpr int cuts = 6;
    constexpr int first_cut_iter = 30;
    constexpr int iters_between_cuts = 40;
    constexpr int total_iters = first_cut_iter + cuts * iters_between_cuts + 40;
    const auto target_stagger = std::chrono::milliseconds(3);

    const std::string stress_config_path = repo_config_path(stress_config_file);
    std::string comm_name = unique_comm_name();
    if (!redis_available(comm_name)) {
        BOOST_TEST_MESSAGE("Skipping: Redis unavailable");
        return;
    }
    if (needs_tcpunchd && !tcp_port_open("127.0.0.1", 10000)) {
        BOOST_TEST_MESSAGE("Skipping: tcpunchd not reachable on 127.0.0.1:10000");
        return;
    }

    FMI::FT::ControlPlane control_plane(stress_config_path, comm_name, world);
    control_plane.clear_job_state();
    control_plane.clear_criu_state();

    std::mutex failures_mutex;
    std::vector<std::string> failures;
    auto record_failure = [&](const std::string& message) {
        std::lock_guard<std::mutex> lock(failures_mutex);
        failures.push_back(message);
    };

    std::array<std::atomic<int>, world> progress{};

    auto rank_fn = [&](FMI::Utils::peer_num rank) {
        try {
            FMI::Communicator comm(rank, world, stress_config_path, comm_name, 128,
                                   "stress-worker-" + std::to_string(rank));
            FMI::Utils::Function<int> sum([](int a, int b) { return a + b; }, true, true);
            for (int it = 0; it < total_iters; it++) {
                if (rank == target_rank) {
                    std::this_thread::sleep_for(target_stagger);
                }
                FMI::Comm::Data<int> in = it * 10 + static_cast<int>(rank);
                FMI::Comm::Data<int> out;
                comm.allreduce(in, out, sum);
                int expected = static_cast<int>(world) * it * 10
                               + static_cast<int>(world * (world - 1) / 2);
                if (out.get() != expected) {
                    std::ostringstream oss;
                    oss << "rank " << rank << " iter " << it << ": allreduce returned "
                        << out.get() << ", expected " << expected;
                    record_failure(oss.str());
                    return;
                }
                progress[rank].fetch_add(1);
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
        } catch (const FMI::Utils::Timeout&) {
            std::ostringstream oss;
            oss << "rank " << rank << " iter " << progress[rank].load()
                << ": FMI::Utils::Timeout (wedged waiting on a peer)";
            record_failure(oss.str());
        } catch (const std::exception& e) {
            std::ostringstream oss;
            oss << "rank " << rank << " iter " << progress[rank].load() << ": " << e.what();
            record_failure(oss.str());
        } catch (const std::string& s) {
            // The TCPunch client throws std::string on rendezvous failures.
            std::ostringstream oss;
            oss << "rank " << rank << " iter " << progress[rank].load()
                << ": TCPunch rendezvous failure: " << s;
            record_failure(oss.str());
        } catch (...) {
            std::ostringstream oss;
            oss << "rank " << rank << " iter " << progress[rank].load() << ": unknown exception";
            record_failure(oss.str());
        }
    };

    std::vector<std::future<void>> ranks;
    ranks.reserve(world);
    for (FMI::Utils::peer_num rank = 0; rank < world; rank++) {
        ranks.push_back(std::async(std::launch::async, rank_fn, rank));
    }

    std::random_device rd;
    const unsigned int seed = rd();
    std::mt19937 rng(seed);
    std::uniform_int_distribution<int> offset_ms(0, 12);
    BOOST_TEST_MESSAGE("cut_timing_stress seed: " << seed);

    int completed_cuts = 0;
    for (int cut = 0; cut < cuts; cut++) {
        int fire_at = first_cut_iter + cut * iters_between_cuts;
        bool ready = FMI::Utils::poll_until(
                [&]() {
                    for (FMI::Utils::peer_num rank = 0; rank < world; rank++) {
                        if (progress[rank].load() < fire_at) {
                            return false;
                        }
                    }
                    return true;
                },
                20000, 2);
        if (!ready) {
            record_failure("orchestrator: ranks never reached iteration " + std::to_string(fire_at)
                           + " for cut " + std::to_string(cut) + " (a prior cut wedged the job)");
            break;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(offset_ms(rng)));
        std::uint64_t epoch = control_plane.epoch();
        control_plane.request_migration(target_rank);

        // Try-promote until the gate opens (the target wrote QUIESCED at its boundary).
        bool promoted = FMI::Utils::poll_until(
                [&]() {
                    try {
                        control_plane.promote_epoch(epoch + 1);
                        return true;
                    } catch (const std::exception&) {
                        return false;   // target not quiesced yet
                    }
                },
                20000, 10);
        if (!promoted) {
            record_failure("orchestrator: cut " + std::to_string(cut)
                           + " never became promotable (target never reached its quiesce point)");
            // Release anything parked so the rank threads terminate instead of waiting forever.
            // Only target_rank is ever requested, so it is the only possibly-pending rank.
            control_plane.mark_rank_quiesced(epoch, target_rank);
            try {
                control_plane.promote_epoch(epoch + 1);
            } catch (const std::exception&) {
                // job is failing anyway; rank-side timeouts bound the remaining waits
            }
            break;
        }
        completed_cuts++;
        std::ostringstream cut_msg;
        cut_msg << "cut " << cut << " promoted to epoch " << epoch + 1 << " (progress";
        for (FMI::Utils::peer_num rank = 0; rank < world; rank++) {
            cut_msg << " " << progress[rank].load();
        }
        cut_msg << ")";
        BOOST_TEST_MESSAGE(cut_msg.str());
    }

    // Rank-side waits are bounded by Direct's max_timeout, but a TCPunch pairing can wedge past
    // its own timeout (known rendezvous flakiness), leaving a rank thread stuck forever. A stuck
    // std::async future would then block the test binary at scope exit, so bound the join and
    // hard-exit after reporting if any rank never terminates.
    bool all_joined = true;
    for (FMI::Utils::peer_num rank = 0; rank < world; rank++) {
        if (ranks[rank].wait_for(std::chrono::seconds(45)) != std::future_status::ready) {
            all_joined = false;
            record_failure("rank " + std::to_string(rank) + " thread never terminated at iter "
                           + std::to_string(progress[rank].load())
                           + " (wedged past every timeout, e.g. inside a TCPunch pairing)");
        }
    }

    BOOST_TEST_MESSAGE("cut_timing_stress completed cuts: " << completed_cuts << "/" << cuts);
    for (const auto& failure : failures) {
        BOOST_ERROR(failure);
    }
    BOOST_CHECK_EQUAL(failures.size(), 0u);

    control_plane.clear_job_state();
    control_plane.clear_criu_state();

    if (!all_joined) {
        // Failures above have already been streamed to the log; exiting is the only way to
        // terminate with a thread stuck in a blocking call that ignores its deadline.
        std::_Exit(201);
    }
}

BOOST_AUTO_TEST_CASE(transparent_migration_cut_timing_stress) {
    run_cut_timing_stress("fmi_ft_stress_test.json", /*needs_tcpunchd=*/true);
}

// The same protocol torture over the Redis (ClientServer) data plane — checkpoint-safe since
// Redis::prepare_for_checkpoint, and therefore allowed under state_transfer="criu". No
// tcpunchd involved, so this variant also isolates the consensus-cut logic from TCPunch
// rendezvous flakiness.
BOOST_AUTO_TEST_CASE(transparent_migration_cut_timing_stress_redis) {
    run_cut_timing_stress("fmi_ft_stress_redis_test.json", /*needs_tcpunchd=*/false);
}
#endif

BOOST_AUTO_TEST_SUITE_END();
