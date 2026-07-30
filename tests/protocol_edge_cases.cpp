#include <boost/test/unit_test.hpp>

#include "forked_rank_guard.h"

#include "../include/fmi.h"
#include "../include/comm/Channel.h"

#include <sys/mman.h>
#include <sys/wait.h>
#include <chrono>
#include <cstdio>
#include <functional>
#include <numeric>
#include <string>
#include <unistd.h>
#include <vector>

//! Edge cases and differential testing for the framed transport.
/*!
 * The central claim of framing is that it is OBSERVATIONALLY TRANSPARENT: for every valid
 * program, a framed run and an unframed run must agree exactly. Most cases here therefore run
 * the same program twice, once under each configuration, and compare — a framing bug that
 * changed a result would show up as a disagreement even if both runs "succeeded".
 */
BOOST_AUTO_TEST_SUITE(ProtocolEdgeCases)

#if FMI_ENABLE_REDIS

namespace {
    const std::string framed_config   = "../../config/fmi_framed_test.json";
    const std::string unframed_config = "../../config/fmi_direct_tcp.json";

    std::string unique_comm(const std::string& suffix) {
        return std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) +
               "_" + std::to_string(getpid()) + "_" + suffix;
    }

    //! Per-rank outcome: a success flag plus a small vector of observed values to compare.
    constexpr int max_values = 64;
    struct RankResult {
        int ok;
        int count;
        long long values[max_values];
    };

    RankResult* shared_results(int n) {
        return static_cast<RankResult*>(mmap(nullptr, n * sizeof(RankResult),
                                             PROT_READ | PROT_WRITE,
                                             MAP_SHARED | MAP_ANONYMOUS, -1, 0));
    }

    using Program = std::function<void(FMI::Communicator&, int, RankResult&)>;

    //! Fork @p num_peers ranks, run @p program under @p config, and collect their results.
    void run_ranks(int num_peers, const std::string& config, const std::string& name,
                   const Program& program, RankResult* out) {
        for (int i = 0; i < num_peers; i++) { out[i].ok = 0; out[i].count = 0; }

        ForkedRankGuard rank_guard;
        int& peer_id = rank_guard.peer_id;
        rank_guard.fork_ranks(num_peers);

        try {
            FMI::Communicator comm(peer_id, num_peers, config, name);
            program(comm, peer_id, out[peer_id]);
            out[peer_id].ok = 1;
        } catch (const std::exception& e) {
            std::fprintf(stderr, "[rank %d] %s\n", peer_id, e.what());
            out[peer_id].ok = 0;
        }

        rank_guard.reap_ranks();
    }

    void push(RankResult& r, long long v) {
        if (r.count < max_values) { r.values[r.count++] = v; }
    }

    //! Run the same program framed and unframed and require identical observations.
    void check_framing_is_transparent(int num_peers, const std::string& label,
                                      const Program& program) {
        RankResult* framed   = shared_results(num_peers);
        RankResult* unframed = shared_results(num_peers);

        run_ranks(num_peers, unframed_config, unique_comm(label + "-raw"), program, unframed);
        run_ranks(num_peers, framed_config,   unique_comm(label + "-fr"),  program, framed);

        for (int i = 0; i < num_peers; i++) {
            BOOST_CHECK_MESSAGE(unframed[i].ok == 1, label << ": unframed rank " << i << " failed");
            BOOST_CHECK_MESSAGE(framed[i].ok == 1,   label << ": framed rank " << i << " failed");
            BOOST_CHECK_MESSAGE(framed[i].count == unframed[i].count,
                                label << ": rank " << i << " produced "
                                      << framed[i].count << " values framed vs "
                                      << unframed[i].count << " unframed");
            const int n = std::min(framed[i].count, unframed[i].count);
            for (int k = 0; k < n; k++) {
                BOOST_CHECK_MESSAGE(framed[i].values[k] == unframed[i].values[k],
                                    label << ": rank " << i << " value " << k << " = "
                                          << framed[i].values[k] << " framed vs "
                                          << unframed[i].values[k] << " unframed");
            }
        }
    }
}

// ------------------------------------------------------------------ differential

BOOST_AUTO_TEST_CASE(framing_is_transparent_for_the_full_collective_set) {
    auto program = [](FMI::Communicator& comm, int rank, RankResult& r) {
        FMI::Utils::Function<int> sum([](int a, int b) { return a + b; }, true, true);
        FMI::Utils::Function<int> ordered([](int a, int b) { return a - b; }, false, false);

        FMI::Comm::Data<int> b = (rank == 0) ? 77 : 0;
        comm.bcast(b, 0);
        push(r, b.get());

        FMI::Comm::Data<int> one = 1, red = 0;
        comm.reduce(one, red, 0, sum);
        push(r, red.get());

        FMI::Comm::Data<int> one2 = 1, ar = 0;
        comm.allreduce(one2, ar, sum);
        push(r, ar.get());

        FMI::Comm::Data<int> one3 = 1, sc = 0;
        comm.scan(one3, sc, sum);
        push(r, sc.get());

        // A non-commutative function takes an entirely different algorithm.
        FMI::Comm::Data<int> o1 = rank + 1, o2 = 0;
        comm.allreduce(o1, o2, ordered);
        push(r, o2.get());

        FMI::Comm::Data<int> s1 = rank + 1, s2 = 0;
        comm.scan(s1, s2, ordered);
        push(r, s2.get());

        comm.barrier();
        push(r, 999);
    };
    check_framing_is_transparent(3, "collectives", program);
}

BOOST_AUTO_TEST_CASE(framing_is_transparent_at_a_non_power_of_two_rank_count) {
    // Binomial trees take different branches when num_peers is not a power of two, which
    // exercises message sizes and peer pairings the 2- and 4-rank cases never produce.
    auto program = [](FMI::Communicator& comm, int rank, RankResult& r) {
        FMI::Utils::Function<int> sum([](int a, int b) { return a + b; }, true, true);
        FMI::Comm::Data<int> one = 1, ar = 0;
        comm.allreduce(one, ar, sum);
        push(r, ar.get());

        std::vector<int> mine_v{rank, rank * 2};
        FMI::Comm::Data<std::vector<int>> mine(mine_v);
        FMI::Comm::Data<std::vector<int>> all(2 * 5);
        comm.gather(mine, all, 0);
        if (rank == 0) {
            for (int v : all.get()) { push(r, v); }
        }
        comm.barrier();
        push(r, 1);
    };
    check_framing_is_transparent(5, "five-ranks", program);
}

BOOST_AUTO_TEST_CASE(bcast_is_transparent_for_every_root) {
    auto program = [](FMI::Communicator& comm, int rank, RankResult& r) {
        for (int root = 0; root < 3; root++) {
            FMI::Comm::Data<int> b = (rank == root) ? (100 + root) : 0;
            comm.bcast(b, root);
            push(r, b.get());
        }
    };
    check_framing_is_transparent(3, "bcast-roots", program);
}

BOOST_AUTO_TEST_CASE(gather_is_transparent_for_root_zero) {
    auto program = [](FMI::Communicator& comm, int rank, RankResult& r) {
        std::vector<int> mine_v{rank};
            FMI::Comm::Data<std::vector<int>> mine(mine_v);
        FMI::Comm::Data<std::vector<int>> all(3);
        comm.gather(mine, all, 0);
        if (rank == 0) { for (int v : all.get()) { push(r, v); } }
    };
    check_framing_is_transparent(3, "gather-r0", program);
}

BOOST_AUTO_TEST_CASE(gather_is_transparent_for_root_one) {
    auto program = [](FMI::Communicator& comm, int rank, RankResult& r) {
        std::vector<int> mine_v{rank};
            FMI::Comm::Data<std::vector<int>> mine(mine_v);
        FMI::Comm::Data<std::vector<int>> all(3);
        comm.gather(mine, all, 1);
        if (rank == 1) { for (int v : all.get()) { push(r, v); } }
    };
    check_framing_is_transparent(3, "gather-r1", program);
}

BOOST_AUTO_TEST_CASE(gather_is_transparent_for_root_two_at_four_peers) {
    auto program = [](FMI::Communicator& comm, int rank, RankResult& r) {
        std::vector<int> mine_v{rank};
            FMI::Comm::Data<std::vector<int>> mine(mine_v);
        FMI::Comm::Data<std::vector<int>> all(4);
        comm.gather(mine, all, 2);
        if (rank == 2) { for (int v : all.get()) { push(r, v); } }
    };
    check_framing_is_transparent(4, "gather-r2-p4", program);
}

BOOST_AUTO_TEST_CASE(scatter_is_transparent_for_every_root) {
    auto program = [](FMI::Communicator& comm, int rank, RankResult& r) {
        for (int root = 0; root < 3; root++) {
            std::vector<int> payload{10, 11, 12};
            FMI::Comm::Data<std::vector<int>> spread(payload);
            FMI::Comm::Data<std::vector<int>> slice(1);
            comm.scatter(spread, slice, root);
            push(r, slice.get()[0]);
        }
    };
    check_framing_is_transparent(3, "scatter-roots", program);
}

BOOST_AUTO_TEST_CASE(framing_is_transparent_for_interleaved_p2p_and_collectives) {
    // The lane split exists for exactly this shape: application traffic and collective
    // fragments sharing one link.
    auto program = [](FMI::Communicator& comm, int rank, RankResult& r) {
        FMI::Utils::Function<int> sum([](int a, int b) { return a + b; }, true, true);
        for (int i = 0; i < 4; i++) {
            FMI::Comm::Data<int> one = 1, ar = 0;
            comm.allreduce(one, ar, sum);
            push(r, ar.get());

            FMI::Comm::Data<int> msg = 1000 + i, got = 0;
            if (rank == 0) {
                comm.send(msg, 1);
            } else if (rank == 1) {
                comm.recv(got, 0);
                push(r, got.get());
            }
            comm.barrier();
        }
    };
    check_framing_is_transparent(3, "interleaved", program);
}

// ------------------------------------------------------------------ edge cases

BOOST_AUTO_TEST_CASE(a_large_payload_survives_framing) {
    // Exercises the partial read/write loops with a header in front of them: a header that is
    // written or parsed as part of the payload would corrupt every byte after it.
    constexpr int elements = 256 * 1024;   // 1 MiB of ints
    auto program = [](FMI::Communicator& comm, int rank, RankResult& r) {
        std::vector<int> payload(elements);
        std::iota(payload.begin(), payload.end(), rank);
        FMI::Comm::Data<std::vector<int>> big(payload);
        comm.bcast(big, 0);
        const auto got = big.get();
        long long checksum = 0;
        for (std::size_t i = 0; i < got.size(); i++) { checksum += got[i]; }
        push(r, checksum);
        push(r, static_cast<long long>(got.size()));
    };
    check_framing_is_transparent(2, "large", program);
}

BOOST_AUTO_TEST_CASE(many_consecutive_operations_keep_their_sequences_aligned) {
    // The collective counter and the per-link transport sequence both advance on every
    // operation; a drift of one on either side surfaces as an identity or sequence error.
    auto program = [](FMI::Communicator& comm, int rank, RankResult& r) {
        FMI::Utils::Function<int> sum([](int a, int b) { return a + b; }, true, true);
        long long total = 0;
        for (int i = 0; i < 60; i++) {
            FMI::Comm::Data<int> one = 1, ar = 0;
            comm.allreduce(one, ar, sum);
            total += ar.get();
            if (i % 10 == 0) { comm.barrier(); }
        }
        push(r, total);
    };
    check_framing_is_transparent(3, "long-run", program);
}

BOOST_AUTO_TEST_CASE(two_communicators_in_one_process_do_not_share_identity_state) {
    // The operation scope is thread_local and the collective counter is per-Communicator.
    // Interleaving two communicators on one thread must not desynchronise either.
    constexpr int num_peers = 2;
    const std::string a_name = unique_comm("dual-a");
    const std::string b_name = unique_comm("dual-b");
    RankResult* out = shared_results(num_peers);
    for (int i = 0; i < num_peers; i++) { out[i].ok = 0; out[i].count = 0; }

    ForkedRankGuard rank_guard;
    int& peer_id = rank_guard.peer_id;
    rank_guard.fork_ranks(num_peers);

    try {
        FMI::Communicator a(peer_id, num_peers, framed_config, a_name);
        FMI::Communicator b(peer_id, num_peers, framed_config, b_name);
        FMI::Utils::Function<int> sum([](int x, int y) { return x + y; }, true, true);

        for (int i = 0; i < 5; i++) {
            FMI::Comm::Data<int> x = 1, ax = 0;
            a.allreduce(x, ax, sum);
            push(out[peer_id], ax.get());

            FMI::Comm::Data<int> y = 2, by = 0;
            b.allreduce(y, by, sum);
            push(out[peer_id], by.get());
        }
        out[peer_id].ok = 1;
    } catch (const std::exception& e) {
        std::fprintf(stderr, "[rank %d] dual: %s\n", peer_id, e.what());
        out[peer_id].ok = 0;
    }

    rank_guard.reap_ranks();

    for (int i = 0; i < num_peers; i++) {
        BOOST_CHECK_MESSAGE(out[i].ok == 1, "rank " << i << " failed with two communicators");
        BOOST_REQUIRE_EQUAL(out[i].count, 10);
        for (int k = 0; k < 10; k += 2) {
            BOOST_CHECK_EQUAL(out[i].values[k], 2);       // communicator a: 1+1
            BOOST_CHECK_EQUAL(out[i].values[k + 1], 4);   // communicator b: 2+2
        }
    }
}

// ---------------------------------------------- degenerate and inconsistent programs

BOOST_AUTO_TEST_CASE(a_zero_length_message_is_legitimate_and_round_trips) {
    // Empty payloads are valid: total_length 0 with payload_length 0 is a well-formed frame.
    auto program = [](FMI::Communicator& comm, int rank, RankResult& r) {
        std::vector<int> empty;
        FMI::Comm::Data<std::vector<int>> nothing(empty);
        comm.bcast(nothing, 0);
        push(r, static_cast<long long>(nothing.get().size()));
        comm.barrier();
        push(r, 1);
    };
    check_framing_is_transparent(2, "zero-len", program);
}

BOOST_AUTO_TEST_CASE(ranks_disagreeing_on_a_collective_buffer_size_are_caught_when_framed) {
    // A genuine and easy-to-make user error: ranks pass differently sized buffers to the same
    // collective. total_length is part of message identity, so the framed transport refuses it.
    // Unframed there is nothing to compare against and the receiver simply takes whatever
    // bytes arrive, which is how this corrupts silently today.
    constexpr int num_peers = 2;
    RankResult* out = shared_results(num_peers);
    for (int i = 0; i < num_peers; i++) { out[i].ok = 0; out[i].count = 0; }
    const std::string name = unique_comm("size-mismatch");

    ForkedRankGuard rank_guard;
    int& peer_id = rank_guard.peer_id;
    rank_guard.fork_ranks(num_peers);

    bool refused = false;
    try {
        FMI::Communicator comm(peer_id, num_peers, framed_config, name);
        // Rank 0 contributes two ints, rank 1 contributes one. Sizes must agree; they do not.
        std::vector<int> mine(peer_id == 0 ? 2 : 1, peer_id + 1);
        FMI::Comm::Data<std::vector<int>> send(mine);
        FMI::Comm::Data<std::vector<int>> all(4);
        comm.gather(send, all, 0);
    } catch (const std::exception&) {
        refused = true;
    }
    out[peer_id].ok = refused ? 1 : 0;

    rank_guard.reap_ranks();

    BOOST_CHECK_MESSAGE(out[0].ok == 1,
                        "framed gather accepted ranks that disagreed on the buffer size");
}

// ------------------------------------------------- gather at channel level, no framing at all
// Diagnostic: the whole repo only covers gather at 2 peers (root 1) and 4 peers (root 0), both
// powers of two. These run the UNFRAMED DirectTCP channel directly, so a failure here is a
// pre-existing PeerToPeer::gather defect and has nothing to do with the link layer.

namespace {
    std::map<std::string, std::string> raw_dtcp_params() {
        return {{"registry_host", "127.0.0.1"}, {"registry_port", "6379"},
                {"max_timeout", "4000"}, {"registry_poll_interval_ms", "2"},
                {"connect_retry_interval_ms", "5"}, {"registry_ttl_s", "120"}};
    }
    std::map<std::string, std::string> raw_dtcp_model() {
        return {{"bandwidth", "250.0"}, {"overhead", "0.20"}, {"transfer_price", "0.0"},
                {"vm_price", "0.0134"}, {"requests_per_hour", "1000"},
                {"include_infrastructure_costs", "true"}};
    }

    //! Channel-level gather with the empty-recvbuf convention the existing suite uses.
    int raw_gather_ok(int num_peers, int root) {
        int* got = static_cast<int*>(mmap(nullptr, num_peers * sizeof(int),
                                          PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0));
        int* done = static_cast<int*>(mmap(nullptr, sizeof(int), PROT_READ | PROT_WRITE,
                                           MAP_SHARED | MAP_ANONYMOUS, -1, 0));
        *done = 0;
        const std::string name = unique_comm("rawgather-" + std::to_string(num_peers) + "-" + std::to_string(root));

        ForkedRankGuard rank_guard;
        int& peer_id = rank_guard.peer_id;
        rank_guard.fork_ranks(num_peers);
        try {
            auto ch = FMI::Comm::Channel::get_channel("DirectTCP", raw_dtcp_params(), raw_dtcp_model());
            ch->set_peer_id(peer_id);
            ch->set_num_peers(num_peers);
            ch->set_comm_name(name);
            int mine = peer_id + 1;
            if (peer_id == root) {
                ch->gather({reinterpret_cast<char*>(&mine), sizeof(int)},
                           {reinterpret_cast<char*>(got), sizeof(int) * num_peers}, root);
                *done = 1;
            } else {
                ch->gather({reinterpret_cast<char*>(&mine), sizeof(int)}, {}, root);
            }
            ch->finalize();
        } catch (const std::exception& e) {
            std::fprintf(stderr, "[raw gather p=%d root=%d rank %d] %s\n", num_peers, root, peer_id, e.what());
        }
        rank_guard.reap_ranks();
        if (*done != 1) { return 0; }
        for (int i = 0; i < num_peers; i++) {
            if (got[i] != i + 1) { return 0; }
        }
        return 1;
    }
}

BOOST_AUTO_TEST_CASE(raw_gather_two_peers_root_one_is_the_covered_case) {
    BOOST_CHECK_EQUAL(raw_gather_ok(2, 1), 1);
}

BOOST_AUTO_TEST_CASE(raw_gather_four_peers_root_zero_is_the_covered_case) {
    BOOST_CHECK_EQUAL(raw_gather_ok(4, 0), 1);
}

BOOST_AUTO_TEST_CASE(raw_gather_three_peers_root_zero) {
    BOOST_CHECK_EQUAL(raw_gather_ok(3, 0), 1);
}

BOOST_AUTO_TEST_CASE(raw_gather_four_peers_root_two) {
    BOOST_CHECK_EQUAL(raw_gather_ok(4, 2), 1);
}

#endif // FMI_ENABLE_REDIS

BOOST_AUTO_TEST_SUITE_END()
