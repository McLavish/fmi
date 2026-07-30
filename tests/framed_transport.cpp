#include <boost/test/unit_test.hpp>

#include "forked_rank_guard.h"

#include "../include/fmi.h"
#include "../include/comm/Channel.h"
#include "../include/comm/OperationScope.h"

#include <sys/mman.h>
#include <sys/wait.h>
#include <chrono>
#include <string>
#include <thread>
#include <unistd.h>
#include <cstdio>

BOOST_AUTO_TEST_SUITE(FramedTransport)

#if FMI_ENABLE_REDIS

using namespace FMI::Comm;

namespace {
    std::map<std::string, std::string> framed_params() {
        return {
                {"registry_host",             "127.0.0.1"},
                {"registry_port",             "6379"},
                {"max_timeout",               "3000"},
                {"registry_poll_interval_ms", "2"},
                {"connect_retry_interval_ms", "5"},
                {"registry_ttl_s",            "120"},
                // The subject of this suite: identity travels with every message.
                {"framed",                    "true"}
        };
    }

    std::map<std::string, std::string> model_params() {
        return {
                {"bandwidth",                    "250.0"},
                {"overhead",                     "0.20"},
                {"transfer_price",               "0.0"},
                {"vm_price",                     "0.0134"},
                {"requests_per_hour",            "1000"},
                {"include_infrastructure_costs", "true"}
        };
    }

    std::string unique_comm(const std::string& suffix) {
        return std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) +
               "_" + std::to_string(getpid()) + "_" + suffix;
    }

    int* shared_flags(int n) {
        return static_cast<int*>(mmap(nullptr, n * sizeof(int), PROT_READ | PROT_WRITE,
                                      MAP_SHARED | MAP_ANONYMOUS, -1, 0));
    }

}

BOOST_AUTO_TEST_CASE(a_framed_point_to_point_message_round_trips) {
    constexpr int num_peers = 2;
    const std::string name = unique_comm("p2p");
    int* ok = shared_flags(num_peers);

    ForkedRankGuard rank_guard;
    int& peer_id = rank_guard.peer_id;
    rank_guard.fork_ranks(num_peers);

    ok[peer_id] = 0;
    try {
        auto ch = Channel::get_channel("DirectTCP", framed_params(), model_params());
        ch->set_peer_id(peer_id);
        ch->set_num_peers(num_peers);
        ch->set_comm_name(name);

        int val = 4242;
        if (peer_id == 0) {
            OperationScope scope(p2p_identity(1));
            ch->send({reinterpret_cast<char*>(&val), sizeof(val)}, 1);
            ok[0] = 1;
        } else {
            int got = 0;
            OperationScope scope(p2p_identity(1));   // rank 1 is the destination
            ch->recv({reinterpret_cast<char*>(&got), sizeof(got)}, 0);
            ok[1] = (got == 4242);
        }
        ch->finalize();
    } catch (const std::exception& e) {
        BOOST_TEST_MESSAGE("rank " << peer_id << ": " << e.what());
    }

    rank_guard.reap_ranks();
    BOOST_CHECK_EQUAL(ok[0], 1);
    BOOST_CHECK_EQUAL(ok[1], 1);
}

BOOST_AUTO_TEST_CASE(a_framed_collective_fragment_round_trips) {
    constexpr int num_peers = 2;
    const std::string name = unique_comm("coll");
    int* ok = shared_flags(num_peers);

    ForkedRankGuard rank_guard;
    int& peer_id = rank_guard.peer_id;
    rank_guard.fork_ranks(num_peers);

    ok[peer_id] = 0;
    try {
        auto ch = Channel::get_channel("DirectTCP", framed_params(), model_params());
        ch->set_peer_id(peer_id);
        ch->set_num_peers(num_peers);
        ch->set_comm_name(name);

        // Both ranks agree they are inside collective #0, a bcast rooted at 0.
        OperationScope scope(collective_identity(OpKind::Bcast, 0, 0));
        int val = 7;
        if (peer_id == 0) {
            ch->send({reinterpret_cast<char*>(&val), sizeof(val)}, 1);
            ok[0] = 1;
        } else {
            int got = 0;
            ch->recv({reinterpret_cast<char*>(&got), sizeof(got)}, 0);
            ok[1] = (got == 7);
        }
        ch->finalize();
    } catch (const std::exception& e) {
        BOOST_TEST_MESSAGE("rank " << peer_id << ": " << e.what());
    }

    rank_guard.reap_ranks();
    BOOST_CHECK_EQUAL(ok[0], 1);
    BOOST_CHECK_EQUAL(ok[1], 1);
}

BOOST_AUTO_TEST_CASE(a_divergent_receive_fails_loudly_instead_of_substituting) {
    // THE headline property. Rank 0 sends a four-byte application message; rank 1 is inside a
    // collective and is waiting for a four-byte fragment. The payloads are the same size and
    // the bytes are indistinguishable, so the unframed transport hands rank 1 rank 0's
    // application payload and the program continues with silently wrong data. Framed, rank 1
    // must refuse it.
    constexpr int num_peers = 2;
    const std::string name = unique_comm("diverge");
    // [0] = sender finished, [1] = receiver threw, [2] = receiver silently accepted
    int* ok = shared_flags(3);

    ForkedRankGuard rank_guard;
    int& peer_id = rank_guard.peer_id;
    rank_guard.fork_ranks(num_peers);

    ok[peer_id] = 0;
    if (peer_id == 0) { ok[2] = 0; }

    try {
        auto ch = Channel::get_channel("DirectTCP", framed_params(), model_params());
        ch->set_peer_id(peer_id);
        ch->set_num_peers(num_peers);
        ch->set_comm_name(name);

        if (peer_id == 0) {
            int val = 999;
            OperationScope scope(p2p_identity(1));           // an application send
            ch->send({reinterpret_cast<char*>(&val), sizeof(val)}, 1);
            ok[0] = 1;
        } else {
            int got = 0;
            OperationScope scope(collective_identity(OpKind::Barrier, 0, 0));  // a barrier
            try {
                ch->recv({reinterpret_cast<char*>(&got), sizeof(got)}, 0);
                ok[2] = 1;   // accepted a p2p payload as a collective fragment
            } catch (const std::exception& e) {
                const std::string what = e.what();
                ok[1] = what.find("identity mismatch") != std::string::npos;
                BOOST_TEST_MESSAGE("rank 1 correctly refused: " << what);
            }
        }
        ch->finalize();
    } catch (const std::exception& e) {
        BOOST_TEST_MESSAGE("rank " << peer_id << " setup: " << e.what());
    }

    rank_guard.reap_ranks();
    BOOST_CHECK_EQUAL(ok[0], 1);
    BOOST_CHECK_MESSAGE(ok[1] == 1, "receiver did not report an identity mismatch");
    BOOST_CHECK_MESSAGE(ok[2] == 0, "receiver SILENTLY ACCEPTED a foreign payload");
}

BOOST_AUTO_TEST_CASE(collectives_issued_in_different_orders_are_refused) {
    // rank0: bcast then barrier;  rank1: barrier then bcast. Both first fragments are the same
    // size on the same lane with the same collective_index — only op_kind separates them.
    constexpr int num_peers = 2;
    const std::string name = unique_comm("order");
    int* ok = shared_flags(3);

    ForkedRankGuard rank_guard;
    int& peer_id = rank_guard.peer_id;
    rank_guard.fork_ranks(num_peers);

    ok[peer_id] = 0;
    if (peer_id == 0) { ok[2] = 0; }

    try {
        auto ch = Channel::get_channel("DirectTCP", framed_params(), model_params());
        ch->set_peer_id(peer_id);
        ch->set_num_peers(num_peers);
        ch->set_comm_name(name);

        if (peer_id == 0) {
            int val = 5;
            OperationScope scope(collective_identity(OpKind::Bcast, 0, 0));
            ch->send({reinterpret_cast<char*>(&val), sizeof(val)}, 1);
            ok[0] = 1;
        } else {
            int got = 0;
            OperationScope scope(collective_identity(OpKind::Barrier, 0, 0));
            try {
                ch->recv({reinterpret_cast<char*>(&got), sizeof(got)}, 0);
                ok[2] = 1;
            } catch (const std::exception& e) {
                const std::string what = e.what();
                ok[1] = what.find("identity mismatch") != std::string::npos;
                BOOST_TEST_MESSAGE("rank 1 correctly refused: " << what);
            }
        }
        ch->finalize();
    } catch (const std::exception& e) {
        BOOST_TEST_MESSAGE("rank " << peer_id << " setup: " << e.what());
    }

    rank_guard.reap_ranks();
    BOOST_CHECK_EQUAL(ok[0], 1);
    BOOST_CHECK_MESSAGE(ok[1] == 1, "receiver did not report an identity mismatch");
    BOOST_CHECK_MESSAGE(ok[2] == 0, "receiver SILENTLY ACCEPTED a different collective");
}

BOOST_AUTO_TEST_CASE(every_collective_works_through_a_framed_communicator) {
    // The transparency claim, end to end: a stock application driving a real Communicator over
    // the framed transport, using every collective plus point-to-point, with no annotation of
    // any kind. Identity is produced by the Communicator and validated on the wire; the
    // application below is unaware that either happens.
    constexpr int num_peers = 3;
    const std::string name = unique_comm("comm-all");
    const std::string config = "../../config/fmi_framed_test.json";
    int* ok = shared_flags(num_peers);
    int* stage = shared_flags(num_peers);

    ForkedRankGuard rank_guard;
    int& peer_id = rank_guard.peer_id;
    rank_guard.fork_ranks(num_peers);

    ok[peer_id] = 0;
    stage[peer_id] = 0;
    try {
        FMI::Communicator comm(peer_id, num_peers, config, name);
        stage[peer_id] = 1;
        FMI::Utils::Function<int> sum([](int a, int b) { return a + b; }, true, true);
        FMI::Utils::Function<int> ordered([](int a, int b) { return a - b; }, false, false);
        bool good = true;
        int failed_check = 0;
        auto check = [&](bool cond, int id) { if (!cond && failed_check == 0) { failed_check = id; } good = good && cond; };

        // point-to-point
        FMI::Comm::Data<int> pv = 11, pr = 0;
        if (peer_id == 0) {
            comm.send(pv, 1);
        } else if (peer_id == 1) {
            comm.recv(pr, 0);
            check(pr.get() == 11, 1);
        }

        comm.barrier();

        stage[peer_id] = 2;
        FMI::Comm::Data<int> b = (peer_id == 0) ? 77 : 0;
        comm.bcast(b, 0);
        stage[peer_id] = 3;
        check(b.get() == 77, 2);

        FMI::Comm::Data<std::vector<int>> mine{{peer_id, peer_id}};
        FMI::Comm::Data<std::vector<int>> all(2 * num_peers);
        comm.gather(mine, all, 0);
        stage[peer_id] = 4;

        // Data<std::vector<A>>::get() returns by value, so the buffer has to be built before
        // it is wrapped rather than mutated through the accessor.
        std::vector<int> payload(2 * num_peers);
        for (int i = 0; i < 2 * num_peers; i++) { payload[i] = i; }
        FMI::Comm::Data<std::vector<int>> spread(payload);
        FMI::Comm::Data<std::vector<int>> slice(2);
        comm.scatter(spread, slice, 0);
        stage[peer_id] = 5;
        check(slice.get()[0] == 2 * peer_id, 3);

        FMI::Comm::Data<int> one = 1, red = 0;
        comm.reduce(one, red, 0, sum);
        stage[peer_id] = 6;
        if (peer_id == 0) { check(red.get() == num_peers, 4); }

        FMI::Comm::Data<int> one2 = 1, ar = 0;
        comm.allreduce(one2, ar, sum);
        stage[peer_id] = 7;
        check(ar.get() == num_peers, 5);

        FMI::Comm::Data<int> one3 = 1, sc = 0;
        comm.scan(one3, sc, sum);
        stage[peer_id] = 8;
        check(sc.get() == peer_id + 1, 6);

        // A non-commutative reduction takes an entirely different algorithm, so it exercises a
        // second set of identities over the same links.
        FMI::Comm::Data<int> o1 = 1, o2 = 0;
        comm.allreduce(o1, o2, ordered);
        stage[peer_id] = 9;

        comm.barrier();
        if (!good) { std::fprintf(stderr, "[rank %d] failed check %d (slice0=%d ar=%d sc=%d b=%d)\n", peer_id, failed_check, slice.get()[0], ar.get(), sc.get(), b.get()); }
        ok[peer_id] = good ? 1 : 0;
    } catch (const std::exception& e) {
        std::fprintf(stderr, "[rank %d] threw at stage %d: %s\n", peer_id, stage[peer_id], e.what());
        ok[peer_id] = 0;
    }

    rank_guard.reap_ranks();
    for (int i = 0; i < num_peers; i++) {
        BOOST_CHECK_MESSAGE(ok[i] == 1, "rank " << i << " stopped at stage " << stage[i]);
    }
}

BOOST_AUTO_TEST_CASE(a_replacement_rank_re_pairs_without_a_spurious_sequence_gap) {
    // Migration shape: rank 0 survives and reconfigures its existing channel in place, while
    // rank 1 is replaced by a fresh process whose link sequences necessarily start at zero.
    // The survivor's per-peer sequence counters must be dropped along with the socket, or its
    // very first framed receive from the replacement reports a gap that never happened.
    constexpr int num_peers = 2;
    const std::string before = unique_comm("epoch0");
    const std::string after  = unique_comm("epoch1");
    int* ok = shared_flags(num_peers);

    ForkedRankGuard rank_guard;
    int& peer_id = rank_guard.peer_id;
    rank_guard.fork_ranks(num_peers);

    ok[peer_id] = 0;
    try {
        auto ch = Channel::get_channel("DirectTCP", framed_params(), model_params());
        ch->set_peer_id(peer_id);
        ch->set_num_peers(num_peers);
        ch->set_comm_name(before);

        // Enough traffic that the survivor's counters are well past zero.
        for (int i = 0; i < 3; i++) {
            int val = 100 + i, got = 0;
            OperationScope scope(p2p_identity(1));
            if (peer_id == 0) {
                ch->send({reinterpret_cast<char*>(&val), sizeof(val)}, 1);
            } else {
                ch->recv({reinterpret_cast<char*>(&got), sizeof(got)}, 0);
            }
        }

        if (peer_id == 0) {
            // Survivor: keep the channel, drop the link to the migrated rank.
            ch->reconfigure_for_epoch(after, {1});
        } else {
            // Replacement: a brand new process would have a brand new channel.
            ch->finalize();
            ch = Channel::get_channel("DirectTCP", framed_params(), model_params());
            ch->set_peer_id(peer_id);
            ch->set_num_peers(num_peers);
            ch->set_comm_name(after);
        }

        int val = 777, got = 0;
        OperationScope scope(p2p_identity(1));
        if (peer_id == 0) {
            ch->send({reinterpret_cast<char*>(&val), sizeof(val)}, 1);
            ok[0] = 1;
        } else {
            ch->recv({reinterpret_cast<char*>(&got), sizeof(got)}, 0);
            ok[1] = (got == 777);
        }
        ch->finalize();
    } catch (const std::exception& e) {
        BOOST_TEST_MESSAGE("rank " << peer_id << ": " << e.what());
    }

    rank_guard.reap_ranks();
    BOOST_CHECK_MESSAGE(ok[0] == 1, "survivor could not send after reconfiguration");
    BOOST_CHECK_MESSAGE(ok[1] == 1, "replacement could not receive after reconfiguration");
}

#endif // FMI_ENABLE_REDIS

BOOST_AUTO_TEST_SUITE_END()
