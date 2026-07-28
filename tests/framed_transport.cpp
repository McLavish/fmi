#include <boost/test/unit_test.hpp>

#include "forked_rank_guard.h"

#include "../include/comm/Channel.h"
#include "../include/comm/OperationScope.h"

#include <sys/mman.h>
#include <sys/wait.h>
#include <chrono>
#include <string>
#include <thread>
#include <unistd.h>

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

    void reap(int peer_id, int num_peers) {
        if (peer_id != 0) {
            std::_Exit(0);
        }
        for (int i = 1; i < num_peers; i++) {
            wait(nullptr);
        }
    }
}

BOOST_AUTO_TEST_CASE(a_framed_point_to_point_message_round_trips) {
    constexpr int num_peers = 2;
    const std::string name = unique_comm("p2p");
    int* ok = shared_flags(num_peers);

    ForkedRankGuard rank_guard;
    int& peer_id = rank_guard.peer_id;
    for (int i = 1; i < num_peers; i++) {
        int pid = fork();
        if (pid == 0) { peer_id = i; break; }
    }

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

    reap(peer_id, num_peers);
    BOOST_CHECK_EQUAL(ok[0], 1);
    BOOST_CHECK_EQUAL(ok[1], 1);
}

BOOST_AUTO_TEST_CASE(a_framed_collective_fragment_round_trips) {
    constexpr int num_peers = 2;
    const std::string name = unique_comm("coll");
    int* ok = shared_flags(num_peers);

    ForkedRankGuard rank_guard;
    int& peer_id = rank_guard.peer_id;
    for (int i = 1; i < num_peers; i++) {
        int pid = fork();
        if (pid == 0) { peer_id = i; break; }
    }

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

    reap(peer_id, num_peers);
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
    for (int i = 1; i < num_peers; i++) {
        int pid = fork();
        if (pid == 0) { peer_id = i; break; }
    }

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

    reap(peer_id, num_peers);
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
    for (int i = 1; i < num_peers; i++) {
        int pid = fork();
        if (pid == 0) { peer_id = i; break; }
    }

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

    reap(peer_id, num_peers);
    BOOST_CHECK_EQUAL(ok[0], 1);
    BOOST_CHECK_MESSAGE(ok[1] == 1, "receiver did not report an identity mismatch");
    BOOST_CHECK_MESSAGE(ok[2] == 0, "receiver SILENTLY ACCEPTED a different collective");
}

#endif // FMI_ENABLE_REDIS

BOOST_AUTO_TEST_SUITE_END()
