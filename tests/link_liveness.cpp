//! Retention has to be released, and on some links nothing arrives to release it.
/*!
 * Retention is pruned by the peer's cumulative ack. That normally rides along on traffic in
 * the opposite direction, which is why two ranks passing messages back and forth never notice
 * the question. A directed link that carries traffic in one direction ONLY has no such
 * traffic — and a binomial tree produces those as soon as a job has three ranks. Without a
 * standalone ack the sender's window fills, admission is refused, and the job stops for good;
 * that is exactly how it failed, at rank 1 -> rank 2, on the first three-rank run.
 *
 * The cases below pin both halves: the receiver offers acks on its own initiative, and a
 * sender whose window is full collects them rather than giving up.
 */
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>

#include "../include/comm/Channel.h"
#include "../include/comm/DirectTCP.h"
#include "../include/comm/OperationScope.h"
#include "../include/comm/SequencedLink.h"
#include "forked_rank_guard.h"

#include <sys/mman.h>
#include <sys/wait.h>
#include <unistd.h>

#include <chrono>
#include <cstdio>
#include <map>
#include <string>
#include <thread>

using namespace FMI::Comm;

namespace {
    std::map<std::string, std::string> one_way_params() {
        // A deliberately tiny window, so "the peer never sends anything back" becomes fatal
        // within a handful of messages instead of 256.
        return {{"registry_host", "127.0.0.1"}, {"registry_port", "6379"},
                {"bind_host", "127.0.0.1"},     {"advertise_host", "127.0.0.1"},
                {"max_timeout", "8000"},        {"registry_poll_interval_ms", "5"},
                {"connect_retry_interval_ms", "10"}, {"registry_ttl_s", "60"},
                {"framed", "true"},             {"recover_links", "true"},
                {"link_window_frames", "4"}};
    }

    std::map<std::string, std::string> model_params() {
        return {{"bandwidth", "400"}, {"overhead", "0.2"}, {"transfer_price", "0"},
                {"vm_price", "0.0134"}, {"requests_per_hour", "1000"},
                {"include_infrastructure_costs", "false"}};
    }

    std::string unique_comm(const char* tag) {
        return std::string("fmi-liveness-") + tag + "-" + std::to_string(::getpid()) + "-" +
               std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    }

    int* shared_flags(int n) {
        return static_cast<int*>(mmap(nullptr, n * sizeof(int), PROT_READ | PROT_WRITE,
                                      MAP_SHARED | MAP_ANONYMOUS, -1, 0));
    }
}

BOOST_AUTO_TEST_SUITE(LinkLiveness)

BOOST_AUTO_TEST_CASE(a_receiver_offers_an_ack_before_the_senders_window_fills) {
    // Pure state machine: the ack interval must be strictly inside the window, or the sender
    // is blocked before the receiver has said anything.
    SequencedLink link({8, 4096, 1u << 20});
    BOOST_CHECK(!link.ack_due(4));
    for (int i = 0; i < 4; i++) {
        FrameHeader arriving;
        arriving.transport_seq = static_cast<std::uint64_t>(i);
        arriving.total_length = 4;
        arriving.payload_length = 4;
        link.commit_inline(arriving);
    }
    BOOST_CHECK_MESSAGE(link.ack_due(4), "four commits must be worth telling the peer about");
    link.note_ack_sent(4);
    BOOST_CHECK_MESSAGE(!link.ack_due(4), "an ack already on the wire must not be repeated");
}

BOOST_AUTO_TEST_CASE(an_ack_frame_carries_no_payload_and_no_sequence) {
    char wire[frame_header_bytes];
    encode_header(make_ack(17), wire);
    FrameHeader got;
    BOOST_REQUIRE(decode_header(wire, frame_header_bytes, 4096, got) == DecodeStatus::Ok);
    BOOST_CHECK(got.frame_type == FrameType::Ack);
    BOOST_CHECK_EQUAL(got.cumulative_ack, 17u);
    BOOST_CHECK_EQUAL(got.payload_length, 0u);
    BOOST_CHECK_EQUAL(got.transport_seq, 0u);
}

BOOST_AUTO_TEST_CASE(an_ack_frame_claiming_a_payload_is_malformed) {
    // Otherwise an ack would be a way to inject bytes the receiver never accounts for.
    FrameHeader forged = make_ack(3);
    forged.payload_length = 8;
    forged.total_length = 8;
    char wire[frame_header_bytes];
    encode_header(forged, wire);
    FrameHeader got;
    BOOST_CHECK(decode_header(wire, frame_header_bytes, 4096, got) == DecodeStatus::Inconsistent);
}

BOOST_AUTO_TEST_CASE(a_one_way_link_keeps_flowing_past_its_window) {
    // The regression that three ranks exposed, reduced to two: rank 0 sends far more messages
    // than the window holds and rank 1 never sends anything back. Under the piggyback-only
    // protocol rank 0 stops at the window and never resumes.
    constexpr int num_peers = 2;
    constexpr int messages = 40;   // ten times the window
    const std::string name = unique_comm("oneway");
    int* ok = shared_flags(num_peers);

    ForkedRankGuard rank_guard;
    int& peer_id = rank_guard.peer_id;
    rank_guard.fork_ranks(num_peers);
    ok[peer_id] = 0;

    try {
        auto ch = Channel::get_channel("DirectTCP", one_way_params(), model_params());
        ch->set_peer_id(peer_id);
        ch->set_num_peers(num_peers);
        ch->set_comm_name(name);

        if (peer_id == 0) {
            for (int i = 0; i < messages; i++) {
                OperationScope scope(p2p_identity(1));
                ch->send({reinterpret_cast<char*>(&i), sizeof(i)}, 1);
            }
            ok[0] = 1;
        } else {
            int failures = 0;
            for (int i = 0; i < messages; i++) {
                int got = -1;
                OperationScope scope(p2p_identity(1));
                ch->recv({reinterpret_cast<char*>(&got), sizeof(got)}, 0);
                if (got != i) {
                    failures++;
                }
            }
            ok[1] = (failures == 0);
        }
        ch->finalize();
    } catch (const std::exception& e) {
        std::fprintf(stderr, "[rank %d] %s\n", peer_id, e.what());
    }

    rank_guard.reap_ranks();
    BOOST_CHECK_MESSAGE(ok[0] == 1, "the sender stalled on a link the peer never writes to");
    BOOST_CHECK_MESSAGE(ok[1] == 1, "the receiver did not get every message in order");
}

BOOST_AUTO_TEST_CASE(a_one_way_collective_pattern_survives_three_ranks) {
    // The shape the failure actually took: a chain 0 -> 1 -> 2 where no link ever carries a
    // reply, repeated until every window would have filled several times over.
    constexpr int num_peers = 3;
    constexpr int rounds = 30;
    const std::string name = unique_comm("chain");
    int* ok = shared_flags(num_peers);

    ForkedRankGuard rank_guard;
    int& peer_id = rank_guard.peer_id;
    rank_guard.fork_ranks(num_peers);
    ok[peer_id] = 0;

    try {
        auto ch = Channel::get_channel("DirectTCP", one_way_params(), model_params());
        ch->set_peer_id(peer_id);
        ch->set_num_peers(num_peers);
        ch->set_comm_name(name);

        int failures = 0;
        for (int r = 0; r < rounds; r++) {
            if (peer_id == 0) {
                int v = r;
                OperationScope scope(p2p_identity(1));
                ch->send({reinterpret_cast<char*>(&v), sizeof(v)}, 1);
            } else if (peer_id == 1) {
                int got = -1;
                {
                    OperationScope scope(p2p_identity(1));
                    ch->recv({reinterpret_cast<char*>(&got), sizeof(got)}, 0);
                }
                if (got != r) {
                    failures++;
                }
                int fwd = got + 1000;
                OperationScope scope(p2p_identity(2));
                ch->send({reinterpret_cast<char*>(&fwd), sizeof(fwd)}, 2);
            } else {
                int got = -1;
                OperationScope scope(p2p_identity(2));
                ch->recv({reinterpret_cast<char*>(&got), sizeof(got)}, 1);
                if (got != r + 1000) {
                    failures++;
                }
            }
        }
        ok[peer_id] = (failures == 0);
        ch->finalize();
    } catch (const std::exception& e) {
        std::fprintf(stderr, "[rank %d] %s\n", peer_id, e.what());
    }

    rank_guard.reap_ranks();
    for (int i = 0; i < num_peers; i++) {
        BOOST_CHECK_MESSAGE(ok[i] == 1, "rank " << i << " did not complete the one-way chain");
    }
}

BOOST_AUTO_TEST_CASE(the_window_is_the_bound_on_unreceived_sends_and_it_fails_loudly) {
    // The one behavioural limit framing imposes that the unframed transport did not have.
    // Unframed, "how many messages may a rank send before its peer receives any" was bounded
    // by the kernel socket buffer, which for small messages is effectively unbounded. Framed
    // with recovery, it is bounded by link_window_frames, because every unacknowledged send is
    // retained. Exceeding it must be a loud, immediate error naming the link -- never a hang,
    // and never a silently dropped message.
    constexpr int num_peers = 2;
    const std::string name = unique_comm("window");
    int* ok = shared_flags(num_peers);
    int* loud = shared_flags(1);

    ForkedRankGuard rank_guard;
    int& peer_id = rank_guard.peer_id;
    rank_guard.fork_ranks(num_peers);
    ok[peer_id] = 0;
    if (peer_id == 0) { loud[0] = 0; }

    try {
        // A short deadline: the sender waits it out collecting acks that will never come, and
        // that wait is the duration of this case.
        auto params = one_way_params();
        params["max_timeout"] = "2000";
        auto ch = Channel::get_channel("DirectTCP", params, model_params());
        ch->set_peer_id(peer_id);
        ch->set_num_peers(num_peers);
        ch->set_comm_name(name);

        if (peer_id == 0) {
            // One exchange first, because DirectTCP builds links lazily: a peer that never does
            // any I/O never connects, and the sender would fail establishing rather than
            // filling its window. Then the window (4, from one_way_params) fills.
            int sent = 0;
            try {
                for (int i = 0; i < 64; i++) {
                    OperationScope scope(p2p_identity(1));
                    ch->send({reinterpret_cast<char*>(&i), sizeof(i)}, 1);
                    sent++;
                }
            } catch (const std::exception& e) {
                const std::string what = e.what();
                loud[0] = what.find("retention limit") != std::string::npos
                          && what.find("peer 1") != std::string::npos;
            }
            // The first message is acknowledged, so a full window fits behind it.
            ok[0] = (sent >= 5);
        } else {
            // Takes delivery of exactly one message, so the link exists, and then stops
            // receiving. Nothing after that can prune the sender's retention.
            int got = -1;
            OperationScope scope(p2p_identity(1));
            ch->recv({reinterpret_cast<char*>(&got), sizeof(got)}, 0);
            std::this_thread::sleep_for(std::chrono::milliseconds(3000));
            ok[1] = (got == 0);
        }
        ch->finalize();
    } catch (const std::exception& e) {
        std::fprintf(stderr, "[rank %d] %s\n", peer_id, e.what());
    }

    rank_guard.reap_ranks();
    BOOST_CHECK_MESSAGE(ok[0] == 1, "a whole window of unreceived sends must be admitted");
    BOOST_CHECK_MESSAGE(ok[1] == 1, "the idle peer did not take its one message");
    BOOST_CHECK_MESSAGE(loud[0] == 1,
                        "exceeding the window must fail loudly, naming the link -- not hang");
}

BOOST_AUTO_TEST_CASE(a_rank_blocked_on_one_peer_still_accepts_another) {
    // The obligation the whole checkpoint story rests on. A rank waiting for one peer must keep
    // accepting connections from the others: accept_one used to run only inside build_mesh, so
    // a rank blocked in a *receive* accepted nothing, and after a restore - when several links
    // must be rebuilt at once - the peer trying to reconnect sat in the backlog while the rank
    // that would answer it waited on a link that peer was part of.
    //
    // The ordering below is what makes this test mean anything, and an earlier version of it
    // did not: rank 0 must ALREADY hold its link to rank 2 before it blocks, or its first
    // receive establishes that link through build_mesh, which polls the listener itself and
    // accepts rank 1 there. The obligation under test is the one inside the blocking read.
    constexpr int num_peers = 3;
    const std::string name = unique_comm("accept-while-waiting");
    int* ok = shared_flags(num_peers);

    ForkedRankGuard rank_guard;
    int& peer_id = rank_guard.peer_id;
    rank_guard.fork_ranks(num_peers);
    ok[peer_id] = 0;

    try {
        auto params = one_way_params();
        params["max_timeout"] = "8000";
        auto ch = Channel::get_channel("DirectTCP", params, model_params());
        ch->set_peer_id(peer_id);
        ch->set_num_peers(num_peers);
        ch->set_comm_name(name);

        if (peer_id == 0) {
            int first = 0, second = 0, from_one = 0;
            {   // establishes the link to rank 2, so the wait below is a pure read
                OperationScope scope(p2p_identity(0));
                ch->recv({reinterpret_cast<char*>(&first), sizeof(first)}, 2);
            }
            {   // blocks INSIDE read_all on an established link; rank 1 arrives during this
                OperationScope scope(p2p_identity(0));
                ch->recv({reinterpret_cast<char*>(&second), sizeof(second)}, 2);
            }
            {
                OperationScope scope(p2p_identity(0));
                ch->recv({reinterpret_cast<char*>(&from_one), sizeof(from_one)}, 1);
            }
            ok[0] = (first == 220 && second == 222 && from_one == 111);
        } else if (peer_id == 1) {
            // Arrives while rank 0 is inside its second receive from rank 2.
            std::this_thread::sleep_for(std::chrono::milliseconds(700));
            int v = 111;
            OperationScope scope(p2p_identity(0));
            ch->send({reinterpret_cast<char*>(&v), sizeof(v)}, 0);
            ok[1] = 1;
        } else {
            int v = 220;
            {
                OperationScope scope(p2p_identity(0));
                ch->send({reinterpret_cast<char*>(&v), sizeof(v)}, 0);
            }
            // Long enough that rank 1's connection must be accepted from inside the wait.
            std::this_thread::sleep_for(std::chrono::milliseconds(1800));
            v = 222;
            OperationScope scope(p2p_identity(0));
            ch->send({reinterpret_cast<char*>(&v), sizeof(v)}, 0);
            ok[2] = 1;
        }
        ch->finalize();
    } catch (const std::exception& e) {
        std::fprintf(stderr, "[rank %d] %s\n", peer_id, e.what());
    }

    rank_guard.reap_ranks();
    BOOST_CHECK_MESSAGE(ok[0] == 1, "rank 0 did not receive all three messages");
    BOOST_CHECK_MESSAGE(ok[1] == 1, "rank 1 could not reach a rank that was busy waiting");
    BOOST_CHECK_MESSAGE(ok[2] == 1, "rank 2 could not complete its sends");
}

BOOST_AUTO_TEST_CASE(a_moved_rank_starts_every_link_afresh_after_reconfigure) {
    // The epoch-reconfigure contract, from both chairs at once. A survivor resets its link TO
    // a moved rank; the moved rank -- which re-enters reconfigure_for_epoch with its OLD
    // process memory when migration is in-place or CRIU-restored, and which appears in its own
    // moved set -- must reset EVERY link it holds. Resetting only links[self] leaves its send
    // counters where the old epoch left them, and its first framed send in the new epoch
    // reports a sequence gap that never happened on the wire (observed as "expected 0 but
    // received 31" before the fix).
    constexpr int num_peers = 2;
    const std::string name = unique_comm("moved-rank-reset");
    int* ok = shared_flags(num_peers);

    ForkedRankGuard rank_guard;
    int& peer_id = rank_guard.peer_id;
    rank_guard.fork_ranks(num_peers);
    ok[peer_id] = 0;

    try {
        auto params = one_way_params();
        params["max_timeout"] = "8000";
        auto ch = Channel::get_channel("DirectTCP", params, model_params());
        ch->set_peer_id(peer_id);
        ch->set_num_peers(num_peers);
        ch->set_comm_name(name);

        // Old epoch: three sends advance rank 1's counters well past zero.
        for (int i = 0; i < 3; i++) {
            int v = 100 + i;
            OperationScope scope(p2p_identity(0));
            if (peer_id == 1) {
                ch->send({reinterpret_cast<char*>(&v), sizeof(v)}, 0);
            } else {
                int got = -1;
                ch->recv({reinterpret_cast<char*>(&got), sizeof(got)}, 1);
                if (got != v) {
                    throw std::runtime_error("old-epoch payload wrong");
                }
            }
        }

        // The cut: rank 1 moved. Both sides reconfigure, exactly as the FT runtime does.
        ch->reconfigure_for_epoch(name + "@1", {1});

        // New epoch: the exchange only completes if rank 1 restarted its link state -- a stale
        // counter surfaces as a loud sequence-gap exception on rank 0, not a wrong value.
        {
            int v = 777;
            OperationScope scope(p2p_identity(0));
            if (peer_id == 1) {
                ch->send({reinterpret_cast<char*>(&v), sizeof(v)}, 0);
            } else {
                int got = -1;
                ch->recv({reinterpret_cast<char*>(&got), sizeof(got)}, 1);
                if (got != 777) {
                    throw std::runtime_error("new-epoch payload wrong");
                }
            }
        }
        ok[peer_id] = 1;
        ch->finalize();
    } catch (const std::exception& e) {
        std::fprintf(stderr, "[rank %d] %s\n", peer_id, e.what());
    }

    rank_guard.reap_ranks();
    BOOST_CHECK_MESSAGE(ok[0] == 1, "rank 0 could not receive across the epoch reconfigure");
    BOOST_CHECK_MESSAGE(ok[1] == 1, "the moved rank could not send in the new epoch");
}

BOOST_AUTO_TEST_SUITE_END()
