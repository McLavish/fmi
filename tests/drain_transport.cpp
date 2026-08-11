//! The steady state of the drain transport, over real sockets and a real registry.
/*!
 * DrainTCP's bargain is that a message costs nothing beyond its own bytes: no header, no
 * sequence number, no ack, no retained copy. What it does own is a per-link lock that is
 * released around every wait, a control thread that accepts while the application is busy
 * elsewhere, and a 56-byte hello whose counters are the only thing standing between a
 * headerless stream and a silent desynchronisation.
 *
 * These cases pin the parts of that which only appear against real peers: both directions on
 * one link, a message far larger than any socket buffer (so it spans many chunks and the
 * offset-resume loop is exercised for real rather than incidentally), the inherited collectives
 * at four and eight ranks, and the refusal of a hello that does not belong to this listener
 * incarnation — after which the channel must still work, because a refusal is not a failure.
 *
 * Forked ranks, never threads: every rank needs its own listener, its own control thread and
 * its own registry connection, and ForkedRankGuard is mandatory (see its header — a case that
 * unwinds without exiting its children multiplies them into the next case).
 */
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>

// The registry this transport rendezvouses through is Redis; without it there is no backend.
#if FMI_ENABLE_REDIS

#include "../include/comm/Channel.h"
#include "../include/comm/DrainProtocol.h"
#include "../include/comm/PeerRegistry.h"
#include "../include/comm/TcpEndpoint.h"
#include "forked_rank_guard.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

#include <chrono>
#include <cstdio>
#include <cstring>
#include <map>
#include <string>
#include <thread>
#include <vector>

using namespace FMI::Comm;

namespace {
    std::map<std::string, std::string> drain_params() {
        return {{"registry_host", "127.0.0.1"},   {"registry_port", "6379"},
                {"bind_host", "127.0.0.1"},       {"advertise_host", "127.0.0.1"},
                {"max_timeout", "8000"},          {"registry_poll_interval_ms", "5"},
                {"connect_retry_interval_ms", "10"}, {"registry_ttl_s", "60"},
                {"control_poll_interval_ms", "20"}};
    }

    std::map<std::string, std::string> model_params() {
        return {{"bandwidth", "400"},   {"overhead", "0.2"}, {"transfer_price", "0"},
                {"vm_price", "0.0134"}, {"requests_per_hour", "1000"},
                {"include_infrastructure_costs", "false"}};
    }

    //! Nanosecond clock plus the pid: a Redis-backed registry persists, so two runs sharing a
    //! name means one dialling the other's live ranks.
    std::string unique_comm(const char* tag) {
        return std::string("fmi-drain-") + tag + "-" + std::to_string(::getpid()) + "-" +
               std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    }

    int* shared_flags(int n) {
        return static_cast<int*>(mmap(nullptr, n * sizeof(int), PROT_READ | PROT_WRITE,
                                      MAP_SHARED | MAP_ANONYMOUS, -1, 0));
    }

    std::shared_ptr<Channel> make_channel(int peer_id, int num_peers, const std::string& name) {
        auto ch = Channel::get_channel("DrainTCP", drain_params(), model_params());
        ch->set_peer_id(static_cast<FMI::Utils::peer_num>(peer_id));
        ch->set_num_peers(static_cast<FMI::Utils::peer_num>(num_peers));
        ch->set_comm_name(name);
        return ch;
    }

    //! Rank-ordered link name, derived exactly as the channel derives it.
    std::string link_name_of(const std::string& comm, unsigned int a, unsigned int b) {
        const unsigned int low = a < b ? a : b;
        const unsigned int high = a < b ? b : a;
        return comm + "|" + std::to_string(low) + "-" + std::to_string(high);
    }
}

BOOST_AUTO_TEST_SUITE(DrainTransport)

//! The hello is the only thing this transport ever puts on the wire besides payload, so its
//! layout is a contract rather than an implementation detail.
BOOST_AUTO_TEST_CASE(the_resume_record_survives_a_round_trip) {
    ResumeRecord out;
    out.sender_rank = 3;
    out.receiver_rank = 7;
    out.link_name_hash = 0x0123456789ABCDEFull;
    out.nonce = 0xFEDCBA9876543210ull;
    out.sender_incarnation = 42;
    out.bytes_sent = 1ull << 40;
    out.bytes_received = (1ull << 40) + 17;

    char wire[resume_record_bytes];
    encode_resume(out, wire);

    ResumeRecord back;
    BOOST_REQUIRE(decode_resume(wire, back));
    BOOST_CHECK_EQUAL(back.wire_version, drain_hello_version);
    BOOST_CHECK_EQUAL(back.sender_rank, out.sender_rank);
    BOOST_CHECK_EQUAL(back.receiver_rank, out.receiver_rank);
    BOOST_CHECK_EQUAL(back.link_name_hash, out.link_name_hash);
    BOOST_CHECK_EQUAL(back.nonce, out.nonce);
    BOOST_CHECK_EQUAL(back.sender_incarnation, out.sender_incarnation);
    BOOST_CHECK_EQUAL(back.bytes_sent, out.bytes_sent);
    BOOST_CHECK_EQUAL(back.bytes_received, out.bytes_received);

    // Whatever else is listening on a recycled ephemeral port does not get to be mistaken for
    // a peer: the magic and the version are checked before a single field is believed.
    char corrupted[resume_record_bytes];
    std::memcpy(corrupted, wire, resume_record_bytes);
    corrupted[0] = static_cast<char>(corrupted[0] ^ 0xFF);
    ResumeRecord ignored;
    BOOST_CHECK(!decode_resume(corrupted, ignored));

    std::memcpy(corrupted, wire, resume_record_bytes);
    corrupted[5] = static_cast<char>(corrupted[5] + 1);   // bump the wire version
    BOOST_CHECK(!decode_resume(corrupted, ignored));
}

//! A peer index past the end of the communicator must be reported, not indexed.
BOOST_AUTO_TEST_CASE(a_peer_index_out_of_range_is_refused) {
    auto ch = make_channel(0, 2, unique_comm("bounds"));
    int value = 1;
    BOOST_CHECK_THROW(ch->send({reinterpret_cast<char*>(&value), sizeof(value)}, 5),
                      std::runtime_error);
    BOOST_CHECK_THROW(ch->send({reinterpret_cast<char*>(&value), sizeof(value)}, 0),
                      std::runtime_error);
    ch->finalize();
}

//! Both directions on one link, repeatedly: the link is full duplex and the counters advance
//! independently per direction.
BOOST_AUTO_TEST_CASE(two_ranks_exchange_in_both_directions) {
    constexpr int num_peers = 2;
    constexpr int rounds = 25;
    const std::string name = unique_comm("pingpong");
    int* ok = shared_flags(num_peers);

    ForkedRankGuard rank_guard;
    int& peer_id = rank_guard.peer_id;
    rank_guard.fork_ranks(num_peers);
    ok[peer_id] = 0;

    try {
        auto ch = make_channel(peer_id, num_peers, name);
        int failures = 0;
        for (int r = 0; r < rounds; r++) {
            if (peer_id == 0) {
                int out = r;
                ch->send({reinterpret_cast<char*>(&out), sizeof(out)}, 1);
                int back = -1;
                ch->recv({reinterpret_cast<char*>(&back), sizeof(back)}, 1);
                if (back != r + 1000) {
                    failures++;
                }
            } else {
                int got = -1;
                ch->recv({reinterpret_cast<char*>(&got), sizeof(got)}, 0);
                if (got != r) {
                    failures++;
                }
                int reply = got + 1000;
                ch->send({reinterpret_cast<char*>(&reply), sizeof(reply)}, 0);
            }
        }
        ok[peer_id] = (failures == 0);
        ch->finalize();
    } catch (const std::exception& e) {
        std::fprintf(stderr, "[rank %d] %s\n", peer_id, e.what());
    }

    rank_guard.reap_ranks();
    BOOST_CHECK_MESSAGE(ok[0] == 1, "rank 0 did not complete the exchange");
    BOOST_CHECK_MESSAGE(ok[1] == 1, "rank 1 did not complete the exchange");
}

//! One message far larger than any socket buffer, so it necessarily spans many chunks: the
//! offset-resume loop must reassemble it byte for byte, and every chunk boundary is a point at
//! which the link lock was released and retaken.
BOOST_AUTO_TEST_CASE(a_message_spanning_many_chunks_arrives_whole) {
    constexpr int num_peers = 2;
    constexpr std::size_t payload_bytes = 6u << 20;   // 6 MiB, well past any socket buffer
    const std::string name = unique_comm("bulk");
    int* ok = shared_flags(num_peers);

    ForkedRankGuard rank_guard;
    int& peer_id = rank_guard.peer_id;
    rank_guard.fork_ranks(num_peers);
    ok[peer_id] = 0;

    try {
        auto ch = make_channel(peer_id, num_peers, name);
        std::vector<char> buffer(payload_bytes);
        if (peer_id == 0) {
            for (std::size_t i = 0; i < payload_bytes; i++) {
                buffer[i] = static_cast<char>((i * 31u + 7u) & 0xFF);
            }
            ch->send({buffer.data(), buffer.size()}, 1);
            ok[0] = 1;
        } else {
            ch->recv({buffer.data(), buffer.size()}, 0);
            std::size_t wrong = 0;
            for (std::size_t i = 0; i < payload_bytes; i++) {
                if (buffer[i] != static_cast<char>((i * 31u + 7u) & 0xFF)) {
                    wrong++;
                }
            }
            ok[1] = (wrong == 0);
        }
        ch->finalize();
    } catch (const std::exception& e) {
        std::fprintf(stderr, "[rank %d] %s\n", peer_id, e.what());
    }

    rank_guard.reap_ranks();
    BOOST_CHECK_MESSAGE(ok[0] == 1, "the sender did not finish a 6 MiB message");
    BOOST_CHECK_MESSAGE(ok[1] == 1, "the receiver did not reassemble the message byte for byte");
}

//! The inherited collectives, unmodified, over this channel. That they need no change is the
//! transparency claim: the subclass surface is send_object/recv_object and nothing else.
BOOST_AUTO_TEST_CASE(four_ranks_run_the_collectives) {
    constexpr int num_peers = 4;
    const std::string name = unique_comm("collectives");
    int* ok = shared_flags(num_peers);

    ForkedRankGuard rank_guard;
    int& peer_id = rank_guard.peer_id;
    rank_guard.fork_ranks(num_peers);
    ok[peer_id] = 0;

    try {
        auto ch = make_channel(peer_id, num_peers, name);
        int failures = 0;

        // bcast from a root that is not 0, so the id transformation is exercised too.
        constexpr FMI::Utils::peer_num bcast_root = 2;
        int broadcast = (peer_id == bcast_root) ? 4242 : -1;
        ch->bcast({reinterpret_cast<char*>(&broadcast), sizeof(broadcast)}, bcast_root);
        if (broadcast != 4242) {
            failures++;
        }

        // gather to root 1.
        constexpr FMI::Utils::peer_num gather_root = 1;
        int contribution = peer_id + 1;
        std::vector<int> gathered(num_peers, 0);
        if (peer_id == gather_root) {
            ch->gather({reinterpret_cast<char*>(&contribution), sizeof(contribution)},
                       {reinterpret_cast<char*>(gathered.data()), gathered.size() * sizeof(int)},
                       gather_root);
            for (int i = 0; i < num_peers; i++) {
                if (gathered[i] != i + 1) {
                    failures++;
                }
            }
        } else {
            ch->gather({reinterpret_cast<char*>(&contribution), sizeof(contribution)}, {},
                       gather_root);
        }

        // scatter from root 3.
        constexpr FMI::Utils::peer_num scatter_root = 3;
        std::vector<int> to_scatter(num_peers, 0);
        for (int i = 0; i < num_peers; i++) {
            to_scatter[i] = 100 + i;
        }
        int slice = -1;
        if (peer_id == scatter_root) {
            ch->scatter({reinterpret_cast<char*>(to_scatter.data()), to_scatter.size() * sizeof(int)},
                        {reinterpret_cast<char*>(&slice), sizeof(slice)}, scatter_root);
        } else {
            ch->scatter({}, {reinterpret_cast<char*>(&slice), sizeof(slice)}, scatter_root);
        }
        if (slice != 100 + peer_id) {
            failures++;
        }

        auto sum = [](char* a, char* b) {
            *reinterpret_cast<int*>(a) = *reinterpret_cast<int*>(a) + *reinterpret_cast<int*>(b);
        };
        int mine = peer_id + 1;
        int reduced = 0;
        ch->allreduce({reinterpret_cast<char*>(&mine), sizeof(mine)},
                      {reinterpret_cast<char*>(&reduced), sizeof(reduced)}, {sum, true, true});
        if (reduced != num_peers * (num_peers + 1) / 2) {
            failures++;
        }

        ch->barrier();

        ok[peer_id] = (failures == 0);
        ch->finalize();
    } catch (const std::exception& e) {
        std::fprintf(stderr, "[rank %d] %s\n", peer_id, e.what());
    }

    rank_guard.reap_ranks();
    for (int i = 0; i < num_peers; i++) {
        BOOST_CHECK_MESSAGE(ok[i] == 1, "rank " << i << " did not complete the collectives");
    }
}

//! Eight ranks: the dial-direction rule (dial down, accept up) has to produce a connected mesh
//! under a recursive-doubling schedule where several ranks dial the same peer at once.
BOOST_AUTO_TEST_CASE(eight_ranks_allreduce) {
    constexpr int num_peers = 8;
    const std::string name = unique_comm("allreduce8");
    int* results = shared_flags(num_peers);

    ForkedRankGuard rank_guard;
    int& peer_id = rank_guard.peer_id;
    rank_guard.fork_ranks(num_peers);
    results[peer_id] = -1;

    try {
        auto ch = make_channel(peer_id, num_peers, name);
        auto sum = [](char* a, char* b) {
            *reinterpret_cast<int*>(a) = *reinterpret_cast<int*>(a) + *reinterpret_cast<int*>(b);
        };
        int mine = peer_id + 1;
        int reduced = 0;
        ch->allreduce({reinterpret_cast<char*>(&mine), sizeof(mine)},
                      {reinterpret_cast<char*>(&reduced), sizeof(reduced)}, {sum, true, true});
        results[peer_id] = reduced;
        ch->finalize();
    } catch (const std::exception& e) {
        std::fprintf(stderr, "[rank %d] %s\n", peer_id, e.what());
    }

    rank_guard.reap_ranks();
    constexpr int expected = num_peers * (num_peers + 1) / 2;
    for (int i = 0; i < num_peers; i++) {
        BOOST_CHECK_MESSAGE(results[i] == expected,
                            "rank " << i << " reduced to " << results[i] << ", expected "
                                    << expected);
    }
}

//! A second pair of ranks under a comm name that has already carried traffic must establish
//! normally: the registry field is the rank, the link name embeds both ranks, and neither
//! carries anything from the pair that went before.
BOOST_AUTO_TEST_CASE(a_second_rank_pair_reuses_the_comm_name) {
    constexpr int num_peers = 4;
    const std::string name = unique_comm("twopairs");
    int* ok = shared_flags(num_peers);

    ForkedRankGuard rank_guard;
    int& peer_id = rank_guard.peer_id;
    rank_guard.fork_ranks(num_peers);
    ok[peer_id] = 0;

    try {
        // Phase 1: ranks 0 and 1 exchange; 2 and 3 build a channel and touch nothing.
        {
            auto ch = make_channel(peer_id, num_peers, name);
            int value = 7;
            if (peer_id == 0) {
                ch->send({reinterpret_cast<char*>(&value), sizeof(value)}, 1);
            } else if (peer_id == 1) {
                int got = -1;
                ch->recv({reinterpret_cast<char*>(&got), sizeof(got)}, 0);
                ok[1] = (got == 7);
            }
            ch->finalize();
        }
        if (peer_id == 0) {
            ok[0] = 1;
        }

        // Phase 2: fresh channels, same comm name, the OTHER pair.
        {
            auto ch = make_channel(peer_id, num_peers, name);
            int value = 9;
            if (peer_id == 2) {
                int got = -1;
                ch->recv({reinterpret_cast<char*>(&got), sizeof(got)}, 3);
                ok[2] = (got == 9);
            } else if (peer_id == 3) {
                ch->send({reinterpret_cast<char*>(&value), sizeof(value)}, 2);
                ok[3] = 1;
            }
            ch->finalize();
        }
    } catch (const std::exception& e) {
        std::fprintf(stderr, "[rank %d] %s\n", peer_id, e.what());
    }

    rank_guard.reap_ranks();
    for (int i = 0; i < num_peers; i++) {
        BOOST_CHECK_MESSAGE(ok[i] == 1, "rank " << i << " did not complete its phase");
    }
}

//! A hand-written hello that does not belong to this listener incarnation is refused, and the
//! refusal costs the channel nothing: the link it claimed keeps carrying traffic afterwards.
/*!
 * The nonce is checked before anything is compared against link state and long before any
 * existing connection could be replaced, which is what makes this safe to assert: a dialer
 * that reached a recycled port has nothing to say about this link, so its bogus counters are
 * never even looked at. That ordering is the reason an intruder cannot poison a live link.
 */
BOOST_AUTO_TEST_CASE(a_hello_for_the_wrong_listener_is_refused) {
    constexpr int num_peers = 2;
    const std::string name = unique_comm("intruder");
    int* ok = shared_flags(num_peers + 1);

    ForkedRankGuard rank_guard;
    int& peer_id = rank_guard.peer_id;
    rank_guard.fork_ranks(num_peers);
    ok[peer_id] = 0;
    if (peer_id == 0) {
        ok[num_peers] = 0;   // the intruder verdict, written by rank 0 only
    }

    try {
        auto ch = make_channel(peer_id, num_peers, name);
        if (peer_id == 1) {
            // Rank 1 dials rank 0, so rank 0 has a listener with a nonce to misquote.
            int first = 11;
            ch->send({reinterpret_cast<char*>(&first), sizeof(first)}, 0);
            int second = -1;
            ch->recv({reinterpret_cast<char*>(&second), sizeof(second)}, 0);
            ok[1] = (second == 22);
        } else {
            int first = -1;
            ch->recv({reinterpret_cast<char*>(&first), sizeof(first)}, 1);

            // Read rank 0's own advertisement, then contradict its nonce. Reported by throwing
            // rather than by BOOST_REQUIRE: this runs inside the case's own try, and a Boost
            // abort here would skip reap_ranks() and leave the forked rank unwaited.
            PeerRegistry probe("127.0.0.1", 6379);
            auto entries = probe.snapshot("fmi:drain:" + name, 2000);
            if (entries.count(0) != 1) {
                throw std::runtime_error("rank 0 published no address");
            }
            const std::string entry = entries[0];
            const auto colon = entry.find(':');
            const auto last = entry.rfind(':');
            if (colon == std::string::npos || last == colon) {
                throw std::runtime_error("malformed registry entry: " + entry);
            }
            const std::string ip = entry.substr(0, colon);
            const int port = std::stoi(entry.substr(colon + 1, last - colon - 1));
            const std::uint64_t nonce = std::stoull(entry.substr(last + 1));

            int intruder = ::socket(AF_INET, SOCK_STREAM, 0);
            if (intruder < 0) {
                throw std::runtime_error("could not open the intruder socket");
            }
            struct sockaddr_in dest{};
            dest.sin_family = AF_INET;
            dest.sin_port = htons(static_cast<std::uint16_t>(port));
            if (::inet_pton(AF_INET, ip.c_str(), &dest.sin_addr) != 1 ||
                ::connect(intruder, reinterpret_cast<struct sockaddr*>(&dest), sizeof(dest)) != 0) {
                ::close(intruder);
                throw std::runtime_error("could not reach rank 0's advertised listener");
            }

            ResumeRecord forged;
            forged.sender_rank = 1;
            forged.receiver_rank = 0;
            forged.link_name_hash = TcpEndpoint::fnv1a64(link_name_of(name, 0, 1));
            forged.nonce = nonce ^ 1ull;      // a listener incarnation that never existed
            forged.sender_incarnation = 0;
            forged.bytes_sent = 999999;       // and counters that could never be right
            forged.bytes_received = 999999;
            char wire[resume_record_bytes];
            encode_resume(forged, wire);
            if (::send(intruder, wire, resume_record_bytes, MSG_NOSIGNAL) !=
                static_cast<ssize_t>(resume_record_bytes)) {
                ::close(intruder);
                throw std::runtime_error("could not write the forged hello");
            }

            // Refusal is a close, so the intruder sees EOF (or a reset) rather than a reply.
            struct pollfd pfd{intruder, POLLIN, 0};
            const int pr = ::poll(&pfd, 1, 3000);
            char reply[resume_record_bytes];
            const ssize_t got = pr > 0 ? ::recv(intruder, reply, sizeof(reply), 0) : -2;
            ::close(intruder);
            ok[num_peers] = (got <= 0 && got != -2);

            // ...and the real link is untouched.
            int second = 22;
            ch->send({reinterpret_cast<char*>(&second), sizeof(second)}, 1);
            ok[0] = (first == 11);
        }
        ch->finalize();
    } catch (const std::exception& e) {
        std::fprintf(stderr, "[rank %d] %s\n", peer_id, e.what());
    }

    rank_guard.reap_ranks();
    BOOST_CHECK_MESSAGE(ok[num_peers] == 1, "the forged hello was answered instead of refused");
    BOOST_CHECK_MESSAGE(ok[0] == 1, "rank 0 lost its link across the refusal");
    BOOST_CHECK_MESSAGE(ok[1] == 1, "rank 1 did not receive the message sent after the refusal");
}

//! The counter cross-check, from the accepting side: everything about this hello is right except
//! the counts, and the counts are what refuses it.
/*!
 * This is the whole safety story of a headerless stream. There is no per-message identity on the
 * wire, so the only thing standing between a cut that lost or duplicated a byte and a permanently
 * desynchronised link is the pair of cumulative counters each end asserts at every reconnect. A
 * test that forges the nonce never reaches them — the nonce is checked first, on purpose — so the
 * comparison has to be approached with the *right* nonce, taken from the target's own registry
 * entry, and with a lineage and a link name that are equally correct.
 *
 * The refusal is deliberately not survivable for that link: it is recorded by the control thread
 * and raised, naming all four counts, on the next application thread to touch it. What must
 * survive is the rest of the channel, which is why there is a third rank here.
 */
BOOST_AUTO_TEST_CASE(a_hello_whose_counters_disagree_is_refused) {
    constexpr int num_peers = 3;
    const std::string name = unique_comm("counters");
    int* ok = shared_flags(num_peers + 2);

    ForkedRankGuard rank_guard;
    int& peer_id = rank_guard.peer_id;
    rank_guard.fork_ranks(num_peers);
    ok[peer_id] = 0;
    if (peer_id == 0) {
        ok[num_peers] = 0;       // was the hello refused
        ok[num_peers + 1] = 0;   // did the failure name the counts
    }

    try {
        auto ch = make_channel(peer_id, num_peers, name);
        if (peer_id == 1) {
            // Four bytes, so rank 0's bytes_received on this link is exactly 4 and its
            // bytes_sent exactly 0 — the numbers the forged hello has to contradict.
            int first = 11;
            ch->send({reinterpret_cast<char*>(&first), sizeof(first)}, 0);
            ok[1] = 1;
        } else if (peer_id == 2) {
            int later = 33;
            ch->send({reinterpret_cast<char*>(&later), sizeof(later)}, 0);
            ok[2] = 1;
        } else {
            int first = -1;
            ch->recv({reinterpret_cast<char*>(&first), sizeof(first)}, 1);

            // Rank 0's own advertisement, believed in full: the address, and the nonce that
            // proves a dialer reached this listener incarnation rather than a recycled port.
            PeerRegistry probe("127.0.0.1", 6379);
            auto entries = probe.snapshot("fmi:drain:" + name, 2000);
            if (entries.count(0) != 1) {
                throw std::runtime_error("rank 0 published no address");
            }
            const std::string entry = entries[0];
            const auto colon = entry.find(':');
            const auto last = entry.rfind(':');
            if (colon == std::string::npos || last == colon) {
                throw std::runtime_error("malformed registry entry: " + entry);
            }
            const std::string ip = entry.substr(0, colon);
            const int port = std::stoi(entry.substr(colon + 1, last - colon - 1));
            const std::uint64_t nonce = std::stoull(entry.substr(last + 1));

            int intruder = ::socket(AF_INET, SOCK_STREAM, 0);
            if (intruder < 0) {
                throw std::runtime_error("could not open the intruder socket");
            }
            struct sockaddr_in dest{};
            dest.sin_family = AF_INET;
            dest.sin_port = htons(static_cast<std::uint16_t>(port));
            if (::inet_pton(AF_INET, ip.c_str(), &dest.sin_addr) != 1 ||
                ::connect(intruder, reinterpret_cast<struct sockaddr*>(&dest), sizeof(dest)) != 0) {
                ::close(intruder);
                throw std::runtime_error("could not reach rank 0's advertised listener");
            }

            ResumeRecord forged;
            forged.sender_rank = 1;
            forged.receiver_rank = 0;
            forged.link_name_hash = TcpEndpoint::fnv1a64(link_name_of(name, 0, 1));
            forged.nonce = nonce;             // the real one: nothing is refused before the counts
            forged.sender_incarnation = 0;    // the lineage rank 0 has already seen
            forged.bytes_received = 0;        // and this half is the truth
            forged.bytes_sent = 7;            // three bytes that were never on this link
            char wire[resume_record_bytes];
            encode_resume(forged, wire);
            if (::send(intruder, wire, resume_record_bytes, MSG_NOSIGNAL) !=
                static_cast<ssize_t>(resume_record_bytes)) {
                ::close(intruder);
                throw std::runtime_error("could not write the forged hello");
            }

            // Refusal is a close: no reply record comes back.
            struct pollfd pfd{intruder, POLLIN, 0};
            const int pr = ::poll(&pfd, 1, 3000);
            char reply[resume_record_bytes];
            const ssize_t got = pr > 0 ? ::recv(intruder, reply, sizeof(reply), 0) : -2;
            ::close(intruder);
            ok[num_peers] = (got <= 0 && got != -2);

            // Recorded by the control thread, raised on the application thread, naming both
            // sides of both counts — the loud failure the design promises in place of a silent
            // stream desynchronisation.
            int ignored = 0;
            try {
                ch->send({reinterpret_cast<char*>(&ignored), sizeof(ignored)}, 1);
            } catch (const std::runtime_error& e) {
                const std::string text = e.what();
                ok[num_peers + 1] = (text.find("peer sent 7") != std::string::npos &&
                                     text.find("we received 4") != std::string::npos);
            }

            // ...and the refusal cost this channel nothing anywhere else: the link to rank 2 is
            // a different link and still carries its message.
            int later = -1;
            ch->recv({reinterpret_cast<char*>(&later), sizeof(later)}, 2);
            ok[0] = (first == 11 && later == 33);
        }
        ch->finalize();
    } catch (const std::exception& e) {
        std::fprintf(stderr, "[rank %d] %s\n", peer_id, e.what());
    }

    rank_guard.reap_ranks();
    BOOST_CHECK_MESSAGE(ok[num_peers] == 1,
                        "a hello whose counters disagree was answered instead of refused");
    BOOST_CHECK_MESSAGE(ok[num_peers + 1] == 1,
                        "the refusal did not reach the application naming both counts");
    BOOST_CHECK_MESSAGE(ok[0] == 1, "rank 0 lost an unrelated link across the refusal");
    BOOST_CHECK_MESSAGE(ok[1] == 1, "rank 1 did not send its message");
    BOOST_CHECK_MESSAGE(ok[2] == 1, "rank 2 did not send its message");
}

//! The other half of the cross-check: the *dialer* asserts the reply's counters, and a reply that
//! claims bytes this link never carried is a failure the application is told about, not a link.
/*!
 * Both directions have to be checked at both ends, because either end can be the one that lost
 * the bytes. The far end here is hand-written — a listener standing where rank 0 would be, saying
 * something a healthy peer never says.
 */
BOOST_AUTO_TEST_CASE(a_reply_whose_counters_disagree_is_refused) {
    const std::string name = unique_comm("dialcounters");
    // Rank 1 of 2: it dials rank 0, so the dialer's half of the hello is the half under test.
    auto ch = make_channel(1, 2, name);

    int listener = ::socket(AF_INET, SOCK_STREAM, 0);
    BOOST_REQUIRE(listener >= 0);
    int one = 1;
    ::setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(0);
    ::inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
    BOOST_REQUIRE(::bind(listener, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) == 0);
    BOOST_REQUIRE(::listen(listener, 4) == 0);
    socklen_t len = sizeof(addr);
    BOOST_REQUIRE(::getsockname(listener, reinterpret_cast<struct sockaddr*>(&addr), &len) == 0);
    const int port = ntohs(addr.sin_port);
    const std::uint64_t nonce = 0xC0FFEEull;

    PeerRegistry registry("127.0.0.1", 6379);
    registry.publish("fmi:drain:" + name, "0",
                     "127.0.0.1:" + std::to_string(port) + ":" + std::to_string(nonce), 60, 2000);

    std::string far_end_trouble;
    std::thread far_end([&]() {
        struct pollfd pfd{listener, POLLIN, 0};
        if (::poll(&pfd, 1, 10000) <= 0) {
            far_end_trouble = "nobody dialled";
            return;
        }
        const int conn = ::accept(listener, nullptr, nullptr);
        if (conn < 0) {
            far_end_trouble = "accept failed";
            return;
        }
        char wire[resume_record_bytes];
        std::size_t moved = 0;
        while (moved < resume_record_bytes) {
            const ssize_t n = ::recv(conn, wire + moved, resume_record_bytes - moved, 0);
            if (n <= 0) {
                far_end_trouble = "no hello arrived";
                ::close(conn);
                return;
            }
            moved += static_cast<std::size_t>(n);
        }
        ResumeRecord theirs;
        if (!decode_resume(wire, theirs)) {
            far_end_trouble = "the hello did not decode";
            ::close(conn);
            return;
        }
        ResumeRecord mine;
        mine.sender_rank = 0;
        mine.receiver_rank = 1;
        mine.link_name_hash = theirs.link_name_hash;   // right link
        mine.nonce = 0;                                // right shape of answer
        mine.sender_incarnation = 0;                   // right lineage
        mine.bytes_sent = 5;                           // five bytes this link never carried
        mine.bytes_received = 0;
        char reply[resume_record_bytes];
        encode_resume(mine, reply);
        if (::send(conn, reply, resume_record_bytes, MSG_NOSIGNAL) !=
            static_cast<ssize_t>(resume_record_bytes)) {
            far_end_trouble = "could not write the reply";
        }
        // Left open until the case is done with it: closing here would let the dialer see a
        // plain EOF and retry, which is a different refusal from the one under test.
        struct pollfd wait_for_close{conn, POLLIN, 0};
        ::poll(&wait_for_close, 1, 5000);
        ::close(conn);
    });

    int value = 7;
    std::string raised;
    try {
        ch->send({reinterpret_cast<char*>(&value), sizeof(value)}, 0);
        raised = "(the send completed)";
    } catch (const std::exception& e) {
        raised = e.what();
    }
    far_end.join();
    ::close(listener);

    BOOST_CHECK_MESSAGE(far_end_trouble.empty(), "the far end could not play its part: "
                                                         << far_end_trouble);
    BOOST_CHECK_MESSAGE(raised.find("counters disagree") != std::string::npos &&
                                raised.find("peer sent 5") != std::string::npos &&
                                raised.find("we received 0") != std::string::npos,
                        "the dialer reported '" << raised
                                                << "', expected the counter disagreement naming "
                                                   "all four counts");
    ch->finalize();
}

BOOST_AUTO_TEST_SUITE_END()

#endif // FMI_ENABLE_REDIS
