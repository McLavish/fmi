#include <boost/test/unit_test.hpp>

#include "../include/comm/LinkFrame.h"
#include "../include/comm/OperationScope.h"
#include "../include/comm/TcpChannelBase.h"
#include "../include/comm/DirectTCP.h"
#include "forked_rank_guard.h"

#include <sys/socket.h>
#include <unistd.h>
#include <atomic>
#include <mutex>
#include <string>
#include <chrono>
#include <thread>
#include <vector>
#include <sys/mman.h>
#include <sys/wait.h>
#include <cstdio>

//! Retention and replay inside the TRANSPORT, across a connection that actually dies.
/*!
 * tests/link_recovery.cpp proves the SequencedLink state machine recovers; this proves
 * TcpChannelBase drives it. The two ends run on separate threads because repair involves a
 * handshake, so both must be live at once — a single-threaded harness would deadlock the
 * moment one side blocked reading the other's handshake.
 *
 * Connectivity is a socketpair handed out by a switchboard rather than a real rendezvous, so
 * these cases isolate the repair/replay logic from DirectTCP's re-registration behaviour.
 */
BOOST_AUTO_TEST_SUITE(TransportRecovery)

using namespace FMI::Comm;

namespace {
    //! Hands each end its side of the current socketpair, and can replace the pair on demand.
    struct Switchboard {
        std::mutex mu;
        int ends[2] = {-1, -1};
        int generation = 0;

        Switchboard() { wire(); }
        ~Switchboard() { cut(); }

        void wire() {
            int fds[2];
            BOOST_REQUIRE_EQUAL(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
            ends[0] = fds[0];
            ends[1] = fds[1];
            generation++;
        }

        void cut() {
            for (int& fd : ends) {
                if (fd >= 0) { ::close(fd); fd = -1; }
            }
        }

        //! Descriptors handed to the channels, which own and will close them.
        std::vector<int> issued;

        //! When set, every checkout yields a socket whose far end is already gone, so each
        //! repair attempt immediately fails again. Models a peer that has left for good.
        bool peer_is_gone = false;

        //! When set, every checkout yields a link whose far end completes the handshake and
        //! then closes. Repair keeps SUCCEEDING and the link keeps dying, which is the only
        //! shape in which the repair budget — rather than a failing repair — ends the loop.
        bool peer_is_flapping = false;

        //! Destroy the connection and put a fresh one in its place.
        /*!
         * shutdown() rather than close(): it tears the connection down so the channels' next
         * read or write fails, while leaving their descriptors valid for them to close
         * themselves. Closing here instead would leave the channels holding stale numbers that
         * the kernel could hand straight back out.
         *
         * Shutting down only the switchboard's own copies would break nothing at all — a
         * dup()ed descriptor keeps the connection alive — which is exactly the mistake that
         * made an earlier version of this harness test nothing.
         */
        void sever() {
            std::lock_guard<std::mutex> g(mu);
            for (int fd : issued) { ::shutdown(fd, SHUT_RDWR); }
            issued.clear();
            cut();
            wire();
        }

        //! A duplicate of this end's current descriptor, so the channel may close its own copy.
        int checkout(int side) {
            std::lock_guard<std::mutex> g(mu);
            BOOST_REQUIRE(ends[side] >= 0);
            if (peer_is_flapping) {
                int pair[2];
                BOOST_REQUIRE_EQUAL(socketpair(AF_UNIX, SOCK_STREAM, 0, pair), 0);
                std::thread([far = pair[1]] {
                    // Answer a handshake if one arrives, then die either way. The FIRST
                    // connection carries a frame header rather than a handshake — handshakes
                    // only happen on repair — so this must not block waiting for one.
                    struct timeval tv {0, 200000};
                    setsockopt(far, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
                    char in[handshake_bytes];
                    ssize_t got = ::recv(far, in, sizeof in, MSG_WAITALL);
                    if (got == static_cast<ssize_t>(sizeof in)) {
                        HandshakePayload reply;   // fresh link: nothing sent, nothing expected
                        char out[handshake_bytes];
                        encode_handshake(reply, out);
                        ssize_t n = ::write(far, out, sizeof out);
                        (void) n;
                    }
                    ::close(far);
                }).detach();
                issued.push_back(pair[0]);
                return pair[0];
            }
            if (peer_is_gone) {
                int dead[2];
                BOOST_REQUIRE_EQUAL(socketpair(AF_UNIX, SOCK_STREAM, 0, dead), 0);
                ::close(dead[1]);          // nothing will ever answer on this link
                issued.push_back(dead[0]);
                return dead[0];
            }
            const int fd = ::dup(ends[side]);
            issued.push_back(fd);
            return fd;
        }
    };

    class PairedChannel : public TcpChannelBase {
    public:
        PairedChannel(Switchboard& board, int side, FMI::Utils::peer_num id, FMI::Utils::peer_num peers)
                : board(board), side(side) {
            std::map<std::string, std::string> params = {
                    {"max_timeout",    "4000"},
                    {"framed",         "true"},
                    {"recover_links",  "true"}
            };
            parse_tcp_params(params);
            std::map<std::string, std::string> model = {
                    {"bandwidth", "1.0"}, {"overhead", "0.0"}, {"transfer_price", "0.0"},
                    {"vm_price", "0.0"}, {"requests_per_hour", "1"},
                    {"include_infrastructure_costs", "false"}
            };
            parse_tcp_model_params(model);
            transport_tag = "Paired";
            set_peer_id(id);
            set_num_peers(peers);
            set_comm_name("recovery");
        }

        //! Shrink the repair budget so the bound is reached quickly in tests.
        void set_repair_budget(int n) { max_link_repairs = n; }

        //! Frames this channel still owes @p peer. Test-only view of protected link state.
        std::size_t retained(FMI::Utils::peer_num peer) {
            return peer < links.size() ? links[peer].replay_suffix().size() : 0;
        }

    protected:
        int establish(FMI::Utils::peer_num, const std::string&) override {
            return board.checkout(side);
        }
        void close_transport_state() override {}

    private:
        Switchboard& board;
        int side;
    };

    constexpr int message_count = 12;

    int payload_for(int i) { return 1000 + i; }
}

BOOST_AUTO_TEST_CASE(a_severed_link_replays_and_loses_nothing) {
    Switchboard board;
    std::atomic<bool> sender_done{false};
    std::vector<int> received;
    std::atomic<int> receiver_errors{0};

    std::thread receiver([&] {
        PairedChannel rx(board, 1, 1, 2);
        OperationScope scope(p2p_identity(1));
        for (int i = 0; i < message_count; i++) {
            try {
                int got = 0;
                rx.recv({reinterpret_cast<char*>(&got), sizeof(got)}, 0);
                received.push_back(got);
            } catch (const std::exception&) {
                receiver_errors++;
                break;
            }
        }
    });

    std::thread sender([&] {
        PairedChannel tx(board, 0, 0, 2);
        OperationScope scope(p2p_identity(1));
        for (int i = 0; i < message_count; i++) {
            int val = payload_for(i);
            try {
                tx.send({reinterpret_cast<char*>(&val), sizeof(val)}, 1);
            } catch (const std::exception&) {
                // Retention holds it; the repair replays it.
            }
            if (i == message_count / 2) {
                board.sever();   // kill the connection mid-stream
            }
        }
        sender_done = true;
    });

    sender.join();
    receiver.join();

    BOOST_TEST_MESSAGE("received " << received.size() << " of " << message_count
                       << ", receiver errors " << receiver_errors.load());
    // EVERY message must arrive: a completed send is a delivery obligation, and the whole
    // point of retention is that severing the link does not discharge it. Checking only that
    // what arrived was a correct prefix would pass even if nothing arrived at all.
    BOOST_CHECK_EQUAL(receiver_errors.load(), 0);
    BOOST_REQUIRE_EQUAL(received.size(), static_cast<std::size_t>(message_count));
    for (int i = 0; i < message_count; i++) {
        BOOST_CHECK_EQUAL(received[i], payload_for(i));
    }
}

BOOST_AUTO_TEST_CASE(retention_holds_every_unacknowledged_send) {
    // A send that returns is a delivery obligation. With no traffic in the reverse direction
    // nothing can be acknowledged, so retention must still hold every frame.
    Switchboard board;
    PairedChannel tx(board, 0, 0, 2);
    OperationScope scope(p2p_identity(1));
    for (int i = 0; i < 5; i++) {
        int val = payload_for(i);
        tx.send({reinterpret_cast<char*>(&val), sizeof(val)}, 1);
    }
    BOOST_CHECK_EQUAL(tx.retained(1), 5u);
}

BOOST_AUTO_TEST_CASE(reverse_traffic_prunes_retention_through_the_piggybacked_ack) {
    // The transport is blocking, so there is no thread free to write a standalone ack. Every
    // frame therefore carries the sender's cumulative ack for the reverse direction, and any
    // traffic the other way is what keeps retention from growing without bound.
    Switchboard board;
    std::atomic<std::size_t> retained_after{0};
    std::atomic<int> errors{0};

    std::thread b([&] {
        try {
            PairedChannel rx(board, 1, 1, 2);
            for (int i = 0; i < 6; i++) {
                int got = 0;
                {
                    OperationScope scope(p2p_identity(1));
                    rx.recv({reinterpret_cast<char*>(&got), sizeof(got)}, 0);
                }
                // Reply, so a cumulative ack travels back to the sender.
                int reply = got + 1;
                OperationScope scope(p2p_identity(0));
                rx.send({reinterpret_cast<char*>(&reply), sizeof(reply)}, 0);
            }
        } catch (const std::exception&) { errors++; }
    });

    std::thread a([&] {
        try {
            PairedChannel tx(board, 0, 0, 2);
            for (int i = 0; i < 6; i++) {
                int val = payload_for(i);
                {
                    OperationScope scope(p2p_identity(1));
                    tx.send({reinterpret_cast<char*>(&val), sizeof(val)}, 1);
                }
                int back = 0;
                OperationScope scope(p2p_identity(0));
                tx.recv({reinterpret_cast<char*>(&back), sizeof(back)}, 1);
            }
            retained_after = tx.retained(1);
        } catch (const std::exception&) { errors++; }
    });

    a.join();
    b.join();

    BOOST_CHECK_EQUAL(errors.load(), 0);
    // Without ack piggybacking every one of the six sends would still be retained.
    BOOST_CHECK_MESSAGE(retained_after.load() < 6,
                        "retention was never pruned: " << retained_after.load()
                                                       << " frames still held after 6 exchanges");
}

BOOST_AUTO_TEST_CASE(a_malformed_handshake_is_rejected_rather_than_reconciled) {
    // The handshake governs what is replayed and what is pruned. Reconciling against garbage
    // would either drop frames the peer never received or replay ones it already holds.
    Switchboard board;
    std::atomic<int> rejected{0};

    std::thread victim([&] {
        try {
            PairedChannel rx(board, 1, 1, 2);
            int got = 0;
            OperationScope scope(p2p_identity(1));
            rx.recv({reinterpret_cast<char*>(&got), sizeof(got)}, 0);
        } catch (const std::exception& e) {
            const std::string what = e.what();
            if (what.find("handshake") != std::string::npos) { rejected++; }
        }
    });

    // Impersonate the far end: sever so the victim repairs, then answer its handshake with
    // bytes that are not one.
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    board.sever();
    {
        std::vector<char> junk(handshake_bytes, '\x5A');
        const int fd = board.checkout(0);
        ssize_t n = ::write(fd, junk.data(), junk.size());
        (void) n;
        ::close(fd);
    }
    victim.join();
    BOOST_CHECK_MESSAGE(rejected.load() == 1, "a malformed handshake was accepted");
}

BOOST_AUTO_TEST_CASE(a_peer_that_never_returns_fails_loudly_instead_of_repairing_forever) {
    // Failure detection belongs to the orchestrator, but that is not the same as spinning on
    // repair. A link that cannot be re-established must give up and say so, bounded.
    Switchboard board;
    PairedChannel rx(board, 1, 1, 2);
    board.peer_is_gone = true;
    board.sever();

    OperationScope scope(p2p_identity(1));
    int got = 0;
    const auto started = std::chrono::steady_clock::now();
    BOOST_CHECK_THROW(rx.recv({reinterpret_cast<char*>(&got), sizeof(got)}, 0),
                      std::runtime_error);
    const auto elapsed = std::chrono::steady_clock::now() - started;
    // Bounded by the repair budget rather than by the socket timeout alone.
    BOOST_CHECK_MESSAGE(elapsed < std::chrono::seconds(20),
                        "giving up took " <<
                        std::chrono::duration_cast<std::chrono::seconds>(elapsed).count() << "s");
}

BOOST_AUTO_TEST_CASE(a_flapping_link_exhausts_the_repair_budget_rather_than_looping) {
    // Repair succeeds every time and the link dies every time. Without a budget this spins
    // forever; the operation must instead fail and name the peer.
    Switchboard board;
    PairedChannel rx(board, 1, 1, 2);
    rx.set_repair_budget(3);
    board.peer_is_flapping = true;
    board.sever();

    OperationScope scope(p2p_identity(1));
    int got = 0;
    bool budget_message = false;
    try {
        rx.recv({reinterpret_cast<char*>(&got), sizeof(got)}, 0);
    } catch (const std::exception& e) {
        BOOST_TEST_MESSAGE("threw: " << e.what());
        budget_message = std::string(e.what()).find("could not be repaired") != std::string::npos;
    }
    BOOST_CHECK_MESSAGE(budget_message,
                        "a permanently flapping link did not exhaust the repair budget");
}

#if FMI_ENABLE_REDIS
namespace {
    //! DirectTCP with a way to tear down a live connection, so recovery can be driven over the
    //! real rendezvous rather than a controllable socketpair.
    class BreakableDirectTCP : public DirectTCP {
    public:
        using DirectTCP::DirectTCP;
        //! shutdown() rather than close(): the channel still owns the descriptor.
        void sever(FMI::Utils::peer_num p) {
            if (p < sockets.size() && sockets[p] >= 0) { ::shutdown(sockets[p], SHUT_RDWR); }
        }
    };

    std::map<std::string, std::string> dtcp_recover_params() {
        return {{"registry_host", "127.0.0.1"}, {"registry_port", "6379"},
                {"max_timeout", "5000"}, {"registry_poll_interval_ms", "2"},
                {"connect_retry_interval_ms", "5"}, {"registry_ttl_s", "120"},
                {"framed", "true"}, {"recover_links", "true"}};
    }
    std::map<std::string, std::string> dtcp_model() {
        return {{"bandwidth", "250.0"}, {"overhead", "0.20"}, {"transfer_price", "0.0"},
                {"vm_price", "0.0134"}, {"requests_per_hour", "1000"},
                {"include_infrastructure_costs", "true"}};
    }
}

BOOST_AUTO_TEST_CASE(recovery_over_the_real_rendezvous) {
    // Contract 2 end to end on the actual backend: break a live DirectTCP connection mid-run
    // and require every message to still arrive, in order, exactly once. Whether the Redis
    // registry supports re-pairing an existing link is precisely what this establishes.
    constexpr int num_peers = 2;
    constexpr int messages = 10;
    const std::string name = "recov_" +
            std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) +
            "_" + std::to_string(getpid());

    struct Shared { int ok; int count; int values[messages]; };
    Shared* out = static_cast<Shared*>(mmap(nullptr, sizeof(Shared), PROT_READ | PROT_WRITE,
                                            MAP_SHARED | MAP_ANONYMOUS, -1, 0));
    out->ok = 0; out->count = 0;

    ForkedRankGuard rank_guard;
    int& peer_id = rank_guard.peer_id;
    rank_guard.fork_ranks(num_peers);

    try {
        BreakableDirectTCP ch(dtcp_recover_params(), dtcp_model());
        ch.set_peer_id(peer_id);
        ch.set_num_peers(num_peers);
        ch.set_comm_name(name);

        for (int i = 0; i < messages; i++) {
            OperationScope scope(p2p_identity(1));
            if (peer_id == 0) {
                int val = payload_for(i);
                ch.send({reinterpret_cast<char*>(&val), sizeof(val)}, 1);
                if (i == messages / 2) { ch.sever(1); }   // break a live connection
            } else {
                int got = 0;
                ch.recv({reinterpret_cast<char*>(&got), sizeof(got)}, 0);
                if (out->count < messages) { out->values[out->count++] = got; }
            }
        }
        if (peer_id == 1) { out->ok = 1; }
        ch.finalize();
    } catch (const std::exception& e) {
        std::fprintf(stderr, "[rank %d] %s\n", peer_id, e.what());
    }

    rank_guard.reap_ranks();

    BOOST_CHECK_MESSAGE(out->ok == 1, "receiver did not complete after the connection broke");
    BOOST_REQUIRE_EQUAL(out->count, messages);
    for (int i = 0; i < messages; i++) {
        BOOST_CHECK_EQUAL(out->values[i], payload_for(i));
    }
}
#endif // FMI_ENABLE_REDIS

BOOST_AUTO_TEST_SUITE_END()
