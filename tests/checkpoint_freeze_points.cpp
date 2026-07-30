#include <boost/test/unit_test.hpp>

#include "../include/comm/Channel.h"
#include "../include/comm/LinkFrame.h"
#include "../include/comm/OperationScope.h"
#include "../include/comm/TcpChannelBase.h"

#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <csignal>
#include <chrono>
#include <cstring>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

//! A checkpoint can land between any two instructions. These are the instants that matter.
/*!
 * docs/tla/SequencedLink.tla model-checks four freeze positions in the receive path, from the
 * frame still being on the wire to its ack already having left. The transport has to be
 * correct at every one of them, and "correct" is a strong statement: after the restore the
 * message is delivered exactly once, in order, with no gap reported.
 *
 * The position that is easy to get wrong — and that the transport did get wrong — is between
 * parsing a header and reading its payload. Committing there advances the receive watermark
 * past a message whose bytes are still in a kernel socket buffer, which no checkpoint image
 * captures. The peer then learns, from the very next handshake, that the frame was received;
 * it prunes the frame from retention and never replays it. The message is gone, and nothing
 * anywhere reports an error.
 *
 * Rather than race a real checkpoint against that window, these cases put the test on the far
 * end of the wire, so the freeze lands exactly where each case says it does.
 */
BOOST_AUTO_TEST_SUITE(CheckpointFreezePoints)

using namespace FMI::Comm;

namespace {
    //! One socketpair at a time, replaceable, with the test holding the far end.
    struct Wire {
        std::mutex mu;
        int near_end = -1;   //!< handed (as a dup) to the channel under test
        int far_end = -1;    //!< the test's own end
        std::vector<int> issued;

        Wire() { rewire(); }

        ~Wire() {
            std::lock_guard<std::mutex> g(mu);
            drop();
        }

        void drop() {
            if (near_end >= 0) { ::close(near_end); near_end = -1; }
            if (far_end >= 0) { ::close(far_end); far_end = -1; }
        }

        void rewire() {
            std::lock_guard<std::mutex> g(mu);
            drop();
            int fds[2];
            BOOST_REQUIRE_EQUAL(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
            near_end = fds[0];
            far_end = fds[1];
        }

        //! Tear the connection down so the channel's next read or write fails.
        /*!
         * shutdown() on the descriptors the channel holds, not close(): the channel owns those
         * numbers and will close them itself, and closing them here would let the kernel hand
         * the same number straight back out to something else.
         */
        void sever() {
            {
                std::lock_guard<std::mutex> g(mu);
                for (int fd : issued) { ::shutdown(fd, SHUT_RDWR); }
                issued.clear();
            }
            rewire();
        }

        int checkout() {
            std::lock_guard<std::mutex> g(mu);
            const int fd = ::dup(near_end);
            issued.push_back(fd);
            return fd;
        }

        int test_end() {
            std::lock_guard<std::mutex> g(mu);
            return far_end;
        }
    };

    class WiredChannel : public TcpChannelBase {
    public:
        WiredChannel(Wire& wire, FMI::Utils::peer_num id, FMI::Utils::peer_num peers)
                : wire(wire) {
            std::map<std::string, std::string> params = {
                    {"max_timeout", "4000"}, {"framed", "true"}, {"recover_links", "true"}};
            parse_tcp_params(params);
            std::map<std::string, std::string> model = {
                    {"bandwidth", "1.0"}, {"overhead", "0.0"}, {"transfer_price", "0.0"},
                    {"vm_price", "0.0"}, {"requests_per_hour", "1"},
                    {"include_infrastructure_costs", "false"}};
            parse_tcp_model_params(model);
            transport_tag = "Wired";
            set_peer_id(id);
            set_num_peers(peers);
            set_comm_name("freeze");
        }

        std::size_t retained(FMI::Utils::peer_num peer) {
            return peer < links.size() ? links[peer].replay_suffix().size() : 0;
        }

    protected:
        int establish(FMI::Utils::peer_num, const std::string&) override { return wire.checkout(); }
        void close_transport_state() override {}

    private:
        Wire& wire;
    };

    //! The frame a rank waiting in `recv` for a 4-byte point-to-point message expects.
    FrameHeader p2p_frame(std::uint64_t seq, std::uint32_t dest) {
        FrameHeader h;
        h.lane = Lane::P2P;
        h.op_kind = OpKind::Send;
        h.collective_index = 0;
        h.root = dest;
        h.total_length = 4;
        h.payload_length = 4;
        h.transport_seq = seq;
        return h;
    }

    bool write_exact(int fd, const char* data, std::size_t len) {
        std::size_t sent = 0;
        while (sent < len) {
            const long n = ::send(fd, data + sent, len - sent, MSG_NOSIGNAL);
            if (n <= 0) { return false; }
            sent += static_cast<std::size_t>(n);
        }
        return true;
    }

    bool read_exact(int fd, char* data, std::size_t len, int timeout_ms = 4000) {
        struct timeval tv {timeout_ms / 1000, (timeout_ms % 1000) * 1000};
        ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
        std::size_t got = 0;
        while (got < len) {
            const long n = ::recv(fd, data + got, len - got, 0);
            if (n <= 0) { return false; }
            got += static_cast<std::size_t>(n);
        }
        return true;
    }

    //! Answer the repair handshake, reporting what the peer said it still expects.
    /*!
     * @param expect_seq what this side claims to have sent, mirrored back so reconcile accepts.
     */
    bool exchange(int fd, std::uint64_t peer_sent, HandshakePayload& theirs) {
        char in[handshake_bytes];
        if (!read_exact(fd, in, handshake_bytes)) { return false; }
        if (decode_handshake(in, handshake_bytes, theirs) != DecodeStatus::Ok) { return false; }
        HandshakePayload mine;
        mine.next_send_seq = peer_sent;
        mine.next_expected_seq = 0;   // the channel under test only receives here
        mine.lowest_retained = 0;
        char out[handshake_bytes];
        encode_handshake(mine, out);
        return write_exact(fd, out, handshake_bytes);
    }
}

BOOST_AUTO_TEST_CASE(a_freeze_between_the_header_and_its_payload_does_not_consume_the_message) {
    // Freeze position 2 in the model: the header has been parsed, the payload has not arrived.
    // The receive watermark must NOT have moved, or the peer prunes a message it never
    // delivered and the loss is silent.
    Wire wire;
    std::atomic<int> delivered{-1};
    std::atomic<bool> threw{false};
    std::atomic<std::uint64_t> reported_expectation{9999};

    std::thread receiver([&] {
        WiredChannel rx(wire, 1, 2);
        OperationScope scope(p2p_identity(1));
        try {
            int got = 0;
            rx.recv({reinterpret_cast<char*>(&got), sizeof(got)}, 0);
            delivered = got;
        } catch (const std::exception&) {
            threw = true;
        }
    });

    // Header only, then the connection dies with the payload still owed.
    char header[frame_header_bytes];
    encode_header(p2p_frame(0, 1), header);
    BOOST_REQUIRE(write_exact(wire.test_end(), header, frame_header_bytes));
    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    wire.sever();

    HandshakePayload theirs;
    const bool handshaken = exchange(wire.test_end(), 1, theirs);
    if (handshaken) {
        reported_expectation = theirs.next_expected_seq;
        // Replay the whole frame, exactly as a sender holding it in retention would.
        char full[frame_header_bytes + 4];
        encode_header(p2p_frame(0, 1), full);
        const int payload = 4242;
        std::memcpy(full + frame_header_bytes, &payload, 4);
        write_exact(wire.test_end(), full, sizeof(full));
    }
    receiver.join();

    BOOST_REQUIRE_MESSAGE(handshaken, "the receiver did not repair the link after the freeze");
    BOOST_CHECK_MESSAGE(reported_expectation == 0u,
                        "the receiver claimed to have taken delivery of a frame whose payload it "
                        "never read (expects " << reported_expectation << ", must be 0)");
    BOOST_CHECK_MESSAGE(!threw, "the receiver failed instead of recovering");
    BOOST_CHECK_EQUAL(delivered, 4242);
}

BOOST_AUTO_TEST_CASE(a_freeze_part_way_through_a_payload_does_not_consume_the_message) {
    // The same position, reached with some of the payload already copied into the
    // application's buffer. The partial bytes are garbage and must be overwritten by the
    // replay, not treated as a delivery.
    Wire wire;
    std::atomic<int> delivered{-1};
    std::atomic<bool> threw{false};
    std::atomic<std::uint64_t> reported_expectation{9999};

    std::thread receiver([&] {
        WiredChannel rx(wire, 1, 2);
        OperationScope scope(p2p_identity(1));
        try {
            int got = 0;
            rx.recv({reinterpret_cast<char*>(&got), sizeof(got)}, 0);
            delivered = got;
        } catch (const std::exception&) {
            threw = true;
        }
    });

    char header_and_half[frame_header_bytes + 2];
    encode_header(p2p_frame(0, 1), header_and_half);
    header_and_half[frame_header_bytes] = 0x7F;
    header_and_half[frame_header_bytes + 1] = 0x7F;
    BOOST_REQUIRE(write_exact(wire.test_end(), header_and_half, sizeof(header_and_half)));
    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    wire.sever();

    HandshakePayload theirs;
    const bool handshaken = exchange(wire.test_end(), 1, theirs);
    if (handshaken) {
        reported_expectation = theirs.next_expected_seq;
        char full[frame_header_bytes + 4];
        encode_header(p2p_frame(0, 1), full);
        const int payload = 777;
        std::memcpy(full + frame_header_bytes, &payload, 4);
        write_exact(wire.test_end(), full, sizeof(full));
    }
    receiver.join();

    BOOST_REQUIRE_MESSAGE(handshaken, "the receiver did not repair the link after the freeze");
    BOOST_CHECK_EQUAL(reported_expectation, 0u);
    BOOST_CHECK_MESSAGE(!threw, "the receiver failed instead of recovering");
    BOOST_CHECK_EQUAL(delivered, 777);
}

BOOST_AUTO_TEST_CASE(a_freeze_after_delivery_makes_the_replay_a_duplicate_not_a_second_delivery) {
    // Freeze position 3: the frame was delivered and committed, but its ack never left. The
    // peer replays; the receiver must recognise the replay, discard it, and go on to deliver
    // the NEXT message rather than handing the same one over twice.
    Wire wire;
    std::vector<int> got;
    std::atomic<bool> threw{false};
    std::atomic<std::uint64_t> reported_expectation{9999};

    std::thread receiver([&] {
        WiredChannel rx(wire, 1, 2);
        OperationScope scope(p2p_identity(1));
        try {
            for (int i = 0; i < 2; i++) {
                int value = 0;
                rx.recv({reinterpret_cast<char*>(&value), sizeof(value)}, 0);
                got.push_back(value);
            }
        } catch (const std::exception&) {
            threw = true;
        }
    });

    char full[frame_header_bytes + 4];
    encode_header(p2p_frame(0, 1), full);
    const int first = 11;
    std::memcpy(full + frame_header_bytes, &first, 4);
    BOOST_REQUIRE(write_exact(wire.test_end(), full, sizeof(full)));
    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    wire.sever();

    HandshakePayload theirs;
    const bool handshaken = exchange(wire.test_end(), 2, theirs);
    if (handshaken) {
        reported_expectation = theirs.next_expected_seq;
        // Replay frame 0 — the peer never heard it was received — then send frame 1.
        write_exact(wire.test_end(), full, sizeof(full));
        char second_frame[frame_header_bytes + 4];
        encode_header(p2p_frame(1, 1), second_frame);
        const int second = 22;
        std::memcpy(second_frame + frame_header_bytes, &second, 4);
        write_exact(wire.test_end(), second_frame, sizeof(second_frame));
    }
    receiver.join();

    BOOST_REQUIRE_MESSAGE(handshaken, "the receiver did not repair the link after the freeze");
    BOOST_CHECK_MESSAGE(reported_expectation == 1u,
                        "a fully delivered frame must be acknowledged in the handshake (expects "
                                << reported_expectation << ", must be 1)");
    BOOST_CHECK_MESSAGE(!threw, "the receiver failed instead of recovering");
    BOOST_REQUIRE_EQUAL(got.size(), 2u);
    BOOST_CHECK_EQUAL(got[0], 11);
    BOOST_CHECK_MESSAGE(got[1] == 22, "the replayed frame was delivered a second time");
}

BOOST_AUTO_TEST_CASE(a_freeze_before_a_frame_ever_reached_the_wire_still_owes_it) {
    // Freeze position 1, on the sending side: the message was admitted to retention and the
    // connection died before or during the write. Once send() has returned, delivery is owed
    // regardless of what reached the socket.
    Wire wire;
    std::atomic<bool> sent_ok{false};
    std::atomic<std::size_t> still_owed{0};

    std::thread sender([&] {
        WiredChannel tx(wire, 0, 2);
        OperationScope scope(p2p_identity(1));
        int value = 31337;
        try {
            tx.send({reinterpret_cast<char*>(&value), sizeof(value)}, 1);
            sent_ok = true;
        } catch (const std::exception&) {
            // Retention still holds it, which is what the check below is about.
        }
        still_owed = tx.retained(1);
    });

    // Let the first frame land, then kill the link before anything acknowledges it.
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    wire.sever();
    // The sender's repair will hand us a handshake; answer it so it can finish.
    HandshakePayload theirs;
    exchange(wire.test_end(), 0, theirs);
    sender.join();

    BOOST_CHECK_MESSAGE(still_owed >= 1u,
                        "an unacknowledged message must stay in retention across the break");
}

BOOST_AUTO_TEST_CASE(identity_is_still_enforced_on_a_frame_that_arrives_by_replay) {
    // The composition the design calls unchecked, at the smallest scale that can exhibit it:
    // contract 2 puts a frame on the wire a second time, and contract 1 still has to judge it.
    // A replay is the one path where a frame reaches the receiver without the sender having
    // just produced it, so it is exactly where a divergent peer's payload could slip past the
    // identity check and be delivered as if it belonged to the operation being waited on.
    Wire wire;
    std::atomic<int> delivered{-1};
    std::atomic<bool> refused{false};

    std::thread receiver([&] {
        WiredChannel rx(wire, 1, 2);
        OperationScope scope(p2p_identity(1));
        try {
            int got = 0;
            rx.recv({reinterpret_cast<char*>(&got), sizeof(got)}, 0);
            delivered = got;
        } catch (const std::exception& e) {
            refused = std::string(e.what()).find("identity mismatch") != std::string::npos;
        }
    });

    char header[frame_header_bytes];
    encode_header(p2p_frame(0, 1), header);
    BOOST_REQUIRE(write_exact(wire.test_end(), header, frame_header_bytes));
    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    wire.sever();

    HandshakePayload theirs;
    const bool handshaken = exchange(wire.test_end(), 1, theirs);
    if (handshaken) {
        // Replay sequence 0 as the protocol requires — but belonging to a DIFFERENT logical
        // operation than the one the receiver is waiting on.
        FrameHeader wrong = p2p_frame(0, 1);
        wrong.lane = Lane::Collective;
        wrong.op_kind = OpKind::Bcast;
        wrong.collective_index = 4;
        wrong.root = 0;
        char full[frame_header_bytes + 4];
        encode_header(wrong, full);
        const int payload = 999;
        std::memcpy(full + frame_header_bytes, &payload, 4);
        write_exact(wire.test_end(), full, sizeof(full));
    }
    receiver.join();

    BOOST_REQUIRE_MESSAGE(handshaken, "the receiver did not repair the link after the freeze");
    BOOST_CHECK_MESSAGE(delivered == -1, "a replayed frame from another operation was delivered");
    BOOST_CHECK_MESSAGE(refused, "the replayed frame was not refused as an identity mismatch");
}

BOOST_AUTO_TEST_CASE(a_restored_rank_survives_writing_to_a_connection_the_restore_dropped) {
    // Every socket a checkpointed process owned comes back dropped (criu --tcp-close), and the
    // first thing a restored rank does is talk to the peer registry over one of them. hiredis
    // writes without MSG_NOSIGNAL, so with the default disposition that write delivers SIGPIPE
    // and the rank dies there — before it can re-establish a single link, before it can print
    // a diagnostic. The recovery machinery is worth nothing if the process is not alive to run
    // it, and this is the whole of what stood between the link layer and a working restore the
    // first time an FMI rank was checkpointed for real.
    std::map<std::string, std::string> params = {
            {"registry_host", "127.0.0.1"}, {"registry_port", "6379"},
            {"bind_host", "127.0.0.1"},     {"advertise_host", "127.0.0.1"},
            {"max_timeout", "2000"},        {"framed", "true"}, {"recover_links", "true"}};
    std::map<std::string, std::string> model = {
            {"bandwidth", "400"}, {"overhead", "0.2"}, {"transfer_price", "0"},
            {"vm_price", "0.0134"}, {"requests_per_hour", "1000"},
            {"include_infrastructure_costs", "false"}};
    auto channel = Channel::get_channel("DirectTCP", params, model);

    struct sigaction current {};
    BOOST_REQUIRE_EQUAL(::sigaction(SIGPIPE, nullptr, &current), 0);
    BOOST_CHECK_MESSAGE(current.sa_handler != SIG_DFL,
                        "a channel that owns a hiredis connection left SIGPIPE fatal");

    // And the disposition actually holds: writing to a closed peer must return EPIPE rather
    // than kill this process. If it does not, the test binary dies here and says so loudly.
    int fds[2];
    BOOST_REQUIRE_EQUAL(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
    ::close(fds[1]);
    const long n = ::send(fds[0], "x", 1, 0);
    BOOST_CHECK_MESSAGE(n < 0 && errno == EPIPE, "expected EPIPE, got " << n);
    ::close(fds[0]);
    channel->finalize();
}

BOOST_AUTO_TEST_SUITE_END()
