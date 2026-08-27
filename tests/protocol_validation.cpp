#include <boost/test/unit_test.hpp>

#include "../include/comm/LinkFrame.h"
#include "../include/comm/SequencedLink.h"
#include "../include/comm/OperationScope.h"
#include "../include/comm/TcpChannelBase.h"

#include <sys/socket.h>
#include <unistd.h>
#include <dirent.h>
#include <cstring>
#include <map>
#include <string>
#include <vector>

//! Adversarial validation of the sequenced link protocol.
/*!
 * Written against a mutation-testing baseline: every case here exists because a deliberate
 * fault injected into the implementation SURVIVED the original suite. Each case is therefore
 * pinned to one specific defect it must detect, and varies exactly one thing at a time —
 * the original tests passed for the wrong reasons precisely because they varied several.
 */
BOOST_AUTO_TEST_SUITE(ProtocolValidation)

using namespace FMI::Comm;
using Accept = SequencedLink::Accept;

namespace {
    FrameHeader base_collective() {
        FrameHeader h;
        h.lane = Lane::Collective;
        h.op_kind = OpKind::Reduce;
        h.collective_index = 3;
        h.root = 1;
        h.commutative = true;
        h.associative = true;
        h.payload_length = 16;
        return h;
    }
}

// ============================================================ identity, one field at a time
// Each of these kills a mutation that nulls out one comparison in same_identity(). The
// original suite missed all six because every test varied two fields or more.

BOOST_AUTO_TEST_CASE(identity_matches_when_nothing_differs) {
    BOOST_CHECK(same_identity(base_collective(), base_collective()));
}

BOOST_AUTO_TEST_CASE(identity_differs_on_lane_alone) {
    FrameHeader a = base_collective(), b = a;
    b.lane = Lane::P2P;
    BOOST_CHECK(!same_identity(a, b));
}

BOOST_AUTO_TEST_CASE(identity_differs_on_op_kind_alone) {
    FrameHeader a = base_collective(), b = a;
    b.op_kind = OpKind::Allreduce;
    BOOST_CHECK(!same_identity(a, b));
}

BOOST_AUTO_TEST_CASE(identity_differs_on_collective_index_alone) {
    // The "one rank is a lap ahead" case: same collective, different iteration. Nothing else
    // distinguishes them, so if collective_index is not compared this substitutes silently.
    FrameHeader a = base_collective(), b = a;
    b.collective_index = a.collective_index + 1;
    BOOST_CHECK(!same_identity(a, b));
}

BOOST_AUTO_TEST_CASE(identity_differs_on_root_alone) {
    // Two gathers of equal size differing only in which rank is collecting.
    FrameHeader a = base_collective(), b = a;
    b.root = a.root + 1;
    BOOST_CHECK(!same_identity(a, b));
}

BOOST_AUTO_TEST_CASE(identity_differs_on_commutative_alone) {
    FrameHeader a = base_collective(), b = a;
    b.commutative = !a.commutative;
    BOOST_CHECK(!same_identity(a, b));
}

BOOST_AUTO_TEST_CASE(identity_differs_on_associative_alone) {
    FrameHeader a = base_collective(), b = a;
    b.associative = !a.associative;
    BOOST_CHECK(!same_identity(a, b));
}

BOOST_AUTO_TEST_CASE(identity_differs_on_payload_length_alone) {
    FrameHeader a = base_collective(), b = a;
    b.payload_length = a.payload_length * 2;
    BOOST_CHECK(!same_identity(a, b));
}

BOOST_AUTO_TEST_CASE(identity_ignores_reliability_fields) {
    // transport_seq is reliability-layer detail. Two frames of the same logical message that
    // differ in it are still the same identity.
    FrameHeader a = base_collective(), b = a;
    b.transport_seq = a.transport_seq + 99;
    BOOST_CHECK(same_identity(a, b));
}

// ================================================== the same holes, at the SequencedLink level

BOOST_AUTO_TEST_CASE(a_later_iteration_of_the_same_collective_is_refused) {
    SequencedLink sender{{8, 1u << 20, 1u << 20}};
    SequencedLink receiver{{8, 1u << 20, 1u << 20}};
    const std::string body = "0123";
    FrameHeader stamped;

    // Sender is in bcast #0; receiver has already moved on to bcast #1.
    BOOST_REQUIRE(sender.admit(
            [] { FrameHeader h; h.lane = Lane::Collective; h.op_kind = OpKind::Bcast;
                 h.collective_index = 0; h.root = 0; h.payload_length = 4; return h; }(),
            body.data(), body.size(), stamped));
    BOOST_REQUIRE(receiver.accept(stamped, body.data()) == Accept::Delivered);

    FrameHeader expected;
    expected.lane = Lane::Collective;
    expected.op_kind = OpKind::Bcast;
    expected.collective_index = 1;
    expected.root = 0;
    expected.payload_length = 4;   // differ in collective_index and NOTHING else
    char dst[4] = {0};
    BOOST_CHECK(receiver.deliver_into(expected, dst, 4) == Accept::IdentityMismatch);
}

BOOST_AUTO_TEST_CASE(a_collective_with_a_different_root_is_refused) {
    SequencedLink sender{{8, 1u << 20, 1u << 20}};
    SequencedLink receiver{{8, 1u << 20, 1u << 20}};
    const std::string body = "0123";
    FrameHeader stamped;
    FrameHeader sent;
    sent.lane = Lane::Collective;
    sent.op_kind = OpKind::Gather;
    sent.collective_index = 0;
    sent.root = 0;
    sent.payload_length = 4;       // so `expected` below differs in root and NOTHING else
    BOOST_REQUIRE(sender.admit(sent, body.data(), body.size(), stamped));
    BOOST_REQUIRE(receiver.accept(stamped, body.data()) == Accept::Delivered);

    FrameHeader expected = sent;
    expected.root = 1;
    char dst[4] = {0};
    BOOST_CHECK(receiver.deliver_into(expected, dst, 4) == Accept::IdentityMismatch);
}

BOOST_AUTO_TEST_CASE(a_payload_of_the_wrong_size_is_refused_and_writes_nothing) {
    // Kills a mutation that dropped the payload-size check in deliver_into. Without it a
    // shorter frame would memcpy into a larger application buffer, or vice versa.
    //
    // The identity here must MATCH, or same_identity short-circuits and the size check below is
    // never reached — which is what silently unpinned this case when payload_length absorbed
    // total_length's identity role. `sent` therefore carries the payload_length it is admitted
    // with, so the ONLY disagreement left for deliver_into to find is the caller's `len`.
    // Production cannot reach this state (expected_identity always builds payload_length from
    // the caller's own buffer length, so same_identity would catch it first); the check is
    // defence in depth, and this case is what keeps it honest.
    SequencedLink sender{{8, 1u << 20, 1u << 20}};
    SequencedLink receiver{{8, 1u << 20, 1u << 20}};
    const std::string body = "0123";
    FrameHeader sent;
    sent.lane = Lane::P2P;
    sent.op_kind = OpKind::Send;
    sent.root = 1;
    sent.payload_length = 4;
    FrameHeader stamped;
    BOOST_REQUIRE(sender.admit(sent, body.data(), body.size(), stamped));
    BOOST_REQUIRE(receiver.accept(stamped, body.data()) == Accept::Delivered);

    char dst[8];
    std::memset(dst, 0xEE, sizeof dst);
    // Identity agrees in every field; the caller simply asks for 8 bytes of a 4-byte message.
    FrameHeader expected = sent;
    BOOST_CHECK(receiver.deliver_into(expected, dst, 8) == Accept::IdentityMismatch);
    for (unsigned char c : dst) {
        BOOST_CHECK_EQUAL(c, 0xEE);   // nothing was written
    }
}

// ================================================================= adversarial wire injection

namespace {
    //! A framed TCP channel over a caller-supplied socket.
    /*!
     * establish() is the only thing a TcpChannelBase subclass must provide, so handing it one
     * end of a socketpair gives a real framed receive path with a fully controllable peer.
     * That is what lets these cases feed it bytes a cooperating peer would never send.
     */
    class LoopbackChannel : public TcpChannelBase {
    public:
        LoopbackChannel(int fd, bool framing) : injected(fd) {
            std::map<std::string, std::string> params = {
                    {"max_timeout", "2000"},
                    {"framed",      framing ? "true" : "false"}
            };
            parse_tcp_params(params);
            std::map<std::string, std::string> model = {
                    {"bandwidth", "1.0"}, {"overhead", "0.0"}, {"transfer_price", "0.0"},
                    {"vm_price", "0.0"}, {"requests_per_hour", "1"},
                    {"include_infrastructure_costs", "false"}
            };
            parse_tcp_model_params(model);
            transport_tag = "Loopback";
            set_peer_id(1);
            set_num_peers(2);
            set_comm_name("validation");
        }

    protected:
        int establish(FMI::Utils::peer_num, const std::string&) override { return injected; }
        void close_transport_state() override {}

    private:
        int injected;
    };

    //! A channel plus the far end of its socket, for writing hostile bytes.
    struct Wire {
        int fds[2];
        LoopbackChannel channel;

        explicit Wire(bool framing) : fds{-1, -1}, channel((make(fds), fds[0]), framing) {}

        ~Wire() {
            if (fds[0] >= 0) ::close(fds[0]);
            if (fds[1] >= 0) ::close(fds[1]);
        }

        static void make(int* out) { socketpair(AF_UNIX, SOCK_STREAM, 0, out); }

        void poke(const void* data, std::size_t len) const {
            BOOST_REQUIRE_EQUAL(::write(fds[1], data, len), static_cast<ssize_t>(len));
        }

        void close_peer() {
            if (fds[1] >= 0) { ::close(fds[1]); fds[1] = -1; }
        }
    };

    FrameHeader wire_p2p(std::uint64_t seq, std::uint32_t len) {
        FrameHeader h;
        h.lane = Lane::P2P;
        h.op_kind = OpKind::Send;
        h.root = 1;
        h.payload_length = len;
        h.transport_seq = seq;
        return h;
    }
}

BOOST_AUTO_TEST_CASE(a_well_formed_frame_is_accepted_over_a_real_socket) {
    Wire w(true);
    FrameHeader h = wire_p2p(0, 4);
    char hdr[frame_header_bytes];
    encode_header(h, hdr);
    w.poke(hdr, sizeof hdr);
    const int payload = 31337;
    w.poke(&payload, sizeof payload);

    int got = 0;
    OperationScope scope(p2p_identity(1));
    w.channel.recv({reinterpret_cast<char*>(&got), sizeof(got)}, 0);
    BOOST_CHECK_EQUAL(got, 31337);
}

BOOST_AUTO_TEST_CASE(garbage_on_the_wire_is_rejected_not_interpreted) {
    Wire w(true);
    std::vector<char> junk(frame_header_bytes + 16, '\x7F');
    w.poke(junk.data(), junk.size());

    int got = 0;
    OperationScope scope(p2p_identity(1));
    BOOST_CHECK_THROW(w.channel.recv({reinterpret_cast<char*>(&got), sizeof(got)}, 0),
                      std::runtime_error);
}

BOOST_AUTO_TEST_CASE(an_unframed_peer_cannot_be_mistaken_for_a_framed_one) {
    // A raw peer's first bytes are application payload; they will not carry our magic. This is
    // what makes a mixed-configuration job fail loudly instead of corrupting.
    Wire w(true);
    // Sized explicitly: poke() reads frame_header_bytes, so a shorter literal would read
    // past the end of the array.
    std::vector<char> raw(frame_header_bytes, 'R');
    w.poke(raw.data(), raw.size());

    int got = 0;
    OperationScope scope(p2p_identity(1));
    BOOST_CHECK_THROW(w.channel.recv({reinterpret_cast<char*>(&got), sizeof(got)}, 0),
                      std::runtime_error);
}

BOOST_AUTO_TEST_CASE(a_sequence_gap_on_the_wire_is_rejected) {
    // Kills a mutation that dropped the on-wire transport_seq check. A peer that skips a
    // sequence has lost a message; accepting the next one silently drops it.
    Wire w(true);
    FrameHeader h = wire_p2p(5, 4);   // receiver expects seq 0
    char hdr[frame_header_bytes];
    encode_header(h, hdr);
    w.poke(hdr, sizeof hdr);
    const int payload = 7;
    w.poke(&payload, sizeof payload);

    int got = 0;
    OperationScope scope(p2p_identity(1));
    BOOST_CHECK_THROW(w.channel.recv({reinterpret_cast<char*>(&got), sizeof(got)}, 0),
                      std::runtime_error);
}

BOOST_AUTO_TEST_CASE(a_frame_claiming_more_payload_than_the_buffer_is_rejected) {
    // Kills a mutation that dropped the decode-status check on the wire: payload_length is
    // attacker-controlled and must be validated against the receive buffer before any read.
    Wire w(true);
    FrameHeader h = wire_p2p(0, 4);
    h.payload_length = 1u << 20;
    char hdr[frame_header_bytes];
    encode_header(h, hdr);
    w.poke(hdr, sizeof hdr);

    int got = 0;
    OperationScope scope(p2p_identity(1));
    BOOST_CHECK_THROW(w.channel.recv({reinterpret_cast<char*>(&got), sizeof(got)}, 0),
                      std::runtime_error);
}

BOOST_AUTO_TEST_CASE(a_header_truncated_by_a_dying_peer_is_rejected) {
    Wire w(true);
    FrameHeader h = wire_p2p(0, 4);
    char hdr[frame_header_bytes];
    encode_header(h, hdr);
    w.poke(hdr, frame_header_bytes / 2);   // half a header, then the peer vanishes
    w.close_peer();

    int got = 0;
    OperationScope scope(p2p_identity(1));
    BOOST_CHECK_THROW(w.channel.recv({reinterpret_cast<char*>(&got), sizeof(got)}, 0),
                      std::runtime_error);
}

BOOST_AUTO_TEST_CASE(a_frame_for_a_different_operation_is_rejected_over_the_wire) {
    Wire w(true);
    FrameHeader h = wire_p2p(0, 4);
    h.lane = Lane::Collective;          // peer is in a barrier
    h.op_kind = OpKind::Barrier;
    h.root = 0;
    char hdr[frame_header_bytes];
    encode_header(h, hdr);
    w.poke(hdr, sizeof hdr);
    const int payload = 1;
    w.poke(&payload, sizeof payload);

    int got = 0;
    OperationScope scope(p2p_identity(1));   // we are in an application recv
    BOOST_CHECK_THROW(w.channel.recv({reinterpret_cast<char*>(&got), sizeof(got)}, 0),
                      std::runtime_error);
    BOOST_CHECK_EQUAL(got, 0);               // and nothing was written
}

BOOST_AUTO_TEST_CASE(the_unframed_path_still_accepts_raw_bytes) {
    // Guards the default: with framing off the receive path must be exactly what it was.
    Wire w(false);
    const int payload = 4242;
    w.poke(&payload, sizeof payload);

    int got = 0;
    OperationScope scope(p2p_identity(1));
    w.channel.recv({reinterpret_cast<char*>(&got), sizeof(got)}, 0);
    BOOST_CHECK_EQUAL(got, 4242);
}

// The cases below probe the decode-status check on the wire. Note what mutation testing showed
// about them: deleting the check entirely leaves all three of the next cases PASSING, because a
// header that fails to decode is left mostly default-constructed and the identity comparison
// rejects it anyway. Only the malformed-ack case further down actually needs the status, since
// an ack is skipped before identity is ever consulted.

BOOST_AUTO_TEST_CASE(a_bad_magic_is_rejected_even_when_the_identity_matches) {
    Wire w(true);
    FrameHeader h = wire_p2p(0, 4);
    char hdr[frame_header_bytes];
    encode_header(h, hdr);
    hdr[0] = static_cast<char>(hdr[0] ^ 0xFF);   // corrupt only the magic
    w.poke(hdr, sizeof hdr);
    const int payload = 5;
    w.poke(&payload, sizeof payload);

    int got = 0;
    OperationScope scope(p2p_identity(1));
    BOOST_CHECK_THROW(w.channel.recv({reinterpret_cast<char*>(&got), sizeof(got)}, 0),
                      std::runtime_error);
    BOOST_CHECK_EQUAL(got, 0);
}

BOOST_AUTO_TEST_CASE(a_future_wire_version_is_rejected_even_when_the_identity_matches) {
    // A later protocol generation may reuse these field offsets with different meanings.
    // Accepting it because the identity happens to parse would be exactly the silent
    // misinterpretation the version field exists to prevent.
    Wire w(true);
    FrameHeader h = wire_p2p(0, 4);
    char hdr[frame_header_bytes];
    encode_header(h, hdr);
    hdr[4] = static_cast<char>(frame_wire_version + 1);   // bump only the version
    w.poke(hdr, sizeof hdr);
    const int payload = 6;
    w.poke(&payload, sizeof payload);

    int got = 0;
    OperationScope scope(p2p_identity(1));
    BOOST_CHECK_THROW(w.channel.recv({reinterpret_cast<char*>(&got), sizeof(got)}, 0),
                      std::runtime_error);
    BOOST_CHECK_EQUAL(got, 0);
}

BOOST_AUTO_TEST_CASE(a_frame_claiming_more_payload_than_expected_is_rejected_before_it_desynchronises) {
    // payload_length is the only length on the wire and is part of message identity, so a frame
    // claiming more than the receiver asked for is refused outright. Accepting it and reading
    // only the expected bytes would leave the surplus in the stream and desynchronise every
    // subsequent frame on this link.
    Wire w(true);
    FrameHeader h = wire_p2p(0, 4);
    h.payload_length = 8;   // claims more payload than the message contains
    char hdr[frame_header_bytes];
    encode_header(h, hdr);
    w.poke(hdr, sizeof hdr);
    const char surplus[8] = {1, 2, 3, 4, 5, 6, 7, 8};
    w.poke(surplus, sizeof surplus);

    int got = 0;
    OperationScope scope(p2p_identity(1));
    BOOST_CHECK_THROW(w.channel.recv({reinterpret_cast<char*>(&got), sizeof(got)}, 0),
                      std::runtime_error);
}

BOOST_AUTO_TEST_CASE(a_frame_claiming_less_payload_than_expected_is_rejected_before_it_desynchronises) {
    // The mirror of the case above, and the more dangerous direction: a receiver that trusted
    // its own expectation over the frame's declared length would read past this frame —
    // swallowing the head of the following frame as if it were the tail of this one, and
    // shifting every message on the link from then on.
    Wire w(true);
    FrameHeader h = wire_p2p(0, 4);
    h.payload_length = 2;   // claims less payload than the message needs
    char hdr[frame_header_bytes];
    encode_header(h, hdr);
    w.poke(hdr, sizeof hdr);
    const char partial[2] = {9, 9};
    w.poke(partial, sizeof partial);
    // The next frame on the wire, which a short read would eat into.
    FrameHeader next = wire_p2p(1, 4);
    char next_hdr[frame_header_bytes];
    encode_header(next, next_hdr);
    w.poke(next_hdr, sizeof next_hdr);

    int got = 0;
    OperationScope scope(p2p_identity(1));
    BOOST_CHECK_THROW(w.channel.recv({reinterpret_cast<char*>(&got), sizeof(got)}, 0),
                      std::runtime_error);
}

BOOST_AUTO_TEST_CASE(an_ack_arriving_before_any_data_is_skipped_not_mistaken_for_a_message) {
    // A standalone ack is the first thing that reaches a rank on a link it has not yet received
    // anything over -- which is the normal case, not a corner one: acks travel the direction
    // that carries no data, so the receiving side's sequence is still zero when the first one
    // lands. It has to be skipped on its own merits. Falling through to the dedup path happens
    // to swallow later acks (their sequence is behind), which is why this case has to use the
    // FIRST one: at sequence zero the ack looks new, and a receiver that does not recognise it
    // as an ack compares it against the operation it is waiting for and reports a spurious
    // identity mismatch on a perfectly correct exchange.
    Wire w(true);
    char ack[frame_header_bytes];
    encode_header(make_ack(0), ack);
    w.poke(ack, sizeof ack);

    FrameHeader data = wire_p2p(0, 4);
    char hdr[frame_header_bytes];
    encode_header(data, hdr);
    w.poke(hdr, sizeof hdr);
    const int payload = 31415;
    w.poke(&payload, sizeof payload);

    int got = 0;
    OperationScope scope(p2p_identity(1));
    BOOST_CHECK_NO_THROW(w.channel.recv({reinterpret_cast<char*>(&got), sizeof(got)}, 0));
    BOOST_CHECK_EQUAL(got, 31415);
}

BOOST_AUTO_TEST_CASE(an_ack_frame_carrying_a_payload_is_reported_as_malformed_not_as_divergence) {
    // The one malformed frame the identity check cannot catch on its own -- and the reason this
    // case asserts on WHICH error, not merely that one occurred. An ack is skipped before
    // identity is ever consulted, so a receiver that ignored the decode status would step over
    // this frame and parse its smuggled payload as the next frame's header. That desynchronised
    // read then fails the identity check, so the receive still throws; it simply blames the
    // wrong thing, reporting a peer executing a different operation when the truth is a
    // malformed frame and a stream that is now off by four bytes. Checking only for an
    // exception here leaves the decode check untested, which is exactly what mutation testing
    // showed before this assertion was tightened.
    Wire w(true);
    FrameHeader forged = make_ack(0);
    forged.payload_length = 4;
    char hdr[frame_header_bytes];
    encode_header(forged, hdr);
    w.poke(hdr, sizeof hdr);
    const int smuggled = 0x41414141;
    w.poke(&smuggled, sizeof smuggled);
    // A legitimate frame behind it, so a receiver that desynchronised would find plausible
    // bytes rather than simply blocking.
    FrameHeader good = wire_p2p(0, 4);
    char good_hdr[frame_header_bytes];
    encode_header(good, good_hdr);
    w.poke(good_hdr, sizeof good_hdr);
    const int payload = 7;
    w.poke(&payload, sizeof payload);

    int got = 0;
    OperationScope scope(p2p_identity(1));
    std::string reported;
    try {
        w.channel.recv({reinterpret_cast<char*>(&got), sizeof(got)}, 0);
    } catch (const std::exception& e) {
        reported = e.what();
    }
    BOOST_REQUIRE_MESSAGE(!reported.empty(), "a malformed ack was accepted");
    BOOST_CHECK_MESSAGE(reported.find("malformed frame") != std::string::npos,
                        "the frame was not reported as malformed: " << reported);
    BOOST_CHECK_MESSAGE(reported.find("identity mismatch") == std::string::npos,
                        "a malformed frame was blamed on the peer diverging: " << reported);
    BOOST_CHECK_MESSAGE(got != smuggled, "the ack's smuggled payload was delivered");
}

BOOST_AUTO_TEST_CASE(a_replayed_frame_is_discarded_rather_than_delivered_as_the_next_message) {
    // Dedup on the wire. After a repair a peer may retransmit a frame the receiver already
    // took; its payload must be consumed and dropped, not handed to the next receive, which
    // would shift every subsequent message by one.
    Wire w(true);
    FrameHeader first = wire_p2p(0, 4);
    FrameHeader second = wire_p2p(1, 4);
    char hdr[frame_header_bytes];

    encode_header(first, hdr);
    w.poke(hdr, sizeof hdr);
    const int a = 111;
    w.poke(&a, sizeof a);

    // The same frame again — a replay the sender had not seen acknowledged.
    encode_header(first, hdr);
    w.poke(hdr, sizeof hdr);
    w.poke(&a, sizeof a);

    encode_header(second, hdr);
    w.poke(hdr, sizeof hdr);
    const int b = 222;
    w.poke(&b, sizeof b);

    OperationScope scope(p2p_identity(1));
    int got = 0;
    w.channel.recv({reinterpret_cast<char*>(&got), sizeof(got)}, 0);
    BOOST_CHECK_EQUAL(got, 111);
    // The duplicate must be skipped entirely, so the next receive yields the SECOND message.
    got = 0;
    w.channel.recv({reinterpret_cast<char*>(&got), sizeof(got)}, 0);
    BOOST_CHECK_EQUAL(got, 222);
}

namespace {
    int open_fd_count() {
        int n = 0;
        DIR* d = opendir("/proc/self/fd");
        if (d == nullptr) { return -1; }
        while (readdir(d) != nullptr) { n++; }
        closedir(d);
        return n;
    }
}

BOOST_AUTO_TEST_CASE(repeated_channel_lifecycles_do_not_leak_descriptors) {
    // finalize() must close every peer socket. A leak here is invisible in a short test but
    // exhausts the descriptor table in a long-running rank, which is exactly the shape of a
    // job that migrates repeatedly.
    int before = 0;
    for (int round = 0; round < 3; round++) {
        for (int i = 0; i < 20; i++) {
            int fds[2];
            socketpair(AF_UNIX, SOCK_STREAM, 0, fds);
            {
                LoopbackChannel ch(fds[0], true);
                FrameHeader h = wire_p2p(0, 4);
                char hdr[frame_header_bytes];
                encode_header(h, hdr);
                BOOST_REQUIRE_EQUAL(::write(fds[1], hdr, sizeof hdr),
                                    static_cast<ssize_t>(sizeof hdr));
                const int payload = 1;
                BOOST_REQUIRE_EQUAL(::write(fds[1], &payload, sizeof payload),
                                    static_cast<ssize_t>(sizeof payload));
                int got = 0;
                OperationScope scope(p2p_identity(1));
                ch.recv({reinterpret_cast<char*>(&got), sizeof(got)}, 0);
                ch.finalize();
            }
            ::close(fds[1]);
        }
        // Take the baseline after the first round so one-off allocations do not count.
        if (round == 0) { before = open_fd_count(); }
    }
    const int after = open_fd_count();
    BOOST_CHECK_MESSAGE(after <= before + 2,
                        "descriptor count grew from " << before << " to " << after);
}

BOOST_AUTO_TEST_SUITE_END()
