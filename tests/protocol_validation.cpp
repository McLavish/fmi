#include <boost/test/unit_test.hpp>

#include "../include/comm/LinkFrame.h"
#include "../include/comm/SequencedLink.h"
#include "../include/comm/OperationScope.h"
#include "../include/comm/TcpChannelBase.h"

#include <sys/socket.h>
#include <unistd.h>
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
        h.total_length = 16;
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

BOOST_AUTO_TEST_CASE(identity_differs_on_total_length_alone) {
    FrameHeader a = base_collective(), b = a;
    b.total_length = a.total_length * 2;
    BOOST_CHECK(!same_identity(a, b));
}

BOOST_AUTO_TEST_CASE(identity_ignores_reliability_fields) {
    // transport_seq, message_id and fragment_index are reliability/fragmentation detail. Two
    // frames of the same logical message that differ in them are still the same identity.
    FrameHeader a = base_collective(), b = a;
    b.transport_seq = a.transport_seq + 99;
    b.message_id = a.message_id + 5;
    b.fragment_index = a.fragment_index + 2;
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
                 h.collective_index = 0; h.root = 0; h.total_length = 4; return h; }(),
            body.data(), body.size(), stamped));
    BOOST_REQUIRE(receiver.accept(stamped, body.data()) == Accept::Delivered);

    FrameHeader expected;
    expected.lane = Lane::Collective;
    expected.op_kind = OpKind::Bcast;
    expected.collective_index = 1;
    expected.root = 0;
    expected.total_length = 4;
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
    sent.total_length = 4;
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
    SequencedLink sender{{8, 1u << 20, 1u << 20}};
    SequencedLink receiver{{8, 1u << 20, 1u << 20}};
    const std::string body = "0123";
    FrameHeader sent;
    sent.lane = Lane::P2P;
    sent.op_kind = OpKind::Send;
    sent.root = 1;
    sent.total_length = 4;
    FrameHeader stamped;
    BOOST_REQUIRE(sender.admit(sent, body.data(), body.size(), stamped));
    BOOST_REQUIRE(receiver.accept(stamped, body.data()) == Accept::Delivered);

    char dst[8];
    std::memset(dst, 0xEE, sizeof dst);
    // Identity says total_length 4, but the caller asks for 8 bytes.
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
        h.total_length = len;
        h.payload_length = len;
        h.message_id = seq;
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
    h.total_length = 1u << 20;
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

// The three cases below isolate the decode-status check on the wire. They are deliberately
// constructed so that the IDENTITY fields all match: if the receiver ignored the decode status
// and looked only at identity, it would accept every one of them.

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

BOOST_AUTO_TEST_CASE(a_fragment_longer_than_its_message_is_rejected_before_it_desynchronises) {
    // payload_length exceeds total_length while total_length still matches what we expect, so
    // the identity check passes. Reading only total_length bytes would leave the surplus in
    // the stream and desynchronise every subsequent frame on this link.
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

BOOST_AUTO_TEST_SUITE_END()
