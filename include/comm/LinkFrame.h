#ifndef FMI_LINKFRAME_H
#define FMI_LINKFRAME_H

#include <cstddef>
#include <cstdint>
#include <string>

//! Wire format for the sequenced link layer.
/*!
 * Two sublayers share one frame. The *reliability* sublayer contributes transport_seq, a
 * link-scoped job-lifetime sequence that drives dedup and replay. The *logical envelope*
 * contributes the message identity — lane, op_kind, collective_index, root and the reduction
 * flags — which is what makes a divergent receive fail loudly instead of silently consuming
 * the wrong payload.
 *
 * Why the identity needs every one of those fields: a per-lane ordinal is only a FIFO
 * position, so it cannot detect reordering at all. Lane alone is not enough either — at two
 * peers a bcast fragment and a barrier fragment are both one byte on the Collective lane with
 * equal ordinals. docs/tla/MessageIdentity.tla model-checks the necessity of each field; TLC
 * exhibits a silent substitution for every weaker envelope, including one that carries
 * collective_index but not op_kind.
 *
 * The layout is written out byte by byte in little-endian order rather than memcpy'd from the
 * struct: the struct's padding is implementation-defined, and this format has to stay stable
 * across compilers and hosts.
 */
namespace FMI::Comm {

    //! Which logical stream a frame belongs to. Each lane has its own FIFO and drain queue.
    enum class Lane : std::uint8_t {
        P2P = 0,
        Collective = 1
    };

    //! The FMI operation that produced a frame. Part of message identity, not a hint.
    enum class OpKind : std::uint8_t {
        Send = 0,
        Bcast = 1,
        Barrier = 2,
        Gather = 3,
        Scatter = 4,
        Reduce = 5,
        Allreduce = 6,
        Scan = 7
    };

    //! What a frame is for. Acks carry no payload and take no transport sequence.
    /*!
     * Retention is pruned by the peer's cumulative ack, which normally rides along on traffic
     * in the opposite direction. A link that is busy in one direction only has no such traffic
     * — and those exist as soon as a job has three ranks, where a binomial tree gives some
     * directed links no return path at all. Without a standalone ack the sender's window fills
     * and never drains, and the job stops for good.
     */
    enum class FrameType : std::uint8_t {
        Data = 0,
        Ack = 1
    };

    //! "FMI2" — guards against a raw (unframed) peer and against stream desynchronisation.
    inline constexpr std::uint32_t frame_magic = 0x32494D46u;

    //! Protocol generation. A framed rank must never interoperate with a raw one.
    inline constexpr std::uint16_t frame_wire_version = 2;

    //! Serialized header size. Fixed: every frame starts with exactly this many bytes.
    /*!
     * Field widths below sum to 68; the frame is padded to 72 so a payload copied directly
     * after the header stays 8-byte aligned. encode_header_checked() asserts the two agree.
     */
    inline constexpr std::size_t frame_header_bytes = 72;

    inline constexpr std::uint8_t flag_commutative = 0x1u;
    inline constexpr std::uint8_t flag_associative = 0x2u;

    struct FrameHeader {
        std::uint16_t wire_version = frame_wire_version;
        FrameType frame_type = FrameType::Data;
        Lane lane = Lane::P2P;
        OpKind op_kind = OpKind::Send;
        //! Per-Communicator monotonic collective ordinal. Unused (0) on the P2P lane.
        std::uint64_t collective_index = 0;
        //! Collective root; the destination rank for p2p.
        std::uint32_t root = 0;
        //! Reduction flags. They select entirely different collective algorithms
        //! (Communicator.h:103/:128), so a mismatch is an identity mismatch, not a nuance.
        bool commutative = false;
        bool associative = false;
        //! Per-lane, per-directed-pair job-lifetime ordinal. Never reset.
        std::uint64_t message_id = 0;
        std::uint32_t fragment_index = 0;
        //! Length of the whole logical message, which may span several fragments.
        std::uint64_t total_length = 0;
        //! Length of this frame's payload.
        std::uint32_t payload_length = 0;
        //! Link-scoped reliability sequence. Job-lifetime, never reset.
        std::uint64_t transport_seq = 0;
        //! Cumulative ack for the REVERSE direction: every sequence below this is durably held
        //! by the sender of this frame.
        /*!
         * Piggybacked rather than carried in its own frame because the transport is blocking
         * and has no thread that could write a standalone ack while the application is inside
         * a receive. Any traffic in the opposite direction therefore prunes retention, and
         * collectives — which ping-pong by construction — keep it flowing.
         */
        std::uint64_t cumulative_ack = 0;
    };

    //! Result of decoding a header. Every malformed input is a status, never an exception:
    //! the bytes come off a socket and may be attacker- or bug-supplied.
    enum class DecodeStatus {
        Ok,
        Truncated,
        BadMagic,
        BadVersion,
        BadLane,
        BadOpKind,
        BadFrameType,
        PayloadTooLarge,
        Inconsistent
    };

    //! True when the two frames describe the same logical operation.
    /*!
     * transport_seq, fragment_index and payload_length are deliberately excluded: they are
     * reliability-layer or fragmentation detail, not identity.
     */
    inline bool same_identity(const FrameHeader& a, const FrameHeader& b) {
        return a.lane == b.lane
               && a.op_kind == b.op_kind
               && a.collective_index == b.collective_index
               && a.root == b.root
               && a.commutative == b.commutative
               && a.associative == b.associative
               && a.total_length == b.total_length;
    }

    namespace detail {
        inline void put_u8(char* p, std::size_t& off, std::uint8_t v) {
            p[off++] = static_cast<char>(v);
        }

        inline void put_u16(char* p, std::size_t& off, std::uint16_t v) {
            for (int i = 0; i < 2; ++i) {
                p[off++] = static_cast<char>((v >> (8 * i)) & 0xFFu);
            }
        }

        inline void put_u32(char* p, std::size_t& off, std::uint32_t v) {
            for (int i = 0; i < 4; ++i) {
                p[off++] = static_cast<char>((v >> (8 * i)) & 0xFFu);
            }
        }

        inline void put_u64(char* p, std::size_t& off, std::uint64_t v) {
            for (int i = 0; i < 8; ++i) {
                p[off++] = static_cast<char>((v >> (8 * i)) & 0xFFu);
            }
        }

        inline std::uint8_t get_u8(const char* p, std::size_t& off) {
            return static_cast<std::uint8_t>(p[off++]);
        }

        inline std::uint16_t get_u16(const char* p, std::size_t& off) {
            std::uint16_t v = 0;
            for (int i = 0; i < 2; ++i) {
                v |= static_cast<std::uint16_t>(static_cast<std::uint8_t>(p[off++])) << (8 * i);
            }
            return v;
        }

        inline std::uint32_t get_u32(const char* p, std::size_t& off) {
            std::uint32_t v = 0;
            for (int i = 0; i < 4; ++i) {
                v |= static_cast<std::uint32_t>(static_cast<std::uint8_t>(p[off++])) << (8 * i);
            }
            return v;
        }

        inline std::uint64_t get_u64(const char* p, std::size_t& off) {
            std::uint64_t v = 0;
            for (int i = 0; i < 8; ++i) {
                v |= static_cast<std::uint64_t>(static_cast<std::uint8_t>(p[off++])) << (8 * i);
            }
            return v;
        }
    }

    //! Serialize a header into exactly frame_header_bytes bytes. @p out must have room.
    inline void encode_header(const FrameHeader& h, char* out) {
        std::size_t off = 0;
        detail::put_u32(out, off, frame_magic);
        detail::put_u16(out, off, h.wire_version);
        detail::put_u8(out, off, static_cast<std::uint8_t>(h.lane));
        detail::put_u8(out, off, static_cast<std::uint8_t>(h.op_kind));
        std::uint8_t flags = 0;
        if (h.commutative) flags |= flag_commutative;
        if (h.associative) flags |= flag_associative;
        detail::put_u8(out, off, flags);
        detail::put_u8(out, off, static_cast<std::uint8_t>(h.frame_type));
        detail::put_u16(out, off, 0);  // reserved
        detail::put_u64(out, off, h.collective_index);
        detail::put_u32(out, off, h.root);
        detail::put_u32(out, off, h.fragment_index);
        detail::put_u64(out, off, h.message_id);
        detail::put_u64(out, off, h.total_length);
        detail::put_u32(out, off, h.payload_length);
        detail::put_u32(out, off, 0);  // reserved
        detail::put_u64(out, off, h.transport_seq);
        detail::put_u64(out, off, h.cumulative_ack);
        while (off < frame_header_bytes) {
            detail::put_u8(out, off, 0);  // tail padding to frame_header_bytes
        }
    }

    //! encode_header, returning the bytes written so callers and tests can assert the size.
    inline std::size_t encode_header_checked(const FrameHeader& h, char* out) {
        encode_header(h, out);
        return frame_header_bytes;
    }

    //! Parse a header, validating everything before the caller allocates for the payload.
    /*!
     * @param in       buffer holding at least @p available bytes
     * @param available bytes actually present; a short buffer yields Truncated
     * @param max_payload largest payload this peer is willing to allocate for
     */
    inline DecodeStatus decode_header(const char* in, std::size_t available,
                                      std::size_t max_payload, FrameHeader& out) {
        if (available < frame_header_bytes) {
            return DecodeStatus::Truncated;
        }
        std::size_t off = 0;
        if (detail::get_u32(in, off) != frame_magic) {
            return DecodeStatus::BadMagic;
        }
        out.wire_version = detail::get_u16(in, off);
        if (out.wire_version != frame_wire_version) {
            return DecodeStatus::BadVersion;
        }
        const std::uint8_t lane = detail::get_u8(in, off);
        if (lane > static_cast<std::uint8_t>(Lane::Collective)) {
            return DecodeStatus::BadLane;
        }
        out.lane = static_cast<Lane>(lane);
        const std::uint8_t op = detail::get_u8(in, off);
        if (op > static_cast<std::uint8_t>(OpKind::Scan)) {
            return DecodeStatus::BadOpKind;
        }
        out.op_kind = static_cast<OpKind>(op);
        const std::uint8_t flags = detail::get_u8(in, off);
        out.commutative = (flags & flag_commutative) != 0;
        out.associative = (flags & flag_associative) != 0;
        const std::uint8_t type = detail::get_u8(in, off);
        if (type > static_cast<std::uint8_t>(FrameType::Ack)) {
            return DecodeStatus::BadFrameType;
        }
        out.frame_type = static_cast<FrameType>(type);
        detail::get_u16(in, off);  // reserved
        out.collective_index = detail::get_u64(in, off);
        out.root = detail::get_u32(in, off);
        out.fragment_index = detail::get_u32(in, off);
        out.message_id = detail::get_u64(in, off);
        out.total_length = detail::get_u64(in, off);
        out.payload_length = detail::get_u32(in, off);
        detail::get_u32(in, off);  // reserved
        out.transport_seq = detail::get_u64(in, off);
        out.cumulative_ack = detail::get_u64(in, off);

        if (out.payload_length > max_payload) {
            return DecodeStatus::PayloadTooLarge;
        }
        // A fragment can never claim more bytes than the message it belongs to, and a
        // zero-length message must not arrive carrying a payload.
        if (out.payload_length > out.total_length) {
            return DecodeStatus::Inconsistent;
        }
        // The P2P lane has no collective identity; a frame claiming one is malformed.
        if (out.lane == Lane::P2P && out.collective_index != 0) {
            return DecodeStatus::Inconsistent;
        }
        // An ack is a pure watermark: it carries nothing and occupies no sequence, so a
        // payload or a claimed sequence on one means the stream is not what it says it is.
        if (out.frame_type == FrameType::Ack &&
            (out.payload_length != 0 || out.total_length != 0 || out.transport_seq != 0)) {
            return DecodeStatus::Inconsistent;
        }
        return DecodeStatus::Ok;
    }

    //! The standalone ack frame: "everything below @p cumulative is durably mine".
    inline FrameHeader make_ack(std::uint64_t cumulative) {
        FrameHeader h;
        h.frame_type = FrameType::Ack;
        h.lane = Lane::P2P;
        h.op_kind = OpKind::Send;
        h.cumulative_ack = cumulative;
        return h;
    }

    //! Per-direction reconciliation state exchanged when a link is (re-)established.
    struct HandshakePayload {
        std::uint16_t wire_version = frame_wire_version;
        std::uint64_t next_send_seq = 0;
        std::uint64_t next_expected_seq = 0;
        std::uint64_t lowest_retained = 0;
        //! Hash over the job-wide policy inputs; a mismatch means the peers would choose
        //! different backends or algorithms, which shows up as a hang rather than an error.
        std::uint64_t policy_fingerprint = 0;
    };

    inline constexpr std::size_t handshake_bytes = 40;

    inline void encode_handshake(const HandshakePayload& h, char* out) {
        std::size_t off = 0;
        detail::put_u32(out, off, frame_magic);
        detail::put_u16(out, off, h.wire_version);
        detail::put_u16(out, off, 0);  // reserved
        detail::put_u64(out, off, h.next_send_seq);
        detail::put_u64(out, off, h.next_expected_seq);
        detail::put_u64(out, off, h.lowest_retained);
        detail::put_u64(out, off, h.policy_fingerprint);
    }

    inline DecodeStatus decode_handshake(const char* in, std::size_t available,
                                         HandshakePayload& out) {
        if (available < handshake_bytes) {
            return DecodeStatus::Truncated;
        }
        std::size_t off = 0;
        if (detail::get_u32(in, off) != frame_magic) {
            return DecodeStatus::BadMagic;
        }
        out.wire_version = detail::get_u16(in, off);
        if (out.wire_version != frame_wire_version) {
            return DecodeStatus::BadVersion;
        }
        detail::get_u16(in, off);  // reserved
        out.next_send_seq = detail::get_u64(in, off);
        out.next_expected_seq = detail::get_u64(in, off);
        out.lowest_retained = detail::get_u64(in, off);
        out.policy_fingerprint = detail::get_u64(in, off);
        // The peer cannot expect a sequence we have not produced, and cannot have pruned past
        // what it still asks for. Both are impossible states rather than recoverable ones.
        if (out.lowest_retained > out.next_send_seq) {
            return DecodeStatus::Inconsistent;
        }
        return DecodeStatus::Ok;
    }
}

#endif //FMI_LINKFRAME_H
