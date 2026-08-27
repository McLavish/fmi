#ifndef FMI_LINKFRAME_H
#define FMI_LINKFRAME_H

#include <cstddef>
#include <cassert>
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
        Ack = 1,
        //! Link state offered on a fresh connection; carries a HandshakePayload as its payload.
        Handshake = 2
    };

    //! "FMI2" — guards against a raw (unframed) peer and against stream desynchronisation.
    inline constexpr std::uint32_t frame_magic = 0x32494D46u;

    //! Protocol generation. A framed rank must never interoperate with a raw one.
    //! 3 added the incarnation pair to the handshake (contract 3). 4 dropped the fields that
    //! nothing read — message_id, fragment_index, total_length and every padding byte — taking
    //! the header from 72 to 42 bytes.
    inline constexpr std::uint16_t frame_wire_version = 4;

    //! Serialized header size. Fixed: every frame starts with exactly this many bytes.
    /*!
     * Every byte carries a field: there is no padding and no reserved region. Nothing in the
     * transport overlays a struct on these bytes — the codec below is explicit little-endian
     * byte-at-a-time, and write_frame issues the header and the payload as two separate
     * writes — so the header has no alignment obligation to meet.
     *
     * header_field_bytes is the sum of the put_* widths in encode_header, and the static_assert
     * pairing the two is the only thing standing between a mistaken edit and a silent overflow
     * of the fixed char[frame_header_bytes] buffers every caller declares. Keep them together.
     */
    inline constexpr std::size_t frame_header_bytes = 42;

    //! The put_* widths in encode_header, summed in the order they appear there.
    inline constexpr std::size_t header_field_bytes =
            4       // magic
            + 2     // wire_version
            + 1     // lane
            + 1     // op_kind
            + 1     // flags
            + 1     // frame_type
            + 8     // collective_index
            + 4     // root
            + 4     // payload_length
            + 8     // transport_seq
            + 8;    // cumulative_ack
    static_assert(header_field_bytes == frame_header_bytes,
                  "encode_header's field widths must sum to frame_header_bytes: every caller "
                  "encodes into a fixed char[frame_header_bytes], so a mismatch is an overflow "
                  "or a short frame, with no diagnostic at either end");

    //! Serialized HandshakePayload size — the payload of every Handshake frame, exactly.
    inline constexpr std::size_t handshake_bytes = 56;

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
        //! (PeerToPeer.cpp:36/:87/:133), so a mismatch is an identity mismatch, not a nuance.
        bool commutative = false;
        bool associative = false;
        //! Length of this frame's payload, and — since a message is never fragmented — of the
        //! whole logical message. It is therefore part of message identity as well as framing.
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
     * transport_seq is deliberately excluded: it is reliability-layer detail, not identity.
     *
     * payload_length carries the length term of the identity, a role the removed total_length
     * used to hold. The two were written from the same value on adjacent lines for every frame
     * any producer could emit, so folding one into the other preserves the comparison exactly
     * while costing four wire bytes instead of twelve.
     */
    inline bool same_identity(const FrameHeader& a, const FrameHeader& b) {
        return a.lane == b.lane
               && a.op_kind == b.op_kind
               && a.collective_index == b.collective_index
               && a.root == b.root
               && a.commutative == b.commutative
               && a.associative == b.associative
               && a.payload_length == b.payload_length;
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
    //! Returns the bytes written, which is frame_header_bytes on every path.
    inline std::size_t encode_header(const FrameHeader& h, char* out) {
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
        detail::put_u64(out, off, h.collective_index);
        detail::put_u32(out, off, h.root);
        detail::put_u32(out, off, h.payload_length);
        detail::put_u64(out, off, h.transport_seq);
        detail::put_u64(out, off, h.cumulative_ack);
        return off;
    }

    //! encode_header, asserting the bytes written match the advertised frame size.
    /*!
     * The static_assert above is the compile-time half of that guarantee; this is the runtime
     * half, for a put_* whose width does not match the arithmetic. Both exist because every
     * caller writes into a fixed char[frame_header_bytes] and nothing else would catch a
     * short or long encode.
     */
    inline std::size_t encode_header_checked(const FrameHeader& h, char* out) {
        const std::size_t written = encode_header(h, out);
        assert(written == frame_header_bytes);
        return written;
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
        if (type > static_cast<std::uint8_t>(FrameType::Handshake)) {
            return DecodeStatus::BadFrameType;
        }
        out.frame_type = static_cast<FrameType>(type);
        out.collective_index = detail::get_u64(in, off);
        out.root = detail::get_u32(in, off);
        out.payload_length = detail::get_u32(in, off);
        out.transport_seq = detail::get_u64(in, off);
        out.cumulative_ack = detail::get_u64(in, off);

        if (out.payload_length > max_payload) {
            return DecodeStatus::PayloadTooLarge;
        }
        // The P2P lane has no collective identity; a frame claiming one is malformed.
        if (out.lane == Lane::P2P && out.collective_index != 0) {
            return DecodeStatus::Inconsistent;
        }
        // An ack is a pure watermark: it carries nothing and occupies no sequence, so a
        // payload or a claimed sequence on one means the stream is not what it says it is.
        if (out.frame_type == FrameType::Ack &&
            (out.payload_length != 0 || out.transport_seq != 0)) {
            return DecodeStatus::Inconsistent;
        }
        // A handshake's payload is the fixed-size HandshakePayload and nothing else. Every
        // reader of that payload reads exactly handshake_bytes of it, so a frame claiming a
        // different length would have them read past what actually arrived — or leave stray
        // bytes to be parsed as the next frame's header.
        if (out.frame_type == FrameType::Handshake && out.payload_length != handshake_bytes) {
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
        //! Which *lineage* of this logical rank is speaking (contract 3).
        /*!
         * A logical rank outlives the processes that serve it. The incarnation names the run
         * of link state, not the rank: a process restored from a checkpoint carries the same
         * incarnation, because it also carries the sequences and the retention buffer that
         * make reconciliation meaningful. A replacement process that starts from nothing takes
         * the next incarnation, because its counters start at zero and its peers must be told
         * to start again too rather than diagnose a sequence gap.
         */
        std::uint64_t incarnation = 0;
        //! The peer incarnation this side's link state was built against.
        /*!
         * Sent back so each end can tell "my peer restarted" (their incarnation is ahead of
         * what I recorded) from "I am talking to a process that has already been superseded"
         * (behind). Without it, a zombie that survived its own replacement reconciles
         * perfectly well against sequences that no longer mean anything.
         */
        std::uint64_t peer_incarnation = 0;
    };

    //! The handshake frame. Its payload is an encoded HandshakePayload.
    inline FrameHeader make_handshake_frame() {
        FrameHeader h;
        h.frame_type = FrameType::Handshake;
        h.lane = Lane::P2P;
        h.op_kind = OpKind::Send;
        h.payload_length = handshake_bytes;
        return h;
    }

    inline void encode_handshake(const HandshakePayload& h, char* out) {
        std::size_t off = 0;
        detail::put_u32(out, off, frame_magic);
        detail::put_u16(out, off, h.wire_version);
        detail::put_u16(out, off, 0);  // reserved
        detail::put_u64(out, off, h.next_send_seq);
        detail::put_u64(out, off, h.next_expected_seq);
        detail::put_u64(out, off, h.lowest_retained);
        detail::put_u64(out, off, h.policy_fingerprint);
        detail::put_u64(out, off, h.incarnation);
        detail::put_u64(out, off, h.peer_incarnation);
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
        out.incarnation = detail::get_u64(in, off);
        out.peer_incarnation = detail::get_u64(in, off);
        // The peer cannot expect a sequence we have not produced, and cannot have pruned past
        // what it still asks for. Both are impossible states rather than recoverable ones.
        if (out.lowest_retained > out.next_send_seq) {
            return DecodeStatus::Inconsistent;
        }
        // Deliberately no cross-check between the two incarnations: a rank on its first
        // lineage (incarnation 0) may perfectly well be talking to a peer already on its
        // third, and the reverse. Only SequencedLink::reconcile, which knows what this side
        // recorded, can judge the pair.
        return DecodeStatus::Ok;
    }
}

#endif //FMI_LINKFRAME_H
