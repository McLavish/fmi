#ifndef FMI_LINKHELLO_H
#define FMI_LINKHELLO_H

#include "TcpEndpoint.h"

#include <cstddef>
#include <cstdint>

namespace FMI::Comm {

    //! Magic of the Local Drain hello; the wire bytes read "FMIL".
    constexpr std::uint32_t link_hello_magic = 0x464D494C;

    constexpr std::uint16_t link_hello_version = 1;

    //! Size of the encoded hello.
    constexpr std::size_t link_hello_bytes = 56;

    //! The hello two ranks exchange once per connection, in both directions.
    /*!
     * It is the only thing a Local Drain link carries besides payload. The counters are what make
     * a raw stream safe to cut: they are cumulative per directed link over the life of the job, and
     * at every reconnect each end checks that what the peer says it sent is what it received, and
     * the other way round. One byte lost or doubled across a move fails loudly here.
     *
     * Wire layout, every integer big-endian:
     *   0   u32  magic 'FMIL'
     *   4   u16  version
     *   6   u16  reserved (0)
     *   8   u32  sender_rank
     *   12  u32  receiver_rank
     *   16  u64  link_name_hash     fnv1a64 of the job name and the ordered rank pair
     *   24  u64  nonce              dialer to listener: the nonce in the listener's rank-table
     *                               row (the dialer reached this listener, not a recycled port);
     *                               0 in the reply
     *   32  u64  sender_generation  never lower than one already seen from that rank
     *   40  u64  bytes_sent
     *   48  u64  bytes_received
     */
    struct LinkHello {
        std::uint16_t version = link_hello_version;
        std::uint32_t sender_rank = 0;
        std::uint32_t receiver_rank = 0;
        std::uint64_t link_name_hash = 0;
        std::uint64_t nonce = 0;
        std::uint64_t sender_generation = 0;
        std::uint64_t bytes_sent = 0;
        std::uint64_t bytes_received = 0;
    };

    inline void encode_hello(const LinkHello& hello, char* out) {
        TcpEndpoint::put32(out, link_hello_magic);
        out[4] = static_cast<char>((hello.version >> 8) & 0xFF);
        out[5] = static_cast<char>(hello.version & 0xFF);
        out[6] = 0;
        out[7] = 0;
        TcpEndpoint::put32(out + 8, hello.sender_rank);
        TcpEndpoint::put32(out + 12, hello.receiver_rank);
        TcpEndpoint::put64(out + 16, hello.link_name_hash);
        TcpEndpoint::put64(out + 24, hello.nonce);
        TcpEndpoint::put64(out + 32, hello.sender_generation);
        TcpEndpoint::put64(out + 40, hello.bytes_sent);
        TcpEndpoint::put64(out + 48, hello.bytes_received);
    }

    //! False on a wrong magic or version. Ranks, hash, nonce and counters are the caller's to
    //! check: only the caller knows which link the hello arrived on.
    inline bool decode_hello(const char* in, LinkHello& out) {
        if (TcpEndpoint::get32(in) != link_hello_magic) {
            return false;
        }
        const auto version = static_cast<std::uint16_t>(
                (static_cast<unsigned char>(in[4]) << 8) | static_cast<unsigned char>(in[5]));
        if (version != link_hello_version) {
            return false;
        }
        out.version = version;
        out.sender_rank = TcpEndpoint::get32(in + 8);
        out.receiver_rank = TcpEndpoint::get32(in + 12);
        out.link_name_hash = TcpEndpoint::get64(in + 16);
        out.nonce = TcpEndpoint::get64(in + 24);
        out.sender_generation = TcpEndpoint::get64(in + 32);
        out.bytes_sent = TcpEndpoint::get64(in + 40);
        out.bytes_received = TcpEndpoint::get64(in + 48);
        return true;
    }
}

#endif //FMI_LINKHELLO_H
