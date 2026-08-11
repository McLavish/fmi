#ifndef FMI_TCPENDPOINT_H
#define FMI_TCPENDPOINT_H

#include <netinet/in.h>

#include <cstdint>
#include <map>
#include <string>

namespace FMI::Comm {
    //! Endpoint and byte-order helpers shared by the TCP transports.
    /*!
     * Nothing here knows about a channel, a rank or a wire protocol: these are the pieces a
     * backend needs to turn config strings into a socket and to put integers on a wire in a
     * layout that does not depend on the compiler. Message framing belongs to the backend that
     * defines the message, not here.
     */
    namespace TcpEndpoint {
        //! Write a 32-bit value in network byte order, byte by byte.
        /*!
         * Field by field rather than memcpy'd from a struct: no padding, no alignment
         * assumptions, no compiler-dependent layout.
         */
        void put32(char* p, std::uint32_t v);

        //! Read a 32-bit value written by put32.
        std::uint32_t get32(const char* p);

        //! Write a 64-bit value in network byte order, byte by byte.
        void put64(char* p, std::uint64_t v);

        //! Read a 64-bit value written by put64.
        std::uint64_t get64(const char* p);

        //! FNV-1a over the bytes of a string. Not a security primitive.
        std::uint64_t fnv1a64(const std::string& s);

        //! A non-zero random 64-bit value, seeded per process so forked ranks do not agree.
        std::uint64_t random_nonce();

        //! Turn O_NONBLOCK on or off on a descriptor; a descriptor that cannot be queried is
        //! left as it is.
        void set_nonblocking(int fd, bool on);

        //! Resolve a host that may be a literal IPv4 address or a name.
        bool resolve_ipv4(const std::string& host, struct in_addr& out);

        //! The value of @p key in @p params, or @p fallback when it is missing or empty.
        std::string param_or(std::map<std::string, std::string>& params, const std::string& key,
                             const std::string& fallback);

        //! Resolve the address peers should dial this rank on.
        /*!
         * @param advertise_host  configured answer; when non-empty it is returned unexamined.
         * @param registry_host   host of the rendezvous point every rank can reach, used only as
         *                        the destination of a routing probe.
         * @param registry_port   its port.
         */
        std::string resolve_advertise_ip(const std::string& advertise_host,
                                         const std::string& registry_host, int registry_port);
    }
}

#endif //FMI_TCPENDPOINT_H
