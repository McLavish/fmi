#include "../../include/comm/TcpEndpoint.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstdint>
#include <random>

void FMI::Comm::TcpEndpoint::put32(char* p, std::uint32_t v) {
    for (int i = 0; i < 4; i++) {
        p[i] = static_cast<char>((v >> (8 * (3 - i))) & 0xFF);
    }
}

std::uint32_t FMI::Comm::TcpEndpoint::get32(const char* p) {
    std::uint32_t v = 0;
    for (int i = 0; i < 4; i++) {
        v = (v << 8) | static_cast<unsigned char>(p[i]);
    }
    return v;
}

void FMI::Comm::TcpEndpoint::put64(char* p, std::uint64_t v) {
    for (int i = 0; i < 8; i++) {
        p[i] = static_cast<char>((v >> (8 * (7 - i))) & 0xFF);
    }
}

std::uint64_t FMI::Comm::TcpEndpoint::get64(const char* p) {
    std::uint64_t v = 0;
    for (int i = 0; i < 8; i++) {
        v = (v << 8) | static_cast<unsigned char>(p[i]);
    }
    return v;
}

std::uint64_t FMI::Comm::TcpEndpoint::fnv1a64(const std::string& s) {
    std::uint64_t hash = 1469598103934665603ULL;
    for (unsigned char c : s) {
        hash ^= c;
        hash *= 1099511628211ULL;
    }
    return hash;
}

std::uint64_t FMI::Comm::TcpEndpoint::random_nonce() {
    std::random_device rd;
    std::mt19937_64 gen(((static_cast<std::uint64_t>(rd()) << 32) ^ rd()) ^
                        static_cast<std::uint64_t>(::getpid()));
    std::uniform_int_distribution<std::uint64_t> dist(1, UINT64_MAX);
    return dist(gen);
}

void FMI::Comm::TcpEndpoint::set_nonblocking(int fd, bool on) {
    int flags = ::fcntl(fd, F_GETFL, 0);
    if (flags < 0) {
        return;
    }
    ::fcntl(fd, F_SETFL, on ? (flags | O_NONBLOCK) : (flags & ~O_NONBLOCK));
}

bool FMI::Comm::TcpEndpoint::resolve_ipv4(const std::string& host, struct in_addr& out) {
    if (::inet_pton(AF_INET, host.c_str(), &out) == 1) {
        return true;
    }
    struct addrinfo hints{};
    struct addrinfo* res = nullptr;
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    if (::getaddrinfo(host.c_str(), nullptr, &hints, &res) != 0 || res == nullptr) {
        return false;
    }
    out = reinterpret_cast<struct sockaddr_in*>(res->ai_addr)->sin_addr;
    ::freeaddrinfo(res);
    return true;
}

std::string FMI::Comm::TcpEndpoint::param_or(std::map<std::string, std::string>& params,
                                             const std::string& key, const std::string& fallback) {
    auto it = params.find(key);
    if (it == params.end() || it->second.empty()) {
        return fallback;
    }
    return it->second;
}

std::string FMI::Comm::TcpEndpoint::resolve_advertise_ip(const std::string& advertise_host,
                                                         const std::string& registry_host,
                                                         int registry_port) {
    if (!advertise_host.empty()) {
        return advertise_host;
    }
    // Ask the kernel which local address it would route to the registry from. A UDP connect
    // sends no packets, and the registry is reachable from every rank by construction, so the
    // interface that reaches it is the one peers can most plausibly reach us on. Beats
    // enumerating interfaces, which picks docker0/veth on any machine that has run a container.
    int probe = ::socket(AF_INET, SOCK_DGRAM, 0);
    if (probe >= 0) {
        struct sockaddr_in dest{};
        dest.sin_family = AF_INET;
        dest.sin_port = htons(static_cast<std::uint16_t>(registry_port));
        if (resolve_ipv4(registry_host, dest.sin_addr) &&
            ::connect(probe, reinterpret_cast<struct sockaddr*>(&dest), sizeof(dest)) == 0) {
            struct sockaddr_in local{};
            socklen_t len = sizeof(local);
            if (::getsockname(probe, reinterpret_cast<struct sockaddr*>(&local), &len) == 0) {
                char buf[INET_ADDRSTRLEN];
                if (::inet_ntop(AF_INET, &local.sin_addr, buf, sizeof(buf)) != nullptr) {
                    ::close(probe);
                    return buf;
                }
            }
        }
        ::close(probe);
    }

    char hostname[256];
    if (::gethostname(hostname, sizeof(hostname)) == 0) {
        struct in_addr addr{};
        if (resolve_ipv4(hostname, addr) && addr.s_addr != htonl(INADDR_LOOPBACK)) {
            char buf[INET_ADDRSTRLEN];
            if (::inet_ntop(AF_INET, &addr, buf, sizeof(buf)) != nullptr) {
                return buf;
            }
        }
    }
    return "127.0.0.1";
}
