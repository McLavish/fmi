#include "../../include/comm/DirectTCP.h"

#include "../../include/comm/PeerRegistry.h"
#include "../../include/comm/TcpEndpoint.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <stdexcept>
#include <vector>

namespace {
    std::atomic<unsigned int> total_connections{0};

    long monotonic_ms() {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now().time_since_epoch()).count();
    }

    // Fixed 32-byte hello, written field by field in network byte order rather than memcpy'd
    // from a struct: no padding, no alignment assumptions, no compiler-dependent layout.
    constexpr std::uint32_t HELLO_MAGIC = 0x464D4931; // 'FMI1'
    constexpr std::uint32_t HELLO_VERSION = 1;
    constexpr std::size_t HELLO_SIZE = 32;
}

FMI::Comm::DirectTCP::DirectTCP(std::map<std::string, std::string> params,
                                std::map<std::string, std::string> model_params) {
    transport_tag = "DirectTCP";
    eof_before_data_is_timeout = true;
    registry_host = TcpEndpoint::param_or(params, "registry_host", "127.0.0.1");
    registry_port = std::stoi(TcpEndpoint::param_or(params, "registry_port", "6379"));
    bind_host = TcpEndpoint::param_or(params, "bind_host", "0.0.0.0");
    advertise_host = TcpEndpoint::param_or(params, "advertise_host", "");
    registry_poll_interval_ms = std::stoi(TcpEndpoint::param_or(params, "registry_poll_interval_ms", "5"));
    connect_retry_interval_ms = std::stoi(TcpEndpoint::param_or(params, "connect_retry_interval_ms", "10"));
    registry_ttl_s = std::stoi(TcpEndpoint::param_or(params, "registry_ttl_s", "3600"));
    parse_tcp_params(params);
    parse_tcp_model_params(model_params);
    // Constructed but not connected: peer_id/num_peers/comm_name are pushed in after
    // construction, and in the forked test harness the connection must be opened by the
    // process that will use it.
    registry = std::make_unique<PeerRegistry>(registry_host, registry_port);
}

FMI::Comm::DirectTCP::~DirectTCP() {
    close_sockets();
    close_transport_state();
}

unsigned int FMI::Comm::DirectTCP::connection_count() {
    return total_connections.load();
}

std::string FMI::Comm::DirectTCP::registry_key() const {
    return "fmi:direct:" + comm_name;
}

void FMI::Comm::DirectTCP::ensure_listener() {
    if (listen_fd >= 0) {
        return;
    }
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        throw std::runtime_error("DirectTCP: socket() failed: " + std::string(strerror(errno)));
    }
    int one = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    if (!TcpEndpoint::resolve_ipv4(bind_host, addr.sin_addr)) {
        addr.sin_addr.s_addr = htonl(INADDR_ANY);
    }
    addr.sin_port = htons(0); // let the kernel pick a free port
    if (::bind(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
        std::string error = strerror(errno);
        ::close(fd);
        throw std::runtime_error("DirectTCP: bind to " + bind_host + " failed: " + error);
    }
    if (::listen(fd, SOMAXCONN) < 0) {
        std::string error = strerror(errno);
        ::close(fd);
        throw std::runtime_error("DirectTCP: listen failed: " + error);
    }
    socklen_t len = sizeof(addr);
    if (::getsockname(fd, reinterpret_cast<struct sockaddr*>(&addr), &len) < 0) {
        std::string error = strerror(errno);
        ::close(fd);
        throw std::runtime_error("DirectTCP: getsockname failed: " + error);
    }
    listen_port = ntohs(addr.sin_port);
    TcpEndpoint::set_nonblocking(fd, true);
    listen_fd = fd;
    listener_nonce = TcpEndpoint::random_nonce();
    advertised_ip = TcpEndpoint::resolve_advertise_ip(advertise_host, registry_host,
                                                      registry_port);
}

void FMI::Comm::DirectTCP::publish_self(long deadline_ms) {
    long remaining = deadline_ms - monotonic_ms();
    if (remaining <= 0) {
        throw Utils::Timeout();
    }
    registry->publish(registry_key(), std::to_string(peer_id),
                      advertised_ip + ":" + std::to_string(listen_port) + ":" +
                              std::to_string(listener_nonce),
                      registry_ttl_s, remaining);
}

std::string FMI::Comm::DirectTCP::hello_for(Utils::peer_num partner_id,
                                            std::uint64_t nonce) const {
    std::string buf(HELLO_SIZE, '\0');
    TcpEndpoint::put32(&buf[0], HELLO_MAGIC);
    TcpEndpoint::put32(&buf[4], HELLO_VERSION);
    TcpEndpoint::put32(&buf[8], static_cast<std::uint32_t>(peer_id));
    TcpEndpoint::put32(&buf[12], static_cast<std::uint32_t>(partner_id));
    TcpEndpoint::put64(&buf[16], TcpEndpoint::fnv1a64(link_name(partner_id, true)));
    TcpEndpoint::put64(&buf[24], nonce);
    return buf;
}

bool FMI::Comm::DirectTCP::send_hello(int fd, Utils::peer_num partner_id,
                                      std::uint64_t nonce) const {
    const std::string hello = hello_for(partner_id, nonce);
    std::size_t sent = 0;
    while (sent < hello.size()) {
        long n = ::send(fd, hello.data() + sent, hello.size() - sent, MSG_NOSIGNAL);
        if (n <= 0) {
            if (n < 0 && errno == EINTR) {
                continue;
            }
            return false;
        }
        sent += static_cast<std::size_t>(n);
    }
    return true;
}

bool FMI::Comm::DirectTCP::read_hello(int fd, Utils::peer_num& sender,
                                      std::uint64_t expect_nonce) const {
    char buf[HELLO_SIZE];
    std::size_t got = 0;
    while (got < HELLO_SIZE) {
        struct pollfd pfd{fd, POLLIN, 0};
        int pr = ::poll(&pfd, 1, 200);
        if (pr < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        if (pr == 0) {
            return false;
        }
        long n = ::recv(fd, buf + got, HELLO_SIZE - got, 0);
        if (n <= 0) {
            if (n < 0 && errno == EINTR) {
                continue;
            }
            return false;
        }
        got += static_cast<std::size_t>(n);
    }

    if (TcpEndpoint::get32(buf) != HELLO_MAGIC ||
        TcpEndpoint::get32(buf + 4) != HELLO_VERSION) {
        return false;
    }
    auto claimed = static_cast<Utils::peer_num>(TcpEndpoint::get32(buf + 8));
    auto receiver = static_cast<Utils::peer_num>(TcpEndpoint::get32(buf + 12));
    if (receiver != peer_id || claimed >= num_peers || claimed == peer_id) {
        return false;
    }
    if (TcpEndpoint::get64(buf + 16) != TcpEndpoint::fnv1a64(link_name(claimed, false))) {
        return false;
    }
    if (TcpEndpoint::get64(buf + 24) != expect_nonce) {
        return false;
    }
    sender = claimed;
    return true;
}

std::vector<FMI::Utils::peer_num> FMI::Comm::DirectTCP::connect_batch(
        const std::map<Utils::peer_num, PeerAddr>& addrs,
        const std::vector<Utils::peer_num>& wanted,
        std::map<Utils::peer_num, int>& unconfirmed,
        long deadline_ms) {
    struct InFlight {
        Utils::peer_num rank;
        int fd;
        std::uint64_t nonce;
    };
    std::vector<InFlight> inflight;
    std::vector<Utils::peer_num> remaining;

    auto finish = [&](int fd, Utils::peer_num rank, std::uint64_t nonce) {
        TcpEndpoint::set_nonblocking(fd, false);
        apply_socket_options(fd);
        if (send_hello(fd, rank, nonce)) {
            unconfirmed[rank] = fd;
        } else {
            ::close(fd);
            remaining.push_back(rank);
        }
    };

    for (auto rank : wanted) {
        auto it = addrs.find(rank);
        if (it == addrs.end()) {
            remaining.push_back(rank);
            continue;
        }
        struct sockaddr_in dest{};
        dest.sin_family = AF_INET;
        dest.sin_port = htons(static_cast<std::uint16_t>(it->second.port));
        if (!TcpEndpoint::resolve_ipv4(it->second.ip, dest.sin_addr)) {
            remaining.push_back(rank);
            continue;
        }
        int fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) {
            remaining.push_back(rank);
            continue;
        }
        TcpEndpoint::set_nonblocking(fd, true);
        int rc = ::connect(fd, reinterpret_cast<struct sockaddr*>(&dest), sizeof(dest));
        if (rc == 0) {
            finish(fd, rank, it->second.nonce);
        } else if (errno == EINPROGRESS || errno == EALREADY) {
            inflight.push_back({rank, fd, it->second.nonce});
        } else {
            ::close(fd);
            remaining.push_back(rank);
        }
    }

    while (!inflight.empty()) {
        long now = monotonic_ms();
        if (now >= deadline_ms) {
            break;
        }
        std::vector<struct pollfd> pfds;
        pfds.reserve(inflight.size());
        for (const auto& entry : inflight) {
            pfds.push_back({entry.fd, POLLOUT, 0});
        }
        int pr = ::poll(pfds.data(), pfds.size(), static_cast<int>(deadline_ms - now));
        if (pr < 0) {
            if (errno == EINTR) {
                continue;
            }
            break;
        }
        if (pr == 0) {
            break;
        }
        std::vector<InFlight> still;
        for (std::size_t i = 0; i < inflight.size(); i++) {
            if (pfds[i].revents == 0) {
                still.push_back(inflight[i]);
                continue;
            }
            int err = 0;
            socklen_t len = sizeof(err);
            if (::getsockopt(inflight[i].fd, SOL_SOCKET, SO_ERROR, &err, &len) < 0 || err != 0) {
                ::close(inflight[i].fd);
                remaining.push_back(inflight[i].rank);
                continue;
            }
            finish(inflight[i].fd, inflight[i].rank, inflight[i].nonce);
        }
        inflight = still;
    }

    for (const auto& entry : inflight) {
        ::close(entry.fd);
        remaining.push_back(entry.rank);
    }
    return remaining;
}

FMI::Comm::DirectTCP::AcceptResult FMI::Comm::DirectTCP::accept_one() {
    int fd = ::accept(listen_fd, nullptr, nullptr);
    if (fd < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return AcceptResult::Empty;
        }
        if (errno == EINTR || errno == ECONNABORTED) {
            return AcceptResult::Rejected;   // transient; there may be more behind it
        }
        throw std::runtime_error("DirectTCP: accept failed: " + std::string(strerror(errno)));
    }
    apply_socket_options(fd);

    Utils::peer_num claimed = 0;
    if (!read_hello(fd, claimed, listener_nonce)) {
        ::close(fd);
        return AcceptResult::Rejected;
    }
    if (pending_links.count(claimed) > 0 || (claimed < sockets.size() && sockets[claimed] >= 0)) {
        ::close(fd);   // already holding a connection for that rank
        return AcceptResult::Rejected;
    }
    if (!send_hello(fd, claimed, 0)) {
        ::close(fd);
        return AcceptResult::Rejected;
    }
    pending_links[claimed] = fd;
    total_connections.fetch_add(1);
    return AcceptResult::Accepted;
}

void FMI::Comm::DirectTCP::service_transport() {
    if (listen_fd < 0) {
        return;
    }
    for (int seen = 0; seen < 64; seen++) {
        try {
            if (accept_one() == AcceptResult::Empty) {
                break;
            }
        } catch (const std::exception&) {
            break;   // the operation in flight owns error reporting, not this
        }
    }
    // Accepting is not enough: the link goes into the socket table now, so that the next
    // operation with that peer finds it there instead of establishing.
    adopt_pending(num_peers);
}

void FMI::Comm::DirectTCP::adopt_pending(Utils::peer_num exclude) {
    for (auto it = pending_links.begin(); it != pending_links.end();) {
        const Utils::peer_num rank = it->first;
        const int fd = it->second;
        const bool unheld = sockets.empty() || (rank < sockets.size() && sockets[rank] < 0);
        if (rank != exclude && unheld && fd >= 0) {
            it = pending_links.erase(it);
            adopt_link(rank, fd);
        } else {
            ++it;
        }
    }
}

void FMI::Comm::DirectTCP::build_mesh(Utils::peer_num target, long deadline_ms) {
    ensure_listener();

    // Only the link actually asked for is built. Establishing the full mesh up front would
    // cost O(num_peers^2) sockets across the job where a binomial-tree collective touches
    // O(log num_peers) of them.
    std::vector<Utils::peer_num> want_connect;
    if (target < peer_id) {
        want_connect.push_back(target);
    }

    std::map<Utils::peer_num, int> unconfirmed;
    long next_connect_attempt = 0;
    long next_publish_attempt = 0;
    bool published = false;

    // One event loop, and it keeps servicing the listener the whole time it waits — accepting
    // from any peer, not just the one we want. The only thing a rank ever waits for here is a
    // HIGHER rank connecting to it or a LOWER rank acknowledging, and a rank that is waiting is
    // still handing out every accept and acknowledgement it owes.
    while (pending_links.count(target) == 0) {
        long now = monotonic_ms();
        if (now >= deadline_ms) {
            for (auto& [rank, fd] : unconfirmed) {
                (void) rank;
                ::close(fd);
            }
            throw Utils::Timeout();
        }

        if (!published && now >= next_publish_attempt) {
            try {
                publish_self(deadline_ms);
                published = true;
            } catch (const std::runtime_error&) {
                next_publish_attempt = monotonic_ms() + registry_poll_interval_ms;
            }
        }

        if (!want_connect.empty() && now >= next_connect_attempt) {
            // One HGETALL for the whole map rather than an HGET per peer.
            std::map<Utils::peer_num, PeerAddr> addrs;
            bool registry_reachable = true;
            try {
                for (const auto& [rank, entry] :
                     registry->snapshot(registry_key(), deadline_ms - monotonic_ms())) {
                    auto first = entry.find(':');
                    auto second = entry.rfind(':');
                    if (first == std::string::npos || second == first) {
                        continue;
                    }
                    PeerAddr addr;
                    addr.ip = entry.substr(0, first);
                    try {
                        addr.port = std::stoi(entry.substr(first + 1, second - first - 1));
                        addr.nonce = std::stoull(entry.substr(second + 1));
                    } catch (const std::exception&) {
                        continue;
                    }
                    addrs[rank] = addr;
                }
            } catch (const std::runtime_error&) {
                registry_reachable = false;
            }
            want_connect = connect_batch(addrs, want_connect, unconfirmed, deadline_ms);
            // Two different waits: a peer whose address is not published yet is a registry
            // question, and that is what registry_poll_interval_ms is for. A peer we have an
            // address for but could not reach is a connection question.
            bool waiting_on_registry = !registry_reachable;
            for (auto rank : want_connect) {
                if (addrs.find(rank) == addrs.end()) {
                    waiting_on_registry = true;
                    break;
                }
            }
            next_connect_attempt = monotonic_ms() + (waiting_on_registry ? registry_poll_interval_ms
                                                                        : connect_retry_interval_ms);
        }

        // Always poll the listener, even when the peer we want is one we connect to: a lower
        // rank blocked here still owes accepts and acknowledgements to the ranks above it.
        std::vector<struct pollfd> pfds;
        std::vector<Utils::peer_num> pfd_rank;
        pfds.push_back({listen_fd, POLLIN, 0});
        pfd_rank.push_back(0);
        for (const auto& [rank, fd] : unconfirmed) {
            pfds.push_back({fd, POLLIN, 0});
            pfd_rank.push_back(rank);
        }

        // Sleep no longer than the nearest pending retry, so a scheduled registry poll or
        // publish is not delayed until the next socket event.
        long budget = deadline_ms - monotonic_ms();
        if (!want_connect.empty()) {
            budget = std::min<long>(budget, std::max<long>(next_connect_attempt - monotonic_ms(), 1));
        }
        if (!published) {
            budget = std::min<long>(budget, std::max<long>(next_publish_attempt - monotonic_ms(), 1));
        }
        int pr = ::poll(pfds.data(), pfds.size(), static_cast<int>(std::max<long>(budget, 0)));
        if (pr < 0) {
            if (errno == EINTR) {
                continue;
            }
            throw std::runtime_error("DirectTCP: poll failed: " + std::string(strerror(errno)));
        }
        if (pr == 0) {
            continue;
        }

        for (std::size_t i = 0; i < pfds.size(); i++) {
            if (pfds[i].revents == 0) {
                continue;
            }
            if (i == 0) {
                accept_one();
                // Every rank except the target goes into the socket table at once; the
                // target's entry is what ends this wait, and establish() takes it.
                adopt_pending(target);
                continue;
            }
            Utils::peer_num rank = pfd_rank[i];
            auto it = unconfirmed.find(rank);
            if (it == unconfirmed.end()) {
                continue;
            }
            Utils::peer_num acked = 0;
            if (read_hello(it->second, acked, 0) && acked == rank) {
                pending_links[rank] = it->second;
                total_connections.fetch_add(1);
            } else {
                // Wrong listener, or it went away: drop this socket and resolve the address
                // again from a fresh snapshot rather than retrying the same endpoint.
                ::close(it->second);
                want_connect.push_back(rank);
                next_connect_attempt = 0;
            }
            unconfirmed.erase(it);
        }
    }
    for (auto& [rank, fd] : unconfirmed) {
        (void) rank;
        ::close(fd);
    }
}

int FMI::Comm::DirectTCP::establish(Utils::peer_num partner_id, const std::string&) {
    if (partner_id == peer_id) {
        throw std::runtime_error("DirectTCP: rank " + std::to_string(peer_id) +
                                 " cannot establish a link to itself");
    }
    if (partner_id >= num_peers) {
        throw std::runtime_error("DirectTCP: peer " + std::to_string(partner_id) +
                                 " is out of range for a communicator of " +
                                 std::to_string(num_peers));
    }

    auto it = pending_links.find(partner_id);
    if (it == pending_links.end()) {
        build_mesh(partner_id, monotonic_ms() + static_cast<long>(max_timeout));
        it = pending_links.find(partner_id);
        if (it == pending_links.end()) {
            throw Utils::Timeout();
        }
    }
    int fd = it->second;
    pending_links.erase(it);
    return fd;
}

void FMI::Comm::DirectTCP::close_transport_state() {
    for (auto& [rank, fd] : pending_links) {
        (void) rank;
        if (fd >= 0) {
            ::close(fd);
        }
    }
    pending_links.clear();
    if (listen_fd >= 0) {
        ::close(listen_fd);
        listen_fd = -1;
    }
    listen_port = 0;
    listener_nonce = 0;
    advertised_ip.clear();
    if (registry) {
        registry->disconnect();
    }
}
