#include "../../include/comm/DirectTCP.h"

#include "../../include/utils/Signals.h"

#include <hiredis/hiredis.h>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
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
#include <initializer_list>
#include <mutex>
#include <random>
#include <stdexcept>
#include <thread>
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

    void put32(char* p, std::uint32_t v) {
        for (int i = 0; i < 4; i++) {
            p[i] = static_cast<char>((v >> (8 * (3 - i))) & 0xFF);
        }
    }

    std::uint32_t get32(const char* p) {
        std::uint32_t v = 0;
        for (int i = 0; i < 4; i++) {
            v = (v << 8) | static_cast<unsigned char>(p[i]);
        }
        return v;
    }

    void put64(char* p, std::uint64_t v) {
        for (int i = 0; i < 8; i++) {
            p[i] = static_cast<char>((v >> (8 * (7 - i))) & 0xFF);
        }
    }

    std::uint64_t get64(const char* p) {
        std::uint64_t v = 0;
        for (int i = 0; i < 8; i++) {
            v = (v << 8) | static_cast<unsigned char>(p[i]);
        }
        return v;
    }

    std::uint64_t fnv1a64(const std::string& s) {
        std::uint64_t hash = 1469598103934665603ULL;
        for (unsigned char c : s) {
            hash ^= c;
            hash *= 1099511628211ULL;
        }
        return hash;
    }

    std::uint64_t random_nonce() {
        std::random_device rd;
        std::mt19937_64 gen(((static_cast<std::uint64_t>(rd()) << 32) ^ rd()) ^
                            static_cast<std::uint64_t>(::getpid()));
        std::uniform_int_distribution<std::uint64_t> dist(1, UINT64_MAX);
        return dist(gen);
    }

    void set_nonblocking(int fd, bool on) {
        int flags = ::fcntl(fd, F_GETFL, 0);
        if (flags < 0) {
            return;
        }
        ::fcntl(fd, F_SETFL, on ? (flags | O_NONBLOCK) : (flags & ~O_NONBLOCK));
    }

    //! Resolve a host that may be a literal IPv4 address or a name.
    bool resolve_ipv4(const std::string& host, struct in_addr& out) {
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

    //! Is this descriptor still a live connection, or one the far end has torn down?
    /*!
     * Asked of a socket this rank still holds a descriptor for but has not touched recently.
     * TCP_INFO rather than a peek: a peek cannot separate "idle" from "gone" without consuming
     * data, and there may legitimately be unread bytes on a connection whose peer has since
     * died. Anything other than ESTABLISHED means the connection is over.
     *
     * Conservative when the state cannot be read at all (a non-TCP socket in a test harness,
     * or a platform without TCP_INFO): reports live, which preserves the previous behaviour.
     */
    bool socket_is_established(int fd) {
#if defined(TCP_INFO)
        struct tcp_info info {};
        socklen_t len = sizeof(info);
        if (::getsockopt(fd, IPPROTO_TCP, TCP_INFO, &info, &len) != 0) {
            return true;
        }
        return info.tcpi_state == TCP_ESTABLISHED;
#else
        (void) fd;
        return true;
#endif
    }

    std::string param_or(std::map<std::string, std::string>& params, const std::string& key,
                         const std::string& fallback) {
        auto it = params.find(key);
        if (it == params.end() || it->second.empty()) {
            return fallback;
        }
        return it->second;
    }
}

//! Minimal Redis client for the peer registry.
/*!
 * Deliberately modelled on FT::ControlPlane's command path (redisCommandArgv, explicit
 * argument lengths, reconnect-and-retry once) rather than on the Redis channel, which builds
 * commands by string concatenation and passes them as printf format strings — a comm_name
 * containing '%' or a space corrupts those.
 *
 * Not shared with ControlPlane: that class throws unless fault tolerance is configured, and
 * this backend must work in plain non-FT runs.
 */
class FMI::Comm::DirectTCP::Registry {
public:
    Registry(std::string host, int port) : host(std::move(host)), port(port) {
        // Here, not at connect time: the first command after a criu restore is issued on the
        // context the checkpoint captured, whose socket the restore already dropped. That
        // write happens *before* any reconnect, so a disposition installed on the reconnect
        // path would arrive one dead write too late.
        FMI::Utils::suppress_sigpipe();
    }

    ~Registry() { disconnect(); }

    void disconnect() {
        std::lock_guard<std::mutex> lock(mutex);
        close_locked();
    }

    //! HSET + EXPIRE pipelined into a single round trip.
    void publish(const std::string& key, const std::string& field, const std::string& value,
                 unsigned int ttl_s, long timeout_ms) {
        std::lock_guard<std::mutex> lock(mutex);
        set_timeout_locked(timeout_ms);
        std::vector<std::vector<std::string>> batch{{"HSET", key, field, value}};
        if (ttl_s > 0) {
            batch.push_back({"EXPIRE", key, std::to_string(ttl_s)});
        }
        pipeline_locked(batch);
    }

    //! Whole rank -> "ip:port:nonce" map in one round trip. Never one HGET per peer.
    std::map<FMI::Utils::peer_num, std::string> snapshot(const std::string& key, long timeout_ms) {
        std::lock_guard<std::mutex> lock(mutex);
        set_timeout_locked(timeout_ms);
        auto replies = pipeline_locked({{"HGETALL", key}});
        std::map<FMI::Utils::peer_num, std::string> out;
        redisReply* reply = replies.front();
        if (reply->type != REDIS_REPLY_ARRAY) {
            return out;
        }
        for (std::size_t i = 0; i + 1 < reply->elements; i += 2) {
            redisReply* field = reply->element[i];
            redisReply* value = reply->element[i + 1];
            if (field->str == nullptr || value->str == nullptr) {
                continue;
            }
            try {
                out[static_cast<FMI::Utils::peer_num>(std::stoul(std::string(field->str, field->len)))] =
                        std::string(value->str, value->len);
            } catch (const std::exception&) {
                // A field that is not a rank number is not ours; ignore it rather than failing
                // the whole discovery pass.
            }
        }
        return out;
    }

private:
    void close_locked() {
        for (auto* reply : owned) {
            freeReplyObject(reply);
        }
        owned.clear();
        if (context != nullptr) {
            redisFree(context);
            context = nullptr;
        }
    }

    //! Clamp every subsequent connect and command to what is left of the caller's deadline.
    /*!
     * Without this, redisConnect and redisGetReply block on the socket with no timeout at all,
     * so an unreachable or wedged registry hangs establishment indefinitely — the mesh deadline
     * is only ever checked between calls, never inside one.
     */
    void set_timeout_locked(long timeout_ms) {
        long clamped = std::max<long>(timeout_ms, 1);
        io_timeout.tv_sec = clamped / 1000;
        io_timeout.tv_usec = (clamped % 1000) * 1000;
        if (context != nullptr && !context->err) {
            redisSetTimeout(context, io_timeout);
        }
    }

    void connect_locked() {
        close_locked();
        context = redisConnectWithTimeout(host.c_str(), port, io_timeout);
        if (context == nullptr || context->err) {
            std::string error = "DirectTCP: could not connect to the peer registry at " + host +
                                ":" + std::to_string(port);
            if (context != nullptr && context->errstr != nullptr) {
                error += ": ";
                error += context->errstr;
            }
            if (context != nullptr) {
                redisFree(context);
                context = nullptr;
            }
            throw std::runtime_error(error);
        }
        redisSetTimeout(context, io_timeout);
        owner_pid = ::getpid();
    }

    //! Issue a batch and collect its replies. Retries the whole batch once on a dead context —
    //! including after a fork, where the inherited connection must not be reused (two processes
    //! reading one RESP stream steal each other's replies).
    std::vector<redisReply*> pipeline_locked(const std::vector<std::vector<std::string>>& batch) {
        for (auto* reply : owned) {
            freeReplyObject(reply);
        }
        owned.clear();

        for (int attempt = 0; attempt < 2; attempt++) {
            if (context == nullptr || context->err || owner_pid != ::getpid()) {
                connect_locked();
            }
            bool ok = true;
            for (const auto& args : batch) {
                std::vector<const char*> argv;
                std::vector<std::size_t> argvlen;
                argv.reserve(args.size());
                argvlen.reserve(args.size());
                for (const auto& arg : args) {
                    argv.push_back(arg.data());
                    argvlen.push_back(arg.size());
                }
                if (redisAppendCommandArgv(context, static_cast<int>(argv.size()), argv.data(),
                                           argvlen.data()) != REDIS_OK) {
                    ok = false;
                    break;
                }
            }
            if (ok) {
                for (std::size_t i = 0; i < batch.size(); i++) {
                    void* raw = nullptr;
                    if (redisGetReply(context, &raw) != REDIS_OK || raw == nullptr) {
                        ok = false;
                        break;
                    }
                    owned.push_back(reinterpret_cast<redisReply*>(raw));
                }
            }
            if (ok && owned.size() == batch.size()) {
                for (auto* reply : owned) {
                    if (reply->type == REDIS_REPLY_ERROR) {
                        std::string error = reply->str == nullptr ? "unknown Redis error" : reply->str;
                        throw std::runtime_error("DirectTCP: registry error: " + error);
                    }
                }
                return owned;
            }
            for (auto* reply : owned) {
                freeReplyObject(reply);
            }
            owned.clear();
            close_locked();
        }
        throw std::runtime_error("DirectTCP: peer registry command failed");
    }

    std::string host;
    int port;
    redisContext* context = nullptr;
    //! Connect and command timeout; reset from the caller's remaining deadline before each op.
    struct timeval io_timeout{1, 0};
    pid_t owner_pid = -1;
    std::vector<redisReply*> owned;
    std::mutex mutex;
};

FMI::Comm::DirectTCP::DirectTCP(std::map<std::string, std::string> params,
                                std::map<std::string, std::string> model_params) {
    transport_tag = "DirectTCP";
    eof_before_data_is_timeout = true;
    registry_host = param_or(params, "registry_host", "127.0.0.1");
    registry_port = std::stoi(param_or(params, "registry_port", "6379"));
    bind_host = param_or(params, "bind_host", "0.0.0.0");
    advertise_host = param_or(params, "advertise_host", "");
    registry_poll_interval_ms = std::stoi(param_or(params, "registry_poll_interval_ms", "5"));
    connect_retry_interval_ms = std::stoi(param_or(params, "connect_retry_interval_ms", "10"));
    registry_ttl_s = std::stoi(param_or(params, "registry_ttl_s", "3600"));
    parse_tcp_params(params);
    parse_tcp_model_params(model_params);
    // Constructed but not connected: peer_id/num_peers/comm_name are pushed in after
    // construction, and in the forked test harness the connection must be opened by the
    // process that will use it.
    registry = std::make_unique<Registry>(registry_host, registry_port);
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

std::string FMI::Comm::DirectTCP::resolve_advertise_ip() const {
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

void FMI::Comm::DirectTCP::ensure_listener() {
    if (listen_fd < 0) {
        int fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) {
            throw std::runtime_error("DirectTCP: socket() failed: " + std::string(strerror(errno)));
        }
        int one = 1;
        ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

        struct sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(0); // let the kernel pick a free port
        if (!resolve_ipv4(bind_host, addr.sin_addr)) {
            addr.sin_addr.s_addr = htonl(INADDR_ANY);
        }
        if (::bind(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
            std::string error = strerror(errno);
            ::close(fd);
            throw std::runtime_error("DirectTCP: bind to " + bind_host + " failed: " + error);
        }
        // SOMAXCONN, not a small constant: in a 32-rank binomial broadcast several peers issue
        // their SYN to the same rank simultaneously, and Linux silently drops the overflow —
        // the connector then retries on the TCP timer, blowing any sane deadline.
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
        // Non-blocking: the event loop only accepts on a poll() hit, but a SYN withdrawn
        // between the poll and the accept would otherwise block the loop indefinitely.
        set_nonblocking(fd, true);
        listen_fd = fd;
        // Fresh per listener incarnation: this is what lets a peer tell a live listener from a
        // recycled ephemeral port belonging to some earlier run under the same comm_name.
        listener_nonce = random_nonce();
        advertised_ip = resolve_advertise_ip();
    }
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

std::string FMI::Comm::DirectTCP::frame_for(Utils::peer_num partner_id,
                                            std::uint64_t nonce) const {
    std::string buf(HELLO_SIZE, '\0');
    put32(&buf[0], HELLO_MAGIC);
    put32(&buf[4], HELLO_VERSION);
    put32(&buf[8], static_cast<std::uint32_t>(peer_id));
    put32(&buf[12], static_cast<std::uint32_t>(partner_id));
    put64(&buf[16], fnv1a64(link_name(partner_id, true)));
    put64(&buf[24], nonce);
    return buf;
}

bool FMI::Comm::DirectTCP::send_frame(int fd, Utils::peer_num partner_id,
                                      std::uint64_t nonce) const {
    const std::string frame = frame_for(partner_id, nonce);
    std::size_t sent = 0;
    while (sent < frame.size()) {
        long n = ::send(fd, frame.data() + sent, frame.size() - sent, MSG_NOSIGNAL);
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

bool FMI::Comm::DirectTCP::read_frame(int fd, Utils::peer_num& sender,
                                      std::uint64_t expect_nonce) const {
    // The caller only ever reads a socket poll() has just reported readable, and the writer
    // sends all 32 bytes in one go, so a short read here means the peer died mid-frame. Bound
    // the wait tightly rather than inheriting the whole mesh deadline: a peer that connects and
    // never speaks (a port scanner, a readiness probe) must not hold up the event loop.
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

    if (get32(buf) != HELLO_MAGIC || get32(buf + 4) != HELLO_VERSION) {
        return false;
    }
    auto claimed = static_cast<Utils::peer_num>(get32(buf + 8));
    auto receiver = static_cast<Utils::peer_num>(get32(buf + 12));
    if (receiver != peer_id || claimed >= num_peers || claimed == peer_id) {
        return false;
    }
    // The link hash carries the epoch-qualified comm_name, so a straggler from an earlier epoch
    // cannot pass itself off as the legitimate party for this one.
    if (get64(buf + 16) != fnv1a64(link_name(claimed, false))) {
        return false;
    }
    if (get64(buf + 24) != expect_nonce) {
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
        set_nonblocking(fd, false);
        apply_socket_options(fd);
        // The hello goes out now, but the link is not usable until the peer acknowledges it:
        // this connect may have landed on a recycled port owned by someone else entirely.
        if (send_frame(fd, rank, nonce)) {
            unconfirmed[rank] = fd;
        } else {
            ::close(fd);
            remaining.push_back(rank);
        }
    };

    // Issue every connect first, then wait on all of them together: one poll for the whole
    // batch rather than a round trip per peer.
    for (auto rank : wanted) {
        auto it = addrs.find(rank);
        if (it == addrs.end()) {
            remaining.push_back(rank);
            continue;
        }
        struct sockaddr_in dest{};
        dest.sin_family = AF_INET;
        dest.sin_port = htons(static_cast<std::uint16_t>(it->second.port));
        if (!resolve_ipv4(it->second.ip, dest.sin_addr)) {
            remaining.push_back(rank);
            continue;
        }
        int fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) {
            remaining.push_back(rank);
            continue;
        }
        set_nonblocking(fd, true);
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
                // Refused or unreachable: the entry we read may be stale, so drop it and let
                // the caller re-resolve from a fresh snapshot.
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

bool FMI::Comm::DirectTCP::accept_one() {
    int fd = ::accept(listen_fd, nullptr, nullptr);
    if (fd < 0) {
        if (errno == EINTR || errno == ECONNABORTED || errno == EAGAIN || errno == EWOULDBLOCK) {
            return false;
        }
        throw std::runtime_error("DirectTCP: accept failed: " + std::string(strerror(errno)));
    }
    apply_socket_options(fd);

    Utils::peer_num claimed = 0;
    // The connector addressed our listener nonce, which is what distinguishes this listener
    // from whatever else has held this ephemeral port.
    if (!read_frame(fd, claimed, listener_nonce)) {
        ::close(fd);
        return false;
    }
    // Already hold a LIVE link to that rank: a duplicate would silently replace a stream that
    // may be mid-message.
    //
    // A link this rank believes it holds may nevertheless be dead, and after a checkpoint that
    // is the normal case rather than a corner one: a restored rank comes back owning a
    // descriptor for every peer, all of them closed by the restore, and every peer likewise
    // holds a torn-down connection to it. Rejecting on the strength of that descriptor is what
    // wedges the job. The rejected peer retries until its deadline while this rank waits to
    // accept somebody else, and because a rank only discovers a dead link by doing I/O on it,
    // the discovery can sit behind a collective that is itself waiting on the rejected peer.
    // That closes a wait-for cycle the lazy-establishment argument in build_mesh explicitly
    // relies on being impossible — it is, for a first establishment, and is not after a
    // restore. Measured: checkpointing rank 0 of an 8-rank job wedged every rank in the job.
    if (pending_links.count(claimed) > 0) {
        ::close(fd);
        return false;
    }
    if (claimed < sockets.size() && sockets[claimed] >= 0) {
        if (socket_is_established(sockets[claimed])) {
            ::close(fd);
            return false;
        }
        // Dead, and this connection is the proof. Drop it so the link can be rebuilt, and mark
        // it as replaced: the peer reached this connection through repair and is waiting to
        // reconcile, so this end owes it a handshake it would otherwise skip.
        ::close(sockets[claimed]);
        sockets[claimed] = -1;
        note_link_replaced(claimed);
    }
    // Acknowledge with a zero nonce: the connector has no nonce of its own to prove, it only
    // needs to learn that it reached the rank it asked for.
    if (!send_frame(fd, claimed, 0)) {
        ::close(fd);
        return false;
    }
    pending_links[claimed] = fd;
    total_connections.fetch_add(1);
    return true;
}

void FMI::Comm::DirectTCP::service_transport() {
    if (listen_fd < 0) {
        return;
    }
    // Bounded, and never blocking: accept_one() returns false the moment the backlog is empty,
    // and this runs inside somebody else's operation.
    for (int accepted = 0; accepted < 16; accepted++) {
        try {
            if (!accept_one()) {
                break;
            }
        } catch (const std::exception&) {
            break;   // the operation in flight owns error reporting, not this
        }
    }

    // Adopt anything accepted for a link this rank does not currently hold. Leaving it parked
    // until the application next talks to that peer means its handshake sits unread while the
    // peer waits for ours — which is a deadlock, not a delay, when that peer is what this rank
    // is ultimately waiting on.
    for (auto it = pending_links.begin(); it != pending_links.end();) {
        const Utils::peer_num rank = it->first;
        const int fd = it->second;
        const bool unheld = sockets.empty() || (rank < sockets.size() && sockets[rank] < 0);
        if (unheld && fd >= 0) {
            it = pending_links.erase(it);
            try {
                adopt_link(rank, fd);
            } catch (const std::exception&) {
                // Its handshake could not go out; the receive path will find the link broken
                // and repair it, which is that path's job and not this one's.
            }
        } else {
            ++it;
        }
    }
}

void FMI::Comm::DirectTCP::build_mesh(Utils::peer_num target, long deadline_ms) {
    ensure_listener();

    auto have_link = [&](Utils::peer_num rank) {
        return pending_links.count(rank) > 0 || (rank < sockets.size() && sockets[rank] >= 0);
    };

    // Only the link actually asked for is built. Establishing the full mesh up front would make
    // the ordering argument trivial, but it costs O(num_peers^2) sockets across the job where a
    // binomial-tree collective touches O(log num_peers) of them — measurably so: it left ~1500
    // sockets per suite run in TIME_WAIT and starved the co-resident TCPunch backend of
    // ephemeral ports.
    std::vector<Utils::peer_num> want_connect;
    if (target < peer_id && !have_link(target)) {
        want_connect.push_back(target);
    }

    std::map<Utils::peer_num, int> unconfirmed;
    long next_connect_attempt = 0;
    long next_publish_attempt = 0;
    bool published = false;

    // One event loop, and it keeps servicing the listener the whole time it waits — accepting
    // from any peer, not just the one we want. That is what makes lazy establishment safe: the
    // only thing a rank ever waits for is a HIGHER rank connecting to it, so the wait-for
    // relation runs strictly upward and cannot close a cycle, and a rank that is waiting is
    // still handing out every accept and acknowledgement it owes.
    while (!have_link(target)) {
        long now = monotonic_ms();
        if (now >= deadline_ms) {
            for (auto& [rank, fd] : unconfirmed) {
                (void) rank;
                ::close(fd);
            }
            throw Utils::Timeout();
        }

        // Re-advertise on every establishment, refreshing the key's TTL. A registry that is
        // briefly unreachable is retried rather than fatal — the deadline above is what ends it.
        if (!published && now >= next_publish_attempt) {
            try {
                publish_self(deadline_ms);
                published = true;
            } catch (const std::runtime_error&) {
                next_publish_attempt = monotonic_ms() + registry_poll_interval_ms;
            }
        }

        if (!want_connect.empty() && now >= next_connect_attempt) {
            // One HGETALL for the whole map rather than an HGET per peer, and one batch of
            // concurrent connects rather than a round trip each.
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

        // Take whatever whole frames are already waiting on this rank's other links. Without
        // this, a rank stuck here reads nothing else, and after a restore — when several links
        // rebuild at once — every rank ends up holding a frame another rank is waiting for.
        service_established_links(target);

        // Always poll the listener, even when the peer we want is one we connect to: a lower
        // rank blocked here still owes accepts and acknowledgements to the ranks above it.
        std::vector<struct pollfd> pfds;
        std::vector<Utils::peer_num> pfd_rank;
        pfds.push_back({listen_fd, POLLIN, 0});
        pfd_rank.push_back(0);
        const std::size_t listener_slots = 1;
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
            if (i < listener_slots) {
                accept_one();
                continue;
            }
            Utils::peer_num rank = pfd_rank[i];
            auto it = unconfirmed.find(rank);
            if (it == unconfirmed.end()) {
                continue;
            }
            Utils::peer_num acked = 0;
            if (read_frame(it->second, acked, 0) && acked == rank) {
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
    // Clearing the listener is what forces a fresh bind and a fresh advertisement on next use.
    // After a CRIU restore the process resumes on another host, so the port it used to hold is
    // gone and the address it used to advertise is wrong.
    advertised_ip.clear();
    if (registry) {
        registry->disconnect();
    }
}

bool FMI::Comm::DirectTCP::reconfigure_for_epoch(const std::string& new_comm_name,
                                                 const std::vector<Utils::peer_num>& moved_ranks) {
    for (auto rank : moved_ranks) {
        auto it = pending_links.find(rank);
        if (it != pending_links.end()) {
            if (it->second >= 0) {
                ::close(it->second);
            }
            pending_links.erase(it);
        }
    }
    TcpChannelBase::reconfigure_for_epoch(new_comm_name, moved_ranks);
    // The registry key is derived from comm_name, so the new epoch needs this rank advertised
    // again under the new key. That happens on its own: every establishment re-publishes, and
    // the listener is kept because its address has not changed.
    return true;
}
