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
#include <cstdio>
#include <cstdlib>
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
 * Commands go out through redisCommandArgv with explicit argument lengths, and a dead context
 * is reconnected and the batch retried once. Deliberately NOT built like the Redis channel,
 * which concatenates commands into a string and passes it as a printf format — a comm_name
 * containing '%' or a space corrupts those.
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
    birth_boot_id = read_boot_id();
}

std::string FMI::Comm::DirectTCP::read_boot_id() {
    std::string id;
    if (FILE* f = std::fopen("/proc/sys/kernel/random/boot_id", "r")) {
        char buf[64] = {};
        if (std::fgets(buf, sizeof(buf), f) != nullptr) {
            id = buf;
            while (!id.empty() && (id.back() == '\n' || id.back() == ' ')) {
                id.pop_back();
            }
        }
        std::fclose(f);
    }
    return id;
}

bool FMI::Comm::DirectTCP::reset_transport_if_relocated() {
    if (birth_boot_id.empty()) {
        return false;
    }
    const std::string current = read_boot_id();
    if (current.empty() || current == birth_boot_id) {
        return false;
    }
    // Restored on a different machine. Contract and rationale on the declaration: wipe the
    // transport wholesale — the same reset the FT-managed checkpoint hook performed — and
    // let establishment start over on truth. Sequenced-link state is deliberately kept;
    // replay over the fresh connections is what recovers the streams, exactly as on the
    // same-host path.
    if (const char* lt = std::getenv("FMI_LINK_TRACE"); lt && lt[0] == '1') {
        std::fprintf(stderr, "[lt] RELOCATION-RESET old_boot=%.8s new_boot=%.8s\n",
                     birth_boot_id.c_str(), current.c_str());
    }
    close_sockets();
    close_transport_state();
    // Every link this reset severed must REPLAY when it re-forms. Closing the fds here
    // bypasses the per-link I/O-error discovery that normally plants the reconcile debt
    // through repair_link, and a markless re-established link skips its handshake and
    // replay entirely — the peer then classifies the retained-frames hole as a FATAL
    // sequence gap (adversarial-review finding, confirmed). Plant the debts wholesale,
    // exactly as per-link repairs would have.
    if (recover_links) {
        if (link_needs_reconcile.size() != num_peers) {
            link_needs_reconcile.assign(num_peers, 0);
        }
        for (Utils::peer_num q = 0; q < num_peers; q++) {
            if (q != peer_id) {
                link_needs_reconcile[q] = 1;
            }
        }
    }
    birth_boot_id = current;
    return true;
}

FMI::Comm::DirectTCP::~DirectTCP() {
    // Settle retention and half-close before the abrupt close below: a straight close() from
    // a finished rank RSTs away frames a still-working peer had not yet consumed, and that
    // peer then re-establishes toward a process that no longer exists. See
    // drain_links_for_shutdown's declaration for the full mechanism.
    drain_links_for_shutdown();
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
    // Re-derived on EVERY publish, never trusted from listener creation: the cached value is
    // the one piece of transport state that goes stale when a criu image is restored on a
    // different machine. The listener itself survives relocation — criu re-binds
    // 0.0.0.0:port wherever the port is free, and the nonce rides in the image — but an
    // address derived on the dump host sends every higher peer to a machine this process no
    // longer lives on, while this process waits for their dials on the new one: a mutual
    // wedge for both sides' full deadlines (observed and traced, baseline@4p, rank 1 moved
    // between hosts). Re-deriving here heals it at the first post-restore establishment,
    // and peers pick the corrected entry up on their next registry poll. The probe is two
    // syscalls ahead of a Redis round trip; with an explicit advertise_host it degenerates
    // to returning the configured string (which is exactly why cross-host restore requires
    // advertise_host to be left empty).
    const std::string current_ip = resolve_advertise_ip();
    // Never DOWNGRADE a working advertisement to the last-resort fallback: the probe can
    // fail transiently (fd exhaustion in a repair storm, a resolver blip when registry_host
    // is a name) while the publish itself still lands on the already-connected context — a
    // poisoned loopback entry would send every dialer to its own machine. Adopting the
    // fallback is right only when nothing better was ever known (single-host dev, where
    // loopback genuinely is the address). The periodic republish retries the derivation, so
    // a real relocation still heals on the next pass once the probe succeeds.
    if (current_ip != advertised_ip &&
        !(current_ip == "127.0.0.1" && !advertised_ip.empty())) {
        if (const char* lt = std::getenv("FMI_LINK_TRACE"); lt && lt[0] == '1') {
            std::fprintf(stderr, "[lt] ADVERTISE-MOVED old=%s new=%s port=%u\n",
                         advertised_ip.c_str(), current_ip.c_str(),
                         static_cast<unsigned>(listen_port));
        }
        advertised_ip = current_ip;
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
    // The link hash carries the comm_name, so a connector belonging to a different
    // communicator cannot pass itself off as the legitimate party for this one.
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
    // The connector addressed our listener nonce, which is what distinguishes this listener
    // from whatever else has held this ephemeral port.
    if (!read_frame(fd, claimed, listener_nonce)) {
        ::close(fd);
        return AcceptResult::Rejected;
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
        if (!recover_links) {
            ::close(fd);   // already holding a fresh connection for that rank
            return AcceptResult::Rejected;
        }
        // The reconnect is the evidence, exactly as for sockets[] below: a peer only dials
        // again after abandoning its previous attempt, so the parked descriptor is that
        // abandoned attempt - observed for real when a dialer missed its hello-ack, closed
        // its end, and every retry was then rejected on the strength of the dead fd it left
        // behind, wedging both ranks until the job's deadline.
        if (const char* lt = std::getenv("FMI_LINK_TRACE"); lt && lt[0] == '1') {
            std::fprintf(stderr, "[lt] pending-REPLACE rank=%u oldfd=%d newfd=%d\n",
                         static_cast<unsigned>(claimed), pending_links[claimed], fd);
        }
        ::close(pending_links[claimed]);
        pending_links.erase(claimed);
    }
    if (claimed < sockets.size() && sockets[claimed] >= 0) {
        if (!recover_links) {
            // Without the link layer a replacement cannot be reconciled, so the existing
            // stream - which may be mid-message - has to win.
            ::close(fd);
            return AcceptResult::Rejected;
        }
        // With it, this connection IS the evidence. A peer only dials a rank it already had a
        // link to after deciding that link is gone, and the hello it just passed proves it is
        // that peer, on this comm. Believing the local descriptor instead is
        // what wedged a restored rank: after criu the old descriptor can still *report*
        // ESTABLISHED while nothing is on the other end, so probing it answers "live", the
        // reconnect is refused, and the peer's handshake sits unread on a connection this rank
        // accepted but never adopted.
        if (const char* lt = std::getenv("FMI_LINK_TRACE"); lt && lt[0] == '1') {
            std::fprintf(stderr, "[lt] accept-REPLACE rank=%u oldfd=%d newfd=%d\n",
                         static_cast<unsigned>(claimed), sockets[claimed], fd);
        }
        ::close(sockets[claimed]);
        sockets[claimed] = -1;
        note_link_replaced(claimed);
    }
    // Acknowledge with a zero nonce: the connector has no nonce of its own to prove, it only
    // needs to learn that it reached the rank it asked for.
    if (!send_frame(fd, claimed, 0)) {
        ::close(fd);
        return AcceptResult::Rejected;
    }
    pending_links[claimed] = fd;
    total_connections.fetch_add(1);
    return AcceptResult::Accepted;
}

std::string FMI::Comm::DirectTCP::transport_state_note() const {
    std::string note = "listen=" + std::to_string(listen_fd) + " pending={";
    for (const auto& [rank, fd] : pending_links) {
        note += std::to_string(rank) + ":" + std::to_string(fd) + " ";
    }
    return note + "}";
}

void FMI::Comm::DirectTCP::service_transport() {
    if (listen_fd < 0) {
        return;
    }
    // Bounded, and never blocking: accept_one() returns false the moment the backlog is empty,
    // and this runs inside somebody else's operation.
    // Keep going past a rejection: only an empty backlog ends the pass. The bound is a
    // backstop, not the exit condition.
    for (int seen = 0; seen < 64; seen++) {
        try {
            if (accept_one() == AcceptResult::Empty) {
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
    adopt_pending(num_peers);
}

void FMI::Comm::DirectTCP::adopt_pending(Utils::peer_num exclude) {
    // `exclude` protects establish()'s contract: build_mesh completes on a PENDING entry for
    // its target, which establish() then pops and returns. Adopting the target here instead
    // used to leave establish() finding nothing and reporting a timeout for a link that is
    // fine — measured as 13 suite failures the one time it was tried; establish() now
    // returns an already-adopted socket, so the exclude is an optimization that keeps the
    // common path on the pop contract, not a correctness requirement. That matters because
    // service_transport — reachable from INSIDE an in-flight establishment through servicing
    // → reconcile → write_all → pump — excludes nothing, and skipping the target there would
    // strand its sender: acknowledged at accept, its handshake and replay pushed into a
    // connection nobody reads for as long as the establishment lasts. Everyone is fair game,
    // and must be adopted even mid-establishment.
    for (auto it = pending_links.begin(); it != pending_links.end();) {
        const Utils::peer_num rank = it->first;
        const int fd = it->second;
        if (rank == exclude) {
            ++it;
            continue;
        }
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


bool FMI::Comm::DirectTCP::redial_dead_link(Utils::peer_num partner_id, long budget_ms) {
    // Only the dialing side of the pair may act; the listener's whole obligation is already
    // met by servicing its accept queue. DirectTCP dials DOWN: build_mesh connects to lower
    // ranks and waits for higher ones.
    if (partner_id >= peer_id) {
        return false;
    }
    // Close the dead descriptor only here, at the moment its replacement is actually being
    // dialed - closing it at observation time forced every reader through re-establishment
    // for links whose death needed no repair at all (a peer that finalized after its last
    // message). The mark makes check_socket pay the handshake and replay on the new link.
    if (partner_id < sockets.size() && sockets[partner_id] >= 0) {
        close(sockets[partner_id]);
        sockets[partner_id] = -1;
    }
    note_link_replaced(partner_id);
    // Failures here were invisible — REDIALED prints only on success, so a redial that
    // fails every slice for a whole deadline looks in a trace exactly like a redial that
    // never ran. One throttled line per outcome makes the rescuer auditable.
    const bool lt_on = [] {
        const char* lt = std::getenv("FMI_LINK_TRACE");
        return lt && lt[0] == '1';
    }();
    try {
        build_mesh(partner_id, monotonic_ms() + std::max<long>(budget_ms, 50));
    } catch (const Utils::Timeout&) {
        if (lt_on) {
            static thread_local long last_rdf_note = 0;
            const long now = monotonic_ms();
            if (now - last_rdf_note > 1000) {
                last_rdf_note = now;
                std::fprintf(stderr, "[lt] REDIAL-FAIL peer=%u reason=timeout budget=%ld\n",
                             static_cast<unsigned>(partner_id), budget_ms);
            }
        }
        return false;   // not now; pump retries next slice
    } catch (const std::exception& e) {
        if (lt_on) {
            static thread_local long last_rde_note = 0;
            const long now = monotonic_ms();
            if (now - last_rde_note > 1000) {
                last_rde_note = now;
                std::fprintf(stderr, "[lt] REDIAL-FAIL peer=%u reason=%s\n",
                             static_cast<unsigned>(partner_id), e.what());
            }
        }
        return false;
    }
    // establish() pops the pending entry and check_socket stores it plus pays the debt.
    if (partner_id < sockets.size() && sockets[partner_id] >= 0) {
        return true;
    }
    if (pending_links.count(partner_id) > 0) {
        check_socket(partner_id, link_name(partner_id, true));
        return partner_id < sockets.size() && sockets[partner_id] >= 0;
    }
    if (lt_on) {
        static thread_local long last_rdn_note = 0;
        const long now = monotonic_ms();
        if (now - last_rdn_note > 1000) {
            last_rdn_note = now;
            std::fprintf(stderr, "[lt] REDIAL-FAIL peer=%u reason=no-link-after-mesh\n",
                         static_cast<unsigned>(partner_id));
        }
    }
    return false;
}

void FMI::Comm::DirectTCP::build_mesh(Utils::peer_num target, long deadline_ms) {
    ++establishment_depth;
    struct DepthGuard {
        int& depth;
        ~DepthGuard() { --depth; }
    } depth_guard{establishment_depth};
    // Before touching any transport state: a criu image restored on a different machine
    // must not build on what it woke up with (see reset_transport_if_relocated). Detecting
    // it here covers every establishment that STARTS after the restore; the in-loop check
    // below covers an establishment the freeze landed inside of.
    reset_transport_if_relocated();
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

    // One event loop, and it keeps servicing the listener the whole time it waits — accepting
    // from any peer, not just the one we want. That is what makes lazy establishment safe: the
    // only thing a rank ever waits for is a HIGHER rank connecting to it, so the wait-for
    // relation runs strictly upward and cannot close a cycle, and a rank that is waiting is
    // still handing out every accept and acknowledgement it owes.
    while (!have_link(target)) {
        long now = monotonic_ms();
        if (const char* lt = std::getenv("FMI_LINK_TRACE"); lt && lt[0] == '1') {
            static thread_local long last_bm_note = 0;
            if (now - last_bm_note > 3000) {
                last_bm_note = now;
                std::fprintf(stderr,
                             "[lt] BUILD-MESH-WAIT target=%u dialer=%d pending=%zu unconfirmed=%zu next_pub_in=%ld\n",
                             static_cast<unsigned>(target), target < peer_id ? 1 : 0,
                             pending_links.size(), unconfirmed.size(),
                             next_publish_attempt - now);
            }
        }
        if (now >= deadline_ms) {
            for (auto& [rank, fd] : unconfirmed) {
                (void) rank;
                ::close(fd);
            }
            throw Utils::Timeout();
        }

        // Re-advertise PERIODICALLY while waiting, never behind a publish-once latch: the
        // latch was a stack local a criu image preserves, so a rank frozen inside this loop
        // and restored on a different machine resumed with it already true and kept the
        // dump host's address in the registry for the establishment's whole remainder — the
        // one freeze position publish-time re-derivation cannot reach on its own
        // (adversarial-review finding, confirmed; also the traced evacuation signature:
        // peers connected to the moved rank while it waited behind the stale entry). A
        // refresh a second is one HSET against a registry already serving every
        // establishment's HGETALL polls, and it also renews the TTL for long waits.
        if (now >= next_publish_attempt) {
            if (reset_transport_if_relocated()) {
                // This loop's locals belong to the pre-relocation attempt; the connects in
                // flight aimed at addresses that meant something on the old machine. Restart
                // THIS establishment on the fresh transport, same target, same deadline.
                for (auto& [rank, fd] : unconfirmed) {
                    (void) rank;
                    ::close(fd);
                }
                unconfirmed.clear();
                ensure_listener();
                // The dropped connect was this establishment's only path to a LOWER target:
                // connect_batch consumed want_connect when it issued it, and nothing refills
                // the queue once unconfirmed is cleared — without this the loop would
                // publish and service until its whole deadline with no dial in flight
                // (adversarial-review finding, confirmed).
                if (target < peer_id &&
                    std::find(want_connect.begin(), want_connect.end(), target) ==
                            want_connect.end()) {
                    want_connect.push_back(target);
                }
            }
            try {
                publish_self(deadline_ms);
                next_publish_attempt = monotonic_ms() + 1000;
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

        // Take whatever frames are already waiting on this rank's other links. Without
        // this, a rank stuck here reads nothing else, and after a restore — when several links
        // rebuild at once — every rank ends up holding a frame another rank is waiting for.
        const bool serviced_progress = service_established_links(target);

        // Re-test the exit condition HERE, not only at the loop top: the servicing pass
        // above nests pumps, pumps run service_transport, and adopt_pending can complete
        // this very establishment from inside the body. Falling through to the poll with
        // the condition already satisfied slept a restored rank for its entire repair
        // deadline once — every peer socket drained, nothing left to wake it, the frame its
        // application wanted already parked in a lane one stack frame above — and its four
        // peers timed out ~50 ms before its own deadline would have woken it.
        if (have_link(target)) {
            continue;
        }

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
        // The established links are polled too, so this loop wakes when their bytes arrive
        // and the servicing pass above gets to run. A listener-side wait used to sleep here
        // until its deadline: harmless when servicing could only ever take whole frames —
        // whatever it left behind stayed left behind — but fatal now that servicing stages
        // oversized frames a socket buffer at a time, where sleeping means the peer's
        // half-delivered frame jams both ends for the rest of the deadline. Only after a
        // pass that made progress, though: a readable socket servicing REFUSED to drain (a
        // frame still growing, an app-owned stream, a decode failure) would wake this poll
        // instantly and spin the loop at full speed; after a refusal the poll sleeps a
        // bounded slice on the listener and timers alone, and the next pass re-judges.
        const std::size_t unconfirmed_end = pfds.size();
        if (serviced_progress) {
            for (Utils::peer_num q = 0; q < num_peers; q++) {
                if (q == peer_id || q >= sockets.size() || sockets[q] < 0) {
                    continue;
                }
                if (q < app_owns_stream.size() && app_owns_stream[q]) {
                    continue;   // mid-frame on the application's side; servicing skips it
                }
                pfds.push_back({sockets[q], POLLIN, 0});
                pfd_rank.push_back(q);
            }
        }

        // Sleep no longer than the nearest pending retry, so a scheduled registry poll or
        // publish is not delayed until the next socket event.
        long budget = deadline_ms - monotonic_ms();
        if (!want_connect.empty()) {
            budget = std::min<long>(budget, std::max<long>(next_connect_attempt - monotonic_ms(), 1));
        }
        // The publish is periodic now, so its retry timer always bounds the sleep.
        budget = std::min<long>(budget, std::max<long>(next_publish_attempt - monotonic_ms(), 1));
        if (!serviced_progress) {
            // The established sockets are out of the poll set this iteration, but the
            // servicing pass must still come back: the staging heuristic judges a frame
            // jammed by observing NO growth across a slice, and a stalled frame's rescue is
            // the pass after that judgement. One pump-slice heartbeat.
            budget = std::min<long>(budget, 20);
        }
        if (const char* lt = std::getenv("FMI_LINK_TRACE"); lt && lt[0] == '1') {
            // Before the poll, not only at the loop top: a body that never returns from its
            // poll is invisible to the loop-top beacon, and that silence cost a forensic
            // pass exactly one inference it could not witness directly.
            static thread_local long last_bp_note = 0;
            const long now_bp = monotonic_ms();
            if (now_bp - last_bp_note > 3000) {
                last_bp_note = now_bp;
                std::fprintf(stderr,
                             "[lt] BUILD-MESH-POLL target=%u budget=%ld nfds=%zu progress=%d\n",
                             static_cast<unsigned>(target), budget, pfds.size(),
                             serviced_progress ? 1 : 0);
            }
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
                // Adopt for every rank EXCEPT the target (whose pending entry is this very
                // wait's completion condition, popped by establish()). Without this, a
                // connection accepted here for a third rank sits parked and unread for as
                // long as this wait lasts, while its sender - acknowledged at accept - has
                // moved on to frames and possibly a replay nobody drains.
                adopt_pending(target);
                continue;
            }
            if (i >= unconfirmed_end) {
                // An established link has bytes; the next iteration's servicing pass takes
                // them. Nothing to do here — and in particular no unconfirmed lookup, whose
                // rank numbers these slots can collide with.
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
            // No pending entry, but the link may nevertheless be up: a servicing pass nested
            // inside this very establishment (pump → service_transport → adopt_pending, which
            // excludes nothing) can have adopted the target already. That is a completed
            // establishment, not a failure — check_socket stores the same fd back and pays
            // any reconcile debt the adoption deferred. Throwing here instead reported a
            // Timeout for a healthy link.
            if (partner_id < sockets.size() && sockets[partner_id] >= 0) {
                return sockets[partner_id];
            }
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
    // Only finalize() and the destructor reach this today, so "next use" means a channel
    // rebuilt from scratch. It also leaves the rank correct if the process is ever resumed
    // somewhere else, where the port it held is gone and the address it advertised is wrong.
    advertised_ip.clear();
    if (registry) {
        registry->disconnect();
    }
}

