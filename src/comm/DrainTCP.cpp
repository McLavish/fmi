#include "../../include/comm/DrainTCP.h"

#include "../../include/comm/PeerRegistry.h"
#include "../../include/comm/TcpEndpoint.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cmath>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <utility>

namespace {
    long monotonic_ms() {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now().time_since_epoch()).count();
    }

    //! How long the control thread sleeps in poll() when nothing is happening. Coarse on
    //! purpose: its only periodic duty is refreshing a registry entry whose TTL is measured in
    //! minutes, and a dormant thread is a load-bearing part of the "≈ zero steady state" claim.
    constexpr int control_housekeeping_tick_ms = 1000;

    //! Upper bound on a dialer's silence after its connection is accepted. A half-open
    //! connection — a port scanner, a readiness probe, a peer that died between connect and
    //! write — must cost the control thread this much and not one accept more.
    constexpr long hello_read_budget_ms = 200;

    //! Accepts taken per readable-listener wake before the loop returns to its other duties.
    constexpr int accept_batch = 16;

    //! Move exactly @p len bytes over a non-blocking fd, bounded by @p deadline_ms and
    //! abandoned promptly when the channel is shutting down. Used for the 56-byte hello only;
    //! the data path never goes through here, because it must be interruptible per chunk.
    bool move_exact(int fd, char* data, std::size_t len, bool writing, long deadline_ms,
                    int slice_ms, const std::atomic<bool>* abort_flag) {
        std::size_t moved = 0;
        while (moved < len) {
            if (abort_flag != nullptr && abort_flag->load(std::memory_order_acquire)) {
                return false;
            }
            const long now = monotonic_ms();
            if (now >= deadline_ms) {
                return false;
            }
            const ssize_t n = writing
                    ? ::send(fd, data + moved, len - moved, MSG_NOSIGNAL | MSG_DONTWAIT)
                    : ::recv(fd, data + moved, len - moved, MSG_DONTWAIT);
            if (n > 0) {
                moved += static_cast<std::size_t>(n);
                continue;
            }
            if (n == 0) {
                return false;   // peer closed mid-hello
            }
            if (errno == EINTR) {
                continue;
            }
            if (errno != EAGAIN && errno != EWOULDBLOCK) {
                return false;
            }
            struct pollfd pfd{fd, static_cast<short>(writing ? POLLOUT : POLLIN), 0};
            const long budget = std::min<long>(slice_ms, deadline_ms - monotonic_ms());
            ::poll(&pfd, 1, static_cast<int>(std::max<long>(budget, 0)));
        }
        return true;
    }
}

void FMI::Comm::encode_resume(const ResumeRecord& record, char* out) {
    TcpEndpoint::put32(out, drain_hello_magic);
    // Version and the reserved half-word share one big-endian word: written as a u32 they land
    // as two u16s in exactly the documented layout, with no put16 helper to add.
    TcpEndpoint::put32(out + 4, static_cast<std::uint32_t>(record.wire_version) << 16);
    TcpEndpoint::put32(out + 8, record.sender_rank);
    TcpEndpoint::put32(out + 12, record.receiver_rank);
    TcpEndpoint::put64(out + 16, record.link_name_hash);
    TcpEndpoint::put64(out + 24, record.nonce);
    TcpEndpoint::put64(out + 32, record.sender_incarnation);
    TcpEndpoint::put64(out + 40, record.bytes_sent);
    TcpEndpoint::put64(out + 48, record.bytes_received);
}

bool FMI::Comm::decode_resume(const char* in, ResumeRecord& out) {
    if (TcpEndpoint::get32(in) != drain_hello_magic) {
        return false;
    }
    const std::uint16_t version = static_cast<std::uint16_t>(TcpEndpoint::get32(in + 4) >> 16);
    if (version != drain_hello_version) {
        return false;
    }
    out.wire_version = version;
    out.sender_rank = TcpEndpoint::get32(in + 8);
    out.receiver_rank = TcpEndpoint::get32(in + 12);
    out.link_name_hash = TcpEndpoint::get64(in + 16);
    out.nonce = TcpEndpoint::get64(in + 24);
    out.sender_incarnation = TcpEndpoint::get64(in + 32);
    out.bytes_sent = TcpEndpoint::get64(in + 40);
    out.bytes_received = TcpEndpoint::get64(in + 48);
    return true;
}

FMI::Comm::DrainTCP::DrainTCP(std::map<std::string, std::string> params,
                              std::map<std::string, std::string> model_params) {
    // Required, exactly as for every other TCP transport: a channel with no patience configured
    // is a channel whose failures are indistinguishable from a hang.
    max_timeout = static_cast<unsigned int>(std::stoul(params.at("max_timeout")));
    registry_host = params.at("registry_host");
    registry_port = std::stoi(params.at("registry_port"));
    bind_host = TcpEndpoint::param_or(params, "bind_host", "0.0.0.0");
    advertise_host = TcpEndpoint::param_or(params, "advertise_host", "");
    registry_poll_interval_ms =
            static_cast<unsigned int>(std::stoul(TcpEndpoint::param_or(params, "registry_poll_interval_ms", "5")));
    connect_retry_interval_ms =
            static_cast<unsigned int>(std::stoul(TcpEndpoint::param_or(params, "connect_retry_interval_ms", "10")));
    registry_ttl_s = static_cast<unsigned int>(std::stoul(TcpEndpoint::param_or(params, "registry_ttl_s", "3600")));
    control_poll_interval_ms =
            static_cast<unsigned int>(std::stoul(TcpEndpoint::param_or(params, "control_poll_interval_ms", "20")));

    drain_enabled = TcpEndpoint::param_or(params, "drain", "false") == "true";
    trigger = TcpEndpoint::param_or(params, "trigger", "both");
    drain_signal_offset = std::stoi(TcpEndpoint::param_or(params, "drain_signal_offset", "3"));
    drain_grace_ms = std::stol(TcpEndpoint::param_or(params, "drain_grace_ms", "5000"));
    migration_max_ms = std::stol(TcpEndpoint::param_or(params, "migration_max_ms", "120000"));

    bandwidth = std::stod(model_params.at("bandwidth"));
    overhead = std::stod(model_params.at("overhead"));
    transfer_price = std::stod(model_params.at("transfer_price"));
    vm_price = std::stod(model_params.at("vm_price"));
    requests_per_hour = static_cast<unsigned int>(std::stoul(model_params.at("requests_per_hour")));
    include_infrastructure_costs = model_params["include_infrastructure_costs"] == "true";

    // Constructed, not connected: the process that will use this channel may be a fork of the
    // one that built it, and a hiredis context must belong to the process that reads its stream.
    registry = std::make_unique<PeerRegistry>(registry_host, registry_port);
}

FMI::Comm::DrainTCP::~DrainTCP() {
    try {
        DrainTCP::finalize();
    } catch (const std::exception&) {
        // A destructor that throws during unwinding terminates the process, and there is no
        // caller left who could act on the failure anyway.
    }
}

int FMI::Comm::DrainTCP::wait_slice_ms() const {
    return static_cast<int>(std::clamp<long>(static_cast<long>(control_poll_interval_ms), 1, 1000));
}

std::string FMI::Comm::DrainTCP::registry_key() const {
    // Never DirectTCP's "fmi:direct:" prefix: the two backends publish different value shapes
    // and a shared key would let one reinterpret the other's entries.
    return "fmi:drain:" + comm_name;
}

std::string FMI::Comm::DrainTCP::link_name(Utils::peer_num partner) const {
    const auto low = std::min(peer_id, partner);
    const auto high = std::max(peer_id, partner);
    return comm_name + "|" + std::to_string(low) + "-" + std::to_string(high);
}

FMI::Comm::DrainTCP::LinkState& FMI::Comm::DrainTCP::link(Utils::peer_num p) {
    if (p >= links.size() || links[p] == nullptr) {
        throw std::runtime_error("DrainTCP: peer " + std::to_string(p) +
                                 " is out of range for a communicator of " +
                                 std::to_string(links.size()));
    }
    return *links[p];
}

void FMI::Comm::DrainTCP::throw_if_unrecoverable(const LinkState& l, Utils::peer_num peer) {
    if (!l.unrecoverable.empty()) {
        throw std::runtime_error("DrainTCP: link to peer " + std::to_string(peer) + " is " +
                                 l.unrecoverable);
    }
}

void FMI::Comm::DrainTCP::ensure_started() {
    if (started.load(std::memory_order_acquire)) {
        return;
    }
    std::lock_guard<std::mutex> lifecycle(lifecycle_mutex);
    if (started.load(std::memory_order_relaxed)) {
        return;
    }
    if (finalized) {
        throw std::runtime_error("DrainTCP: channel was finalized and cannot be used again");
    }
    if (num_peers == 0 || peer_id >= num_peers) {
        throw std::runtime_error("DrainTCP: peer id " + std::to_string(peer_id) +
                                 " is not valid for a communicator of " +
                                 std::to_string(num_peers) +
                                 "; set_peer_id/set_num_peers must precede any operation");
    }

    // Everything below either completes or is undone: a half-built channel whose listener
    // exists but whose control thread does not would accept nothing and never say why.
    std::vector<std::unique_ptr<LinkState>> table;
    table.reserve(num_peers);
    for (Utils::peer_num p = 0; p < num_peers; p++) {
        table.push_back(std::make_unique<LinkState>());
    }

    int fd = -1;
    int pipe_fds[2] = {-1, -1};
    auto undo = [&]() {
        if (fd >= 0) {
            ::close(fd);
        }
        for (int end : pipe_fds) {
            if (end >= 0) {
                ::close(end);
            }
        }
    };

    try {
        fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) {
            throw std::runtime_error("DrainTCP: socket() failed: " + std::string(strerror(errno)));
        }
        int one = 1;
        ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

        struct sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(0);   // the kernel picks a free port
        if (!TcpEndpoint::resolve_ipv4(bind_host, addr.sin_addr)) {
            addr.sin_addr.s_addr = htonl(INADDR_ANY);
        }
        if (::bind(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
            throw std::runtime_error("DrainTCP: bind to " + bind_host + " failed: " +
                                     std::string(strerror(errno)));
        }
        // SOMAXCONN rather than a small constant: several peers of a binomial collective issue
        // their SYN to the same rank at once, and Linux silently drops the overflow.
        if (::listen(fd, SOMAXCONN) < 0) {
            throw std::runtime_error("DrainTCP: listen failed: " + std::string(strerror(errno)));
        }
        socklen_t len = sizeof(addr);
        if (::getsockname(fd, reinterpret_cast<struct sockaddr*>(&addr), &len) < 0) {
            throw std::runtime_error("DrainTCP: getsockname failed: " +
                                     std::string(strerror(errno)));
        }
        // Non-blocking: the control thread only accepts after poll() reports the listener
        // readable, but a SYN withdrawn between the poll and the accept would block it forever.
        TcpEndpoint::set_nonblocking(fd, true);

        if (::pipe(pipe_fds) != 0) {
            throw std::runtime_error("DrainTCP: pipe() failed: " + std::string(strerror(errno)));
        }
        TcpEndpoint::set_nonblocking(pipe_fds[0], true);
        TcpEndpoint::set_nonblocking(pipe_fds[1], true);

        links = std::move(table);
        listen_fd = fd;
        listen_port = ntohs(addr.sin_port);
        listener_nonce = TcpEndpoint::random_nonce();
        advertised_ip = TcpEndpoint::resolve_advertise_ip(advertise_host, registry_host,
                                                          registry_port);
        wake_pipe[0] = pipe_fds[0];
        wake_pipe[1] = pipe_fds[1];
        stopping.store(false, std::memory_order_release);
        // After this point the link table is read by two threads and must never be resized.
        // Starting the thread here is what publishes it safely: the launch is the release.
        control_thread = std::thread(&DrainTCP::control_loop, this);
        started.store(true, std::memory_order_release);
    } catch (...) {
        undo();
        links.clear();
        listen_fd = -1;
        listen_port = 0;
        listener_nonce = 0;
        advertised_ip.clear();
        wake_pipe[0] = -1;
        wake_pipe[1] = -1;
        throw;
    }
}

void FMI::Comm::DrainTCP::set_incarnation(std::uint64_t value) {
    local_incarnation.store(value, std::memory_order_release);
    if (num_peers == 0 || peer_id >= num_peers) {
        return;
    }
    try {
        ensure_started();
    } catch (const std::exception&) {
        // Arming early is an optimisation, not a contract. The data path arms again and
        // reports the failure to the caller that actually needs a link.
    }
}

bool FMI::Comm::DrainTCP::try_publish(long deadline_ms, bool force) {
    const long now = monotonic_ms();
    const long last = published_at_ms.load(std::memory_order_acquire);
    if (!force && last >= 0) {
        // Half the TTL, so an entry is refreshed a full TTL/2 before it could lapse. A rank
        // whose entry expires becomes permanently undiscoverable, and links are built lazily,
        // so "a peer needs me much later" is the normal case rather than the corner one.
        const long half_ttl_ms = registry_ttl_s == 0 ? std::numeric_limits<long>::max()
                                                     : static_cast<long>(registry_ttl_s) * 500;
        if (now - last < half_ttl_ms) {
            return true;
        }
    }
    const long remaining = deadline_ms - now;
    if (remaining <= 0) {
        return false;
    }
    try {
        registry->publish(registry_key(), std::to_string(peer_id),
                          advertised_ip + ":" + std::to_string(listen_port) + ":" +
                                  std::to_string(listener_nonce),
                          registry_ttl_s, remaining);
    } catch (const std::exception&) {
        return false;   // a briefly unreachable registry is retried, not fatal
    }
    published_at_ms.store(monotonic_ms(), std::memory_order_release);
    return true;
}

bool FMI::Comm::DrainTCP::lookup_peer(Utils::peer_num peer, PeerAddr& out, long deadline_ms) {
    const long remaining = deadline_ms - monotonic_ms();
    if (remaining <= 0) {
        return false;
    }
    std::map<Utils::peer_num, std::string> entries;
    try {
        entries = registry->snapshot(registry_key(), remaining);
    } catch (const std::exception&) {
        return false;
    }
    auto it = entries.find(peer);
    if (it == entries.end()) {
        return false;
    }
    const std::string& entry = it->second;
    const auto first = entry.find(':');
    const auto second = entry.rfind(':');
    if (first == std::string::npos || second == first) {
        return false;
    }
    try {
        out.ip = entry.substr(0, first);
        out.port = std::stoi(entry.substr(first + 1, second - first - 1));
        out.nonce = std::stoull(entry.substr(second + 1));
    } catch (const std::exception&) {
        return false;
    }
    return true;
}

void FMI::Comm::DrainTCP::apply_socket_options(int fd) const {
    struct timeval timeout{};
    timeout.tv_sec = max_timeout / 1000;
    timeout.tv_usec = (max_timeout % 1000) * 1000;
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
    // Nagle off: this transport's messages are small and latency-shaped, and a 40 ms delayed
    // ack on a collective's critical path costs more than every byte in it.
    int one = 1;
    #if !defined(SOL_TCP) && defined(IPPROTO_TCP)
    #define SOL_TCP IPPROTO_TCP
    #endif
    ::setsockopt(fd, SOL_TCP, TCP_NODELAY, &one, sizeof(one));
}

int FMI::Comm::DrainTCP::connect_to(const PeerAddr& addr, long deadline_ms) {
    struct sockaddr_in dest{};
    dest.sin_family = AF_INET;
    dest.sin_port = htons(static_cast<std::uint16_t>(addr.port));
    if (!TcpEndpoint::resolve_ipv4(addr.ip, dest.sin_addr)) {
        return -1;
    }
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return -1;
    }
    TcpEndpoint::set_nonblocking(fd, true);
    apply_socket_options(fd);
    int rc = ::connect(fd, reinterpret_cast<struct sockaddr*>(&dest), sizeof(dest));
    if (rc != 0 && errno != EINPROGRESS && errno != EALREADY) {
        ::close(fd);
        return -1;
    }
    while (rc != 0) {
        const long budget = std::min<long>(wait_slice_ms(), deadline_ms - monotonic_ms());
        if (budget < 0) {
            ::close(fd);
            return -1;
        }
        struct pollfd pfd{fd, POLLOUT, 0};
        const int pr = ::poll(&pfd, 1, static_cast<int>(budget));
        if (pr < 0) {
            if (errno == EINTR) {
                continue;
            }
            ::close(fd);
            return -1;
        }
        if (pr == 0) {
            if (monotonic_ms() >= deadline_ms) {
                ::close(fd);
                return -1;
            }
            continue;
        }
        int err = 0;
        socklen_t len = sizeof(err);
        if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len) < 0 || err != 0) {
            // Refused or unreachable: the entry that produced this address may be stale, so
            // the caller re-resolves rather than retrying the same endpoint.
            ::close(fd);
            return -1;
        }
        break;
    }
    return fd;
}

bool FMI::Comm::DrainTCP::exchange_hello_as_dialer(Utils::peer_num peer, int fd,
                                                   std::uint64_t nonce, long deadline_ms) {
    ResumeRecord mine;
    mine.sender_rank = static_cast<std::uint32_t>(peer_id);
    mine.receiver_rank = static_cast<std::uint32_t>(peer);
    mine.link_name_hash = TcpEndpoint::fnv1a64(link_name(peer));
    mine.nonce = nonce;
    mine.sender_incarnation = local_incarnation.load(std::memory_order_acquire);
    {
        // The counters cannot move while this runs: the application thread reaches the socket
        // only through a filed fd, and this link has none until file_link() below.
        LinkState& l = link(peer);
        std::lock_guard<std::mutex> lock(l.mu);
        mine.bytes_sent = l.bytes_sent;
        mine.bytes_received = l.bytes_received;
    }

    char wire[resume_record_bytes];
    encode_resume(mine, wire);
    if (!move_exact(fd, wire, resume_record_bytes, true, deadline_ms, wait_slice_ms(), &stopping)) {
        return false;
    }
    char reply[resume_record_bytes];
    if (!move_exact(fd, reply, resume_record_bytes, false, deadline_ms, wait_slice_ms(), &stopping)) {
        return false;
    }
    ResumeRecord theirs;
    if (!decode_resume(reply, theirs)) {
        return false;
    }
    if (theirs.receiver_rank != peer_id || theirs.sender_rank != peer) {
        return false;
    }
    // The hash embeds comm_name, so a listener belonging to a different communicator that
    // happens to hold this ephemeral port cannot pass itself off as the peer.
    if (theirs.link_name_hash != mine.link_name_hash) {
        return false;
    }
    // The acceptor proves nothing with a nonce of its own; it answers with zero.
    if (theirs.nonce != 0) {
        return false;
    }

    LinkState& l = link(peer);
    {
        std::lock_guard<std::mutex> lock(l.mu);
        if (theirs.sender_incarnation < l.peer_incarnation) {
            // A superseded lineage: this is the process that was replaced, not the one that
            // replaced it, and adopting its stream would rewind the link.
            l.unrecoverable = "answered by peer incarnation " +
                              std::to_string(theirs.sender_incarnation) + ", below the " +
                              std::to_string(l.peer_incarnation) + " already seen";
            l.back_up.notify_all();
            throw std::runtime_error("DrainTCP: rank " + std::to_string(peer_id) +
                                     ": peer " + std::to_string(peer) + " " + l.unrecoverable);
        }
        if (theirs.bytes_sent != mine.bytes_received || theirs.bytes_received != mine.bytes_sent) {
            // The whole point of carrying counters on a headerless stream: a byte lost or
            // duplicated across a cut fails here, naming all four counts, instead of
            // desynchronising the stream silently and permanently.
            l.unrecoverable = "counter mismatch at reconnect";
            l.back_up.notify_all();
            throw std::runtime_error(
                    "DrainTCP: rank " + std::to_string(peer_id) + " <-> peer " +
                    std::to_string(peer) + ": stream counters disagree at reconnect (peer sent " +
                    std::to_string(theirs.bytes_sent) + ", we received " +
                    std::to_string(mine.bytes_received) + "; peer received " +
                    std::to_string(theirs.bytes_received) + ", we sent " +
                    std::to_string(mine.bytes_sent) + ")");
        }
    }

    TcpEndpoint::set_nonblocking(fd, false);
    apply_socket_options(fd);
    file_link(peer, fd, theirs.sender_incarnation);
    return true;
}

bool FMI::Comm::DrainTCP::exchange_hello_as_acceptor(int fd) {
    const long deadline = monotonic_ms() + hello_read_budget_ms;
    char wire[resume_record_bytes];
    if (!move_exact(fd, wire, resume_record_bytes, false, deadline, wait_slice_ms(), &stopping)) {
        return false;
    }
    ResumeRecord theirs;
    if (!decode_resume(wire, theirs)) {
        return false;
    }
    if (theirs.receiver_rank != peer_id) {
        return false;
    }
    if (theirs.sender_rank >= num_peers || theirs.sender_rank == peer_id) {
        return false;
    }
    const auto sender = static_cast<Utils::peer_num>(theirs.sender_rank);
    if (theirs.link_name_hash != TcpEndpoint::fnv1a64(link_name(sender))) {
        return false;
    }
    // Checked before anything is compared against link state, and long before any existing
    // connection is replaced: a dialer echoing a nonce this listener never advertised reached a
    // recycled port, and nothing it says is about this link.
    if (theirs.nonce != listener_nonce) {
        return false;
    }

    ResumeRecord mine;
    mine.sender_rank = static_cast<std::uint32_t>(peer_id);
    mine.receiver_rank = theirs.sender_rank;
    mine.link_name_hash = theirs.link_name_hash;
    mine.nonce = 0;
    mine.sender_incarnation = local_incarnation.load(std::memory_order_acquire);

    LinkState& l = link(sender);
    {
        std::lock_guard<std::mutex> lock(l.mu);
        mine.bytes_sent = l.bytes_sent;
        mine.bytes_received = l.bytes_received;
        if (theirs.sender_incarnation < l.peer_incarnation ||
            theirs.bytes_sent != mine.bytes_received ||
            theirs.bytes_received != mine.bytes_sent) {
            // Recorded, never thrown: this is the control thread, and a throw here would
            // terminate the process while application threads are parked on this very link.
            l.unrecoverable = "refused a hello from peer " + std::to_string(sender) +
                              " (incarnation " + std::to_string(theirs.sender_incarnation) +
                              " vs " + std::to_string(l.peer_incarnation) + "; peer sent " +
                              std::to_string(theirs.bytes_sent) + ", we received " +
                              std::to_string(mine.bytes_received) + "; peer received " +
                              std::to_string(theirs.bytes_received) + ", we sent " +
                              std::to_string(mine.bytes_sent) + ")";
            l.back_up.notify_all();
            return false;
        }
    }

    char reply[resume_record_bytes];
    encode_resume(mine, reply);
    if (!move_exact(fd, reply, resume_record_bytes, true, monotonic_ms() + hello_read_budget_ms,
                    wait_slice_ms(), &stopping)) {
        return false;
    }
    TcpEndpoint::set_nonblocking(fd, false);
    apply_socket_options(fd);
    file_link(sender, fd, theirs.sender_incarnation);
    return true;
}

void FMI::Comm::DrainTCP::file_link(Utils::peer_num peer, int fd, std::uint64_t peer_incarnation) {
    LinkState& l = link(peer);
    std::lock_guard<std::mutex> lock(l.mu);
    if (l.fd >= 0) {
        ::close(l.fd);
    }
    l.fd = fd;
    ++l.generation;
    l.peer_incarnation = peer_incarnation;
    // Wake whatever is parked on this link now, rather than at the next slice expiry: a data
    // path that sleeps out its slice after its link is back is the reference implementation's
    // reject-stall, one layer down.
    l.back_up.notify_all();
}

FMI::Comm::DrainTCP::AcceptResult FMI::Comm::DrainTCP::accept_one() {
    const int fd = ::accept(listen_fd, nullptr, nullptr);
    if (fd < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return AcceptResult::Empty;
        }
        return AcceptResult::Rejected;   // EINTR, ECONNABORTED, EMFILE: there may be more behind
    }
    TcpEndpoint::set_nonblocking(fd, true);
    apply_socket_options(fd);
    if (!exchange_hello_as_acceptor(fd)) {
        // Closing is the refusal: the dialer reads EOF and resolves the peer's address again.
        ::close(fd);
        return AcceptResult::Rejected;
    }
    return AcceptResult::Accepted;
}

void FMI::Comm::DrainTCP::housekeeping() {
    // Never force: this refreshes an entry that is about to lapse, and a rank whose first
    // establishment has not happened yet has nothing to advertise that anyone wants.
    if (published_at_ms.load(std::memory_order_acquire) < 0) {
        return;
    }
    try_publish(monotonic_ms() + control_housekeeping_tick_ms, false);
}

void FMI::Comm::DrainTCP::control_loop() {
    while (!stopping.load(std::memory_order_acquire)) {
        struct pollfd pfds[2] = {{listen_fd, POLLIN, 0}, {wake_pipe[0], POLLIN, 0}};
        const int pr = ::poll(pfds, 2, control_housekeeping_tick_ms);
        if (stopping.load(std::memory_order_acquire)) {
            break;
        }
        if (pr < 0) {
            if (errno == EINTR) {
                continue;
            }
            // The listener is gone in a way polling cannot describe. Leaving is right: the
            // application threads' own deadlines report it, and spinning here would not.
            break;
        }
        if ((pfds[1].revents & POLLIN) != 0) {
            char drain[64];
            while (::read(wake_pipe[0], drain, sizeof(drain)) > 0) {
            }
        }
        if ((pfds[0].revents & POLLIN) != 0) {
            for (int taken = 0; taken < accept_batch; taken++) {
                AcceptResult result = AcceptResult::Empty;
                try {
                    result = accept_one();
                } catch (const std::exception&) {
                    // Nothing above this frame can report it, and the link's own state carries
                    // whatever the application needs to hear.
                    break;
                }
                if (result == AcceptResult::Empty || stopping.load(std::memory_order_acquire)) {
                    break;
                }
            }
        }
        try {
            housekeeping();
        } catch (const std::exception&) {
        }
    }
}

void FMI::Comm::DrainTCP::stop_control_thread() {
    if (!control_thread.joinable()) {
        return;
    }
    stopping.store(true, std::memory_order_release);
    if (wake_pipe[1] >= 0) {
        const char byte = 1;
        while (::write(wake_pipe[1], &byte, 1) < 0 && errno == EINTR) {
        }
    }
    control_thread.join();
}

void FMI::Comm::DrainTCP::ensure_link(Utils::peer_num peer) {
    if (peer == peer_id) {
        throw std::runtime_error("DrainTCP: rank " + std::to_string(peer_id) +
                                 " cannot establish a link to itself");
    }
    LinkState& l = link(peer);   // bounds-checked; peer >= num_peers throws here
    {
        std::lock_guard<std::mutex> lock(l.mu);
        throw_if_unrecoverable(l, peer);
        // A draining link is not established here — the data path parks on it and the
        // migration sequence owns its repair.
        if (l.fd >= 0 || l.draining) {
            return;
        }
    }
    // Shared for the whole establishment. Stage 2's drain takes it exclusively, which is how
    // "no connection may be built while this rank is being cut" stops being a comment.
    std::shared_lock<std::shared_mutex> gate(establish_gate);
    {
        std::lock_guard<std::mutex> lock(l.mu);
        throw_if_unrecoverable(l, peer);
        if (l.fd >= 0 || l.draining) {
            return;
        }
    }
    const long deadline = monotonic_ms() + static_cast<long>(max_timeout);
    // Rank order decides direction, so both ends agree without negotiating: a rank dials every
    // peer below it and accepts from every peer above it.
    if (peer < peer_id) {
        dial_peer(peer, deadline);
    } else {
        await_peer(peer, deadline);
    }
}

void FMI::Comm::DrainTCP::dial_peer(Utils::peer_num peer, long deadline_ms) {
    LinkState& l = link(peer);
    bool published = false;
    long next_publish = 0;
    long next_attempt = 0;

    while (true) {
        {
            std::lock_guard<std::mutex> lock(l.mu);
            throw_if_unrecoverable(l, peer);
            if (l.fd >= 0 || l.draining) {
                return;   // the control thread accepted a dial that crossed ours
            }
        }
        long now = monotonic_ms();
        if (now >= deadline_ms) {
            throw Utils::Timeout();
        }
        if (!published && now >= next_publish) {
            // A dialer publishes too: its own higher-ranked peers find it only this way.
            published = try_publish(deadline_ms, true);
            if (!published) {
                next_publish = monotonic_ms() + static_cast<long>(registry_poll_interval_ms);
            }
        }
        now = monotonic_ms();
        if (now < next_attempt) {
            std::this_thread::sleep_for(std::chrono::milliseconds(
                    std::min<long>(next_attempt - now, wait_slice_ms())));
            continue;
        }

        PeerAddr addr;
        if (!lookup_peer(peer, addr, deadline_ms)) {
            // Not published yet, or the registry is briefly away. Either way this is a
            // registry question and registry_poll_interval_ms is what answers it.
            next_attempt = monotonic_ms() + static_cast<long>(registry_poll_interval_ms);
            continue;
        }
        const int fd = connect_to(addr, deadline_ms);
        if (fd < 0) {
            next_attempt = monotonic_ms() + static_cast<long>(connect_retry_interval_ms);
            continue;
        }
        if (!exchange_hello_as_dialer(peer, fd, addr.nonce, deadline_ms)) {
            ::close(fd);
            next_attempt = monotonic_ms() + static_cast<long>(connect_retry_interval_ms);
            continue;
        }
        return;
    }
}

void FMI::Comm::DrainTCP::await_peer(Utils::peer_num peer, long deadline_ms) {
    LinkState& l = link(peer);
    bool published = false;
    long next_publish = 0;

    while (true) {
        long now = monotonic_ms();
        if (now >= deadline_ms) {
            throw Utils::Timeout();
        }
        if (!published && now >= next_publish) {
            // The only thing this rank owes a higher peer is a discoverable address; its
            // listener is already up and its control thread is already accepting.
            published = try_publish(deadline_ms, true);
            if (!published) {
                next_publish = monotonic_ms() + static_cast<long>(registry_poll_interval_ms);
            }
        }
        std::unique_lock<std::mutex> lock(l.mu);
        throw_if_unrecoverable(l, peer);
        if (l.fd >= 0 || l.draining) {
            return;
        }
        l.back_up.wait_for(lock, std::chrono::milliseconds(wait_slice_ms()));
    }
}

void FMI::Comm::DrainTCP::wait_for_link(std::unique_lock<std::mutex>& lock, LinkState& l,
                                        Utils::peer_num peer, OperationClock& clock) {
    const long before = monotonic_ms();
    const bool suspended = l.draining;
    if (suspended) {
        const long elapsed = before - l.draining_since_ms;
        if (elapsed >= migration_max_ms) {
            throw std::runtime_error("DrainTCP: rank " + std::to_string(peer_id) + ": peer " +
                                     std::to_string(peer) + " has been migrating for " +
                                     std::to_string(elapsed) +
                                     " ms, past migration_max_ms=" +
                                     std::to_string(migration_max_ms));
        }
    } else if (clock.expired(before)) {
        throw Utils::Timeout();
    }
    // wait_for releases the mutex for the whole wait, which is the property that matters: no
    // wait in this channel ever happens with a link lock held.
    l.back_up.wait_for(lock, std::chrono::milliseconds(wait_slice_ms()));
    if (suspended) {
        clock.suspended_ms += monotonic_ms() - before;
    }
}

void FMI::Comm::DrainTCP::send_object(channel_data buf, Utils::peer_num rcpt_id) {
    ensure_started();
    ensure_link(rcpt_id);
    LinkState& l = link(rcpt_id);
    OperationClock clock{monotonic_ms(), static_cast<long>(max_timeout), 0};
    std::size_t moved = 0;

    while (moved < buf.len) {
        int wait_fd = -1;
        {
            std::unique_lock<std::mutex> lock(l.mu);
            throw_if_unrecoverable(l, rcpt_id);
            if (l.fd < 0 || l.draining) {
                wait_for_link(lock, l, rcpt_id, clock);
                continue;
            }
            // Exactly one non-blocking syscall per acquisition of this lock. Everything that
            // could wait happens below, with the lock released.
            const ssize_t n = ::send(l.fd, buf.buf + moved, buf.len - moved,
                                     MSG_NOSIGNAL | MSG_DONTWAIT);
            if (n > 0) {
                moved += static_cast<std::size_t>(n);
                l.bytes_sent += static_cast<std::uint64_t>(n);
                continue;
            }
            const int err = n < 0 ? errno : 0;
            if (err == EINTR) {
                continue;
            }
            if (err == EAGAIN || err == EWOULDBLOCK) {
                wait_fd = l.fd;
            } else if (moved == 0 && (err == EPIPE || err == ECONNRESET)) {
                // A peer that tore the connection down before a single byte of this message
                // went out has abandoned the collective, which is what Timeout means. Past the
                // first byte the same condition is truncation and must be loud.
                throw Utils::Timeout();
            } else {
                throw std::runtime_error(
                        "DrainTCP: rank " + std::to_string(peer_id) + " -> peer " +
                        std::to_string(rcpt_id) + ": connection died after " +
                        std::to_string(moved) + "/" + std::to_string(buf.len) + " bytes: " +
                        (n == 0 ? "send moved nothing" : strerror(err)) +
                        " (an unplanned death is not recoverable in the drain protocol)");
            }
        }
        if (clock.expired(monotonic_ms())) {
            throw Utils::Timeout();
        }
        // Polling a descriptor read under the lock and released since: the generation check on
        // the next acquisition is what makes a replacement visible, and the slice bounds how
        // long a stale wait can last.
        struct pollfd pfd{wait_fd, POLLOUT, 0};
        ::poll(&pfd, 1, wait_slice_ms());
    }
}

void FMI::Comm::DrainTCP::recv_object(channel_data buf, Utils::peer_num sender_id) {
    ensure_started();
    ensure_link(sender_id);
    LinkState& l = link(sender_id);
    OperationClock clock{monotonic_ms(), static_cast<long>(max_timeout), 0};
    std::size_t moved = 0;

    while (moved < buf.len) {
        int wait_fd = -1;
        {
            std::unique_lock<std::mutex> lock(l.mu);
            throw_if_unrecoverable(l, sender_id);
            if (l.inbound_at < l.inbound.size()) {
                // Drained bytes first, always: they were moved into user memory before the
                // socket they arrived on was closed, and the connection that replaced it
                // carries only what the peer sent afterwards. Reading the socket first would
                // deliver the stream out of order.
                const std::size_t take = std::min(buf.len - moved, l.inbound.size() - l.inbound_at);
                std::memcpy(buf.buf + moved, l.inbound.data() + l.inbound_at, take);
                l.inbound_at += take;
                moved += take;
                if (l.inbound_at == l.inbound.size()) {
                    // Freed the moment it is spent; a per-link buffer that is only ever
                    // appended to is how the reference implementation grew without bound.
                    std::vector<char>().swap(l.inbound);
                    l.inbound_at = 0;
                }
                continue;
            }
            if (l.fd < 0 || l.draining) {
                wait_for_link(lock, l, sender_id, clock);
                continue;
            }
            const ssize_t n = ::recv(l.fd, buf.buf + moved, buf.len - moved, MSG_DONTWAIT);
            if (n > 0) {
                moved += static_cast<std::size_t>(n);
                // Advanced only once the bytes are in the application's buffer, never at any
                // earlier point: the counter is what a reconnect cross-checks, so it must
                // describe what this rank has actually taken delivery of.
                l.bytes_received += static_cast<std::uint64_t>(n);
                continue;
            }
            if (n == 0) {
                if (moved == 0) {
                    throw Utils::Timeout();   // abandonment, as in send_object
                }
                throw std::runtime_error(
                        "DrainTCP: rank " + std::to_string(peer_id) + " <- peer " +
                        std::to_string(sender_id) + ": connection closed after " +
                        std::to_string(moved) + "/" + std::to_string(buf.len) +
                        " bytes (an unplanned death is not recoverable in the drain protocol)");
            }
            const int err = errno;
            if (err == EINTR) {
                continue;
            }
            if (err == EAGAIN || err == EWOULDBLOCK) {
                wait_fd = l.fd;
            } else if (moved == 0 && err == ECONNRESET) {
                throw Utils::Timeout();
            } else {
                throw std::runtime_error(
                        "DrainTCP: rank " + std::to_string(peer_id) + " <- peer " +
                        std::to_string(sender_id) + ": connection died after " +
                        std::to_string(moved) + "/" + std::to_string(buf.len) + " bytes: " +
                        strerror(err) +
                        " (an unplanned death is not recoverable in the drain protocol)");
            }
        }
        if (clock.expired(monotonic_ms())) {
            throw Utils::Timeout();
        }
        struct pollfd pfd{wait_fd, POLLIN, 0};
        ::poll(&pfd, 1, wait_slice_ms());
    }
}

double FMI::Comm::DrainTCP::get_latency(Utils::peer_num producer, Utils::peer_num consumer,
                                        std::size_t size_in_bytes) {
    const double agg_bandwidth = bandwidth;
    const double trans_time =
            producer * consumer * ((double) size_in_bytes / 1000000.) / agg_bandwidth;
    return log2(producer + consumer) * overhead + trans_time;
}

double FMI::Comm::DrainTCP::get_price(Utils::peer_num producer, Utils::peer_num consumer,
                                      std::size_t size_in_bytes) {
    double total_costs =
            2 * consumer * producer * ((double) size_in_bytes / 1000000000.) * transfer_price;
    if (include_infrastructure_costs) {
        total_costs += 1. / requests_per_hour * vm_price;
    }
    return total_costs;
}

void FMI::Comm::DrainTCP::finalize() {
    std::lock_guard<std::mutex> lifecycle(lifecycle_mutex);
    if (finalized) {
        return;
    }
    finalized = true;
    started.store(false, std::memory_order_release);

    // First, so nothing can file a link or touch a descriptor after this point.
    stop_control_thread();

    if (listen_fd >= 0) {
        ::close(listen_fd);
        listen_fd = -1;
    }
    listen_port = 0;
    listener_nonce = 0;
    // Cleared so a channel that is ever resumed elsewhere cannot keep advertising a port it no
    // longer holds and an address that is no longer its own.
    advertised_ip.clear();
    for (int& end : wake_pipe) {
        if (end >= 0) {
            ::close(end);
            end = -1;
        }
    }
    for (auto& state : links) {
        if (state == nullptr) {
            continue;
        }
        std::lock_guard<std::mutex> lock(state->mu);
        if (state->fd >= 0) {
            // A plain close, the same abrupt goodbye raw DirectTCP gives: this transport keeps
            // no retention that a FIN would have to settle first.
            ::close(state->fd);
            state->fd = -1;
        }
        state->back_up.notify_all();
    }
    if (registry) {
        registry->disconnect();
    }
}

void FMI::Comm::DrainTCP::quiesce_and_drain(const std::vector<Utils::peer_num>&) {
    throw std::logic_error("DrainTCP::quiesce_and_drain: not yet implemented (Stage 2)");
}

void FMI::Comm::DrainTCP::release_transport_sockets() {
    throw std::logic_error("DrainTCP::release_transport_sockets: not yet implemented (Stage 2)");
}

void FMI::Comm::DrainTCP::resume_after_restore() noexcept {
    // noexcept by contract, so this stage's placeholder has to be a no-op rather than a throw.
    // Nothing calls it yet: the migration runtime that would is Stage 2.
}

void FMI::Comm::DrainTCP::peer_is_leaving(Utils::peer_num, std::uint64_t) {
    throw std::logic_error("DrainTCP::peer_is_leaving: not yet implemented (Stage 2)");
}
