#include "../../include/comm/DrainTCP.h"

#include "../../include/comm/DrainCoordinator.h"
#include "../../include/comm/PeerRegistry.h"
#include "../../include/comm/TcpEndpoint.h"
#include "../../include/utils/MigrationTrigger.h"

#include <boost/log/trivial.hpp>

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

    //! Read size of the drain. Large enough that a socket buffer empties in a syscall or two,
    //! small enough that the buffer is a stack-free allocation nobody notices.
    constexpr std::size_t drain_chunk_bytes = 64u * 1024u;

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
    establish_yield_ms = std::stol(TcpEndpoint::param_or(params, "establish_yield_ms", "2000"));
    batch_lease_ms = std::stol(TcpEndpoint::param_or(params, "batch_lease_ms", "120000"));
    // Spelled out in the config rather than inferred from anything: turning a real CRIU stop
    // into an in-place restore is exactly the difference between a dry run and a migration, and
    // it must never be something a job acquires by accident.
    rehearsal_only = TcpEndpoint::param_or(params, "drain_rehearsal_only", "false") == "true";

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
        listen_fd.store(fd, std::memory_order_release);
        listen_port = ntohs(addr.sin_port);
        listener_nonce = TcpEndpoint::random_nonce();
        advertised_ip = TcpEndpoint::resolve_advertise_ip(advertise_host, registry_host,
                                                          registry_port);
        wake_pipe[0] = pipe_fds[0];
        wake_pipe[1] = pipe_fds[1];
        stopping.store(false, std::memory_order_release);
        control_pause_request.store(false, std::memory_order_release);
        control_parked.store(false, std::memory_order_release);
        // After this point the link table is read by two threads and must never be resized.
        // Starting the thread here is what publishes it safely: the launch is the release.
        control_thread = std::thread(&DrainTCP::control_loop, this);
        started.store(true, std::memory_order_release);

        if (drain_enabled) {
            // The arming point, and the first instant at which it is possible: a coordinator
            // needs the communicator's name, and a member record needs the port and nonce the
            // listener above has only just been given.
            if (coordinator == nullptr) {
                coordinator = std::make_unique<RedisDrainCoordinator>(
                        registry_host, registry_port, comm_name, registry_ttl_s,
                        static_cast<long>(max_timeout));
            }
            // From the tail, not from the beginning: events older than this rank's arrival
            // describe a job it was not part of, and replaying them would drain links against
            // migrations that finished before it existed.
            last_stream_id = coordinator->tail_id();
            coordinator->publish_member(peer_id, MemberRecord{
                    Utils::MigrationTrigger::instance().epoch(), advertised_ip, listen_port,
                    listener_nonce});
            if (trigger != "none") {
                Utils::TriggerConfig trigger_config;
                trigger_config.signal_offset = drain_signal_offset;
                trigger_config.control_poll_interval_ms = control_poll_interval_ms;
                trigger_config.mode = trigger;
                trigger_config.rehearsal_only = rehearsal_only;
                Utils::MigrationTrigger::instance().attach(this, trigger_config);
                attached_to_trigger = true;
            }
        }
    } catch (...) {
        if (attached_to_trigger) {
            Utils::MigrationTrigger::instance().detach(this);
            attached_to_trigger = false;
        }
        started.store(false, std::memory_order_release);
        stop_control_thread();
        undo();
        links.clear();
        listen_fd.store(-1, std::memory_order_release);
        listen_port = 0;
        listener_nonce = 0;
        advertised_ip.clear();
        wake_pipe[0] = -1;
        wake_pipe[1] = -1;
        throw;
    }
}

void FMI::Comm::DrainTCP::set_coordinator_for_testing(std::unique_ptr<DrainCoordinator> replacement) {
    std::lock_guard<std::mutex> lifecycle(lifecycle_mutex);
    if (started.load(std::memory_order_acquire)) {
        throw std::logic_error("DrainTCP::set_coordinator_for_testing: the channel has already "
                               "armed; a control plane cannot be swapped underneath it");
    }
    coordinator = std::move(replacement);
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
    const int listener = listen_fd.load(std::memory_order_acquire);
    if (listener < 0) {
        // The migration sequence took the listener away between the poll and here. Nothing is
        // pending on a listener that does not exist.
        return AcceptResult::Empty;
    }
    const int fd = ::accept(listener, nullptr, nullptr);
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
    if (migration_active.load(std::memory_order_acquire)) {
        // A republish would dial the registry, and this rank has just told the world it owns no
        // sockets at all. The entry it would refresh is also about to be replaced wholesale by
        // the restore leg.
        return;
    }
    try_publish(monotonic_ms() + control_housekeeping_tick_ms, false);
}

void FMI::Comm::DrainTCP::pause_control_thread(long deadline_ms) {
    if (!control_thread.joinable()) {
        return;
    }
    control_pause_request.store(true, std::memory_order_release);
    if (wake_pipe[1] >= 0) {
        const char byte = 1;
        while (::write(wake_pipe[1], &byte, 1) < 0 && errno == EINTR) {
        }
    }
    std::unique_lock<std::mutex> lock(control_pause_mutex);
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(std::max<long>(deadline_ms - monotonic_ms(), 1));
    if (!control_pause_cv.wait_until(lock, deadline, [this]() {
            return control_parked.load(std::memory_order_acquire) ||
                   stopping.load(std::memory_order_acquire);
        })) {
        // It is still holding something — most likely an accepted connection mid-hello. Dumping
        // now would capture a socket this rank has promised does not exist, so say so instead.
        throw std::runtime_error("DrainTCP: rank " + std::to_string(peer_id) +
                                 ": the control thread did not park within " +
                                 std::to_string(drain_grace_ms) +
                                 " ms; the migration cannot promise a socket-free image");
    }
}

void FMI::Comm::DrainTCP::resume_control_thread() {
    control_pause_request.store(false, std::memory_order_release);
    if (wake_pipe[1] >= 0) {
        const char byte = 1;
        while (::write(wake_pipe[1], &byte, 1) < 0 && errno == EINTR) {
        }
    }
}

void FMI::Comm::DrainTCP::control_loop() {
    while (!stopping.load(std::memory_order_acquire)) {
        if (control_pause_request.load(std::memory_order_acquire)) {
            // Parked where it owns nothing: no listener, no half-accepted connection, only the
            // wake pipe — which is a pipe and survives a checkpoint on any host.
            {
                std::lock_guard<std::mutex> lock(control_pause_mutex);
                control_parked.store(true, std::memory_order_release);
            }
            control_pause_cv.notify_all();
            while (control_pause_request.load(std::memory_order_acquire) &&
                   !stopping.load(std::memory_order_acquire)) {
                struct pollfd pfd{wake_pipe[0], POLLIN, 0};
                ::poll(&pfd, 1, control_housekeeping_tick_ms);
                char drain[64];
                while (::read(wake_pipe[0], drain, sizeof(drain)) > 0) {
                }
            }
            {
                std::lock_guard<std::mutex> lock(control_pause_mutex);
                control_parked.store(false, std::memory_order_release);
            }
            control_pause_cv.notify_all();
            continue;
        }
        struct pollfd pfds[2] = {{listen_fd.load(std::memory_order_acquire), POLLIN, 0},
                                 {wake_pipe[0], POLLIN, 0}};
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

void FMI::Comm::DrainTCP::ensure_link(Utils::peer_num peer, long deadline_ms) {
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
    // Shared for the whole establishment; a drain takes it exclusively, which is how "no
    // connection may be built while this rank is being cut" stops being a comment.
    std::shared_lock<std::shared_timed_mutex> gate(establish_gate);
    {
        std::lock_guard<std::mutex> lock(l.mu);
        throw_if_unrecoverable(l, peer);
        if (l.fd >= 0 || l.draining) {
            return;
        }
    }
    // Rank order decides direction, so both ends agree without negotiating: a rank dials every
    // peer below it and accepts from every peer above it.
    if (peer < peer_id) {
        dial_peer(peer, deadline_ms);
    } else {
        await_peer(peer, deadline_ms);
    }
}

bool FMI::Comm::DrainTCP::yield_establishment_to_drain(Utils::peer_num peer) {
    if (!drain_pending.load(std::memory_order_acquire)) {
        return false;
    }
    // This rank's own migration is waiting for the gate this establishment holds. Letting go
    // here — at a retry boundary, between two attempts, owning no half-built connection — is
    // what keeps the drain's bound a bound instead of the establishment's deadline.
    LinkState& l = link(peer);
    std::lock_guard<std::mutex> lock(l.mu);
    if (!l.draining) {
        l.draining = true;
        l.draining_since_ms = monotonic_ms();
    }
    l.back_up.notify_all();
    return true;
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
        if (yield_establishment_to_drain(peer)) {
            return;
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
        if (yield_establishment_to_drain(peer)) {
            return;
        }
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
        if (l.drain_unconfirmed && elapsed >= drain_grace_ms) {
            // The data path guessed a migration from a FIN, and no leave notice followed within
            // the grace period. That is what an unplanned death looks like from here, and this
            // protocol does not recover from one — so it is named rather than waited out to the
            // much longer migration bound.
            throw std::runtime_error(
                    "DrainTCP: rank " + std::to_string(peer_id) + " <-> peer " +
                    std::to_string(peer) + ": the connection died " + std::to_string(elapsed) +
                    " ms ago and no migration notice followed within drain_grace_ms=" +
                    std::to_string(drain_grace_ms) +
                    " (an unplanned death is not recoverable in the drain protocol)");
        }
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
    OperationClock clock{monotonic_ms(), static_cast<long>(max_timeout), 0};
    ensure_link(rcpt_id, clock.deadline(monotonic_ms()));
    LinkState& l = link(rcpt_id);
    std::size_t moved = 0;

    while (moved < buf.len) {
        int wait_fd = -1;
        {
            std::unique_lock<std::mutex> lock(l.mu);
            throw_if_unrecoverable(l, rcpt_id);
            if (l.draining) {
                wait_for_link(lock, l, rcpt_id, clock);
                continue;
            }
            if (l.fd < 0) {
                // Re-established from here rather than waited for: after a migration the link
                // is deliberately left closed, and this thread — parked at its offset with the
                // rest of the message still to send — is the one that rebuilds it.
                lock.unlock();
                ensure_link(rcpt_id, clock.deadline(monotonic_ms()));
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
            } else if (drain_armed() && (err == EPIPE || err == ECONNRESET)) {
                // With drain armed a FIN in the middle of an operation is more often a
                // neighbour beginning its migration than a death: the peer's leave notice and
                // its FIN travel over different media and neither is reliably first. So this is
                // recorded as a migration this rank has not been told about yet, and the wait
                // helper turns it into the loud unplanned-death error if no notice follows
                // within drain_grace_ms.
                begin_draining_locked(l, true);
                continue;
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
    OperationClock clock{monotonic_ms(), static_cast<long>(max_timeout), 0};
    ensure_link(sender_id, clock.deadline(monotonic_ms()));
    LinkState& l = link(sender_id);
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
            if (l.draining) {
                wait_for_link(lock, l, sender_id, clock);
                continue;
            }
            if (l.fd < 0) {
                // As in send_object: the thread that wants the link is the thread that rebuilds
                // it, so a message parked across a migration resumes at its offset instead of
                // waiting for a dial nobody was going to make.
                lock.unlock();
                ensure_link(sender_id, clock.deadline(monotonic_ms()));
                continue;
            }
            const ssize_t n = ::recv(l.fd, buf.buf + moved, buf.len - moved, MSG_DONTWAIT);
            if (n > 0) {
                moved += static_cast<std::size_t>(n);
                // Advanced only once the bytes are in the application's buffer, never at any
                // earlier point: the counter is what a reconnect cross-checks, so it must
                // describe what this rank has actually taken delivery of. A drain advances it
                // too, for the same reason — bytes it moves into `inbound` are in user memory
                // and have been taken delivery of, they are simply not consumed yet.
                l.bytes_received += static_cast<std::uint64_t>(n);
                continue;
            }
            if (n == 0) {
                if (drain_armed()) {
                    begin_draining_locked(l, true);
                    continue;
                }
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
            } else if (drain_armed() && (err == ECONNRESET || err == EPIPE)) {
                begin_draining_locked(l, true);
                continue;
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
    // Before the lifecycle lock, never under it: detaching joins the migration runtime's
    // trigger thread, and that thread runs the restore leg, which needs this very lock. A
    // finalize that held it while waiting for the join would deadlock against a migration that
    // is halfway through coming back.
    if (attached_to_trigger) {
        Utils::MigrationTrigger::instance().detach(this);
        attached_to_trigger = false;
    }
    std::lock_guard<std::mutex> lifecycle(lifecycle_mutex);
    if (finalized) {
        return;
    }
    finalized = true;
    started.store(false, std::memory_order_release);

    // Nothing can be waiting on the pause handshake any more; make sure the control thread is
    // not parked in it, or its stop would have to wait out a tick that never comes.
    control_pause_request.store(false, std::memory_order_release);
    // First, so nothing can file a link or touch a descriptor after this point.
    stop_control_thread();

    const int listener = listen_fd.exchange(-1, std::memory_order_acq_rel);
    if (listener >= 0) {
        ::close(listener);
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
    if (coordinator) {
        try {
            // Its own entry only: a rank that leaves says so, and one that does not is left to
            // the TTL. Nothing here deletes another rank's record.
            coordinator->remove_member(peer_id);
        } catch (const std::exception& e) {
            BOOST_LOG_TRIVIAL(warning) << "DrainTCP: rank " << peer_id
                                       << " could not withdraw its member record: " << e.what();
        }
        coordinator->disconnect();
    }
    if (registry) {
        registry->disconnect();
    }
}

// ---------------------------------------------------------------------------------------------
// The migration sequence. Everything below runs on the migration runtime's trigger thread (or,
// for a rehearsal, on whichever thread asked for one) and never on an application thread.
// ---------------------------------------------------------------------------------------------

long FMI::Comm::DrainTCP::drain_budget_ms() const {
    // The quiet budget re-arms on progress, so the absolute cap is what stops a peer that
    // trickles bytes forever. It is the larger of the two numbers on purpose: a max_timeout
    // below one quiet budget would make drain_grace_ms unreachable and silently redefine the
    // seal's patience as the transport's.
    return std::max<long>(static_cast<long>(max_timeout), drain_grace_ms);
}

void FMI::Comm::DrainTCP::take_readable_locked(LinkState& l, int fd) {
    std::vector<char> chunk(drain_chunk_bytes);
    while (true) {
        const ssize_t n = ::recv(fd, chunk.data(), chunk.size(), MSG_DONTWAIT);
        if (n > 0) {
            l.inbound.insert(l.inbound.end(), chunk.data(), chunk.data() + n);
            l.bytes_received += static_cast<std::uint64_t>(n);
            continue;
        }
        if (n < 0 && errno == EINTR) {
            continue;
        }
        // EOF, EAGAIN or a reset: in all three there is nothing more the kernel is holding, and
        // this must not wait for anything to arrive.
        return;
    }
}

void FMI::Comm::DrainTCP::begin_draining_locked(LinkState& l, bool unconfirmed) {
    if (!l.draining) {
        l.draining = true;
        l.draining_since_ms = monotonic_ms();
    }
    if (unconfirmed) {
        l.drain_unconfirmed = true;
    }
    if (l.fd >= 0) {
        take_readable_locked(l, l.fd);
        // Closed rather than half-closed: this end has decided the connection is over, and the
        // FIN the close sends is what lets a migrating peer's own drain reach EOF.
        ::close(l.fd);
        l.fd = -1;
        ++l.generation;
    }
    l.back_up.notify_all();
}

void FMI::Comm::DrainTCP::break_every_link(const std::string& reason) {
    // A drain holds every link lock from the seal to the end of the restore leg; outside that
    // window it holds none. Both callers of this are on that one thread, so which of the two it
    // is decides whether the locks have to be taken here.
    const bool locks_held = !drain_locks.empty();
    for (auto& state : links) {
        if (state == nullptr) {
            continue;
        }
        if (locks_held) {
            state->unrecoverable = reason;
            state->back_up.notify_all();
        } else {
            std::lock_guard<std::mutex> lock(state->mu);
            state->unrecoverable = reason;
            state->back_up.notify_all();
        }
    }
}

bool FMI::Comm::DrainTCP::drain_one_locked(LinkState& l, int fd, Utils::peer_num, long deadline_ms) {
    std::vector<char> chunk(drain_chunk_bytes);
    long quiet_deadline = monotonic_ms() + drain_grace_ms;
    while (true) {
        const ssize_t n = ::recv(fd, chunk.data(), chunk.size(), MSG_DONTWAIT);
        if (n > 0) {
            // Appended at the END: these are bytes the application has not consumed, and the
            // receive path takes `inbound` before it ever reads a socket again, so the stream
            // stays in order across the cut.
            l.inbound.insert(l.inbound.end(), chunk.data(), chunk.data() + n);
            l.bytes_received += static_cast<std::uint64_t>(n);
            quiet_deadline = monotonic_ms() + drain_grace_ms;
            continue;
        }
        if (n == 0) {
            return true;   // EOF: the peer half-closed, and this is the clean seal
        }
        if (errno == EINTR) {
            continue;
        }
        if (errno != EAGAIN && errno != EWOULDBLOCK) {
            // A reset is not a seal: bytes may have been dropped, and this protocol's whole
            // claim is that none were.
            return false;
        }
        const long now = monotonic_ms();
        if (now >= quiet_deadline || now >= deadline_ms) {
            return false;
        }
        struct pollfd pfd{fd, POLLIN, 0};
        const long budget = std::min<long>({static_cast<long>(wait_slice_ms()),
                                            quiet_deadline - now, deadline_ms - now});
        ::poll(&pfd, 1, static_cast<int>(std::max<long>(budget, 0)));
    }
}

void FMI::Comm::DrainTCP::seal_links(std::vector<Utils::peer_num>& targets) {
    struct Pending {
        Utils::peer_num peer;
        int fd;
    };
    std::vector<Pending> pending;
    for (const auto peer : targets) {
        LinkState& l = link(peer);
        if (l.fd >= 0) {
            pending.push_back({peer, l.fd});
        }
    }
    // Every one of them half-closed before any of them is read. Never one link at a time: two
    // ranks migrating in the same batch are each other's peers, and each would wait for a FIN
    // the other has not sent because it is waiting for the first one's.
    for (const auto& p : pending) {
        ::shutdown(p.fd, SHUT_WR);
    }

    const long absolute_deadline = monotonic_ms() + drain_budget_ms();
    long quiet_deadline = monotonic_ms() + drain_grace_ms;
    std::vector<char> chunk(drain_chunk_bytes);
    std::vector<Pending> open = pending;
    std::vector<Utils::peer_num> stuck;

    while (!open.empty()) {
        bool progress = false;
        for (std::size_t i = 0; i < open.size();) {
            LinkState& l = link(open[i].peer);
            const ssize_t n = ::recv(open[i].fd, chunk.data(), chunk.size(), MSG_DONTWAIT);
            if (n > 0) {
                l.inbound.insert(l.inbound.end(), chunk.data(), chunk.data() + n);
                l.bytes_received += static_cast<std::uint64_t>(n);
                progress = true;
                continue;   // same descriptor again: it may have more queued
            }
            if (n == 0) {
                open.erase(open.begin() + static_cast<long>(i));
                progress = true;
                continue;
            }
            if (errno == EINTR) {
                continue;
            }
            if (errno != EAGAIN && errno != EWOULDBLOCK) {
                stuck.push_back(open[i].peer);
                open.erase(open.begin() + static_cast<long>(i));
                progress = true;
                continue;
            }
            i++;
        }
        if (open.empty()) {
            break;
        }
        const long now = monotonic_ms();
        if (progress) {
            quiet_deadline = now + drain_grace_ms;
        }
        if (now >= quiet_deadline || now >= absolute_deadline) {
            for (const auto& p : open) {
                stuck.push_back(p.peer);
            }
            break;
        }
        std::vector<struct pollfd> pfds;
        pfds.reserve(open.size());
        for (const auto& p : open) {
            pfds.push_back({p.fd, POLLIN, 0});
        }
        const long budget = std::min<long>({static_cast<long>(wait_slice_ms()),
                                            quiet_deadline - now, absolute_deadline - now});
        ::poll(pfds.data(), pfds.size(), static_cast<int>(std::max<long>(budget, 0)));
    }

    for (const auto& p : pending) {
        LinkState& l = link(p.peer);
        ::close(p.fd);
        l.fd = -1;
        ++l.generation;
    }
    if (!stuck.empty()) {
        std::string peers;
        for (const auto peer : stuck) {
            peers += (peers.empty() ? "" : ", ") + std::to_string(peer);
        }
        // Never conflated with EOF, and never followed by a stop: a rank that could not account
        // for every byte in flight must not be frozen, because nothing downstream would ever
        // find out that it was not sealed.
        throw std::runtime_error("DrainTCP: rank " + std::to_string(peer_id) +
                                 ": drain incomplete, peer(s) " + peers +
                                 " never closed their side within drain_grace_ms=" +
                                 std::to_string(drain_grace_ms) + " (cap " +
                                 std::to_string(drain_budget_ms()) +
                                 " ms); the migration is off and the channel is broken");
    }
}

std::unique_lock<std::shared_timed_mutex> FMI::Comm::DrainTCP::take_establish_gate() {
    std::unique_lock<std::shared_timed_mutex> gate(establish_gate, std::defer_lock);
    if (!gate.try_lock_for(std::chrono::milliseconds(std::max<long>(establish_yield_ms, 1)))) {
        throw std::runtime_error("DrainTCP: rank " + std::to_string(peer_id) +
                                 ": an establishment still holds the establish gate after " +
                                 std::to_string(establish_yield_ms) +
                                 " ms; the migration is off");
    }
    return gate;
}

void FMI::Comm::DrainTCP::quiesce_and_drain(const std::vector<Utils::peer_num>& targets) {
    ensure_started();
    // Held from here to the end of the restore leg, so a rehearsal running on an application
    // thread and the trigger thread's event dispatch cannot be inside this state at once.
    held_migration_lock = std::unique_lock<std::mutex>(migration_mutex);
    if (!drain_locks.empty()) {
        held_migration_lock = std::unique_lock<std::mutex>();
        throw std::logic_error("DrainTCP: rank " + std::to_string(peer_id) +
                               " is already sealed; a second migration cannot begin before the "
                               "first one has restored");
    }
    const std::uint64_t my_epoch = Utils::MigrationTrigger::instance().epoch();
    migration_active.store(true, std::memory_order_release);

    // 1. The establish gate, exclusively. An establishment in flight either finishes or lets go
    //    at its next retry boundary, which is what drain_pending tells it to do.
    drain_pending.store(true, std::memory_order_release);
    std::unique_lock<std::shared_timed_mutex> gate;
    try {
        gate = take_establish_gate();
    } catch (...) {
        drain_pending.store(false, std::memory_order_release);
        migration_active.store(false, std::memory_order_release);
        held_migration_lock = std::unique_lock<std::mutex>();
        throw;
    }
    drain_pending.store(false, std::memory_order_release);

    try {
        if (batch_id.empty()) {
            batch_id = comm_name + "|" + std::to_string(peer_id) + "@" + std::to_string(my_epoch);
        }
        if (coordinator != nullptr && !batch_lease_external && !holds_batch_lock) {
            // One batch in flight per communicator. A rank that asks for itself takes the lease;
            // a driver that asked for it holds the lease already and handed its own batch id
            // down, in which case this rank must not try to take what it is running under.
            if (!coordinator->try_batch_lock(batch_id, batch_lease_ms)) {
                throw std::runtime_error("DrainTCP: rank " + std::to_string(peer_id) +
                                         ": another migration holds the batch lease of '" +
                                         comm_name + "'; this one is off");
            }
            holds_batch_lock = true;
        }

        // 2. The leave notice, before a single socket is touched: it is what tells every peer to
        //    half-close, and this rank's drain finishes only once they have.
        if (coordinator != nullptr) {
            DrainEvent leaving;
            leaving.type = DrainEvent::Type::Leaving;
            leaving.rank = peer_id;
            leaving.epoch = my_epoch;
            leaving.batch = batch_id;
            // Which lineage of this rank is leaving. A survivor that reads this notice late —
            // after this rank has restored and dialled it again — needs it to tell the
            // migration it is being told about from the one that has already finished.
            leaving.extra["incarnation"] =
                    std::to_string(local_incarnation.load(std::memory_order_acquire));
            coordinator->emit(leaving);
        } else {
            BOOST_LOG_TRIVIAL(warning)
                    << "DrainTCP: rank " << peer_id
                    << " is draining with no control plane; no peer will be told to half-close "
                       "(this is only correct for a single-rank rehearsal)";
        }

        // 3. Stop accepting. The control thread parks where it owns no socket at all — not even
        //    a connection it has accepted but not yet greeted — and the listener goes with it.
        pause_control_thread(monotonic_ms() + drain_budget_ms());
        const int listener = listen_fd.exchange(-1, std::memory_order_acq_rel);
        if (listener >= 0) {
            ::close(listener);
        }
        listen_port = 0;
        listener_nonce = 0;
        published_at_ms.store(-1, std::memory_order_release);

        // 4. Every link lock, in ascending peer order, held from here through the stop.
        std::vector<Utils::peer_num> to_seal;
        if (targets.empty()) {
            for (Utils::peer_num p = 0; p < links.size(); p++) {
                if (p != peer_id) {
                    to_seal.push_back(p);
                }
            }
        } else {
            to_seal = targets;
            std::sort(to_seal.begin(), to_seal.end());
        }
        drain_locks.reserve(links.size());
        for (Utils::peer_num p = 0; p < links.size(); p++) {
            // All of them, not only the ones being sealed: an application thread parked on a
            // link that was never established must also be held across the stop, and must wake
            // onto the migration clock rather than the transport's.
            drain_locks.emplace_back(links[p]->mu);
        }
        for (Utils::peer_num p = 0; p < links.size(); p++) {
            if (p == peer_id) {
                continue;
            }
            LinkState& l = *links[p];
            if (!l.draining) {
                l.draining = true;
                l.draining_since_ms = monotonic_ms();
            }
            // This rank knows why it is draining; nothing here is a guess.
            l.drain_unconfirmed = false;
        }
        seal_links(to_seal);
        drained_peers = to_seal;

        // 5. The seal, with the counters the driver cross-checks pairwise before it dumps.
        if (coordinator != nullptr) {
            DrainEvent sealed;
            sealed.type = DrainEvent::Type::Sealed;
            sealed.rank = peer_id;
            sealed.epoch = my_epoch;
            sealed.batch = batch_id;
            sealed.extra["last_stream_id"] = last_stream_id;
            for (const auto peer : to_seal) {
                LinkState& l = *links[peer];
                sealed.extra["sent." + std::to_string(peer)] = std::to_string(l.bytes_sent);
                sealed.extra["received." + std::to_string(peer)] =
                        std::to_string(l.bytes_received);
            }
            coordinator->emit(sealed);
        }
    } catch (const std::exception& e) {
        // Loud, and left broken: every parked application thread now throws this text, which is
        // how a migration that could not complete becomes a job that dies instead of a job that
        // quietly carries on with a channel nobody drained.
        BOOST_LOG_TRIVIAL(error) << "DrainTCP: rank " << peer_id << " could not seal: "
                                 << e.what();
        break_every_link(std::string("broken by a migration that could not complete: ") +
                         e.what());
        drain_locks.clear();
        drained_peers.clear();
        migration_active.store(false, std::memory_order_release);
        resume_control_thread();
        held_migration_lock = std::unique_lock<std::mutex>();
        throw;
    }
    // The gate stays taken across the stop: it is released with the link locks at the very end
    // of the restore leg, and until then no thread may build a connection on this rank.
    held_establish_gate = std::move(gate);
}

void FMI::Comm::DrainTCP::release_transport_sockets() {
    // The listener went in step 3 and the links in step 4, so these two are the last sockets
    // this process owns. After this it owns none, which is the entire point: the image is
    // host-agnostic and neither CRIU leg needs --tcp-close.
    if (registry) {
        registry->disconnect();
    }
    if (coordinator) {
        coordinator->disconnect();
    }
    published_at_ms.store(-1, std::memory_order_release);
}

void FMI::Comm::DrainTCP::rebind_listener() {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        throw std::runtime_error("DrainTCP: socket() failed on the restore leg: " +
                                 std::string(strerror(errno)));
    }
    int one = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(0);
    if (!TcpEndpoint::resolve_ipv4(bind_host, addr.sin_addr)) {
        addr.sin_addr.s_addr = htonl(INADDR_ANY);
    }
    if (::bind(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
        ::close(fd);
        throw std::runtime_error("DrainTCP: bind to " + bind_host + " failed on the restore leg: " +
                                 std::string(strerror(errno)));
    }
    if (::listen(fd, SOMAXCONN) < 0) {
        ::close(fd);
        throw std::runtime_error("DrainTCP: listen failed on the restore leg: " +
                                 std::string(strerror(errno)));
    }
    socklen_t len = sizeof(addr);
    if (::getsockname(fd, reinterpret_cast<struct sockaddr*>(&addr), &len) < 0) {
        ::close(fd);
        throw std::runtime_error("DrainTCP: getsockname failed on the restore leg: " +
                                 std::string(strerror(errno)));
    }
    TcpEndpoint::set_nonblocking(fd, true);

    listen_port = ntohs(addr.sin_port);
    // A fresh nonce with the fresh port: whatever else has been handed this ephemeral number in
    // the meantime must not be able to pass itself off as this listener.
    listener_nonce = TcpEndpoint::random_nonce();
    // Re-resolved rather than remembered, which is what makes a restore on a different host
    // correct by construction instead of by a boot-id check.
    advertised_ip = TcpEndpoint::resolve_advertise_ip(advertise_host, registry_host, registry_port);
    listen_fd.store(fd, std::memory_order_release);
}

void FMI::Comm::DrainTCP::resume_after_restore() noexcept {
    // noexcept by contract: this runs on the trigger thread while every application thread is
    // parked on link state this very function holds, so a throw here would take the process
    // down with the locks held. Failures are recorded per link and raised on the application
    // thread instead.
    try {
        const std::uint64_t incarnation =
                local_incarnation.fetch_add(1, std::memory_order_acq_rel) + 1;
        const std::uint64_t epoch = Utils::MigrationTrigger::instance().epoch();

        rebind_listener();
        if (!try_publish(monotonic_ms() + drain_budget_ms(), true)) {
            throw std::runtime_error("DrainTCP: rank " + std::to_string(peer_id) +
                                     " could not re-advertise itself after the restore");
        }
        if (coordinator != nullptr) {
            coordinator->publish_member(peer_id, MemberRecord{epoch, advertised_ip, listen_port,
                                                              listener_nonce});
            DrainEvent restored;
            restored.type = DrainEvent::Type::Restored;
            restored.rank = peer_id;
            restored.epoch = epoch;
            restored.batch = batch_id;
            restored.extra["incarnation"] = std::to_string(incarnation);
            coordinator->emit(restored);
        }
        // Accepting again, on the new listener, before any peer can dial it: the survivors that
        // outrank this one learn the new address from the member record above.
        resume_control_thread();

        for (auto& state : links) {
            if (state == nullptr) {
                continue;
            }
            // Left at fd = -1 on purpose. Re-establishment is lazy and belongs to whichever
            // thread next wants the link: the application threads parked here re-enter their
            // own establish path the moment they wake, and a peer that is not talking to this
            // rank right now costs nothing until it does.
            state->draining = false;
            state->drain_unconfirmed = false;
            state->back_up.notify_all();
        }
        if (holds_batch_lock && coordinator != nullptr) {
            coordinator->release_batch_lock(batch_id);
        }
        holds_batch_lock = false;
        batch_lease_external = false;
        batch_id.clear();
        drained_peers.clear();
        migration_active.store(false, std::memory_order_release);
        BOOST_LOG_TRIVIAL(info) << "DrainTCP: rank " << peer_id << " of " << comm_name
                                << " resumed at epoch " << epoch << ", incarnation "
                                << incarnation << ", listening on " << advertised_ip << ":"
                                << listen_port;
    } catch (const std::exception& e) {
        BOOST_LOG_TRIVIAL(error) << "DrainTCP: rank " << peer_id
                                 << " could not resume after its migration: " << e.what();
        break_every_link(std::string("broken by a restore that could not complete: ") + e.what());
        migration_active.store(false, std::memory_order_release);
    }
    // Released last, and in this order: the locks the application threads are parked on, then
    // the gate that stops anyone building a connection. Until both are gone no thread can
    // observe the half-restored channel that existed in between.
    drain_locks.clear();
    held_establish_gate = std::unique_lock<std::shared_timed_mutex>();
    held_migration_lock = std::unique_lock<std::mutex>();
}

void FMI::Comm::DrainTCP::peer_is_leaving(Utils::peer_num peer, std::uint64_t epoch) {
    // No lineage in the notice: tear down whatever is there. This is the interface a control
    // plane that carries no incarnation reaches, which in practice means a test's.
    apply_leave_notice(peer, epoch, std::numeric_limits<std::uint64_t>::max());
}

void FMI::Comm::DrainTCP::apply_leave_notice(Utils::peer_num peer, std::uint64_t epoch,
                                             std::uint64_t leaver_incarnation) {
    if (peer == peer_id) {
        return;   // this rank's own notice, read back off the stream
    }
    LinkState& l = link(peer);   // bounds-checked
    std::unique_lock<std::mutex> lock(l.mu);
    if (epoch < l.peer_epoch) {
        BOOST_LOG_TRIVIAL(warning)
                << "DrainTCP: rank " << peer_id << " dropped a leave notice from peer " << peer
                << " at epoch " << epoch << "; it has already seen epoch " << l.peer_epoch;
        return;
    }
    if (l.peer_incarnation > leaver_incarnation) {
        // The connection this rank holds was built by a LATER lineage of that peer, so the
        // migration this notice describes is already over and its reconnect has already
        // happened. Acting on it would close a link both ends believe in. The notice is
        // therefore treated as the confirmation it is — the peer did migrate, and it is back.
        BOOST_LOG_TRIVIAL(info)
                << "DrainTCP: rank " << peer_id << " dropped a leave notice from peer " << peer
                << " written by incarnation " << leaver_incarnation
                << "; it is already connected to incarnation " << l.peer_incarnation;
        l.peer_epoch = epoch;
        l.draining = false;
        l.drain_unconfirmed = false;
        l.back_up.notify_all();
        return;
    }
    l.peer_epoch = epoch;
    if (!l.draining) {
        l.draining = true;
        l.draining_since_ms = monotonic_ms();
    }
    // Confirmed by the control plane: whatever the data path guessed, this is the answer.
    l.drain_unconfirmed = false;

    if (l.fd >= 0) {
        const int fd = l.fd;
        ++l.generation;
        // Half-close first, so everything this rank had queued for the migrating peer reaches
        // it before its FIN does, and its drain can account for every byte.
        ::shutdown(fd, SHUT_WR);
        const bool clean = drain_one_locked(l, fd, peer, monotonic_ms() + drain_budget_ms());
        ::close(fd);
        l.fd = -1;
        if (!clean) {
            l.unrecoverable = "the drain of migrating peer " + std::to_string(peer) +
                              " never reached EOF within drain_grace_ms=" +
                              std::to_string(drain_grace_ms);
            l.back_up.notify_all();
            throw std::runtime_error("DrainTCP: rank " + std::to_string(peer_id) + ": " +
                                     l.unrecoverable);
        }
    }
    // Idempotent by construction: a second notice for the same migration finds fd = -1 and
    // does nothing but re-affirm what is already true. At-least-once delivery is the only
    // kind a stream offers.
    l.back_up.notify_all();
}

void FMI::Comm::DrainTCP::peer_has_restored(Utils::peer_num peer, std::uint64_t epoch) {
    if (peer == peer_id) {
        return;
    }
    LinkState& l = link(peer);
    std::lock_guard<std::mutex> lock(l.mu);
    if (epoch < l.peer_epoch) {
        BOOST_LOG_TRIVIAL(warning)
                << "DrainTCP: rank " << peer_id << " dropped a restore notice from peer " << peer
                << " at epoch " << epoch << "; it has already seen epoch " << l.peer_epoch;
        return;
    }
    l.peer_epoch = epoch;
    // Deliberately NOT pre-setting peer_incarnation to anything: the hello's monotonicity check
    // is what admits the peer's new lineage and refuses a superseded one, and a value guessed
    // here could only make that check refuse something legitimate.
    l.draining = false;
    l.drain_unconfirmed = false;
    // Whoever is parked on this link retries at once. A parked operation on a LOWER-ranked peer
    // re-dials it through the ordinary establish path; one on a HIGHER-ranked peer waits for
    // that peer's dial, which the peer makes because it saw this same event.
    l.back_up.notify_all();
}

bool FMI::Comm::DrainTCP::poll_control_events() {
    if (!started.load(std::memory_order_acquire) || coordinator == nullptr) {
        return false;
    }
    std::unique_lock<std::mutex> guard(migration_mutex, std::try_to_lock);
    if (!guard.owns_lock()) {
        // A migration of this rank is in flight. Its events are its own business and the stream
        // keeps them; this comes back for them on the next tick.
        return false;
    }
    // Non-blocking: the trigger thread owes its signal wait the next tick.
    auto events = coordinator->read_after(last_stream_id, 0);
    bool migrate_me = false;
    for (auto& [id, event] : events) {
        last_stream_id = id;
        if (event.rank >= num_peers) {
            continue;
        }
        if (event.rank == peer_id) {
            if (event.type == DrainEvent::Type::Migrate) {
                // Carried down so the seal and the restore are attributable to the batch that
                // asked for them — and so this rank does not try to take a lease its driver
                // already holds.
                if (!event.batch.empty()) {
                    batch_id = event.batch;
                    batch_lease_external = true;
                }
                migrate_me = true;
            }
            continue;
        }
        switch (event.type) {
            case DrainEvent::Type::Leaving: {
                std::uint64_t leaver_incarnation = std::numeric_limits<std::uint64_t>::max();
                auto it = event.extra.find("incarnation");
                if (it != event.extra.end()) {
                    try {
                        leaver_incarnation = std::stoull(it->second);
                    } catch (const std::exception&) {
                        // A notice whose lineage cannot be read is treated as carrying none,
                        // which is the conservative reading: act on it.
                    }
                }
                apply_leave_notice(event.rank, event.epoch, leaver_incarnation);
                break;
            }
            case DrainEvent::Type::Restored:
                peer_has_restored(event.rank, event.epoch);
                break;
            case DrainEvent::Type::Migrate:
            case DrainEvent::Type::Sealed:
            case DrainEvent::Type::Unknown:
            default:
                // Not this rank's business: a migrate addressed to someone else, a seal that
                // only a driver reads, or an event kind a newer build emits.
                break;
        }
    }
    return migrate_me;
}

void FMI::Comm::DrainTCP::rehearse_migration_in_place(long hold_ms) {
    ensure_started();
    Utils::MigrationTrigger::instance().run_migration(*this, true, hold_ms);
}
