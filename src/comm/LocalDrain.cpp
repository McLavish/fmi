#include "../../include/comm/LocalDrain.h"

#include "../../include/comm/LinkHello.h"
#include "../../include/comm/TcpEndpoint.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#if defined(__GLIBC__)
#include <malloc.h>
#endif

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cmath>
#include <csignal>
#include <cstdio>
#include <cstring>
#include <sstream>
#include <stdexcept>

#include <boost/log/trivial.hpp>

namespace {
    long monotonic_ms() {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now().time_since_epoch()).count();
    }

    //! How long a blocked operation sleeps before it looks at its deadline again. Every wait
    //! here ends on an event (bytes, a FIN, a row); the slice only bounds how late a timeout
    //! is noticed. An idle rank waits on nothing.
    constexpr int wait_slice_ms = 50;

    //! How long an accepted connection may stay silent before its hello. A port scanner or a
    //! peer that died between connect and write costs the service thread this much.
    constexpr long hello_read_budget_ms = 200;

    //! Connections accepted per wake of the listener before the service thread looks elsewhere.
    constexpr int accept_batch = 16;

    //! Read size of a drain.
    constexpr std::size_t drain_chunk_bytes = 64u * 1024u;

    constexpr std::uint64_t tag_wake = 0;
    constexpr std::uint64_t tag_listener = 1;
    constexpr std::uint64_t tag_control = 2;
    constexpr std::uint64_t tag_link_base = 16;

    //! Move exactly @p len bytes over a non-blocking descriptor, or give up at the deadline or
    //! when one of the two flags is raised. Used for the hello only: the data path must be
    //! interruptible between chunks and never comes through here.
    bool move_exact(int fd, char* data, std::size_t len, bool writing, long deadline_ms,
                    const std::atomic<bool>& stop, const std::atomic<bool>* yield) {
        std::size_t moved = 0;
        while (moved < len) {
            if (stop.load(std::memory_order_acquire) ||
                (yield != nullptr && yield->load(std::memory_order_acquire))) {
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
                return false;   // the peer closed inside the hello
            }
            if (errno == EINTR) {
                continue;
            }
            if (errno != EAGAIN && errno != EWOULDBLOCK) {
                return false;
            }
            struct pollfd pfd{fd, static_cast<short>(writing ? POLLOUT : POLLIN), 0};
            const long budget = std::min<long>(wait_slice_ms, deadline_ms - monotonic_ms());
            ::poll(&pfd, 1, static_cast<int>(std::max<long>(budget, 0)));
        }
        return true;
    }

    //! Split a control line into its verb and its `key=value` fields.
    std::map<std::string, std::string> fields_of(const std::string& line, std::string& verb) {
        std::map<std::string, std::string> out;
        std::istringstream words(line);
        std::string word;
        bool first = true;
        while (words >> word) {
            if (first) {
                verb = word;
                first = false;
                continue;
            }
            const auto eq = word.find('=');
            if (eq == std::string::npos || eq == 0) {
                continue;
            }
            out[word.substr(0, eq)] = word.substr(eq + 1);
        }
        return out;
    }

    bool to_u64(const std::map<std::string, std::string>& fields, const char* key,
                std::uint64_t& out) {
        const auto it = fields.find(key);
        if (it == fields.end() || it->second.empty()) {
            return false;
        }
        try {
            std::size_t used = 0;
            out = std::stoull(it->second, &used);
            return used == it->second.size();
        } catch (const std::exception&) {
            return false;
        }
    }
}

FMI::Comm::LocalDrain::LocalDrain(std::map<std::string, std::string> params,
                                  std::map<std::string, std::string> model_params) {
    host_link = TcpEndpoint::param_or(params, "host_link", "");
    if (const char* from_host = std::getenv("FMI_HOST_LINK"); from_host != nullptr && *from_host != 0) {
        // The host manager knows where it listens; what it tells its ranks wins over the file.
        host_link = from_host;
    }
    if (host_link.empty()) {
        throw std::runtime_error("LocalDrain: host_link, the socket of the host manager, is not set");
    }
    if (host_link.size() >= sizeof(sockaddr_un{}.sun_path)) {
        throw std::runtime_error("LocalDrain: host_link is too long for a Unix socket path: " + host_link);
    }
    max_timeout = static_cast<unsigned int>(std::stoul(params.at("max_timeout")));
    bind_host = TcpEndpoint::param_or(params, "bind_host", "0.0.0.0");
    connect_retry_interval_ms = static_cast<unsigned int>(
            std::stoul(TcpEndpoint::param_or(params, "connect_retry_interval_ms", "10")));
    drain_grace_ms = std::stol(TcpEndpoint::param_or(params, "drain_grace_ms", "5000"));
    migration_max_ms = std::stol(TcpEndpoint::param_or(params, "migration_max_ms", "120000"));
    establish_yield_ms = std::stol(TcpEndpoint::param_or(params, "establish_yield_ms", "2000"));
    const std::string stop = TcpEndpoint::param_or(params, "stop", "sigstop");
    if (stop == "sigstop") {
        stop_by_signal = true;
    } else if (stop == "hold") {
        stop_by_signal = false;
    } else {
        throw std::runtime_error("LocalDrain: stop must be sigstop or hold, not " + stop);
    }

    bandwidth = std::stod(model_params.at("bandwidth"));
    overhead = std::stod(model_params.at("overhead"));
    transfer_price = std::stod(model_params.at("transfer_price"));
    vm_price = std::stod(model_params.at("vm_price"));
    requests_per_hour = static_cast<unsigned int>(std::stoul(model_params.at("requests_per_hour")));
    include_infrastructure_costs = model_params["include_infrastructure_costs"] == "true";
}

FMI::Comm::LocalDrain::~LocalDrain() {
    try {
        LocalDrain::finalize();
    } catch (const std::exception&) {
    }
}

void FMI::Comm::LocalDrain::on_registered() {
    // The first moment the rank, the size of the job and its name are known. Starting here,
    // not at the first operation, puts the rank into the rank table at once: a rank that
    // computes for its first seconds still answers a peer that dials it, and can be told to leave.
    try {
        ensure_started();
    } catch (const std::exception& e) {
        // Must not throw: the first operation tries again and reports it where a link is needed.
        BOOST_LOG_TRIVIAL(warning) << "LocalDrain: rank " << peer_id
                                   << " could not start at registration: " << e.what();
    }
}

std::string FMI::Comm::LocalDrain::link_name(Utils::peer_num partner) const {
    const auto low = std::min(peer_id, partner);
    const auto high = std::max(peer_id, partner);
    return comm_name + "|" + std::to_string(low) + "-" + std::to_string(high);
}

FMI::Comm::LocalDrain::LinkState& FMI::Comm::LocalDrain::link(Utils::peer_num peer) {
    if (peer >= links.size() || links[peer] == nullptr || peer == peer_id) {
        throw std::runtime_error("LocalDrain: rank " + std::to_string(peer_id) + " has no link to rank " +
                                 std::to_string(peer) + " in a job of " + std::to_string(links.size()));
    }
    return *links[peer];
}

void FMI::Comm::LocalDrain::throw_if_broken(const LinkState& l, Utils::peer_num peer) {
    if (!l.broken.empty()) {
        throw std::runtime_error("LocalDrain: the link to rank " + std::to_string(peer) +
                                 " cannot be used: " + l.broken);
    }
}

void FMI::Comm::LocalDrain::throw_if_abandoned(const LinkState& l, Utils::peer_num peer,
                                               std::size_t moved, std::size_t wanted) const {
    if (!l.peer_gone || l.fd >= 0) {
        return;   // not finalized, or its last bytes are still to be read off the socket
    }
    if (moved == 0) {
        // A peer that finalized before a single byte of this message crossed has left the
        // collective, which is what Timeout means. Past the first byte it is truncation.
        throw Utils::Timeout();
    }
    throw std::runtime_error("LocalDrain: rank " + std::to_string(peer_id) + ": rank " +
                             std::to_string(peer) + " finalized after " + std::to_string(moved) +
                             "/" + std::to_string(wanted) + " bytes of a message");
}

long FMI::Comm::LocalDrain::drain_budget_ms() const {
    // The quiet period starts again on every byte, so this cap is what stops a peer that
    // trickles forever. The larger of the two on purpose.
    return std::max<long>(static_cast<long>(max_timeout), drain_grace_ms);
}

// ------------------------------------------------------------------------------------ start

void FMI::Comm::LocalDrain::open_listener() {
    int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (fd < 0) {
        throw std::runtime_error("LocalDrain: socket() failed: " + std::string(strerror(errno)));
    }
    int one = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(0);   // the kernel picks a free port
    if (!TcpEndpoint::resolve_ipv4(bind_host, addr.sin_addr)) {
        addr.sin_addr.s_addr = htonl(INADDR_ANY);
    }
    socklen_t len = sizeof(addr);
    // SOMAXCONN: several ranks of a collective dial the same rank at once, and Linux drops
    // what does not fit without saying so.
    if (::bind(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0 ||
        ::listen(fd, SOMAXCONN) < 0 ||
        ::getsockname(fd, reinterpret_cast<struct sockaddr*>(&addr), &len) < 0) {
        const std::string why = strerror(errno);
        ::close(fd);
        throw std::runtime_error("LocalDrain: no listener on " + bind_host + ": " + why);
    }
    TcpEndpoint::set_nonblocking(fd, true);
    listen_port = ntohs(addr.sin_port);
    // A new nonce with every listener: whatever is given this port later cannot pass for it.
    listener_nonce = TcpEndpoint::random_nonce();
    struct epoll_event ev{};
    ev.events = EPOLLIN;
    ev.data.u64 = tag_listener;
    if (::epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &ev) != 0) {
        const std::string why = strerror(errno);
        ::close(fd);
        throw std::runtime_error("LocalDrain: cannot watch the listener: " + why);
    }
    listen_fd.store(fd, std::memory_order_release);
}

void FMI::Comm::LocalDrain::open_control_link(long deadline_ms) {
    if (comm_name.find(' ') != std::string::npos || comm_name.find('=') != std::string::npos ||
        comm_name.empty()) {
        throw std::runtime_error("LocalDrain: a job name must be non-empty without spaces or '='");
    }
    struct sockaddr_un addr{};
    addr.sun_family = AF_UNIX;
    std::strncpy(addr.sun_path, host_link.c_str(), sizeof(addr.sun_path) - 1);
    int fd = -1;
    while (true) {
        fd = ::socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
        if (fd < 0) {
            throw std::runtime_error("LocalDrain: socket() failed: " + std::string(strerror(errno)));
        }
        if (::connect(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) == 0) {
            break;
        }
        const int err = errno;
        ::close(fd);
        fd = -1;
        // After a restore the host manager of the new host may still be coming up.
        if ((err != ENOENT && err != ECONNREFUSED && err != EAGAIN && err != EINTR) ||
            monotonic_ms() >= deadline_ms) {
            throw std::runtime_error("LocalDrain: rank " + std::to_string(peer_id) +
                                     " cannot reach its host manager at " + host_link + ": " +
                                     strerror(err));
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    TcpEndpoint::set_nonblocking(fd, true);
    struct epoll_event ev{};
    ev.events = EPOLLIN | EPOLLRDHUP;
    ev.data.u64 = tag_control;
    if (::epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &ev) != 0) {
        const std::string why = strerror(errno);
        ::close(fd);
        throw std::runtime_error("LocalDrain: cannot watch the control link: " + why);
    }
    control_buffer.clear();
    {
        // A new host manager knows nothing of what this rank asked the last one.
        std::lock_guard<std::mutex> lock(asked_mutex);
        std::fill(asked.begin(), asked.end(), 0);
    }
    {
        std::lock_guard<std::mutex> lock(control_wait_mutex);
        welcomed = false;
    }
    control_lost.store(false, std::memory_order_release);
    control_fd.store(fd, std::memory_order_release);
    say("hello job=" + comm_name + " rank=" + std::to_string(peer_id) + " size=" +
        std::to_string(num_peers) + " generation=" +
        std::to_string(own_generation.load(std::memory_order_acquire)) + " pid=" +
        std::to_string(::getpid()) + " port=" + std::to_string(listen_port) + " nonce=" +
        std::to_string(listener_nonce));
}

void FMI::Comm::LocalDrain::ensure_started() {
    if (started.load(std::memory_order_acquire)) {
        return;
    }
    std::lock_guard<std::mutex> lifecycle(lifecycle_mutex);
    if (started.load(std::memory_order_relaxed)) {
        return;
    }
    if (finalized) {
        throw std::runtime_error("LocalDrain: the channel was finalized and cannot be used again");
    }
    if (num_peers == 0 || peer_id >= num_peers) {
        throw std::runtime_error("LocalDrain: rank " + std::to_string(peer_id) +
                                 " is not valid in a job of " + std::to_string(num_peers) +
                                 "; set_peer_id and set_num_peers come before any operation");
    }

    try {
        links.clear();
        links.reserve(num_peers);
        for (Utils::peer_num p = 0; p < num_peers; p++) {
            links.push_back(std::make_unique<LinkState>());
        }
        table.resize(num_peers);
        asked.assign(num_peers, 0);

        if (::pipe(wake_pipe) != 0) {
            throw std::runtime_error("LocalDrain: pipe() failed: " + std::string(strerror(errno)));
        }
        TcpEndpoint::set_nonblocking(wake_pipe[0], true);
        TcpEndpoint::set_nonblocking(wake_pipe[1], true);
        epoll_fd = ::epoll_create1(EPOLL_CLOEXEC);
        if (epoll_fd < 0) {
            throw std::runtime_error("LocalDrain: epoll_create1 failed: " + std::string(strerror(errno)));
        }
        struct epoll_event ev{};
        ev.events = EPOLLIN;
        ev.data.u64 = tag_wake;
        ::epoll_ctl(epoll_fd, EPOLL_CTL_ADD, wake_pipe[0], &ev);

        open_listener();
        open_control_link(monotonic_ms() + static_cast<long>(max_timeout));

        stopping.store(false, std::memory_order_release);
        {
            std::lock_guard<std::mutex> lock(hold_mutex);
            hold_release_requested = false;
        }
        // From here two threads read the link table; it is never resized again.
        service_thread = std::thread(&LocalDrain::service_loop, this);

        std::unique_lock<std::mutex> lock(control_wait_mutex);
        const bool answered = control_wait.wait_for(
                lock, std::chrono::milliseconds(max_timeout),
                [this]() { return welcomed || control_lost.load(std::memory_order_acquire); });
        if (!answered || !welcomed) {
            throw std::runtime_error("LocalDrain: rank " + std::to_string(peer_id) +
                                     " was not welcomed by its host manager at " + host_link +
                                     (control_lost_reason.empty() ? "" : ": " + control_lost_reason));
        }
    } catch (...) {
        stop_service_thread();
        for (int* fd : {&epoll_fd, &wake_pipe[0], &wake_pipe[1]}) {
            if (*fd >= 0) {
                ::close(*fd);
                *fd = -1;
            }
        }
        for (std::atomic<int>* fd : {&listen_fd, &control_fd}) {
            const int old = fd->exchange(-1, std::memory_order_acq_rel);
            if (old >= 0) {
                ::close(old);
            }
        }
        links.clear();
        throw;
    }
    started.store(true, std::memory_order_release);
}

void FMI::Comm::LocalDrain::stop_service_thread() {
    stopping.store(true, std::memory_order_release);
    {
        std::lock_guard<std::mutex> lock(hold_mutex);
        hold_release_requested = true;
    }
    hold_released.notify_all();
    if (wake_pipe[1] >= 0) {
        const char byte = 1;
        while (::write(wake_pipe[1], &byte, 1) < 0 && errno == EINTR) {
        }
    }
    if (service_thread.joinable()) {
        service_thread.join();
    }
}

void FMI::Comm::LocalDrain::finalize() {
    std::lock_guard<std::mutex> lifecycle(lifecycle_mutex);
    if (finalized) {
        return;
    }
    finalized = true;
    const bool was_started = started.exchange(false, std::memory_order_acq_rel);
    if (was_started) {
        try {
            say("bye");
        } catch (const std::exception&) {
            // A host manager that is already gone needs no goodbye.
        }
    }
    stop_service_thread();
    table.wake_all();
    for (std::atomic<int>* fd : {&listen_fd, &control_fd}) {
        const int old = fd->exchange(-1, std::memory_order_acq_rel);
        if (old >= 0) {
            ::close(old);
        }
    }
    for (auto& state : links) {
        if (state == nullptr) {
            continue;
        }
        std::lock_guard<std::mutex> lock(state->mu);
        if (state->fd >= 0) {
            ::close(state->fd);
            state->fd = -1;
        }
        state->changed.notify_all();
    }
    for (int* fd : {&epoll_fd, &wake_pipe[0], &wake_pipe[1]}) {
        if (*fd >= 0) {
            ::close(*fd);
            *fd = -1;
        }
    }
}

int FMI::Comm::LocalDrain::open_sockets() {
    int count = 0;
    if (listen_fd.load(std::memory_order_acquire) >= 0) {
        count++;
    }
    if (control_fd.load(std::memory_order_acquire) >= 0) {
        count++;
    }
    if (holding_all_links) {
        // Asked of a drained rank: its service thread holds every link lock, and every link
        // it holds is closed.
        for (const auto& state : links) {
            if (state != nullptr && state->fd >= 0) {
                count++;
            }
        }
        return count;
    }
    for (auto& state : links) {
        if (state == nullptr) {
            continue;
        }
        std::lock_guard<std::mutex> lock(state->mu);
        if (state->fd >= 0) {
            count++;
        }
    }
    return count;
}

// ----------------------------------------------------------------------------- control link

void FMI::Comm::LocalDrain::say(const std::string& line) {
    std::lock_guard<std::mutex> lock(control_write_mutex);
    const int fd = control_fd.load(std::memory_order_acquire);
    if (fd < 0) {
        throw std::runtime_error("LocalDrain: rank " + std::to_string(peer_id) +
                                 " has no control link to its host manager");
    }
    const std::string wire = line + "\n";
    std::size_t sent = 0;
    const long deadline = monotonic_ms() + static_cast<long>(max_timeout);
    while (sent < wire.size()) {
        const ssize_t n = ::send(fd, wire.data() + sent, wire.size() - sent, MSG_NOSIGNAL);
        if (n > 0) {
            sent += static_cast<std::size_t>(n);
            continue;
        }
        if (n < 0 && errno == EINTR) {
            continue;
        }
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK) && monotonic_ms() < deadline) {
            struct pollfd pfd{fd, POLLOUT, 0};
            ::poll(&pfd, 1, wait_slice_ms);
            continue;
        }
        throw std::runtime_error("LocalDrain: rank " + std::to_string(peer_id) +
                                 " could not write to its host manager: " + strerror(errno));
    }
}

void FMI::Comm::LocalDrain::ask_where(Utils::peer_num peer) {
    {
        std::lock_guard<std::mutex> lock(asked_mutex);
        if (peer >= asked.size() || asked[peer] != 0) {
            return;   // asked on this control link already: newer rows follow by themselves
        }
        asked[peer] = 1;
    }
    try {
        say("where rank=" + std::to_string(peer));
    } catch (const std::exception&) {
        std::lock_guard<std::mutex> lock(asked_mutex);
        asked[peer] = 0;
        throw;
    }
}

void FMI::Comm::LocalDrain::lose_control_link(const std::string& why) {
    {
        std::lock_guard<std::mutex> write(control_write_mutex);
        const int fd = control_fd.exchange(-1, std::memory_order_acq_rel);
        if (fd >= 0) {
            ::close(fd);
        }
    }
    {
        std::lock_guard<std::mutex> lock(control_wait_mutex);
        control_lost_reason = why;
        control_lost.store(true, std::memory_order_release);
    }
    control_wait.notify_all();
    table.wake_all();
    if (!stopping.load(std::memory_order_acquire)) {
        BOOST_LOG_TRIVIAL(error) << "LocalDrain: rank " << peer_id << " of " << comm_name
                                 << " lost its host manager: " << why
                                 << "; links that exist keep working, nothing new can be built";
    }
}

std::string FMI::Comm::LocalDrain::lost_reason() {
    std::lock_guard<std::mutex> lock(control_wait_mutex);
    return control_lost_reason;
}

bool FMI::Comm::LocalDrain::read_control_lines() {
    const int fd = control_fd.load(std::memory_order_acquire);
    if (fd < 0) {
        return false;
    }
    bool closed = false;
    char chunk[4096];
    while (true) {
        const ssize_t n = ::recv(fd, chunk, sizeof(chunk), MSG_DONTWAIT);
        if (n > 0) {
            control_buffer.append(chunk, static_cast<std::size_t>(n));
            continue;
        }
        if (n == 0) {
            closed = true;
            break;
        }
        if (errno == EINTR) {
            continue;
        }
        if (errno != EAGAIN && errno != EWOULDBLOCK) {
            closed = true;
        }
        break;
    }
    std::size_t end = 0;
    while ((end = control_buffer.find('\n')) != std::string::npos) {
        const std::string line = control_buffer.substr(0, end);
        control_buffer.erase(0, end + 1);
        handle_line(line);
        if (control_fd.load(std::memory_order_acquire) != fd) {
            // The line made this rank leave and come back: the link it arrived on is gone, and
            // what was behind it was said to a rank that no longer listens there.
            return true;
        }
    }
    if (closed) {
        lose_control_link("the host manager closed the control link");
        return false;
    }
    return true;
}

void FMI::Comm::LocalDrain::handle_line(const std::string& line) {
    std::string verb;
    const auto fields = fields_of(line, verb);
    if (verb == "welcome") {
        {
            std::lock_guard<std::mutex> lock(control_wait_mutex);
            welcomed = true;
        }
        control_wait.notify_all();
    } else if (verb == "rank") {
        std::uint64_t rank = 0;
        RankRow row;
        row.known = true;
        if (!to_u64(fields, "rank", rank) || !to_u64(fields, "generation", row.generation)) {
            return;
        }
        const auto state = fields.find("state");
        row.leaving = state != fields.end() && state->second == "leaving";
        row.gone = state != fields.end() && state->second == "gone";
        const auto addr = fields.find("addr");
        if (addr != fields.end()) {
            const auto colon = addr->second.rfind(':');
            if (colon != std::string::npos) {
                try {
                    row.ip = addr->second.substr(0, colon);
                    row.port = std::stoi(addr->second.substr(colon + 1));
                } catch (const std::exception&) {
                    row.ip.clear();
                    row.port = 0;
                }
            }
        }
        to_u64(fields, "nonce", row.nonce);
        if (rank < num_peers) {
            on_row(static_cast<Utils::peer_num>(rank), row);
        }
    } else if (verb == "leave") {
        std::uint64_t generation = 0;
        const auto move = fields.find("move");
        if (!to_u64(fields, "generation", generation) || move == fields.end()) {
            return;
        }
        if (generation != own_generation.load(std::memory_order_acquire)) {
            // A request for a generation this rank has left behind, or has not reached.
            BOOST_LOG_TRIVIAL(warning) << "LocalDrain: rank " << peer_id << " ignored a leave for "
                                       << "generation " << generation << "; it is at generation "
                                       << own_generation.load(std::memory_order_acquire);
            return;
        }
        if (in_transition) {
            // Asked to leave again before it is fully back: after the way back, not inside it.
            deferred_leave_move = move->second;
            deferred_leave_generation = generation;
            return;
        }
        leave(move->second);
    } else if (verb == "synced") {
        std::uint64_t token = 0;
        if (to_u64(fields, "token", token)) {
            {
                std::lock_guard<std::mutex> lock(control_wait_mutex);
                synced_token = std::max(synced_token, token);
            }
            control_wait.notify_all();
        }
    }
    // Anything else is from a newer host manager and is not for this build.
}

void FMI::Comm::LocalDrain::apply_row_locked(LinkState& l, Utils::peer_num, const RankRow& row,
                                             long now_ms) {
    if (row.gone) {
        // The peer finalized. Whoever still waits for it is told at once.
        l.peer_gone = true;
        l.changed.notify_all();
        return;
    }
    if (row.leaving) {
        if (!l.leaving_known || row.generation > l.leaving_generation) {
            l.leaving_known = true;
            l.leaving_generation = row.generation;
        }
        if (l.fd >= 0) {
            // A live link stays as it is. The FIN of the leaving rank is the signal to close
            // it; the row only makes sure that FIN is understood when it comes.
        } else if (l.draining) {
            if (l.unconfirmed && row.generation >= l.away_generation) {
                l.unconfirmed = false;
            }
        } else {
            // No connection: one must not be built towards a rank that is on its way out.
            l.draining = true;
            l.draining_since_ms = now_ms;
            l.away_generation = row.generation;
            l.unconfirmed = false;
        }
    } else if (l.draining && row.generation > l.away_generation) {
        // The peer is back. Whoever waited re-builds the link when it next needs it.
        end_window_locked(l, now_ms);
    }
    l.changed.notify_all();
}

void FMI::Comm::LocalDrain::on_row(Utils::peer_num peer, const RankRow& row) {
    if (peer == peer_id || peer >= links.size()) {
        return;   // this rank's own row is its own business
    }
    if (!table.apply(peer, row)) {
        return;   // not newer than what is known: a repeat or a late arrival
    }
    LinkState& l = *links[peer];
    const long now = monotonic_ms();
    if (holding_all_links) {
        apply_row_locked(l, peer, row, now);
    } else {
        std::lock_guard<std::mutex> lock(l.mu);
        apply_row_locked(l, peer, row, now);
    }
}

// --------------------------------------------------------------------------- service thread

void FMI::Comm::LocalDrain::service_loop() {
    struct epoll_event events[32];
    while (!stopping.load(std::memory_order_acquire)) {
        const int n = ::epoll_wait(epoll_fd, events, 32, -1);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            break;   // the epoll descriptor is gone: the channel is being torn down
        }
        for (int i = 0; i < n && !stopping.load(std::memory_order_acquire); i++) {
            const std::uint64_t tag = events[i].data.u64;
            if (tag == tag_wake) {
                char sink[64];
                while (::read(wake_pipe[0], sink, sizeof(sink)) > 0) {
                }
            } else if (tag == tag_listener) {
                accept_pending();
            } else if (tag == tag_control) {
                read_control_lines();
            } else if (tag >= tag_link_base) {
                peer_closed(static_cast<Utils::peer_num>(tag - tag_link_base));
            }
        }
        if (!deferred_leave_move.empty() && !stopping.load(std::memory_order_acquire)) {
            const std::string move = std::move(deferred_leave_move);
            deferred_leave_move.clear();
            if (deferred_leave_generation == own_generation.load(std::memory_order_acquire)) {
                leave(move);
            }
        }
    }
}

void FMI::Comm::LocalDrain::accept_pending() {
    for (int taken = 0; taken < accept_batch; taken++) {
        if (!accept_one()) {
            return;
        }
    }
}

bool FMI::Comm::LocalDrain::accept_one() {
    const int listener = listen_fd.load(std::memory_order_acquire);
    if (listener < 0) {
        return false;
    }
    const int fd = ::accept4(listener, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
    if (fd < 0) {
        if (errno == EINTR || errno == ECONNABORTED) {
            return true;   // the backlog moved; there may be more behind it
        }
        if (errno != EAGAIN && errno != EWOULDBLOCK) {
            // Out of descriptors or memory: the connection stays queued and the listener stays
            // readable. A short sleep keeps this from becoming a busy loop.
            BOOST_LOG_TRIVIAL(warning) << "LocalDrain: rank " << peer_id << " could not accept: "
                                       << strerror(errno);
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        return false;
    }
    apply_socket_options(fd);
    if (!hello_as_acceptor(fd)) {
        // Closing is the refusal: the dialer reads EOF and looks the rank up again.
        ::close(fd);
    }
    return true;
}

void FMI::Comm::LocalDrain::watch_link(Utils::peer_num peer, int fd) {
    // Only the half-close of the peer is of interest. Bytes that arrive are the business of
    // the thread that receives them, and must not wake this one.
    struct epoll_event ev{};
    ev.events = EPOLLRDHUP;
    ev.data.u64 = tag_link_base + peer;
    ::epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &ev);
}

void FMI::Comm::LocalDrain::peer_closed(Utils::peer_num peer) {
    if (peer >= links.size() || peer == peer_id) {
        return;
    }
    LinkState& l = *links[peer];
    bool unconfirmed = false;
    {
        std::lock_guard<std::mutex> lock(l.mu);
        if (l.fd < 0) {
            return;   // an application thread saw the EOF first
        }
        if (!take_readable_locked(l, l.fd)) {
            return;   // no EOF after all: an event for a descriptor that was replaced
        }
        close_after_peer_locked(l, peer, monotonic_ms());
        unconfirmed = l.unconfirmed;
    }
    if (unconfirmed) {
        try {
            ask_where(peer);
        } catch (const std::exception&) {
            // Without a host manager nothing can confirm the move; the grace period decides.
        }
    }
}

// ------------------------------------------------------------------------------ establishing

void FMI::Comm::LocalDrain::apply_socket_options(int fd) const {
    // Nagle off: the messages are small and shaped by latency, and one delayed acknowledgment
    // on the critical path of a collective costs more than every byte in it.
    int one = 1;
    ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
}

bool FMI::Comm::LocalDrain::lookup_peer(Utils::peer_num peer, PeerAddr& out, long wait_ms) {
    RankRow row = table.get(peer);
    if (!row.dialable()) {
        if (control_lost.load(std::memory_order_acquire)) {
            throw std::runtime_error("LocalDrain: rank " + std::to_string(peer_id) +
                                     " cannot find rank " + std::to_string(peer) +
                                     ": " + lost_reason());
        }
        try {
            ask_where(peer);
        } catch (const std::exception&) {
            // This rank is between two hosts, or the host manager is gone; the wait below
            // ends either way and the caller decides.
        }
        table.wait_newer(peer, row.version(), wait_ms);
        row = table.get(peer);
        if (!row.dialable()) {
            return false;
        }
    }
    out.ip = row.ip;
    out.port = row.port;
    out.nonce = row.nonce;
    return true;
}

int FMI::Comm::LocalDrain::connect_to(const PeerAddr& addr, long deadline_ms) {
    struct sockaddr_in dest{};
    dest.sin_family = AF_INET;
    dest.sin_port = htons(static_cast<std::uint16_t>(addr.port));
    if (!TcpEndpoint::resolve_ipv4(addr.ip, dest.sin_addr)) {
        return -1;
    }
    int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
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
        const long budget = std::min<long>(wait_slice_ms, deadline_ms - monotonic_ms());
        if (budget < 0 || stopping.load(std::memory_order_acquire) ||
            drain_pending.load(std::memory_order_acquire)) {
            ::close(fd);
            return -1;
        }
        struct pollfd pfd{fd, POLLOUT, 0};
        const int pr = ::poll(&pfd, 1, static_cast<int>(budget));
        if (pr < 0 && errno != EINTR) {
            ::close(fd);
            return -1;
        }
        if (pr <= 0) {
            continue;
        }
        int err = 0;
        socklen_t len = sizeof(err);
        if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len) < 0 || err != 0) {
            ::close(fd);
            return -1;
        }
        break;
    }
    return fd;
}

bool FMI::Comm::LocalDrain::hello_as_dialer(Utils::peer_num peer, int fd, std::uint64_t nonce,
                                            long deadline_ms) {
    LinkHello mine;
    mine.sender_rank = static_cast<std::uint32_t>(peer_id);
    mine.receiver_rank = static_cast<std::uint32_t>(peer);
    mine.link_name_hash = TcpEndpoint::fnv1a64(link_name(peer));
    mine.nonce = nonce;
    mine.sender_generation = own_generation.load(std::memory_order_acquire);
    LinkState& l = link(peer);
    {
        std::lock_guard<std::mutex> lock(l.mu);
        mine.bytes_sent = l.bytes_sent;
        mine.bytes_received = l.bytes_received;
    }

    char wire[link_hello_bytes];
    encode_hello(mine, wire);
    char reply[link_hello_bytes];
    if (!move_exact(fd, wire, link_hello_bytes, true, deadline_ms, stopping, &drain_pending) ||
        !move_exact(fd, reply, link_hello_bytes, false, deadline_ms, stopping, &drain_pending)) {
        return false;
    }
    LinkHello theirs;
    if (!decode_hello(reply, theirs) || theirs.receiver_rank != peer_id ||
        theirs.sender_rank != peer || theirs.link_name_hash != mine.link_name_hash ||
        theirs.nonce != 0) {
        return false;
    }
    {
        std::lock_guard<std::mutex> lock(l.mu);
        if (theirs.sender_generation < l.peer_generation) {
            l.broken = "answered by generation " + std::to_string(theirs.sender_generation) +
                       " of the peer, below the " + std::to_string(l.peer_generation) +
                       " already seen";
            l.changed.notify_all();
            throw std::runtime_error("LocalDrain: rank " + std::to_string(peer_id) + ": rank " +
                                     std::to_string(peer) + " " + l.broken);
        }
        if (theirs.bytes_sent != mine.bytes_received || theirs.bytes_received != mine.bytes_sent) {
            l.broken = "the byte counters disagree at reconnect";
            l.changed.notify_all();
            throw std::runtime_error(
                    "LocalDrain: rank " + std::to_string(peer_id) + " and rank " +
                    std::to_string(peer) + " disagree at reconnect (peer sent " +
                    std::to_string(theirs.bytes_sent) + ", we received " +
                    std::to_string(mine.bytes_received) + "; peer received " +
                    std::to_string(theirs.bytes_received) + ", we sent " +
                    std::to_string(mine.bytes_sent) + ")");
        }
    }
    return file_link(peer, fd, theirs.sender_generation);
}

bool FMI::Comm::LocalDrain::hello_as_acceptor(int fd) {
    const long deadline = monotonic_ms() + hello_read_budget_ms;
    char wire[link_hello_bytes];
    if (!move_exact(fd, wire, link_hello_bytes, false, deadline, stopping, nullptr)) {
        return false;
    }
    LinkHello theirs;
    if (!decode_hello(wire, theirs) || theirs.receiver_rank != peer_id ||
        theirs.sender_rank >= num_peers || theirs.sender_rank == peer_id) {
        return false;
    }
    const auto sender = static_cast<Utils::peer_num>(theirs.sender_rank);
    if (theirs.link_name_hash != TcpEndpoint::fnv1a64(link_name(sender)) ||
        theirs.nonce != listener_nonce) {
        return false;   // a stale row: this is not the listener the dialer looked up
    }

    LinkHello mine;
    mine.sender_rank = static_cast<std::uint32_t>(peer_id);
    mine.receiver_rank = theirs.sender_rank;
    mine.link_name_hash = theirs.link_name_hash;
    mine.nonce = 0;
    mine.sender_generation = own_generation.load(std::memory_order_acquire);
    LinkState& l = *links[sender];
    {
        std::lock_guard<std::mutex> lock(l.mu);
        mine.bytes_sent = l.bytes_sent;
        mine.bytes_received = l.bytes_received;
        if (theirs.sender_generation < l.peer_generation ||
            theirs.bytes_sent != mine.bytes_received || theirs.bytes_received != mine.bytes_sent) {
            l.broken = "refused a hello from rank " + std::to_string(sender) + " (generation " +
                       std::to_string(theirs.sender_generation) + ", seen " +
                       std::to_string(l.peer_generation) + "; peer sent " +
                       std::to_string(theirs.bytes_sent) + ", we received " +
                       std::to_string(mine.bytes_received) + "; peer received " +
                       std::to_string(theirs.bytes_received) + ", we sent " +
                       std::to_string(mine.bytes_sent) + ")";
            l.changed.notify_all();
            return false;
        }
    }
    char reply[link_hello_bytes];
    encode_hello(mine, reply);
    if (!move_exact(fd, reply, link_hello_bytes, true, monotonic_ms() + hello_read_budget_ms,
                    stopping, nullptr)) {
        return false;
    }
    if (!file_link(sender, fd, theirs.sender_generation)) {
        return false;
    }
    // The hello names a generation the table may not have yet. Recording it keeps an older
    // leave row, still on its way, from being taken for news.
    RankRow seen;
    seen.known = true;
    seen.generation = theirs.sender_generation;
    table.apply(sender, seen);
    try {
        ask_where(sender);   // so that this rank is told when the dialer leaves
    } catch (const std::exception&) {
    }
    return true;
}

bool FMI::Comm::LocalDrain::file_link(Utils::peer_num peer, int fd, std::uint64_t peer_generation) {
    LinkState& l = link(peer);
    std::lock_guard<std::mutex> lock(l.mu);
    if (l.draining) {
        if (peer_generation <= l.away_generation) {
            // The link waits for this peer to leave, and the connection comes from the
            // generation that is leaving: it would be cut as soon as it was built.
            return false;
        }
        // A connection from the next generation: the peer is back, whatever the table says.
        end_window_locked(l, monotonic_ms());
    }
    if (l.fd >= 0) {
        ::close(l.fd);
    }
    l.fd = fd;
    l.peer_generation = peer_generation;
    watch_link(peer, fd);
    l.changed.notify_all();
    return true;
}

bool FMI::Comm::LocalDrain::yield_to_drain(Utils::peer_num peer) {
    if (!drain_pending.load(std::memory_order_acquire)) {
        return false;
    }
    // This rank wants to leave and waits for the gate this establishment holds. Letting go
    // here, between two attempts and owning no half-built connection, bounds the drain.
    LinkState& l = link(peer);
    std::lock_guard<std::mutex> lock(l.mu);
    if (!l.draining) {
        l.draining = true;
        l.draining_since_ms = monotonic_ms();
        l.unconfirmed = false;
    }
    l.changed.notify_all();
    return true;
}

void FMI::Comm::LocalDrain::dial_peer(Utils::peer_num peer, long deadline_ms) {
    LinkState& l = link(peer);
    while (true) {
        {
            std::lock_guard<std::mutex> lock(l.mu);
            throw_if_broken(l, peer);
            if (l.fd >= 0 || l.draining || l.peer_gone) {
                return;   // a finalized peer is not dialled; the data path says what that means
            }
        }
        if (yield_to_drain(peer)) {
            return;
        }
        const long now = monotonic_ms();
        if (now >= deadline_ms || stopping.load(std::memory_order_acquire)) {
            throw Utils::Timeout();
        }
        PeerAddr addr;
        if (!lookup_peer(peer, addr, std::min<long>(wait_slice_ms, deadline_ms - now))) {
            continue;   // no row yet, or the peer is leaving: the next row ends the wait
        }
        const int fd = connect_to(addr, deadline_ms);
        if (fd >= 0) {
            if (hello_as_dialer(peer, fd, addr.nonce, deadline_ms)) {
                return;
            }
            ::close(fd);
        }
        // The row may be older than the peer: wait for a newer one, or try again shortly.
        table.wait_newer(peer, table.get(peer).version(),
                         std::min<long>(connect_retry_interval_ms, deadline_ms - monotonic_ms()));
    }
}

void FMI::Comm::LocalDrain::await_peer(Utils::peer_num peer, long deadline_ms) {
    LinkState& l = link(peer);
    try {
        ask_where(peer);   // the higher rank dials; asking makes sure its departure is told
    } catch (const std::exception&) {
    }
    while (true) {
        if (yield_to_drain(peer)) {
            return;
        }
        if (monotonic_ms() >= deadline_ms || stopping.load(std::memory_order_acquire)) {
            throw Utils::Timeout();
        }
        std::unique_lock<std::mutex> lock(l.mu);
        throw_if_broken(l, peer);
        if (l.fd >= 0 || l.draining || l.peer_gone) {
            return;
        }
        l.changed.wait_for(lock, std::chrono::milliseconds(wait_slice_ms));
    }
}

void FMI::Comm::LocalDrain::ensure_link(Utils::peer_num peer, long deadline_ms) {
    LinkState& l = link(peer);
    {
        std::lock_guard<std::mutex> lock(l.mu);
        throw_if_broken(l, peer);
        if (l.fd >= 0 || l.draining || l.peer_gone) {
            return;   // a draining link is waited out by the data path, not rebuilt here
        }
    }
    // Shared for the whole establishment. A drain takes the gate exclusively.
    std::shared_lock<std::shared_timed_mutex> gate(establish_gate);
    // Rank order decides the direction, so both ends agree without talking.
    if (peer < peer_id) {
        dial_peer(peer, deadline_ms);
    } else {
        await_peer(peer, deadline_ms);
    }
}

void FMI::Comm::LocalDrain::wait_for_link(std::unique_lock<std::mutex>& lock, LinkState& l,
                                          Utils::peer_num peer, OperationClock& clock) {
    const long now = monotonic_ms();
    clock.absorb(l.migration_ms_total);
    if (l.draining) {
        const long elapsed = now - l.draining_since_ms;
        if (l.unconfirmed && elapsed >= drain_grace_ms) {
            throw std::runtime_error(
                    "LocalDrain: rank " + std::to_string(peer_id) + " lost rank " +
                    std::to_string(peer) + ": the connection closed " + std::to_string(elapsed) +
                    " ms ago and the rank table reports no move (a rank that dies is not "
                    "recovered)");
        }
        if (elapsed >= migration_max_ms) {
            throw std::runtime_error("LocalDrain: rank " + std::to_string(peer_id) + ": rank " +
                                     std::to_string(peer) + " has been away for " +
                                     std::to_string(elapsed) + " ms, past migration_max_ms=" +
                                     std::to_string(migration_max_ms));
        }
    } else if (clock.expired(now)) {
        throw Utils::Timeout();
    }
    // The lock is released for the whole wait: nothing in this channel waits holding one.
    l.changed.wait_for(lock, std::chrono::milliseconds(wait_slice_ms));
}

// --------------------------------------------------------------------------------- data path

void FMI::Comm::LocalDrain::send_object(channel_data buf, Utils::peer_num rcpt_id) {
    ensure_started();
    LinkState& l = link(rcpt_id);
    OperationClock clock;
    {
        // The clock starts under the lock. An operation that had to wait here for a drain to
        // let go has not started yet, and is not charged for that wait.
        std::lock_guard<std::mutex> lock(l.mu);
        clock.started_ms = monotonic_ms();
        clock.budget_ms = static_cast<long>(max_timeout);
        clock.baseline = l.migration_ms_total;
    }
    ensure_link(rcpt_id, clock.deadline(monotonic_ms()));
    std::size_t moved = 0;

    while (moved < buf.len) {
        int wait_fd = -1;
        bool ask = false;
        {
            std::unique_lock<std::mutex> lock(l.mu);
            throw_if_broken(l, rcpt_id);
            throw_if_abandoned(l, rcpt_id, moved, buf.len);
            if (l.draining) {
                wait_for_link(lock, l, rcpt_id, clock);
                continue;
            }
            if (l.fd < 0) {
                // The thread that wants the link rebuilds it, and continues at its offset.
                clock.absorb(l.migration_ms_total);
                const long deadline = clock.deadline(monotonic_ms());
                lock.unlock();
                ensure_link(rcpt_id, deadline);
                continue;
            }
            // One non-blocking system call per turn with the lock.
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
                clock.absorb(l.migration_ms_total);
                if (clock.expired(monotonic_ms())) {
                    throw Utils::Timeout();
                }
                wait_fd = l.fd;
            } else if (err == EPIPE || err == ECONNRESET) {
                // The peer closed. Most often it is leaving; the row says so, or the grace
                // period turns this into the death it then was.
                take_readable_locked(l, l.fd);
                close_after_peer_locked(l, rcpt_id, monotonic_ms());
                ask = l.unconfirmed;
            } else {
                throw std::runtime_error(
                        "LocalDrain: rank " + std::to_string(peer_id) + " to rank " +
                        std::to_string(rcpt_id) + ": send failed after " + std::to_string(moved) +
                        "/" + std::to_string(buf.len) + " bytes: " +
                        (n == 0 ? "nothing was accepted" : strerror(err)));
            }
        }
        if (wait_fd >= 0) {
            struct pollfd pfd{wait_fd, POLLOUT, 0};
            ::poll(&pfd, 1, wait_slice_ms);
        } else if (ask) {
            try {
                ask_where(rcpt_id);
            } catch (const std::exception&) {
            }
        }
    }
}

void FMI::Comm::LocalDrain::recv_object(channel_data buf, Utils::peer_num sender_id) {
    ensure_started();
    LinkState& l = link(sender_id);
    OperationClock clock;
    {
        std::lock_guard<std::mutex> lock(l.mu);
        clock.started_ms = monotonic_ms();
        clock.budget_ms = static_cast<long>(max_timeout);
        clock.baseline = l.migration_ms_total;
    }
    // No establishment before the loop: bytes that a drain moved into memory are delivered as
    // far as the counters go, and a message they complete needs no connection at all.
    std::size_t moved = 0;

    while (moved < buf.len) {
        int wait_fd = -1;
        bool ask = false;
        {
            std::unique_lock<std::mutex> lock(l.mu);
            throw_if_broken(l, sender_id);
            if (l.drained_at < l.drained.size()) {
                // Drained bytes first, always: they left the socket before it was closed, and
                // the connection that replaces it carries only what was sent afterwards.
                const std::size_t take = std::min(buf.len - moved, l.drained.size() - l.drained_at);
                std::memcpy(buf.buf + moved, l.drained.data() + l.drained_at, take);
                l.drained_at += take;
                moved += take;
                if (l.drained_at == l.drained.size()) {
                    std::vector<char>().swap(l.drained);   // freed the moment it is spent
                    l.drained_at = 0;
                }
                continue;
            }
            throw_if_abandoned(l, sender_id, moved, buf.len);
            if (l.draining) {
                wait_for_link(lock, l, sender_id, clock);
                continue;
            }
            if (l.fd < 0) {
                clock.absorb(l.migration_ms_total);
                const long deadline = clock.deadline(monotonic_ms());
                lock.unlock();
                ensure_link(sender_id, deadline);
                continue;
            }
            const ssize_t n = ::recv(l.fd, buf.buf + moved, buf.len - moved, MSG_DONTWAIT);
            if (n > 0) {
                moved += static_cast<std::size_t>(n);
                l.bytes_received += static_cast<std::uint64_t>(n);
                continue;
            }
            const int err = n < 0 ? errno : 0;
            if (n < 0 && err == EINTR) {
                continue;
            }
            if (n < 0 && (err == EAGAIN || err == EWOULDBLOCK)) {
                clock.absorb(l.migration_ms_total);
                if (clock.expired(monotonic_ms())) {
                    throw Utils::Timeout();
                }
                wait_fd = l.fd;
            } else if (n == 0 || err == ECONNRESET || err == EPIPE) {
                close_after_peer_locked(l, sender_id, monotonic_ms());
                ask = l.unconfirmed;
            } else {
                throw std::runtime_error(
                        "LocalDrain: rank " + std::to_string(peer_id) + " from rank " +
                        std::to_string(sender_id) + ": receive failed after " +
                        std::to_string(moved) + "/" + std::to_string(buf.len) + " bytes: " +
                        strerror(err));
            }
        }
        if (wait_fd >= 0) {
            struct pollfd pfd{wait_fd, POLLIN, 0};
            ::poll(&pfd, 1, wait_slice_ms);
        } else if (ask) {
            try {
                ask_where(sender_id);
            } catch (const std::exception&) {
            }
        }
    }
}

double FMI::Comm::LocalDrain::get_latency(Utils::peer_num producer, Utils::peer_num consumer,
                                          std::size_t size_in_bytes) {
    const double trans_time = producer * consumer * ((double) size_in_bytes / 1000000.) / bandwidth;
    return log2(producer + consumer) * overhead + trans_time;
}

double FMI::Comm::LocalDrain::get_price(Utils::peer_num producer, Utils::peer_num consumer,
                                        std::size_t size_in_bytes) {
    double total_costs =
            2 * consumer * producer * ((double) size_in_bytes / 1000000000.) * transfer_price;
    if (include_infrastructure_costs) {
        total_costs += 1. / requests_per_hour * vm_price;
    }
    return total_costs;
}

// ---------------------------------------------------------------------------------- draining

bool FMI::Comm::LocalDrain::take_readable_locked(LinkState& l, int fd) {
    std::vector<char> chunk(drain_chunk_bytes);
    while (true) {
        const ssize_t n = ::recv(fd, chunk.data(), chunk.size(), MSG_DONTWAIT);
        if (n > 0) {
            // Appended at the end: these are bytes the application has not consumed yet, and a
            // receive takes this buffer before it reads a socket again.
            l.drained.insert(l.drained.end(), chunk.data(), chunk.data() + n);
            l.bytes_received += static_cast<std::uint64_t>(n);
            continue;
        }
        if (n == 0) {
            return true;
        }
        if (errno == EINTR) {
            continue;
        }
        // Nothing more for now is not the end. A reset is: bytes may be lost with it, and the
        // counters say so at the next reconnect.
        return errno != EAGAIN && errno != EWOULDBLOCK;
    }
}

void FMI::Comm::LocalDrain::end_window_locked(LinkState& l, long now_ms) {
    if (l.draining) {
        const long elapsed = now_ms - l.draining_since_ms;
        if (elapsed > 0) {
            l.migration_ms_total += static_cast<std::uint64_t>(elapsed);
        }
        l.draining = false;
    }
    l.unconfirmed = false;
}

void FMI::Comm::LocalDrain::close_after_peer_locked(LinkState& l, Utils::peer_num peer, long now_ms) {
    if (l.fd >= 0) {
        // Closed, not half-closed: this end has nothing more to say on this connection, and
        // the FIN it sends is what lets the leaving rank read to EOF.
        ::close(l.fd);
        l.fd = -1;
    }
    if (l.peer_gone) {
        l.changed.notify_all();
        return;   // a finalized peer closes its links; there is nothing to wait out
    }
    const RankRow row = table.get(peer);
    if (row.known && !row.leaving && row.generation > l.peer_generation) {
        // The table already knows a later generation: the peer left and is back.
        end_window_locked(l, now_ms);
        l.changed.notify_all();
        return;
    }
    std::uint64_t away = l.peer_generation;
    bool confirmed = false;
    if (l.leaving_known && l.leaving_generation >= l.peer_generation) {
        confirmed = true;
        away = std::max(away, l.leaving_generation);
    }
    if (row.known && row.leaving && row.generation >= l.peer_generation) {
        confirmed = true;
        away = std::max(away, row.generation);
    }
    if (!l.draining) {
        l.draining = true;
        l.draining_since_ms = now_ms;
    }
    l.away_generation = away;
    l.unconfirmed = !confirmed;
    l.changed.notify_all();
}

void FMI::Comm::LocalDrain::break_every_link(const std::string& reason, bool locks_held) {
    for (auto& state : links) {
        if (state == nullptr) {
            continue;
        }
        if (locks_held) {
            state->broken = reason;
            state->changed.notify_all();
        } else {
            std::lock_guard<std::mutex> lock(state->mu);
            state->broken = reason;
            state->changed.notify_all();
        }
    }
}

void FMI::Comm::LocalDrain::drain_all_links() {
    struct Open {
        Utils::peer_num peer;
        int fd;
    };
    std::vector<Open> open;
    for (Utils::peer_num p = 0; p < links.size(); p++) {
        if (p != peer_id && links[p]->fd >= 0) {
            open.push_back({p, links[p]->fd});
        }
    }
    const std::vector<Open> all = open;
    // Every socket half-closed before any is read. Never one link after the other: two ranks
    // leaving together are each other's peers, and each would wait for a FIN the other has
    // not sent because it waits for the first one's.
    for (const auto& o : open) {
        ::shutdown(o.fd, SHUT_WR);
    }

    const long absolute_deadline = monotonic_ms() + drain_budget_ms();
    long quiet_deadline = monotonic_ms() + drain_grace_ms;
    std::vector<char> chunk(drain_chunk_bytes);
    std::vector<Utils::peer_num> stuck;

    while (!open.empty()) {
        bool progress = false;
        for (std::size_t i = 0; i < open.size();) {
            LinkState& l = *links[open[i].peer];
            const ssize_t n = ::recv(open[i].fd, chunk.data(), chunk.size(), MSG_DONTWAIT);
            if (n > 0) {
                l.drained.insert(l.drained.end(), chunk.data(), chunk.data() + n);
                l.bytes_received += static_cast<std::uint64_t>(n);
                progress = true;
                continue;   // the same descriptor again: it may hold more
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
                // A reset is not an end: bytes may have gone with it.
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
            for (const auto& o : open) {
                stuck.push_back(o.peer);
            }
            break;
        }
        std::vector<struct pollfd> pfds;
        pfds.reserve(open.size());
        for (const auto& o : open) {
            pfds.push_back({o.fd, POLLIN, 0});
        }
        const long budget = std::min<long>({static_cast<long>(wait_slice_ms), quiet_deadline - now,
                                            absolute_deadline - now});
        ::poll(pfds.data(), pfds.size(), static_cast<int>(std::max<long>(budget, 0)));
    }

    for (const auto& o : all) {
        ::close(o.fd);
        links[o.peer]->fd = -1;
    }
    if (!stuck.empty()) {
        std::string names;
        for (const auto peer : stuck) {
            names += (names.empty() ? "" : ", ") + std::to_string(peer);
        }
        // Silence is not EOF. A rank that cannot account for every byte in flight must not be
        // stopped, because nothing afterwards would find out.
        throw std::runtime_error("LocalDrain: rank " + std::to_string(peer_id) +
                                 " could not drain: rank(s) " + names +
                                 " did not close their side within drain_grace_ms=" +
                                 std::to_string(drain_grace_ms));
    }
}

// -------------------------------------------------------------------- leaving and coming back

void FMI::Comm::LocalDrain::leave(const std::string& move) {
    const std::uint64_t generation = own_generation.load(std::memory_order_acquire);
    const long started_ms = monotonic_ms();
    in_transition = true;
    struct Done {
        bool& flag;
        ~Done() { flag = false; }
    } done{in_transition};
    BOOST_LOG_TRIVIAL(info) << "LocalDrain: rank " << peer_id << " of " << comm_name
                            << " leaves at generation " << generation << " (move " << move << ")";

    // 1. The gate, exclusively. An establishment in flight finishes or lets go.
    drain_pending.store(true, std::memory_order_release);
    std::unique_lock<std::shared_timed_mutex> gate(establish_gate, std::defer_lock);
    const bool gated = gate.try_lock_for(std::chrono::milliseconds(std::max<long>(establish_yield_ms, 1)));
    drain_pending.store(false, std::memory_order_release);
    if (!gated) {
        BOOST_LOG_TRIVIAL(error) << "LocalDrain: rank " << peer_id << " could not leave: an "
                                 << "establishment still held the gate after " << establish_yield_ms
                                 << " ms";
        try {
            say("failed move=" + move + " reason=establishment");
        } catch (const std::exception&) {
        }
        return;
    }

    try {
        // 2. No new connection: the listener goes.
        const int listener = listen_fd.exchange(-1, std::memory_order_acq_rel);
        if (listener >= 0) {
            ::close(listener);
        }
        listen_port = 0;
        listener_nonce = 0;

        // 3. Every link lock, in rank order, held from here until this rank is back.
        all_link_locks.reserve(links.size());
        for (auto& state : links) {
            all_link_locks.emplace_back(state->mu);
        }
        holding_all_links = true;
        const long now = monotonic_ms();
        for (Utils::peer_num p = 0; p < links.size(); p++) {
            if (p == peer_id) {
                continue;
            }
            LinkState& l = *links[p];
            if (!l.draining) {
                l.draining = true;
                l.draining_since_ms = now;
            }
            l.unconfirmed = false;   // this rank knows why its links are closing
        }

        // 4. Half-close all, read all to EOF.
        drain_all_links();

        // 5. The report, with the counters the coordinator compares pair by pair.
        std::string line = "drained move=" + move + " generation=" + std::to_string(generation);
        for (Utils::peer_num p = 0; p < links.size(); p++) {
            if (p == peer_id) {
                continue;
            }
            const LinkState& l = *links[p];
            if (l.bytes_sent != 0) {
                line += " s" + std::to_string(p) + "=" + std::to_string(l.bytes_sent);
            }
            if (l.bytes_received != 0) {
                line += " r" + std::to_string(p) + "=" + std::to_string(l.bytes_received);
            }
        }
        say(line);
    } catch (const std::exception& e) {
        // Loud, and left broken: a move that could not complete must end the job, not leave a
        // job running on a channel nobody drained.
        BOOST_LOG_TRIVIAL(error) << "LocalDrain: rank " << peer_id << " could not leave: " << e.what();
        break_every_link(std::string("broken by a move that could not complete: ") + e.what(),
                         holding_all_links);
        holding_all_links = false;
        all_link_locks.clear();
        try {
            say("failed move=" + move + " reason=drain");
        } catch (const std::exception&) {
        }
        return;
    }
    held_gate = std::move(gate);

    // 6. The control link goes last. After this the rank owns no socket at all.
    {
        std::lock_guard<std::mutex> write(control_write_mutex);
        const int control = control_fd.exchange(-1, std::memory_order_acq_rel);
        if (control >= 0) {
            ::close(control);
        }
    }
    control_buffer.clear();

#if defined(__GLIBC__)
    ::malloc_trim(0);
#endif
    std::fflush(nullptr);

    if (stop_by_signal) {
        // The host manager, this rank's parent, sees the stop and checkpoints it. Execution
        // continues here, on whichever host the image was restored.
        ::raise(SIGSTOP);
    } else {
        held.store(true, std::memory_order_release);
        std::unique_lock<std::mutex> lock(hold_mutex);
        hold_released.wait(lock, [this]() { return hold_release_requested; });
        hold_release_requested = false;
        held.store(false, std::memory_order_release);
    }

    come_back();
    BOOST_LOG_TRIVIAL(info) << "LocalDrain: rank " << peer_id << " of " << comm_name
                            << " is back at generation "
                            << own_generation.load(std::memory_order_acquire) << " after "
                            << (monotonic_ms() - started_ms) << " ms";
}

void FMI::Comm::LocalDrain::come_back() {
    try {
        if (stopping.load(std::memory_order_acquire)) {
            throw std::runtime_error("the channel was finalized while the rank was away");
        }
        own_generation.fetch_add(1, std::memory_order_acq_rel);
        const long deadline = monotonic_ms() + drain_budget_ms();
        open_listener();
        open_control_link(deadline);   // its hello, with the new generation, is the "back"

        // What happened while this rank was away: ask again for every peer it knows, and wait
        // until the host manager has answered all of it. Without this a thread would wake up
        // and dial the address a peer had before both of them moved.
        for (Utils::peer_num p = 0; p < links.size(); p++) {
            if (p == peer_id) {
                continue;
            }
            const LinkState& l = *links[p];
            if (l.bytes_sent != 0 || l.bytes_received != 0 || table.get(p).known) {
                ask_where(p);
            }
        }
        const std::uint64_t token = next_sync_token++;
        say("sync token=" + std::to_string(token));
        while (true) {
            {
                std::lock_guard<std::mutex> lock(control_wait_mutex);
                if (synced_token >= token && welcomed) {
                    break;
                }
            }
            if (control_lost.load(std::memory_order_acquire)) {
                throw std::runtime_error(lost_reason());
            }
            if (monotonic_ms() >= deadline) {
                throw std::runtime_error("the host manager did not answer after the restore");
            }
            struct pollfd pfd{control_fd.load(std::memory_order_acquire), POLLIN, 0};
            ::poll(&pfd, 1, wait_slice_ms);
            read_control_lines();
        }

        const long now = monotonic_ms();
        for (Utils::peer_num p = 0; p < links.size(); p++) {
            if (p == peer_id) {
                continue;
            }
            LinkState& l = *links[p];
            // The window of this rank's own move ends here, for every link, and is credited
            // to whoever waited behind the locks.
            end_window_locked(l, now);
            const RankRow row = table.get(p);
            if (row.known && row.leaving && row.generation >= l.peer_generation) {
                // A peer that left with this rank, or after it, and is not back yet: from
                // here on the wait is the peer's move.
                l.draining = true;
                l.draining_since_ms = now;
                l.away_generation = row.generation;
                l.leaving_known = true;
                l.leaving_generation = std::max(l.leaving_generation, row.generation);
            }
            l.changed.notify_all();
        }
    } catch (const std::exception& e) {
        BOOST_LOG_TRIVIAL(error) << "LocalDrain: rank " << peer_id
                                 << " could not come back: " << e.what();
        break_every_link(std::string("broken by a restore that could not complete: ") + e.what(),
                         true);
    }
    // Released last, and in this order: the locks the application threads wait on, then the
    // gate that keeps anyone from building a connection.
    holding_all_links = false;
    all_link_locks.clear();
    held_gate = std::unique_lock<std::shared_timed_mutex>();
}

void FMI::Comm::LocalDrain::release_hold() {
    {
        std::lock_guard<std::mutex> lock(hold_mutex);
        hold_release_requested = true;
    }
    hold_released.notify_all();
}
