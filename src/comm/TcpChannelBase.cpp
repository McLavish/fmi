#include "../../include/comm/TcpChannelBase.h"

#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>
#include <netinet/tcp.h>
#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cmath>
#include <cstring>
#include <initializer_list>
#include <stdexcept>

namespace {
    long steady_now_ms() {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now().time_since_epoch()).count();
    }

    //! How long one wait on a link lasts before the transport is served again.
    constexpr long wait_slice_ms = 20;
}

void FMI::Comm::TcpChannelBase::parse_tcp_params(std::map<std::string, std::string>& params) {
    max_timeout = std::stoi(params["max_timeout"]);
    // Said out loud rather than ignored: a configuration that still asks for sequenced links
    // expects a job that survives a broken connection, and this transport no longer is one.
    for (const char* removed : {"framed", "recover_links"}) {
        if (params.count(removed) > 0 && params[removed] == "true") {
            throw std::runtime_error(transport_tag + ": \"" + removed + "\" is set, but sequenced "
                                     "links were removed from this library; a rank that must "
                                     "survive a move uses the LocalDrain backend");
        }
    }
}

void FMI::Comm::TcpChannelBase::parse_tcp_model_params(std::map<std::string, std::string>& model_params) {
    bandwidth = std::stod(model_params["bandwidth"]);
    overhead = std::stod(model_params["overhead"]);
    transfer_price = std::stod(model_params["transfer_price"]);
    vm_price = std::stod(model_params["vm_price"]);
    requests_per_hour = std::stoi(model_params["requests_per_hour"]);
    if (model_params["include_infrastructure_costs"] == "true") {
        include_infrastructure_costs = true;
    } else {
        include_infrastructure_costs = false;
    }
}

std::string FMI::Comm::TcpChannelBase::link_name(Utils::peer_num partner_id, bool) const {
    auto low = std::min(peer_id, partner_id);
    auto high = std::max(peer_id, partner_id);
    return comm_name + "|" + std::to_string(low) + "-" + std::to_string(high);
}

bool FMI::Comm::TcpChannelBase::pump(Utils::peer_num peer, short events, long deadline_ms) {
    while (true) {
        struct pollfd want{sockets[peer], events, 0};
        // A short slice, not the whole deadline: the listener has to be looked at even while
        // this descriptor stays quiet, and it is not pollable from here (it belongs to the
        // subclass).
        const long remaining = deadline_ms - steady_now_ms();
        if (remaining <= 0) {
            return false;
        }
        const int ready = ::poll(&want, 1, static_cast<int>(std::min(remaining, wait_slice_ms)));
        if (ready > 0) {
            return true;
        }
        if (ready < 0 && errno != EINTR) {
            return false;
        }
        // Nothing yet on the descriptor we need. Pay what we owe everyone else.
        service_transport();
    }
}

void FMI::Comm::TcpChannelBase::write_all(Utils::peer_num rcpt_id, const char* data, std::size_t len) {
    // MSG_NOSIGNAL: a peer that closed must surface as EPIPE, not kill the process with
    // SIGPIPE. Loop: a send may accept only part of a large buffer, and a silent short send
    // would desynchronize the byte stream.
    std::size_t sent = 0;
    const long deadline = steady_now_ms() + static_cast<long>(max_timeout);
    while (sent < len) {
        long n = ::send(sockets[rcpt_id], data + sent, len - sent, MSG_NOSIGNAL | MSG_DONTWAIT);
        if (n == -1) {
            if (errno == EINTR) {
                continue;
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // The socket is full. Wait for room while still serving the transport.
                if (pump(rcpt_id, POLLOUT, deadline)) {
                    continue;
                }
                throw Utils::Timeout();
            }
            throw std::runtime_error(transport_tag + ": send to peer " + std::to_string(rcpt_id) +
                                     " failed after " + std::to_string(sent) + "/" +
                                     std::to_string(len) + " bytes: " + strerror(errno));
        }
        sent += static_cast<std::size_t>(n);
    }
}

void FMI::Comm::TcpChannelBase::read_all(Utils::peer_num sender_id, char* data, std::size_t len) {
    // Never return with a partially filled buffer: EOF or an error mid-message must be loud.
    // A silent short read here hands garbage to the collective and corrupts the reduction.
    std::size_t received = 0;
    const long deadline = steady_now_ms() + static_cast<long>(max_timeout);
    while (received < len) {
        long n = ::recv(sockets[sender_id], data + received, len - received, MSG_DONTWAIT);
        if (n == 0) {
            if (received == 0 && eof_before_data_is_timeout) {
                throw Utils::Timeout();
            }
            throw std::runtime_error(transport_tag + ": connection to peer " + std::to_string(sender_id) +
                                     " closed after " + std::to_string(received) + "/" +
                                     std::to_string(len) + " bytes");
        }
        if (n == -1) {
            if (errno == EINTR) {
                continue;
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // Nothing here yet. Wait for it while serving the transport, which is what
                // stops a rank waiting on one link from leaving a dialing peer unanswered.
                if (pump(sender_id, POLLIN, deadline)) {
                    continue;
                }
                if (received > 0) {
                    // Half a message came and the rest did not. "Nothing arrived in time" is
                    // the wrong thing to say: the bytes already taken are gone from the
                    // stream, so every message boundary after this one is off by however many
                    // were read. Report a broken link instead.
                    throw std::runtime_error(transport_tag + ": message from peer " +
                                             std::to_string(sender_id) + " stalled after " +
                                             std::to_string(received) + "/" +
                                             std::to_string(len) + " bytes");
                }
                throw Utils::Timeout();
            }
            // A peer that tears the connection down before sending any of this message has
            // abandoned the collective; it reaches us as ECONNRESET rather than EOF whenever it
            // closed with data of ours still unread. Same condition as the n == 0 case above.
            if (received == 0 && errno == ECONNRESET && eof_before_data_is_timeout) {
                throw Utils::Timeout();
            }
            throw std::runtime_error(transport_tag + ": recv from peer " + std::to_string(sender_id) +
                                     " failed after " + std::to_string(received) + "/" +
                                     std::to_string(len) + " bytes: " + strerror(errno));
        }
        received += static_cast<std::size_t>(n);
    }
}

// The name of a link is needed to build the link, and for nothing after that. FMI built it on
// every operation: a string per message, and a heap allocation once the name passes 15
// characters. Measured on a compute node, that was about 400 cycles of every ping-pong
// iteration (fmi-spot-migration, benchmarks/REDESIGN-OVERHEAD-ATTRIBUTION-2026-11-11.md). As
// the baseline of the overhead experiments this channel must not carry a cost that plain TCP
// does not have, so the name is built only when the socket is missing.

void FMI::Comm::TcpChannelBase::send_object(channel_data buf, Utils::peer_num rcpt_id) {
    if (rcpt_id >= sockets.size() || sockets[rcpt_id] == -1) {
        check_socket(rcpt_id, link_name(rcpt_id, true));
    }
    write_all(rcpt_id, buf.buf, buf.len);
}

void FMI::Comm::TcpChannelBase::recv_object(channel_data buf, Utils::peer_num sender_id) {
    if (sender_id >= sockets.size() || sockets[sender_id] == -1) {
        check_socket(sender_id, link_name(sender_id, false));
    }
    read_all(sender_id, buf.buf, buf.len);
}

void FMI::Comm::TcpChannelBase::adopt_link(Utils::peer_num partner_id, int fd) {
    if (sockets.empty()) {
        sockets = std::vector<int>(num_peers, -1);
    }
    if (partner_id >= sockets.size() || sockets[partner_id] >= 0) {
        return;   // already hold one; the caller keeps its descriptor
    }
    sockets[partner_id] = fd;
    apply_socket_options(fd);
}

void FMI::Comm::TcpChannelBase::check_socket(FMI::Utils::peer_num partner_id, const std::string& name) {
    if (sockets.empty()) {
        sockets = std::vector<int>(num_peers, -1);
    }
    if (partner_id >= sockets.size()) {
        throw std::runtime_error(transport_tag + ": peer " + std::to_string(partner_id) +
                                 " is out of range for a communicator of " +
                                 std::to_string(num_peers));
    }
    if (sockets[partner_id] != -1) {
        return;
    }
    sockets[partner_id] = establish(partner_id, name);
    apply_socket_options(sockets[partner_id]);
}

void FMI::Comm::TcpChannelBase::apply_socket_options(int fd) const {
    struct timeval timeout;
    timeout.tv_sec = max_timeout / 1000;
    timeout.tv_usec = (max_timeout % 1000) * 1000;
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, (const char*)&timeout, sizeof timeout);
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, (const char*)&timeout, sizeof timeout);
    // Disable Nagle algorithm to avoid 40ms TCP ack delays
    int one = 1;
    // SOL_TCP not defined on macOS
    #if !defined(SOL_TCP) && defined(IPPROTO_TCP)
    #define SOL_TCP IPPROTO_TCP
    #endif
    setsockopt(fd, SOL_TCP, TCP_NODELAY, &one, sizeof(one));
}

double FMI::Comm::TcpChannelBase::get_latency(Utils::peer_num producer, Utils::peer_num consumer, std::size_t size_in_bytes) {
    double agg_bandwidth = bandwidth;
    double trans_time = producer * consumer * ((double) size_in_bytes / 1000000.) / agg_bandwidth;
    return log2(producer + consumer) * overhead + trans_time;
}

double FMI::Comm::TcpChannelBase::get_price(Utils::peer_num producer, Utils::peer_num consumer, std::size_t size_in_bytes) {
    double transfer_costs = 2 * consumer * producer * ((double) size_in_bytes / 1000000000.) * transfer_price;
    double total_costs = transfer_costs;
    if (include_infrastructure_costs) {
        total_costs += 1. / requests_per_hour * vm_price;
    }
    return total_costs;
}

void FMI::Comm::TcpChannelBase::finalize() {
    close_sockets();
    close_transport_state();
}

void FMI::Comm::TcpChannelBase::close_sockets() {
    for (auto& socket_fd : sockets) {
        if (socket_fd >= 0) {
            close(socket_fd);
            socket_fd = -1;
        }
    }
}
