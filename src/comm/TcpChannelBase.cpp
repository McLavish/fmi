#include "../../include/comm/TcpChannelBase.h"

#include <sys/socket.h>
#include <unistd.h>
#include <netinet/tcp.h>
#include <algorithm>
#include <cerrno>
#include <cmath>
#include <cstring>
#include <stdexcept>

void FMI::Comm::TcpChannelBase::parse_tcp_params(std::map<std::string, std::string>& params) {
    max_timeout = std::stoi(params["max_timeout"]);
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

void FMI::Comm::TcpChannelBase::send_object(channel_data buf, Utils::peer_num rcpt_id) {
    check_socket(rcpt_id, link_name(rcpt_id, true));
    // MSG_NOSIGNAL: a peer that closed mid-migration must surface as EPIPE, not kill the
    // process with SIGPIPE. Loop: a blocking send under SO_SNDTIMEO may accept only part of
    // a large buffer, and a silent short send would desynchronize the byte stream.
    std::size_t sent = 0;
    while (sent < buf.len) {
        long n = ::send(sockets[rcpt_id], buf.buf + sent, buf.len - sent, MSG_NOSIGNAL);
        if (n == -1) {
            if (errno == EINTR) {
                continue;
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                throw Utils::Timeout();
            }
            throw std::runtime_error(transport_tag + ": send to peer " + std::to_string(rcpt_id) +
                                     " failed after " + std::to_string(sent) + "/" +
                                     std::to_string(buf.len) + " bytes: " + strerror(errno));
        }
        sent += static_cast<std::size_t>(n);
    }
}

void FMI::Comm::TcpChannelBase::recv_object(channel_data buf, Utils::peer_num sender_id) {
    check_socket(sender_id, link_name(sender_id, false));
    // Never return with a partially filled buffer: EOF or an error mid-message must be loud.
    // A silent short read here hands garbage to the collective and corrupts the reduction.
    // MSG_WAITALL under SO_RCVTIMEO can legitimately return a partial chunk on timeout, so
    // loop while progress is made and only throw Timeout when a call yields nothing.
    std::size_t received = 0;
    while (received < buf.len) {
        long n = ::recv(sockets[sender_id], buf.buf + received, buf.len - received, MSG_WAITALL);
        if (n == 0) {
            if (received == 0 && eof_before_data_is_timeout) {
                throw Utils::Timeout();
            }
            throw std::runtime_error(transport_tag + ": connection to peer " + std::to_string(sender_id) +
                                     " closed after " + std::to_string(received) + "/" +
                                     std::to_string(buf.len) + " bytes");
        }
        if (n == -1) {
            if (errno == EINTR) {
                continue;
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
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
                                     std::to_string(buf.len) + " bytes: " + strerror(errno));
        }
        received += static_cast<std::size_t>(n);
    }
}

void FMI::Comm::TcpChannelBase::check_socket(FMI::Utils::peer_num partner_id, const std::string& name) {
    if (sockets.empty()) {
        sockets = std::vector<int>(num_peers, -1);
    }
    if (sockets[partner_id] == -1) {
        sockets[partner_id] = establish(partner_id, name);
        apply_socket_options(sockets[partner_id]);
    }
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

void FMI::Comm::TcpChannelBase::prepare_for_checkpoint() {
    close_sockets();
    close_transport_state();
}

bool FMI::Comm::TcpChannelBase::reconfigure_for_epoch(const std::string& new_comm_name,
                                                      const std::vector<Utils::peer_num>& moved_ranks) {
    for (auto rank : moved_ranks) {
        if (rank < sockets.size() && sockets[rank] >= 0) {
            close(sockets[rank]);
            sockets[rank] = -1;
        }
    }
    set_comm_name(new_comm_name);
    return true;
}

void FMI::Comm::TcpChannelBase::close_sockets() {
    for (auto& socket_fd : sockets) {
        if (socket_fd >= 0) {
            close(socket_fd);
            socket_fd = -1;
        }
    }
}
