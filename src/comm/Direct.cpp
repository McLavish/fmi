#include "../../include/comm/Direct.h"
#include <tcpunch.h>
#include <sys/socket.h>
#include <unistd.h>
#include <boost/log/trivial.hpp>
#include <thread>
#include <netinet/tcp.h>
#include <cmath>
#include <cstring>
#include <stdexcept>

FMI::Comm::Direct::Direct(std::map<std::string, std::string> params, std::map<std::string, std::string> model_params) {
    hostname = params["host"];
    port = std::stoi(params["port"]);
    max_timeout = std::stoi(params["max_timeout"]);
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

void FMI::Comm::Direct::send_object(channel_data buf, Utils::peer_num rcpt_id) {
    check_socket(rcpt_id, comm_name + std::to_string(peer_id) + "_" + std::to_string(rcpt_id));
    // MSG_NOSIGNAL: a peer that closed mid-migration must surface as EPIPE, not kill the
    // process with SIGPIPE. Loop: a blocking send under SO_SNDTIMEO may accept only part of
    // a large buffer, and a silent short send would desynchronize the byte stream.
    std::size_t sent = 0;
    while (sent < buf.len) {
        long n = ::send(sockets[rcpt_id], buf.buf + sent, buf.len - sent, MSG_NOSIGNAL);
        if (n == -1) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                throw Utils::Timeout();
            }
            throw std::runtime_error("Direct: send to peer " + std::to_string(rcpt_id) +
                                     " failed after " + std::to_string(sent) + "/" +
                                     std::to_string(buf.len) + " bytes: " + strerror(errno));
        }
        sent += static_cast<std::size_t>(n);
    }
}

void FMI::Comm::Direct::recv_object(channel_data buf, Utils::peer_num sender_id) {
    check_socket(sender_id, comm_name + std::to_string(sender_id) + "_" + std::to_string(peer_id));
    // Never return with a partially filled buffer: EOF or an error mid-message must be loud.
    // A silent short read here hands garbage to the collective and corrupts the reduction.
    // MSG_WAITALL under SO_RCVTIMEO can legitimately return a partial chunk on timeout, so
    // loop while progress is made and only throw Timeout when a call yields nothing.
    std::size_t received = 0;
    while (received < buf.len) {
        long n = ::recv(sockets[sender_id], buf.buf + received, buf.len - received, MSG_WAITALL);
        if (n == 0) {
            throw std::runtime_error("Direct: connection to peer " + std::to_string(sender_id) +
                                     " closed after " + std::to_string(received) + "/" +
                                     std::to_string(buf.len) + " bytes");
        }
        if (n == -1) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                throw Utils::Timeout();
            }
            throw std::runtime_error("Direct: recv from peer " + std::to_string(sender_id) +
                                     " failed after " + std::to_string(received) + "/" +
                                     std::to_string(buf.len) + " bytes: " + strerror(errno));
        }
        received += static_cast<std::size_t>(n);
    }
}

namespace {
    std::atomic<unsigned int> total_pairings{0};
}

unsigned int FMI::Comm::Direct::pairing_count() {
    return total_pairings.load();
}

void FMI::Comm::Direct::check_socket(FMI::Utils::peer_num partner_id, std::string pair_name) {
    if (sockets.empty()) {
        sockets = std::vector<int>(num_peers, -1);
    }
    if (sockets[partner_id] == -1) {
        try {
            sockets[partner_id] = pair(pair_name, hostname, port, max_timeout);
            total_pairings.fetch_add(1);
        } catch (Timeout) {
            throw Utils::Timeout();
        }

        struct timeval timeout;
        timeout.tv_sec = max_timeout / 1000;
        timeout.tv_usec = (max_timeout % 1000) * 1000;
        setsockopt(sockets[partner_id], SOL_SOCKET, SO_RCVTIMEO, (const char*)&timeout, sizeof timeout);
        setsockopt(sockets[partner_id], SOL_SOCKET, SO_SNDTIMEO, (const char*)&timeout, sizeof timeout);
        // Disable Nagle algorithm to avoid 40ms TCP ack delays
        int one = 1;
        // SOL_TCP not defined on macOS
        #if !defined(SOL_TCP) && defined(IPPROTO_TCP)
        #define SOL_TCP IPPROTO_TCP
        #endif
        setsockopt(sockets[partner_id], SOL_TCP, TCP_NODELAY, &one, sizeof(one));
    }
}

double FMI::Comm::Direct::get_latency(Utils::peer_num producer, Utils::peer_num consumer, std::size_t size_in_bytes) {
    double agg_bandwidth = bandwidth;
    double trans_time = producer * consumer * ((double) size_in_bytes / 1000000.) / agg_bandwidth;
    return log2(producer + consumer) * overhead + trans_time;
}

double FMI::Comm::Direct::get_price(Utils::peer_num producer, Utils::peer_num consumer, std::size_t size_in_bytes) {
    double transfer_costs = 2 * consumer * producer * ((double) size_in_bytes / 1000000000.) * transfer_price;
    double total_costs = transfer_costs;
    if (include_infrastructure_costs) {
        total_costs += 1. / requests_per_hour * vm_price;
    }
    return total_costs;
}

void FMI::Comm::Direct::finalize() {
    close_sockets();
}

void FMI::Comm::Direct::prepare_for_checkpoint() {
    close_sockets();
}

bool FMI::Comm::Direct::reconfigure_for_epoch(const std::string& new_comm_name,
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

void FMI::Comm::Direct::close_sockets() {
    for (auto& socket_fd : sockets) {
        if (socket_fd >= 0) {
            close(socket_fd);
            socket_fd = -1;
        }
    }
}
