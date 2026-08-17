#include "../../include/comm/Direct.h"
#include <tcpunch.h>
#include <sys/socket.h>
#include <boost/log/trivial.hpp>
#include <thread>
#include <netinet/tcp.h>
#include <cmath>
#include <cerrno>
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
    // send() on a stream socket may accept fewer bytes than it was offered — the kernel's send
    // buffer is finite, and a large message routinely takes several calls. A single send() whose
    // short return was only logged left the peer waiting for bytes this rank believed it had
    // delivered, and the operation completed with a silently truncated message.
    std::size_t total = 0;
    while (total < buf.len) {
        long sent = ::send(sockets[rcpt_id], buf.buf + total, buf.len - total, 0);
        if (sent > 0) {
            total += static_cast<std::size_t>(sent);
            continue;
        }
        if (sent < 0 && errno == EINTR) {
            continue;
        }
        if (sent < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            // SO_SNDTIMEO lapsed. Report it as a timeout, as before.
            throw Utils::Timeout();
        }
        throw std::runtime_error("FMI: Direct: sending to peer " + std::to_string(rcpt_id) +
                                 " failed after " + std::to_string(total) + " of " +
                                 std::to_string(buf.len) + " bytes: " + std::strerror(errno));
    }
}

void FMI::Comm::Direct::recv_object(channel_data buf, Utils::peer_num sender_id) {
    check_socket(sender_id, comm_name + std::to_string(sender_id) + "_" + std::to_string(peer_id));
    // MSG_WAITALL does not survive a socket timeout: when SO_RCVTIMEO lapses it returns the bytes
    // that did arrive rather than the whole message. The previous code logged that case and
    // returned, handing the caller a partly-filled buffer whose tail was uninitialised — for a
    // reduction, silently wrong data. Loop instead, and distinguish "nothing arrived" (a timeout)
    // from "half a message arrived" (a broken link), which are not the same failure.
    std::size_t total = 0;
    while (total < buf.len) {
        long received = ::recv(sockets[sender_id], buf.buf + total, buf.len - total, 0);
        if (received > 0) {
            total += static_cast<std::size_t>(received);
            continue;
        }
        if (received == 0) {
            throw std::runtime_error("FMI: Direct: peer " + std::to_string(sender_id) +
                                     " closed the connection after " + std::to_string(total) +
                                     " of " + std::to_string(buf.len) + " bytes");
        }
        if (errno == EINTR) {
            continue;
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            if (total == 0) {
                throw Utils::Timeout();
            }
            throw std::runtime_error("FMI: Direct: receiving from peer " + std::to_string(sender_id) +
                                     " stalled after " + std::to_string(total) + " of " +
                                     std::to_string(buf.len) + " bytes");
        }
        throw std::runtime_error("FMI: Direct: receiving from peer " + std::to_string(sender_id) +
                                 " failed after " + std::to_string(total) + " of " +
                                 std::to_string(buf.len) + " bytes: " + std::strerror(errno));
    }
}

void FMI::Comm::Direct::check_socket(FMI::Utils::peer_num partner_id, std::string pair_name) {
    if (sockets.empty()) {
        sockets = std::vector<int>(num_peers, -1);
    }
    if (sockets[partner_id] == -1) {
        try {
            sockets[partner_id] = pair(pair_name, hostname, port, max_timeout);
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
