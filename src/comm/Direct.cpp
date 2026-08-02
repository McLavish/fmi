#include "../../include/comm/Direct.h"
#include <tcpunch.h>
#include <atomic>

FMI::Comm::Direct::Direct(std::map<std::string, std::string> params, std::map<std::string, std::string> model_params) {
    transport_tag = "Direct";
    hostname = params["host"];
    port = std::stoi(params["port"]);
    parse_tcp_params(params);
    parse_tcp_model_params(model_params);
}

std::string FMI::Comm::Direct::link_name(Utils::peer_num partner_id, bool outbound) const {
    if (outbound) {
        return comm_name + std::to_string(peer_id) + "_" + std::to_string(partner_id);
    }
    return comm_name + std::to_string(partner_id) + "_" + std::to_string(peer_id);
}

int FMI::Comm::Direct::establish(Utils::peer_num, const std::string& link_name) {
    try {
        return pair(link_name, hostname, port, max_timeout);
    // The unqualified name below is TCPunch's GLOBAL ::Timeout from <tcpunch.h>: neither FMI
    // nor FMI::Comm declares a Timeout, so unqualified lookup falls through to global scope.
    // Do not "fix" this to FMI::Utils::Timeout — that catch would silently never fire, and
    // TCPunch's timeout would escape untranslated to callers that only handle FMI's.
    } catch (Timeout) {
        throw Utils::Timeout();
    }
}
