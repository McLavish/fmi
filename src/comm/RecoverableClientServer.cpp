#include "../../include/comm/RecoverableClientServer.h"

#include <stdexcept>
#include <string>

namespace {

    //! The value a key was configured with, signed, or an error naming the key.
    /*!
     * Signed on purpose: timeout and max_timeout are unsigned members, so a configured "-1" has
     * already become 4294967295 by the time it is one, and a check on the member could not tell
     * that from a deliberate value. The raw string is also what any complaint should quote back.
     */
    long configured(const std::map<std::string, std::string>& params, const std::string& key) {
        auto it = params.find(key);
        if (it == params.end() || it->second.empty()) {
            // Unreachable via the ClientServer constructor, which runs first and whose std::stoi
            // rejects a missing or empty value already; here so that this function has an answer
            // for every input rather than an undefined one.
            throw std::runtime_error("ClientServer recover: " + key + " is required");
        }
        return std::stol(it->second);
    }

}

FMI::Comm::RecoverableClientServer::RecoverableClientServer(std::map<std::string, std::string> params)
        : ClientServer(params) {
    // Exact-string "true", the convention TcpChannelBase::parse_tcp_params uses for framed and
    // recover_links (src/comm/TcpChannelBase.cpp:21-22). Config values reach a channel as an
    // untyped map<string, string>, so anything that is not literally "true" — "1", "yes", "True"
    // — leaves the flag off, in both places, rather than each backend inventing its own spelling.
    recover = params.count("recover") > 0 && params.at("recover") == "true";
    if (params.count("object_ttl_s") > 0 && !params.at("object_ttl_s").empty()) {
        object_ttl_s = static_cast<unsigned int>(std::stoul(params.at("object_ttl_s")));
    }

    if (!recover) {
        // Flag off: this class is transparent, and that includes not rejecting a configuration
        // the family accepts today.
        return;
    }

    // The poll budget is the failure detector under recover, so it has to be a budget at all.
    // timeout is the sleep between polls and also the amount elapsed_time advances by, so at 0
    // the ClientServer loops neither sleep nor make progress towards max_timeout: a download of
    // an object that will never appear spins on the store at full speed, forever. That is simply
    // what "timeout": 0 does today, but a channel that offers to survive a broken connection
    // cannot also offer never to notice one, so it is refused here instead of shipped.
    const long poll_interval = configured(params, "timeout");
    const long poll_budget = configured(params, "max_timeout");
    if (poll_interval < 1) {
        throw std::runtime_error("ClientServer recover: timeout must be >= 1, got "
                                 + params.at("timeout"));
    }
    if (poll_budget < poll_interval) {
        // A budget below one interval is a single poll at best and zero at worst; either way the
        // configured patience is not what the operator wrote down.
        throw std::runtime_error("ClientServer recover: max_timeout must be >= timeout ("
                                 + std::to_string(poll_interval) + "), got "
                                 + params.at("max_timeout"));
    }
}

std::string FMI::Comm::RecoverableClientServer::object_key_prefix() const {
    // The separator exists only under recover because it renames every object this channel
    // touches, and a flag-off channel has to keep writing the names it always wrote — including
    // for a peer built from an older revision.
    //
    // Without it the prefix runs straight into a rank number, so communicator "job" rank 11 and
    // communicator "job1" rank 1 both name their first send to rank 2 "job11_2_0": two unrelated
    // jobs read each other's messages, silently and only when the names happen to line up.
    // '|' can appear in neither a rank number nor an operation tag, and is ordinary in both a
    // Redis key and an S3 object name.
    return recover ? comm_name + "|" : comm_name;
}
