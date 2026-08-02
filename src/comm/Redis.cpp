#include "../../include/comm/Redis.h"
#include "../../include/utils/Signals.h"
#include <boost/log/trivial.hpp>
#include <cmath>
#include <cstring>
#include <stdexcept>

FMI::Comm::Redis::Redis(std::map<std::string, std::string> params, std::map<std::string, std::string> model_params) : RecoverableClientServer(params) {
    hostname = params["host"];
    port = std::stoi(params["port"]);
    bandwidth_single = std::stod(model_params["bandwidth_single"]);
    bandwidth_multiple = std::stod(model_params["bandwidth_multiple"]);
    overhead = std::stod(model_params["overhead"]);
    transfer_price = std::stod(model_params["transfer_price"]);
    instance_price = std::stod(model_params["instance_price"]);
    requests_per_hour = std::stoi(model_params["requests_per_hour"]);
    if (model_params["include_infrastructure_costs"] == "true") {
        include_infrastructure_costs = true;
    } else {
        include_infrastructure_costs = false;
    }

    context = nullptr;
    // See Utils::suppress_sigpipe: hiredis writes without MSG_NOSIGNAL, and after a restore
    // the first write lands on the socket the checkpoint captured and the restore dropped.
    Utils::suppress_sigpipe();
    // Eagerly, as before, so a misconfigured or unreachable store is reported at construction
    // rather than at the first collective. connect() only logs, so a store that is down does not
    // stop the channel from being built (nor a rank frozen inside this constructor from restoring
    // into a working one).
    connect();
}

FMI::Comm::Redis::~Redis() {
    drop_connection();
}

void FMI::Comm::Redis::connect() {
    drop_connection();
    context = redisConnect(hostname.c_str(), port);
    if (context == nullptr || context->err) {
        last_error = context == nullptr ? "could not allocate a Redis context" : context->errstr;
        if (!connection_warned) {
            connection_warned = true;
            BOOST_LOG_TRIVIAL(error) << "Redis: could not connect to " << hostname << ":" << port
                                     << ": " << last_error;
        }
    } else {
        // Re-arm the latch, so a later outage is reported once too.
        connection_warned = false;
        last_error.clear();
    }
}

void FMI::Comm::Redis::drop_connection() {
    if (context != nullptr) {
        if (context->err && context->errstr[0] != '\0') {
            last_error = context->errstr;
        }
        redisFree(context);
        context = nullptr;
    }
}

std::string FMI::Comm::Redis::last_failure() const {
    if (last_error.empty()) {
        return "no connection to " + hostname + ":" + std::to_string(port);
    }
    return last_error;
}

//! One command, one attempt, arguments passed out of band.
/*!
 * redisCommandArgv rather than redisCommand: the latter takes a printf format, and the callers
 * build their argument from a key name, which carries a user-chosen communicator name — a '%'
 * or a space in it corrupts the command.
 *
 * A NULL reply means hiredis could not complete the command (the socket died, or the context was
 * already in an error state). The context is dropped rather than reused: a half-written command
 * leaves bytes in its output buffer and a half-read reply in its reader, and reusing it would
 * prepend those to the next command. The caller decides what an incomplete command means for it —
 * an empty ReplyPtr is never a value.
 *
 * DirectTCP::Registry (src/comm/DirectTCP.cpp:131) is the sibling of this helper for the peer
 * registry. The two stay separate deliberately: the registry pipelines batches, treats an error
 * reply as fatal and is called from several threads, none of which holds for a data plane whose
 * poll loops must keep going. A shared helper is worth revisiting once both have settled.
 */
FMI::Comm::Redis::ReplyPtr FMI::Comm::Redis::command(int argc, const char** argv, const std::size_t* argvlen) {
    if (context == nullptr || context->err) {
        connect();
    }
    if (context == nullptr) {
        return ReplyPtr();
    }
    auto* reply = static_cast<redisReply*>(redisCommandArgv(context, argc, argv, argvlen));
    if (reply == nullptr) {
        drop_connection();
        return ReplyPtr();
    }
    return ReplyPtr(reply);
}

void FMI::Comm::Redis::upload_object(channel_data buf, std::string name) {
    const char* argv[] = {"SET", name.c_str(), buf.buf};
    const std::size_t argvlen[] = {sizeof("SET") - 1, name.size(), buf.len};
    auto reply = command(3, argv, argvlen);
    if (!reply) {
        throw std::runtime_error("Redis: could not SET " + name + ": " + last_failure());
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        BOOST_LOG_TRIVIAL(error) << "Error when uploading to Redis: "
                                 << (reply->str == nullptr ? "unknown error" : reply->str);
    }
}

bool FMI::Comm::Redis::download_object(channel_data buf, std::string name) {
    const char* argv[] = {"GET", name.c_str()};
    const std::size_t argvlen[] = {sizeof("GET") - 1, name.size()};
    auto reply = command(2, argv, argvlen);
    if (!reply) {
        throw std::runtime_error("Redis: could not GET " + name + ": " + last_failure());
    }
    if (reply->type == REDIS_REPLY_NIL || reply->type == REDIS_REPLY_ERROR || reply->str == nullptr) {
        // The str guard is unreachable for GET, which answers with a string, a nil or an error;
        // it is here so that no reply shape can turn into a copy from a null pointer.
        return false;
    }
    std::memcpy(buf.buf, reply->str, std::min(buf.len, reply->len));
    return true;
}

void FMI::Comm::Redis::delete_object(std::string name) {
    const char* argv[] = {"DEL", name.c_str()};
    const std::size_t argvlen[] = {sizeof("DEL") - 1, name.size()};
    auto reply = command(2, argv, argvlen);
    if (!reply) {
        // Deletion runs from finalize(), which runs from the communicator's destructor. Nothing
        // may leave this function: the objects outlive the process either way.
        BOOST_LOG_TRIVIAL(warning) << "Redis: could not DEL " << name << ": " << last_failure();
        return;
    }
    // The number of keys removed is not interesting: an object that is already gone is fine.
}

std::vector<std::string> FMI::Comm::Redis::get_object_names() {
    const char* argv[] = {"KEYS", "*"};
    const std::size_t argvlen[] = {sizeof("KEYS") - 1, sizeof("*") - 1};
    auto reply = command(2, argv, argvlen);
    if (!reply) {
        throw std::runtime_error("Redis: could not list objects: " + last_failure());
    }
    std::vector<std::string> keys;
    if (reply->type != REDIS_REPLY_ARRAY) {
        return keys;
    }
    keys.reserve(reply->elements);
    for (std::size_t i = 0; i < reply->elements; i++) {
        const redisReply* element = reply->element[i];
        if (element == nullptr || element->str == nullptr) {
            continue;
        }
        // Length-based: a key is an arbitrary byte string, not a C string.
        keys.emplace_back(element->str, element->len);
    }
    return keys;
}

double FMI::Comm::Redis::get_latency(Utils::peer_num producer, Utils::peer_num consumer, std::size_t size_in_bytes) {
    double agg_bandwidth = std::min(producer * consumer * bandwidth_single, bandwidth_multiple);
    double trans_time = producer * consumer * ((double) size_in_bytes / 1000000.) / agg_bandwidth;
    return log2(producer + consumer) * overhead + trans_time;
}

double FMI::Comm::Redis::get_price(Utils::peer_num producer, Utils::peer_num consumer, std::size_t size_in_bytes) {
    double transfer_costs = (1 + consumer) * producer * ((double) size_in_bytes / 1000000000.) * transfer_price;
    double total_costs = transfer_costs;
    if (include_infrastructure_costs) {
        total_costs += 1. / requests_per_hour * instance_price;
    }
    return total_costs;
}

