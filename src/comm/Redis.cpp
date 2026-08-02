#include "../../include/comm/Redis.h"
#include "../../include/utils/Signals.h"
#include <boost/log/trivial.hpp>
#include <unistd.h>
#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstring>
#include <stdexcept>
#include <thread>

namespace {

    //! A configured millisecond value, clamped, or the default when the key is absent.
    long timeout_param(const std::map<std::string, std::string>& params, const std::string& key,
                       long fallback, long low, long high) {
        auto it = params.find(key);
        if (it == params.end() || it->second.empty()) {
            return fallback;
        }
        return std::min(high, std::max(low, std::stol(it->second)));
    }

}

FMI::Comm::Redis::Redis(std::map<std::string, std::string> params, std::map<std::string, std::string> model_params) : RecoverableClientServer(params) {
    hostname = params["host"];
    port = std::stoi(params["port"]);
    // A connect that is allowed to take longer than the whole job's patience is not a timeout,
    // and one below a millisecond cannot be expressed; between those, the operator's number.
    connect_timeout_ms = timeout_param(params, "connect_timeout_ms", 1000, 1, 3600000);
    // The lower clamp is what keeps a plausible-looking configuration from turning every command
    // into a race against the store: 100 ms is already far above a healthy round trip, and below
    // it the timeouts would start firing on load rather than on failure.
    io_timeout_ms = timeout_param(params, "io_timeout_ms", 1000, 100, 60000);
    // Under recover they are the point — an unbounded command is a rank that never notices its
    // store is gone. Without it they still apply if the operator asked for them by name, so that
    // a job that wants bounded I/O and nothing else can have exactly that.
    apply_timeouts = recover || params.count("connect_timeout_ms") > 0 ||
                     params.count("io_timeout_ms") > 0;
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
    const auto dial_started = std::chrono::steady_clock::now();
    if (apply_timeouts) {
        struct timeval connect_timeout {connect_timeout_ms / 1000, (connect_timeout_ms % 1000) * 1000};
        context = redisConnectWithTimeout(hostname.c_str(), port, connect_timeout);
        if (context != nullptr && !context->err) {
            // redisConnectWithTimeout bounds the dial only; every command after it needs its own.
            struct timeval io_timeout {io_timeout_ms / 1000, (io_timeout_ms % 1000) * 1000};
            redisSetTimeout(context, io_timeout);
        }
    } else {
        context = redisConnect(hostname.c_str(), port);
    }
    owner_pid = ::getpid();
    if (context == nullptr || context->err) {
        // A dial that fails is not free. Against an address that answers nothing at all it costs
        // the whole connect timeout, while the poll loop that asked for it charges its budget one
        // `timeout` — a millisecond in every shipped config — for the pass that paid it. Dialling
        // on every pass therefore overruns max_timeout by the ratio between the two, which is a
        // thousandfold at the shipped numbers: the budget would stop being a failure detector at
        // exactly the failure it is meant to detect.
        //
        // So a failed dial buys silence equal to what it cost: until that much time has passed
        // again, command() reports "could not complete" without touching the network. The polls in
        // between are free and advance the budget at the rate it assumes, which keeps the wall
        // clock of a doomed operation within a factor of two of the patience configured for it,
        // and still notices a store that comes back within twice the time it takes to fail.
        const auto cost = std::chrono::steady_clock::now() - dial_started;
        dial_not_before = std::chrono::steady_clock::now() + cost;
        last_error = context == nullptr ? "could not allocate a Redis context" : context->errstr;
        if (!connection_warned) {
            connection_warned = true;
            BOOST_LOG_TRIVIAL(error) << "Redis: could not connect to " << hostname << ":" << port
                                     << ": " << last_error;
        }
    } else {
        // Re-arm the latch, so a later outage is reported once too.
        connection_warned = false;
        dial_not_before = std::chrono::steady_clock::time_point{};
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

int FMI::Comm::Redis::connection_fd() const {
    return context == nullptr ? -1 : context->fd;
}

unsigned int FMI::Comm::Redis::poll_attempts() const {
    if (timeout == 0) {
        // Only reachable with the flag off — RecoverableClientServer refuses a zero interval
        // under recover — and nothing on this path retries there. One pass, no division.
        return 1;
    }
    // Rounded up, because that is how many passes the download loops make: they stop when
    // elapsed_time reaches max_timeout having added timeout per pass, so a budget that is not a
    // multiple of the interval buys one more pass, not one fewer.
    return std::max(1u, (max_timeout + timeout - 1) / timeout);
}

std::string FMI::Comm::Redis::last_failure() const {
    if (last_error.empty()) {
        return "no connection to " + hostname + ":" + std::to_string(port);
    }
    return last_error;
}

//! One command, arguments passed out of band, reconnected and reissued once under recover.
/*!
 * redisCommandArgv rather than redisCommand: the latter takes a printf format, and the callers
 * build their argument from a key name, which carries a user-chosen communicator name — a '%'
 * or a space in it corrupts the command.
 *
 * A NULL reply means hiredis could not complete the command (the socket died, or the context was
 * already in an error state). The context is dropped rather than reused: a half-written command
 * leaves bytes in its output buffer and a half-read reply in its reader, and reusing it would
 * prepend those to the next command or attribute a late reply to it. THE ONLY RECOVERY OF A
 * CONTEXT IS redisFree FOLLOWED BY A FRESH redisConnectWithTimeout — never clearing err in place,
 * never reusing a context that failed. That rule is what makes the second attempt below sound,
 * and it is the load-bearing invariant of the whole recovery design.
 *
 * The second attempt is what a restored rank needs. criu preserves the pid and the context's
 * memory, so after a restore the captured context looks perfectly healthy — err is 0 and the
 * descriptor is valid — and there is nothing to pre-flight: attempt 0 has to burn on it, fail,
 * and let attempt 1 dial a live connection. Blind reissue is legal because every command this
 * channel sends is idempotent (SET / GET / DEL / KEYS) and no caller reads a retry-sensitive
 * part of the reply.
 *
 * That is the only thing the second attempt is for, so it is spent only on an attempt that
 * inherited a connection. An attempt that dialled one itself and still failed has already learnt
 * what a repeat would tell it, and repeating would double the cost of every failing call — the
 * dial is the expensive part, and the budget the caller is counting against pays for it. The
 * quiet window connect() sets is the other half of that accounting.
 *
 * The pid check is the fork guard, mirroring DirectTCP::Registry (src/comm/DirectTCP.cpp:131):
 * an inherited connection must not be shared, because two processes reading one RESP stream
 * steal each other's replies. That helper stays separate deliberately — it pipelines batches,
 * treats an error reply as fatal and is called from several threads, none of which holds for a
 * data plane whose poll loops must keep going.
 */
FMI::Comm::Redis::ReplyPtr FMI::Comm::Redis::command(int argc, const char** argv, const std::size_t* argvlen) {
    const int attempts = recover ? 2 : 1;
    for (int attempt = 0; attempt < attempts; attempt++) {
        bool dialed = false;
        if (context == nullptr || context->err || (recover && owner_pid != ::getpid())) {
            if (recover && std::chrono::steady_clock::now() < dial_not_before) {
                // Still inside the window the last failed dial bought. Saying so costs nothing,
                // and every caller under recover is a loop that will come back and ask again.
                break;
            }
            connect();
            dialed = true;
        }
        if (context != nullptr) {
            auto* reply = static_cast<redisReply*>(redisCommandArgv(context, argc, argv, argvlen));
            if (reply != nullptr) {
                return ReplyPtr(reply);
            }
            // Read the reason before dropping the context, which frees it.
            const int reason = context->err;
            drop_connection();
            if (reason == REDIS_ERR_OTHER) {
                // Not a transport failure: a malformed command, or hiredis refusing the arguments.
                // Reconnecting changes nothing, and a second identical failure would report the
                // outage that never happened instead of the argument that did.
                break;
            }
        }
        if (dialed) {
            // See above: the connection this attempt failed on was its own.
            break;
        }
    }
    return ReplyPtr();
}

//! Write one object. Under recover this call carries a delivery obligation and does not give up quietly.
/*!
 * An upload is the store-backed family's equivalent of a TCP sender's retention: the peer that
 * will read this object has no other way of learning that it was written, and its only reaction
 * to an absent object is to keep polling until its budget runs out. Dropping the write and
 * returning would therefore surface at the reader as an unexplained Timeout, on a different rank,
 * seconds later. So under recover the whole command is retried under the reader's own budget and,
 * if it still cannot be completed, the failure is raised here where the cause is known.
 *
 * A store that answers with an error (-OOM, -MISCONF, -READONLY) has completed the command and
 * refused it. That is not something waiting can fix, so it is raised immediately.
 */
void FMI::Comm::Redis::upload_object(channel_data buf, std::string name) {
    if (!recover) {
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
        return;
    }

    // SET ... EX: recovered jobs never delete what they wrote (see
    // RecoverableClientServer::finalize), so the store is what eventually reclaims it. A ttl of 0
    // means "keep it", which is also the only spelling Redis rejects outright, so it drops the
    // two extra arguments instead.
    const std::string ttl = std::to_string(object_ttl_s);
    const char* argv[] = {"SET", name.c_str(), buf.buf, "EX", ttl.c_str()};
    const std::size_t argvlen[] = {sizeof("SET") - 1, name.size(), buf.len, sizeof("EX") - 1, ttl.size()};
    const int argc = object_ttl_s > 0 ? 5 : 3;

    const unsigned int attempts = poll_attempts();
    for (unsigned int attempt = 0; attempt < attempts; attempt++) {
        auto reply = command(argc, argv, argvlen);
        if (reply) {
            if (reply->type == REDIS_REPLY_ERROR) {
                throw std::runtime_error("Redis: store refused SET " + name + ": " +
                                         (reply->str == nullptr ? "unknown error" : reply->str));
            }
            return;
        }
        if (attempt + 1 < attempts) {
            // The same interval the readers sleep between polls, so a store that is coming back
            // is waited for at the same rate on both sides.
            std::this_thread::sleep_for(std::chrono::milliseconds(timeout));
        }
    }
    throw std::runtime_error("Redis: could not SET " + name + " within the poll budget: " +
                             last_failure());
}

//! Read one object if it is there. Absence and unreachability are both "not yet", by design.
/*!
 * Every caller is a bounded poll loop that re-enters only on false and gives up on its own
 * budget, so under recover a connection failure must NOT be raised here: the loops in
 * download/reduce/scan/barrier have no catch, and an exception thrown at poll number three of a
 * thousand would end the operation at the exact moment the recovery machinery exists to survive.
 * The budget is the failure detector; this function's job is to report what it knows and let the
 * caller keep counting.
 *
 * A length mismatch is the one thing it does raise. SET is atomic — a value is stored whole or
 * not at all — so a stored object of the wrong size is never a partial write. It is two ranks
 * disagreeing about the message size, or two jobs sharing a key, and copying the shorter of the
 * two into the caller's buffer (which is what happens with the flag off) leaves the tail as
 * whatever it was and tells nobody.
 */
bool FMI::Comm::Redis::download_object(channel_data buf, std::string name) {
    const char* argv[] = {"GET", name.c_str()};
    const std::size_t argvlen[] = {sizeof("GET") - 1, name.size()};
    auto reply = command(2, argv, argvlen);
    if (!reply) {
        if (!recover) {
            throw std::runtime_error("Redis: could not GET " + name + ": " + last_failure());
        }
        if (!download_warned) {
            download_warned = true;
            BOOST_LOG_TRIVIAL(error) << "Redis: could not GET " << name << ": " << last_failure()
                                     << " (waiting out the poll budget)";
        }
        return false;
    }
    // A completed command re-arms the latch, so a later outage is reported once too.
    download_warned = false;
    if (reply->type == REDIS_REPLY_NIL) {
        return false;
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        if (recover) {
            // Loud, because none of the errors a GET can draw (-MISCONF, -LOADING, -NOAUTH) get
            // better by polling, and the caller can only report a timeout minutes later.
            BOOST_LOG_TRIVIAL(error) << "Redis: error on GET " << name << ": "
                                     << (reply->str == nullptr ? "unknown error" : reply->str);
        }
        return false;
    }
    if (reply->str == nullptr) {
        // Unreachable for GET, which answers with a string, a nil or an error; here so that no
        // reply shape can turn into a copy from a null pointer.
        return false;
    }
    if (recover && reply->len != buf.len) {
        throw std::runtime_error("Redis: object " + name + " is " + std::to_string(reply->len) +
                                 " bytes, expected " + std::to_string(buf.len));
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

