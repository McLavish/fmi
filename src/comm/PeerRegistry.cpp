#include "../../include/comm/PeerRegistry.h"

#include "../../include/utils/Signals.h"

#include <unistd.h>

#include <algorithm>
#include <exception>
#include <stdexcept>
#include <utility>

FMI::Comm::PeerRegistry::PeerRegistry(std::string host, int port)
        : host(std::move(host)), port(port) {
    // Here, not at connect time: the first command after a criu restore is issued on the
    // context the checkpoint captured, whose socket the restore already dropped. That
    // write happens *before* any reconnect, so a disposition installed on the reconnect
    // path would arrive one dead write too late.
    FMI::Utils::suppress_sigpipe();
}

FMI::Comm::PeerRegistry::~PeerRegistry() { disconnect(); }

void FMI::Comm::PeerRegistry::disconnect() {
    std::lock_guard<std::mutex> lock(mutex);
    close_locked();
}

void FMI::Comm::PeerRegistry::publish(const std::string& key, const std::string& field,
                                      const std::string& value, unsigned int ttl_s,
                                      long timeout_ms) {
    std::lock_guard<std::mutex> lock(mutex);
    set_timeout_locked(timeout_ms);
    std::vector<std::vector<std::string>> batch{{"HSET", key, field, value}};
    if (ttl_s > 0) {
        batch.push_back({"EXPIRE", key, std::to_string(ttl_s)});
    }
    pipeline_locked(batch);
}

std::map<FMI::Utils::peer_num, std::string> FMI::Comm::PeerRegistry::snapshot(
        const std::string& key, long timeout_ms) {
    std::lock_guard<std::mutex> lock(mutex);
    set_timeout_locked(timeout_ms);
    auto replies = pipeline_locked({{"HGETALL", key}});
    std::map<FMI::Utils::peer_num, std::string> out;
    redisReply* reply = replies.front();
    if (reply->type != REDIS_REPLY_ARRAY) {
        return out;
    }
    for (std::size_t i = 0; i + 1 < reply->elements; i += 2) {
        redisReply* field = reply->element[i];
        redisReply* value = reply->element[i + 1];
        if (field->str == nullptr || value->str == nullptr) {
            continue;
        }
        try {
            out[static_cast<FMI::Utils::peer_num>(std::stoul(std::string(field->str, field->len)))] =
                    std::string(value->str, value->len);
        } catch (const std::exception&) {
            // A field that is not a rank number is not ours; ignore it rather than failing
            // the whole discovery pass.
        }
    }
    return out;
}

void FMI::Comm::PeerRegistry::hdel(const std::string& key, const std::string& field,
                                   long timeout_ms) {
    std::lock_guard<std::mutex> lock(mutex);
    set_timeout_locked(timeout_ms);
    pipeline_locked({{"HDEL", key, field}});
}

std::string FMI::Comm::PeerRegistry::xadd(
        const std::string& key, const std::vector<std::pair<std::string, std::string>>& fields,
        unsigned int ttl_s, long timeout_ms) {
    std::lock_guard<std::mutex> lock(mutex);
    set_timeout_locked(timeout_ms);
    std::vector<std::string> command{"XADD", key, "*"};
    command.reserve(3 + 2 * fields.size());
    for (const auto& [field, value] : fields) {
        command.push_back(field);
        command.push_back(value);
    }
    std::vector<std::vector<std::string>> batch{command};
    if (ttl_s > 0) {
        batch.push_back({"EXPIRE", key, std::to_string(ttl_s)});
    }
    auto replies = pipeline_locked(batch);
    redisReply* reply = replies.front();
    if (reply->type != REDIS_REPLY_STRING || reply->str == nullptr) {
        throw std::runtime_error("registry: XADD did not answer with an entry id");
    }
    return std::string(reply->str, reply->len);
}

std::string FMI::Comm::PeerRegistry::tail_id(const std::string& key, long timeout_ms) {
    std::lock_guard<std::mutex> lock(mutex);
    set_timeout_locked(timeout_ms);
    auto replies = pipeline_locked({{"XREVRANGE", key, "+", "-", "COUNT", "1"}});
    redisReply* reply = replies.front();
    // An empty or absent stream answers with an empty array: "0-0" is the id every later read
    // resumes from, and it is the id of nothing rather than the id of the first entry.
    if (reply->type != REDIS_REPLY_ARRAY || reply->elements == 0) {
        return "0-0";
    }
    redisReply* entry = reply->element[0];
    if (entry == nullptr || entry->type != REDIS_REPLY_ARRAY || entry->elements == 0 ||
        entry->element[0]->str == nullptr) {
        return "0-0";
    }
    return std::string(entry->element[0]->str, entry->element[0]->len);
}

std::vector<std::pair<std::string, std::map<std::string, std::string>>>
FMI::Comm::PeerRegistry::xread_after(const std::string& key, const std::string& last_id,
                                     long block_ms, long timeout_ms) {
    std::lock_guard<std::mutex> lock(mutex);
    // A blocking read must not be cut short by the I/O timeout it is nested in: the deadline the
    // caller passed bounds the round trip, and the block is part of that round trip.
    set_timeout_locked(std::max<long>(timeout_ms, block_ms + 1000));
    std::vector<std::string> command{"XREAD"};
    if (block_ms > 0) {
        command.push_back("BLOCK");
        command.push_back(std::to_string(block_ms));
    }
    command.push_back("STREAMS");
    command.push_back(key);
    command.push_back(last_id);
    auto replies = pipeline_locked({command});
    std::vector<std::pair<std::string, std::map<std::string, std::string>>> out;
    redisReply* reply = replies.front();
    // Nil when nothing arrived within the block: not an error, just no events.
    if (reply->type != REDIS_REPLY_ARRAY || reply->elements == 0) {
        return out;
    }
    for (std::size_t s = 0; s < reply->elements; s++) {
        redisReply* stream = reply->element[s];
        if (stream == nullptr || stream->type != REDIS_REPLY_ARRAY || stream->elements < 2) {
            continue;
        }
        redisReply* entries = stream->element[1];
        if (entries == nullptr || entries->type != REDIS_REPLY_ARRAY) {
            continue;
        }
        for (std::size_t e = 0; e < entries->elements; e++) {
            redisReply* entry = entries->element[e];
            if (entry == nullptr || entry->type != REDIS_REPLY_ARRAY || entry->elements < 2 ||
                entry->element[0]->str == nullptr) {
                continue;
            }
            std::map<std::string, std::string> fields;
            redisReply* pairs = entry->element[1];
            if (pairs != nullptr && pairs->type == REDIS_REPLY_ARRAY) {
                for (std::size_t i = 0; i + 1 < pairs->elements; i += 2) {
                    if (pairs->element[i]->str == nullptr || pairs->element[i + 1]->str == nullptr) {
                        continue;
                    }
                    fields[std::string(pairs->element[i]->str, pairs->element[i]->len)] =
                            std::string(pairs->element[i + 1]->str, pairs->element[i + 1]->len);
                }
            }
            out.emplace_back(std::string(entry->element[0]->str, entry->element[0]->len),
                             std::move(fields));
        }
    }
    return out;
}

bool FMI::Comm::PeerRegistry::set_nx_px(const std::string& key, const std::string& value,
                                        long px_ms, long timeout_ms) {
    std::lock_guard<std::mutex> lock(mutex);
    set_timeout_locked(timeout_ms);
    auto replies = pipeline_locked(
            {{"SET", key, value, "NX", "PX", std::to_string(std::max<long>(px_ms, 1))}});
    redisReply* reply = replies.front();
    // A lease that was already held answers nil; only an OK status means this caller took it.
    return reply->type == REDIS_REPLY_STATUS && reply->str != nullptr &&
           std::string(reply->str, reply->len) == "OK";
}

void FMI::Comm::PeerRegistry::del_if_equal(const std::string& key, const std::string& value,
                                           long timeout_ms) {
    std::lock_guard<std::mutex> lock(mutex);
    set_timeout_locked(timeout_ms);
    static const char* script =
            "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) "
            "else return 0 end";
    pipeline_locked({{"EVAL", script, "1", key, value}});
}

void FMI::Comm::PeerRegistry::close_locked() {
    for (auto* reply : owned) {
        freeReplyObject(reply);
    }
    owned.clear();
    if (context != nullptr) {
        redisFree(context);
        context = nullptr;
    }
}

void FMI::Comm::PeerRegistry::set_timeout_locked(long timeout_ms) {
    long clamped = std::max<long>(timeout_ms, 1);
    io_timeout.tv_sec = clamped / 1000;
    io_timeout.tv_usec = (clamped % 1000) * 1000;
    if (context != nullptr && !context->err) {
        redisSetTimeout(context, io_timeout);
    }
}

void FMI::Comm::PeerRegistry::connect_locked() {
    close_locked();
    context = redisConnectWithTimeout(host.c_str(), port, io_timeout);
    if (context == nullptr || context->err) {
        std::string error = "DirectTCP: could not connect to the peer registry at " + host +
                            ":" + std::to_string(port);
        if (context != nullptr && context->errstr != nullptr) {
            error += ": ";
            error += context->errstr;
        }
        if (context != nullptr) {
            redisFree(context);
            context = nullptr;
        }
        throw std::runtime_error(error);
    }
    redisSetTimeout(context, io_timeout);
    owner_pid = ::getpid();
}

std::vector<redisReply*> FMI::Comm::PeerRegistry::pipeline_locked(
        const std::vector<std::vector<std::string>>& batch) {
    for (auto* reply : owned) {
        freeReplyObject(reply);
    }
    owned.clear();

    for (int attempt = 0; attempt < 2; attempt++) {
        if (context == nullptr || context->err || owner_pid != ::getpid()) {
            connect_locked();
        }
        bool ok = true;
        for (const auto& args : batch) {
            std::vector<const char*> argv;
            std::vector<std::size_t> argvlen;
            argv.reserve(args.size());
            argvlen.reserve(args.size());
            for (const auto& arg : args) {
                argv.push_back(arg.data());
                argvlen.push_back(arg.size());
            }
            if (redisAppendCommandArgv(context, static_cast<int>(argv.size()), argv.data(),
                                       argvlen.data()) != REDIS_OK) {
                ok = false;
                break;
            }
        }
        if (ok) {
            for (std::size_t i = 0; i < batch.size(); i++) {
                void* raw = nullptr;
                if (redisGetReply(context, &raw) != REDIS_OK || raw == nullptr) {
                    ok = false;
                    break;
                }
                owned.push_back(reinterpret_cast<redisReply*>(raw));
            }
        }
        if (ok && owned.size() == batch.size()) {
            for (auto* reply : owned) {
                if (reply->type == REDIS_REPLY_ERROR) {
                    std::string error = reply->str == nullptr ? "unknown Redis error" : reply->str;
                    throw std::runtime_error("DirectTCP: registry error: " + error);
                }
            }
            return owned;
        }
        for (auto* reply : owned) {
            freeReplyObject(reply);
        }
        owned.clear();
        close_locked();
    }
    throw std::runtime_error("DirectTCP: peer registry command failed");
}
