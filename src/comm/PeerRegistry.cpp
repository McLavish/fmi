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
