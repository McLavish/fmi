#include "../../include/ft/Coordinator.h"
#include "../../include/utils/Configuration.h"

#include <stdexcept>
#include <utility>

#if FMI_ENABLE_REDIS
#include <hiredis/hiredis.h>

#include <memory>
#include <string>
#endif

namespace {
#if FMI_ENABLE_REDIS
    using ReplyPtr = std::unique_ptr<redisReply, decltype(&freeReplyObject)>;

    ReplyPtr run_command(redisContext* context, const std::string& command) {
        auto* raw_reply = reinterpret_cast<redisReply*>(redisCommand(context, command.c_str()));
        if (raw_reply == nullptr) {
            throw std::runtime_error("Redis command failed: " + command);
        }
        ReplyPtr reply(raw_reply, &freeReplyObject);
        if (reply->type == REDIS_REPLY_ERROR) {
            std::string error = reply->str == nullptr ? "unknown Redis error" : reply->str;
            throw std::runtime_error("Redis error for command '" + command + "': " + error);
        }
        return reply;
    }

    std::string state_to_string(FMI::FT::RankState state) {
        switch (state) {
            case FMI::FT::RankState::Active:
                return "ACTIVE";
            case FMI::FT::RankState::MigrationPending:
                return "MIGRATION_PENDING";
            case FMI::FT::RankState::Quiesced:
                return "QUIESCED";
            case FMI::FT::RankState::Replaced:
                return "REPLACED";
        }
        throw std::runtime_error("Unknown rank state");
    }
#endif
}

struct FMI::FT::Coordinator::Impl {
    Utils::FaultToleranceConfig config;
    std::string comm_name;
    Utils::peer_num num_peers;

#if FMI_ENABLE_REDIS
    redisContext* context = nullptr;
#endif

    explicit Impl(Utils::FaultToleranceConfig config, std::string comm_name, Utils::peer_num num_peers) :
            config(std::move(config)),
            comm_name(std::move(comm_name)),
            num_peers(num_peers) {
#if FMI_ENABLE_REDIS
        context = redisConnect(this->config.control_host.c_str(), static_cast<int>(this->config.control_port));
        if (context == nullptr || context->err) {
            std::string error = "Could not connect to Redis control plane";
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
#else
        throw std::runtime_error("Fault tolerance requires FMI to be built with Redis support");
#endif
    }

    ~Impl() {
#if FMI_ENABLE_REDIS
        if (context != nullptr) {
            redisFree(context);
            context = nullptr;
        }
#endif
    }

    [[nodiscard]] std::string prefix() const {
        return "fmi:ft:" + comm_name + ":";
    }

    [[nodiscard]] std::string meta_key() const {
        return prefix() + "meta";
    }

    [[nodiscard]] std::string pending_key() const {
        return prefix() + "pending";
    }

    [[nodiscard]] std::string members_key(std::uint64_t epoch) const {
        return prefix() + "epoch:" + std::to_string(epoch) + ":members";
    }

    [[nodiscard]] std::string states_key(std::uint64_t epoch) const {
        return prefix() + "epoch:" + std::to_string(epoch) + ":states";
    }

    [[nodiscard]] std::string lease_key(std::uint64_t epoch, Utils::peer_num rank) const {
        return prefix() + "epoch:" + std::to_string(epoch) + ":lease:" + std::to_string(rank);
    }
};

FMI::FT::Coordinator::Coordinator(std::string config_path, std::string comm_name, FMI::Utils::peer_num num_peers) {
    Utils::Configuration config(std::move(config_path));
    auto ft_config = config.get_fault_tolerance_config();
    if (!ft_config.enabled) {
        throw std::runtime_error("Fault tolerance is not enabled in the configuration");
    }
    if (!ft_config.safe_point_only) {
        throw std::runtime_error("Only safe_point_only fault tolerance is supported");
    }
    if (ft_config.control_backend != "Redis") {
        throw std::runtime_error("Only the Redis fault-tolerance control backend is supported");
    }
    impl = std::make_shared<Impl>(std::move(ft_config), std::move(comm_name), num_peers);
    ensure_job();
}

FMI::FT::Coordinator::~Coordinator() = default;

void FMI::FT::Coordinator::ensure_job() const {
#if FMI_ENABLE_REDIS
    auto exists = run_command(impl->context, "EXISTS " + impl->meta_key());
    if (exists->integer == 0) {
        run_command(impl->context, "HSET " + impl->meta_key() + " world_size " + std::to_string(impl->num_peers) +
                                   " current_epoch 0");
        return;
    }

    auto world_size = run_command(impl->context, "HGET " + impl->meta_key() + " world_size");
    if (world_size->type == REDIS_REPLY_NIL) {
        run_command(impl->context, "HSET " + impl->meta_key() + " world_size " + std::to_string(impl->num_peers));
    } else if (std::stoul(world_size->str) != impl->num_peers) {
        throw std::runtime_error("Fault-tolerance world size does not match the stored communicator metadata");
    }

    auto current_epoch = run_command(impl->context, "HEXISTS " + impl->meta_key() + " current_epoch");
    if (current_epoch->integer == 0) {
        run_command(impl->context, "HSET " + impl->meta_key() + " current_epoch 0");
    }
#endif
}

std::uint64_t FMI::FT::Coordinator::epoch() const {
#if FMI_ENABLE_REDIS
    ensure_job();
    auto reply = run_command(impl->context, "HGET " + impl->meta_key() + " current_epoch");
    if (reply->type == REDIS_REPLY_NIL || reply->str == nullptr) {
        return 0;
    }
    return std::stoull(reply->str);
#else
    return 0;
#endif
}

void FMI::FT::Coordinator::request_migration(FMI::Utils::peer_num rank) {
#if FMI_ENABLE_REDIS
    ensure_job();
    auto current_epoch = epoch();
    run_command(impl->context, "SADD " + impl->pending_key() + " " + std::to_string(rank));
    set_rank_state(current_epoch, rank, RankState::MigrationPending);
#endif
}

void FMI::FT::Coordinator::clear_job_state() {
#if FMI_ENABLE_REDIS
    auto reply = run_command(impl->context, "KEYS " + impl->prefix() + "*");
    if (reply->type != REDIS_REPLY_ARRAY) {
        return;
    }
    for (std::size_t i = 0; i < reply->elements; i++) {
        auto* key = reply->element[i];
        if (key != nullptr && key->str != nullptr) {
            run_command(impl->context, "DEL " + std::string(key->str));
        }
    }
#endif
}

bool FMI::FT::Coordinator::has_pending_migration() const {
#if FMI_ENABLE_REDIS
    auto reply = run_command(impl->context, "SCARD " + impl->pending_key());
    return reply->integer > 0;
#else
    return false;
#endif
}

bool FMI::FT::Coordinator::is_rank_pending(FMI::Utils::peer_num rank) const {
#if FMI_ENABLE_REDIS
    auto reply = run_command(impl->context, "SISMEMBER " + impl->pending_key() + " " + std::to_string(rank));
    return reply->integer == 1;
#else
    return false;
#endif
}

void FMI::FT::Coordinator::register_rank(std::uint64_t epoch, FMI::Utils::peer_num rank, const std::string& worker_id,
                                         FMI::FT::RankState state) const {
#if FMI_ENABLE_REDIS
    run_command(impl->context, "HSET " + impl->members_key(epoch) + " " + std::to_string(rank) + " " + worker_id);
    run_command(impl->context, "HSET " + impl->states_key(epoch) + " " + std::to_string(rank) + " " + state_to_string(state));
#endif
}

void FMI::FT::Coordinator::set_rank_state(std::uint64_t epoch, FMI::Utils::peer_num rank, FMI::FT::RankState state) const {
#if FMI_ENABLE_REDIS
    run_command(impl->context, "HSET " + impl->states_key(epoch) + " " + std::to_string(rank) + " " + state_to_string(state));
#endif
}

std::string FMI::FT::Coordinator::worker_for_rank(std::uint64_t epoch, FMI::Utils::peer_num rank) const {
#if FMI_ENABLE_REDIS
    auto reply = run_command(impl->context, "HGET " + impl->members_key(epoch) + " " + std::to_string(rank));
    if (reply->type == REDIS_REPLY_NIL || reply->str == nullptr) {
        return "";
    }
    return reply->str;
#else
    return "";
#endif
}

void FMI::FT::Coordinator::refresh_lease(std::uint64_t epoch, FMI::Utils::peer_num rank, const std::string& worker_id) const {
#if FMI_ENABLE_REDIS
    run_command(impl->context,
                "PSETEX " + impl->lease_key(epoch, rank) + " " + std::to_string(impl->config.lease_ms) + " " + worker_id);
#endif
}

std::size_t FMI::FT::Coordinator::live_member_count(std::uint64_t epoch) const {
#if FMI_ENABLE_REDIS
    auto members = run_command(impl->context, "HKEYS " + impl->members_key(epoch));
    if (members->type != REDIS_REPLY_ARRAY) {
        return 0;
    }

    std::size_t count = 0;
    for (std::size_t i = 0; i < members->elements; i++) {
        auto* rank = members->element[i];
        if (rank == nullptr || rank->str == nullptr) {
            continue;
        }
        auto exists = run_command(impl->context, "EXISTS " + impl->lease_key(epoch, static_cast<Utils::peer_num>(std::stoul(rank->str))));
        if (exists->integer == 1) {
            count++;
        }
    }
    return count;
#else
    return 0;
#endif
}

void FMI::FT::Coordinator::promote_epoch(std::uint64_t next_epoch) const {
#if FMI_ENABLE_REDIS
    run_command(impl->context, "HSET " + impl->meta_key() + " current_epoch " + std::to_string(next_epoch));
    run_command(impl->context, "DEL " + impl->pending_key());
#endif
}

unsigned int FMI::FT::Coordinator::heartbeat_ms() const {
    return impl->config.heartbeat_ms;
}

unsigned int FMI::FT::Coordinator::lease_ms() const {
    return impl->config.lease_ms;
}
