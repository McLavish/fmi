#include "../../include/ft/ControlPlane.h"
#include "../../include/utils/Configuration.h"

#include <algorithm>
#include <chrono>
#include <initializer_list>
#include <mutex>
#include <stdexcept>
#include <utility>
#include <vector>

#if FMI_ENABLE_REDIS
#include <hiredis/hiredis.h>

#include <memory>
#include <optional>
#include <string>
#endif

namespace {
#if FMI_ENABLE_REDIS
    using ReplyPtr = std::unique_ptr<redisReply, decltype(&freeReplyObject)>;

    redisContext* connect_redis(const FMI::Utils::FaultToleranceConfig& config) {
        auto* context = redisConnect(config.control_host.c_str(), static_cast<int>(config.control_port));
        if (context == nullptr || context->err) {
            std::string error = "Could not connect to Redis control plane";
            if (context != nullptr && context->errstr != nullptr) {
                error += ": ";
                error += context->errstr;
            }
            if (context != nullptr) {
                redisFree(context);
            }
            throw std::runtime_error(error);
        }
        return context;
    }

    std::uint64_t current_time_millis() {
        auto now = std::chrono::system_clock::now().time_since_epoch();
        return static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(now).count());
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

    FMI::FT::RankState rank_state_from_string(const std::string& state) {
        if (state == "ACTIVE") {
            return FMI::FT::RankState::Active;
        }
        if (state == "MIGRATION_PENDING") {
            return FMI::FT::RankState::MigrationPending;
        }
        if (state == "QUIESCED") {
            return FMI::FT::RankState::Quiesced;
        }
        if (state == "REPLACED") {
            return FMI::FT::RankState::Replaced;
        }
        throw std::runtime_error("Unknown rank state string: " + state);
    }

#ifdef FMI_ENABLE_CRIU
    std::string state_to_string(FMI::FT::CriuRankState state) {
        switch (state) {
            case FMI::FT::CriuRankState::Running:
                return "RUNNING";
            case FMI::FT::CriuRankState::Quiesced:
                return "QUIESCED";
        }
        throw std::runtime_error("Unknown CRIU rank state");
    }

    FMI::FT::CriuRankState criu_rank_state_from_string(const std::string& state) {
        if (state == "RUNNING") {
            return FMI::FT::CriuRankState::Running;
        }
        if (state == "QUIESCED") {
            return FMI::FT::CriuRankState::Quiesced;
        }
        throw std::runtime_error("Unknown CRIU rank state string: " + state);
    }
#endif
#endif
}

struct FMI::FT::ControlPlane::Impl {
    Utils::FaultToleranceConfig config;
    std::string comm_name;
    Utils::peer_num num_peers;
#if FMI_ENABLE_REDIS
    redisContext* context = nullptr;
    std::mutex command_mutex;
#endif

    explicit Impl(Utils::FaultToleranceConfig config, std::string comm_name, Utils::peer_num num_peers) :
            config(std::move(config)),
            comm_name(std::move(comm_name)),
            num_peers(num_peers) {
#if FMI_ENABLE_REDIS
        connect();
#endif
    }

    ~Impl() {
#if FMI_ENABLE_REDIS
        if (context != nullptr) {
            redisFree(context);
        }
#endif
    }

#if FMI_ENABLE_REDIS
    void connect() {
        if (context != nullptr) {
            redisFree(context);
        }
        context = connect_redis(config);
    }

    ReplyPtr command(std::initializer_list<std::string> args) {
        return command(std::vector<std::string>(args));
    }

    ReplyPtr command(const std::vector<std::string>& args) {
        std::lock_guard<std::mutex> lock(command_mutex);
        if (args.empty()) {
            throw std::runtime_error("Redis command called with no arguments");
        }
        if (context == nullptr || context->err) {
            connect();
        }

        std::vector<const char*> argv;
        std::vector<std::size_t> argvlen;
        argv.reserve(args.size());
        argvlen.reserve(args.size());
        for (const auto& arg : args) {
            argv.push_back(arg.data());
            argvlen.push_back(arg.size());
        }

        auto* raw_reply = reinterpret_cast<redisReply*>(redisCommandArgv(
                context, static_cast<int>(argv.size()), argv.data(), argvlen.data()));
        if (raw_reply == nullptr) {
            connect();
            raw_reply = reinterpret_cast<redisReply*>(redisCommandArgv(
                    context, static_cast<int>(argv.size()), argv.data(), argvlen.data()));
        }
        if (raw_reply == nullptr) {
            throw std::runtime_error("Redis command failed");
        }

        ReplyPtr reply(raw_reply, &freeReplyObject);
        if (reply->type == REDIS_REPLY_ERROR) {
            std::string error = reply->str == nullptr ? "unknown Redis error" : reply->str;
            throw std::runtime_error("Redis error: " + error);
        }
        return reply;
    }

    //! HGET that returns nullopt when the field is missing, collapsing the repeated NIL/nullptr guard.
    std::optional<std::string> hget(const std::string& key, const std::string& field) {
        auto reply = command({"HGET", key, field});
        if (reply->type == REDIS_REPLY_NIL || reply->str == nullptr) {
            return std::nullopt;
        }
        return std::string(reply->str);
    }

    std::uint64_t hget_u64(const std::string& key, const std::string& field, std::uint64_t fallback = 0) {
        auto value = hget(key, field);
        return value ? std::stoull(*value) : fallback;
    }

    int hget_int(const std::string& key, const std::string& field, int fallback = 0) {
        auto value = hget(key, field);
        return value ? std::stoi(*value) : fallback;
    }
#endif

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

    [[nodiscard]] std::string placement_key(std::uint64_t epoch) const {
        return prefix() + "epoch:" + std::to_string(epoch) + ":placement";
    }

#ifdef FMI_ENABLE_CRIU
    [[nodiscard]] std::string criu_prefix() const {
        return prefix() + "criu:";
    }

    [[nodiscard]] std::string criu_meta_key() const {
        return criu_prefix() + "meta";
    }

    [[nodiscard]] std::string criu_ranks_key() const {
        return criu_prefix() + "ranks";
    }

    [[nodiscard]] std::string criu_rank_key(Utils::peer_num rank) const {
        return criu_prefix() + "rank:" + std::to_string(rank);
    }
#endif
};

FMI::FT::ControlPlane::ControlPlane(std::string config_path, std::string comm_name, FMI::Utils::peer_num num_peers) {
    Utils::Configuration config(std::move(config_path));
    auto ft_config = config.get_fault_tolerance_config();
    if (!ft_config.enabled) {
        throw std::runtime_error("Fault tolerance is not enabled in the configuration");
    }
    if (ft_config.control_backend != "Redis") {
        throw std::runtime_error("Only the Redis fault-tolerance control backend is supported");
    }
    impl = std::make_shared<Impl>(std::move(ft_config), std::move(comm_name), num_peers);
    ensure_job();
}

FMI::FT::ControlPlane::~ControlPlane() = default;

void FMI::FT::ControlPlane::check_world_size(const std::string& meta_key, const char* error_message) const {
#if FMI_ENABLE_REDIS
    auto world_size = impl->command({"HGET", meta_key, "world_size"});
    if (world_size->type == REDIS_REPLY_NIL) {
        impl->command({"HSET", meta_key, "world_size", std::to_string(impl->num_peers)});
    } else if (std::stoul(world_size->str) != impl->num_peers) {
        throw std::runtime_error(error_message);
    }
#else
    (void) meta_key;
    (void) error_message;
#endif
}

void FMI::FT::ControlPlane::ensure_job() const {
#if FMI_ENABLE_REDIS
    auto exists = impl->command({"EXISTS", impl->meta_key()});
    if (exists->integer == 0) {
        impl->command({"HSET", impl->meta_key(), "world_size", std::to_string(impl->num_peers), "current_epoch", "0"});
        return;
    }

    check_world_size(impl->meta_key(), "Fault-tolerance world size does not match the stored communicator metadata");

    auto current_epoch = impl->command({"HEXISTS", impl->meta_key(), "current_epoch"});
    if (current_epoch->integer == 0) {
        impl->command({"HSET", impl->meta_key(), "current_epoch", "0"});
    }
#else
    throw std::runtime_error("Fault tolerance requires FMI to be built with Redis support");
#endif
}

std::uint64_t FMI::FT::ControlPlane::epoch() const {
#if FMI_ENABLE_REDIS
    ensure_job();
    return impl->hget_u64(impl->meta_key(), "current_epoch");
#else
    return 0;
#endif
}

void FMI::FT::ControlPlane::request_migration(FMI::Utils::peer_num rank) {
#if FMI_ENABLE_REDIS
    ensure_job();
    auto current_epoch = epoch();
    impl->command({"SADD", impl->pending_key(), std::to_string(rank)});
    set_rank_state(current_epoch, rank, RankState::MigrationPending);
#endif
}

void FMI::FT::ControlPlane::set_placement(std::uint64_t epoch, FMI::Utils::peer_num rank, const std::string& placement) const {
#if FMI_ENABLE_REDIS
    impl->command({"HSET", impl->placement_key(epoch), std::to_string(rank), placement});
#endif
}

std::string FMI::FT::ControlPlane::placement_for_rank(std::uint64_t epoch, FMI::Utils::peer_num rank) const {
#if FMI_ENABLE_REDIS
    return impl->hget(impl->placement_key(epoch), std::to_string(rank)).value_or("");
#else
    return "";
#endif
}

std::vector<FMI::FT::RankDirectoryEntry> FMI::FT::ControlPlane::directory_snapshot(std::uint64_t epoch) const {
#if FMI_ENABLE_REDIS
    std::vector<FMI::FT::RankDirectoryEntry> ranks;
    auto reply = impl->command({"HKEYS", impl->members_key(epoch)});
    if (reply->type != REDIS_REPLY_ARRAY) {
        return ranks;
    }
    for (std::size_t i = 0; i < reply->elements; i++) {
        auto* rank_reply = reply->element[i];
        if (rank_reply == nullptr || rank_reply->str == nullptr) {
            continue;
        }
        auto rank_id = static_cast<FMI::Utils::peer_num>(std::stoul(rank_reply->str));
        FMI::FT::RankDirectoryEntry info;
        info.rank = rank_id;

        if (auto worker = impl->hget(impl->members_key(epoch), std::to_string(rank_id))) {
            info.worker_id = *worker;
        }
        if (auto state = impl->hget(impl->states_key(epoch), std::to_string(rank_id))) {
            info.state = rank_state_from_string(*state);
        }
        if (auto placement = impl->hget(impl->placement_key(epoch), std::to_string(rank_id))) {
            info.placement = *placement;
        }
        ranks.push_back(info);
    }

    std::sort(ranks.begin(), ranks.end(), [](const FMI::FT::RankDirectoryEntry& left, const FMI::FT::RankDirectoryEntry& right) {
        return left.rank < right.rank;
    });
    return ranks;
#else
    return {};
#endif
}

void FMI::FT::ControlPlane::clear_job_state() {
#if FMI_ENABLE_REDIS
    impl->command({"DEL", impl->meta_key()});
    impl->command({"DEL", impl->pending_key()});
    auto reply = impl->command({"KEYS", impl->prefix() + "epoch:*"});
    if (reply->type != REDIS_REPLY_ARRAY) {
        return;
    }
    for (std::size_t i = 0; i < reply->elements; i++) {
        auto* key = reply->element[i];
        if (key != nullptr && key->str != nullptr) {
            impl->command({"DEL", std::string(key->str)});
        }
    }
#endif
}

#ifdef FMI_ENABLE_CRIU
void FMI::FT::ControlPlane::clear_criu_state() {
#if FMI_ENABLE_REDIS
    auto reply = impl->command({"KEYS", impl->criu_prefix() + "*"});
    if (reply->type != REDIS_REPLY_ARRAY) {
        return;
    }
    for (std::size_t i = 0; i < reply->elements; i++) {
        auto* key = reply->element[i];
        if (key != nullptr && key->str != nullptr) {
            impl->command({"DEL", std::string(key->str)});
        }
    }
#endif
}
#endif

bool FMI::FT::ControlPlane::has_pending_migration() const {
#if FMI_ENABLE_REDIS
    auto reply = impl->command({"SCARD", impl->pending_key()});
    return reply->integer > 0;
#else
    return false;
#endif
}

bool FMI::FT::ControlPlane::is_rank_pending(FMI::Utils::peer_num rank) const {
#if FMI_ENABLE_REDIS
    auto reply = impl->command({"SISMEMBER", impl->pending_key(), std::to_string(rank)});
    return reply->integer == 1;
#else
    return false;
#endif
}

void FMI::FT::ControlPlane::register_rank(std::uint64_t epoch, FMI::Utils::peer_num rank, const std::string& worker_id,
                                         FMI::FT::RankState state) const {
#if FMI_ENABLE_REDIS
    impl->command({"HSET", impl->members_key(epoch), std::to_string(rank), worker_id});
    impl->command({"HSET", impl->states_key(epoch), std::to_string(rank), state_to_string(state)});
#endif
}

void FMI::FT::ControlPlane::set_rank_state(std::uint64_t epoch, FMI::Utils::peer_num rank, FMI::FT::RankState state) const {
#if FMI_ENABLE_REDIS
    impl->command({"HSET", impl->states_key(epoch), std::to_string(rank), state_to_string(state)});
#endif
}

void FMI::FT::ControlPlane::promote_epoch(std::uint64_t next_epoch) const {
#if FMI_ENABLE_REDIS
    static const std::string script =
            "local current = redis.call('HGET', KEYS[1], 'current_epoch') "
            "if not current then current = '0' end "
            "if tonumber(ARGV[1]) > tonumber(current) then "
            "redis.call('HSET', KEYS[1], 'current_epoch', ARGV[1]) "
            "redis.call('DEL', KEYS[2]) "
            "return 1 "
            "end "
            "return 0";
    impl->command({"EVAL", script, "2", impl->meta_key(), impl->pending_key(), std::to_string(next_epoch)});
#endif
}

#ifdef FMI_ENABLE_CRIU
void FMI::FT::ControlPlane::ensure_criu_registry() const {
#if FMI_ENABLE_REDIS
    auto exists = impl->command({"EXISTS", impl->criu_meta_key()});
    if (exists->integer == 0) {
        impl->command({"HSET", impl->criu_meta_key(), "world_size", std::to_string(impl->num_peers)});
    } else {
        check_world_size(impl->criu_meta_key(), "CRIU FT world size does not match the stored communicator metadata");
    }
#endif
}

void FMI::FT::ControlPlane::criu_write_rank(FMI::Utils::peer_num rank, int pid, const std::string& host_id,
                                           const std::string& backend, CriuRankState state, bool write_generation,
                                           std::uint64_t generation) const {
#if FMI_ENABLE_REDIS
    ensure_criu_registry();
    std::vector<std::string> args = {"HSET", impl->criu_rank_key(rank),
                                     "pid", std::to_string(pid),
                                     "host_id", host_id,
                                     "backend", backend,
                                     "state", state_to_string(state),
                                     "last_heartbeat_ms", std::to_string(current_time_millis())};
    if (write_generation) {
        args.emplace_back("quiesced_generation");
        args.emplace_back(std::to_string(generation));
    }
    impl->command(args);
#else
    (void) rank; (void) pid; (void) host_id; (void) backend;
    (void) state; (void) write_generation; (void) generation;
#endif
}

void FMI::FT::ControlPlane::criu_register_rank(FMI::Utils::peer_num rank, int pid, const std::string& host_id,
                                              const std::string& backend) const {
#if FMI_ENABLE_REDIS
    criu_write_rank(rank, pid, host_id, backend, CriuRankState::Running, true, 0);
    impl->command({"SADD", impl->criu_ranks_key(), std::to_string(rank)});
#endif
}

void FMI::FT::ControlPlane::criu_mark_rank_running(FMI::Utils::peer_num rank, int pid, const std::string& host_id,
                                                  const std::string& backend) const {
    criu_write_rank(rank, pid, host_id, backend, CriuRankState::Running, false, 0);
}

void FMI::FT::ControlPlane::criu_mark_rank_quiesced(FMI::Utils::peer_num rank, int pid, const std::string& host_id,
                                                   const std::string& backend, std::uint64_t generation) const {
    criu_write_rank(rank, pid, host_id, backend, CriuRankState::Quiesced, true, generation);
}

std::vector<FMI::FT::CriuRankInfo> FMI::FT::ControlPlane::criu_rank_info() const {
#if FMI_ENABLE_REDIS
    std::vector<FMI::FT::CriuRankInfo> ranks;
    auto reply = impl->command({"SMEMBERS", impl->criu_ranks_key()});
    if (reply->type != REDIS_REPLY_ARRAY) {
        return ranks;
    }
    for (std::size_t i = 0; i < reply->elements; i++) {
        auto* rank_reply = reply->element[i];
        if (rank_reply == nullptr || rank_reply->str == nullptr) {
            continue;
        }
        auto rank_id = static_cast<FMI::Utils::peer_num>(std::stoul(rank_reply->str));
        FMI::FT::CriuRankInfo info;
        info.rank = rank_id;

        const auto& rank_key = impl->criu_rank_key(rank_id);
        info.pid = impl->hget_int(rank_key, "pid", info.pid);
        info.host_id = impl->hget(rank_key, "host_id").value_or(info.host_id);
        info.backend = impl->hget(rank_key, "backend").value_or(info.backend);
        if (auto state = impl->hget(rank_key, "state")) {
            info.state = criu_rank_state_from_string(*state);
        }
        info.quiesced_generation = impl->hget_u64(rank_key, "quiesced_generation", info.quiesced_generation);
        info.last_heartbeat_ms = impl->hget_u64(rank_key, "last_heartbeat_ms", info.last_heartbeat_ms);
        ranks.push_back(info);
    }

    std::sort(ranks.begin(), ranks.end(), [](const FMI::FT::CriuRankInfo& left, const FMI::FT::CriuRankInfo& right) {
        return left.rank < right.rank;
    });
    return ranks;
#else
    return {};
#endif
}
#endif
