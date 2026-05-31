#include "../../include/ft/Coordinator.h"
#include "../../include/utils/Configuration.h"

#include <algorithm>
#include <chrono>
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

    ReplyPtr run_command(const FMI::Utils::FaultToleranceConfig& config, const std::string& command) {
        redisContext* context = connect_redis(config);
        auto* raw_reply = reinterpret_cast<redisReply*>(redisCommand(context, command.c_str()));
        redisFree(context);
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

    std::string state_to_string(FMI::FT::CriuJobState state) {
        switch (state) {
            case FMI::FT::CriuJobState::Running:
                return "RUNNING";
            case FMI::FT::CriuJobState::CheckpointRequested:
                return "CHECKPOINT_REQUESTED";
            case FMI::FT::CriuJobState::Quiesced:
                return "QUIESCED";
            case FMI::FT::CriuJobState::CheckpointComplete:
                return "CHECKPOINT_COMPLETE";
            case FMI::FT::CriuJobState::RestoreRequested:
                return "RESTORE_REQUESTED";
            case FMI::FT::CriuJobState::Restored:
                return "RESTORED";
        }
        throw std::runtime_error("Unknown CRIU job state");
    }

    std::string state_to_string(FMI::FT::CriuRankState state) {
        switch (state) {
            case FMI::FT::CriuRankState::Running:
                return "RUNNING";
            case FMI::FT::CriuRankState::Quiesced:
                return "QUIESCED";
        }
        throw std::runtime_error("Unknown CRIU rank state");
    }

    FMI::FT::CriuJobState criu_job_state_from_string(const std::string& state) {
        if (state == "RUNNING") {
            return FMI::FT::CriuJobState::Running;
        }
        if (state == "CHECKPOINT_REQUESTED") {
            return FMI::FT::CriuJobState::CheckpointRequested;
        }
        if (state == "QUIESCED") {
            return FMI::FT::CriuJobState::Quiesced;
        }
        if (state == "CHECKPOINT_COMPLETE") {
            return FMI::FT::CriuJobState::CheckpointComplete;
        }
        if (state == "RESTORE_REQUESTED") {
            return FMI::FT::CriuJobState::RestoreRequested;
        }
        if (state == "RESTORED") {
            return FMI::FT::CriuJobState::Restored;
        }
        throw std::runtime_error("Unknown CRIU job state string: " + state);
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
}

struct FMI::FT::Coordinator::Impl {
    Utils::FaultToleranceConfig config;
    std::string comm_name;
    Utils::peer_num num_peers;

    explicit Impl(Utils::FaultToleranceConfig config, std::string comm_name, Utils::peer_num num_peers) :
            config(std::move(config)),
            comm_name(std::move(comm_name)),
            num_peers(num_peers) {}

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

    [[nodiscard]] std::string lease_key(std::uint64_t epoch, Utils::peer_num rank) const {
        return prefix() + "epoch:" + std::to_string(epoch) + ":lease:" + std::to_string(rank);
    }

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
};

FMI::FT::Coordinator::Coordinator(std::string config_path, std::string comm_name, FMI::Utils::peer_num num_peers) {
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

FMI::FT::Coordinator::~Coordinator() = default;

void FMI::FT::Coordinator::ensure_job() const {
#if FMI_ENABLE_REDIS
    auto exists = run_command(impl->config, "EXISTS " + impl->meta_key());
    if (exists->integer == 0) {
        run_command(impl->config, "HSET " + impl->meta_key() + " world_size " + std::to_string(impl->num_peers) +
                                  " current_epoch 0");
        return;
    }

    auto world_size = run_command(impl->config, "HGET " + impl->meta_key() + " world_size");
    if (world_size->type == REDIS_REPLY_NIL) {
        run_command(impl->config, "HSET " + impl->meta_key() + " world_size " + std::to_string(impl->num_peers));
    } else if (std::stoul(world_size->str) != impl->num_peers) {
        throw std::runtime_error("Fault-tolerance world size does not match the stored communicator metadata");
    }

    auto current_epoch = run_command(impl->config, "HEXISTS " + impl->meta_key() + " current_epoch");
    if (current_epoch->integer == 0) {
        run_command(impl->config, "HSET " + impl->meta_key() + " current_epoch 0");
    }
#else
    throw std::runtime_error("Fault tolerance requires FMI to be built with Redis support");
#endif
}

std::uint64_t FMI::FT::Coordinator::epoch() const {
#if FMI_ENABLE_REDIS
    ensure_job();
    auto reply = run_command(impl->config, "HGET " + impl->meta_key() + " current_epoch");
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
    run_command(impl->config, "SADD " + impl->pending_key() + " " + std::to_string(rank));
    set_rank_state(current_epoch, rank, RankState::MigrationPending);
#endif
}

void FMI::FT::Coordinator::set_placement(std::uint64_t epoch, FMI::Utils::peer_num rank, const std::string& placement) const {
#if FMI_ENABLE_REDIS
    run_command(impl->config, "HSET " + impl->placement_key(epoch) + " " + std::to_string(rank) + " " + placement);
#endif
}

std::string FMI::FT::Coordinator::placement_for_rank(std::uint64_t epoch, FMI::Utils::peer_num rank) const {
#if FMI_ENABLE_REDIS
    auto reply = run_command(impl->config, "HGET " + impl->placement_key(epoch) + " " + std::to_string(rank));
    if (reply->type == REDIS_REPLY_NIL || reply->str == nullptr) {
        return "";
    }
    return reply->str;
#else
    return "";
#endif
}

std::vector<FMI::FT::RankDirectoryEntry> FMI::FT::Coordinator::directory_snapshot(std::uint64_t epoch) const {
#if FMI_ENABLE_REDIS
    std::vector<FMI::FT::RankDirectoryEntry> ranks;
    auto reply = run_command(impl->config, "HKEYS " + impl->members_key(epoch));
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

        auto worker = run_command(impl->config, "HGET " + impl->members_key(epoch) + " " + std::to_string(rank_id));
        if (worker->type != REDIS_REPLY_NIL && worker->str != nullptr) {
            info.worker_id = worker->str;
        }
        auto state = run_command(impl->config, "HGET " + impl->states_key(epoch) + " " + std::to_string(rank_id));
        if (state->type != REDIS_REPLY_NIL && state->str != nullptr) {
            info.state = rank_state_from_string(state->str);
        }
        auto placement = run_command(impl->config, "HGET " + impl->placement_key(epoch) + " " + std::to_string(rank_id));
        if (placement->type != REDIS_REPLY_NIL && placement->str != nullptr) {
            info.placement = placement->str;
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

void FMI::FT::Coordinator::clear_job_state() {
#if FMI_ENABLE_REDIS
    run_command(impl->config, "DEL " + impl->meta_key());
    run_command(impl->config, "DEL " + impl->pending_key());
    auto reply = run_command(impl->config, "KEYS " + impl->prefix() + "epoch:*");
    if (reply->type != REDIS_REPLY_ARRAY) {
        return;
    }
    for (std::size_t i = 0; i < reply->elements; i++) {
        auto* key = reply->element[i];
        if (key != nullptr && key->str != nullptr) {
            run_command(impl->config, "DEL " + std::string(key->str));
        }
    }
#endif
}

void FMI::FT::Coordinator::clear_criu_job_state() {
#if FMI_ENABLE_REDIS
    auto reply = run_command(impl->config, "KEYS " + impl->criu_prefix() + "*");
    if (reply->type != REDIS_REPLY_ARRAY) {
        return;
    }
    for (std::size_t i = 0; i < reply->elements; i++) {
        auto* key = reply->element[i];
        if (key != nullptr && key->str != nullptr) {
            run_command(impl->config, "DEL " + std::string(key->str));
        }
    }
#endif
}

bool FMI::FT::Coordinator::has_pending_migration() const {
#if FMI_ENABLE_REDIS
    auto reply = run_command(impl->config, "SCARD " + impl->pending_key());
    return reply->integer > 0;
#else
    return false;
#endif
}

bool FMI::FT::Coordinator::is_rank_pending(FMI::Utils::peer_num rank) const {
#if FMI_ENABLE_REDIS
    auto reply = run_command(impl->config, "SISMEMBER " + impl->pending_key() + " " + std::to_string(rank));
    return reply->integer == 1;
#else
    return false;
#endif
}

void FMI::FT::Coordinator::register_rank(std::uint64_t epoch, FMI::Utils::peer_num rank, const std::string& worker_id,
                                         FMI::FT::RankState state) const {
#if FMI_ENABLE_REDIS
    run_command(impl->config, "HSET " + impl->members_key(epoch) + " " + std::to_string(rank) + " " + worker_id);
    run_command(impl->config, "HSET " + impl->states_key(epoch) + " " + std::to_string(rank) + " " + state_to_string(state));
#endif
}

void FMI::FT::Coordinator::set_rank_state(std::uint64_t epoch, FMI::Utils::peer_num rank, FMI::FT::RankState state) const {
#if FMI_ENABLE_REDIS
    run_command(impl->config, "HSET " + impl->states_key(epoch) + " " + std::to_string(rank) + " " + state_to_string(state));
#endif
}

std::string FMI::FT::Coordinator::worker_for_rank(std::uint64_t epoch, FMI::Utils::peer_num rank) const {
#if FMI_ENABLE_REDIS
    auto reply = run_command(impl->config, "HGET " + impl->members_key(epoch) + " " + std::to_string(rank));
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
    run_command(impl->config,
                "PSETEX " + impl->lease_key(epoch, rank) + " " + std::to_string(impl->config.lease_ms) + " " + worker_id);
#endif
}

std::size_t FMI::FT::Coordinator::live_member_count(std::uint64_t epoch) const {
#if FMI_ENABLE_REDIS
    auto members = run_command(impl->config, "HKEYS " + impl->members_key(epoch));
    if (members->type != REDIS_REPLY_ARRAY) {
        return 0;
    }

    std::size_t count = 0;
    for (std::size_t i = 0; i < members->elements; i++) {
        auto* rank = members->element[i];
        if (rank == nullptr || rank->str == nullptr) {
            continue;
        }
        auto exists = run_command(
                impl->config,
                "EXISTS " + impl->lease_key(epoch, static_cast<Utils::peer_num>(std::stoul(rank->str))));
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
    run_command(impl->config, "HSET " + impl->meta_key() + " current_epoch " + std::to_string(next_epoch));
    run_command(impl->config, "DEL " + impl->pending_key());
#endif
}

unsigned int FMI::FT::Coordinator::heartbeat_ms() const {
    return impl->config.heartbeat_ms;
}

unsigned int FMI::FT::Coordinator::lease_ms() const {
    return impl->config.lease_ms;
}

void FMI::FT::Coordinator::criu_register_rank(FMI::Utils::peer_num rank, int pid, const std::string& host_id,
                                              const std::string& backend) const {
#if FMI_ENABLE_REDIS
    (void) criu_job_info();
    auto now_ms = current_time_millis();
    run_command(impl->config, "SADD " + impl->criu_ranks_key() + " " + std::to_string(rank));
    run_command(impl->config,
                "HSET " + impl->criu_rank_key(rank) +
                        " pid " + std::to_string(pid) +
                        " host_id " + host_id +
                        " backend " + backend +
                        " state " + state_to_string(CriuRankState::Running) +
                        " quiesced_generation 0" +
                        " last_heartbeat_ms " + std::to_string(now_ms));
#endif
}

void FMI::FT::Coordinator::criu_mark_rank_running(FMI::Utils::peer_num rank, int pid, const std::string& host_id,
                                                  const std::string& backend) const {
#if FMI_ENABLE_REDIS
    (void) criu_job_info();
    auto now_ms = current_time_millis();
    run_command(impl->config,
                "HSET " + impl->criu_rank_key(rank) +
                        " pid " + std::to_string(pid) +
                        " host_id " + host_id +
                        " backend " + backend +
                        " state " + state_to_string(CriuRankState::Running) +
                        " last_heartbeat_ms " + std::to_string(now_ms));
#endif
}

void FMI::FT::Coordinator::criu_mark_rank_quiesced(FMI::Utils::peer_num rank, int pid, const std::string& host_id,
                                                   const std::string& backend, std::uint64_t generation) const {
#if FMI_ENABLE_REDIS
    (void) criu_job_info();
    auto now_ms = current_time_millis();
    run_command(impl->config,
                "HSET " + impl->criu_rank_key(rank) +
                        " pid " + std::to_string(pid) +
                        " host_id " + host_id +
                        " backend " + backend +
                        " state " + state_to_string(CriuRankState::Quiesced) +
                        " quiesced_generation " + std::to_string(generation) +
                        " last_heartbeat_ms " + std::to_string(now_ms));
#endif
}

std::uint64_t FMI::FT::Coordinator::criu_request_checkpoint(const std::string& supervisor_id) const {
#if FMI_ENABLE_REDIS
    auto info = criu_job_info();
    std::uint64_t next_generation = std::max(info.requested_generation, info.completed_generation) + 1;
    run_command(impl->config,
                "HSET " + impl->criu_meta_key() +
                        " state " + state_to_string(CriuJobState::CheckpointRequested) +
                        " requested_generation " + std::to_string(next_generation) +
                        " supervisor " + supervisor_id);
    return next_generation;
#else
    return 0;
#endif
}

bool FMI::FT::Coordinator::criu_all_ranks_quiesced(std::uint64_t generation, const std::string& host_id) const {
#if FMI_ENABLE_REDIS
    auto ranks = criu_rank_info();
    std::size_t relevant_ranks = 0;
    for (const auto& rank : ranks) {
        if (!host_id.empty() && rank.host_id != host_id) {
            continue;
        }
        relevant_ranks++;
        if (rank.state != CriuRankState::Quiesced || rank.quiesced_generation != generation) {
            return false;
        }
    }
    return relevant_ranks > 0;
#else
    return false;
#endif
}

void FMI::FT::Coordinator::criu_mark_job_quiesced(std::uint64_t generation, const std::string& supervisor_id) const {
#if FMI_ENABLE_REDIS
    run_command(impl->config,
                "HSET " + impl->criu_meta_key() +
                        " state " + state_to_string(CriuJobState::Quiesced) +
                        " requested_generation " + std::to_string(generation) +
                        " supervisor " + supervisor_id);
#endif
}

void FMI::FT::Coordinator::criu_mark_checkpoint_complete(std::uint64_t generation, const std::string& supervisor_id) const {
#if FMI_ENABLE_REDIS
    run_command(impl->config,
                "HSET " + impl->criu_meta_key() +
                        " state " + state_to_string(CriuJobState::CheckpointComplete) +
                        " completed_generation " + std::to_string(generation) +
                        " supervisor " + supervisor_id);
#endif
}

std::uint64_t FMI::FT::Coordinator::criu_request_restore(std::uint64_t generation, const std::string& supervisor_id) const {
#if FMI_ENABLE_REDIS
    run_command(impl->config,
                "HSET " + impl->criu_meta_key() +
                        " state " + state_to_string(CriuJobState::RestoreRequested) +
                        " restore_generation " + std::to_string(generation) +
                        " supervisor " + supervisor_id);
    return generation;
#else
    return 0;
#endif
}

void FMI::FT::Coordinator::criu_mark_job_restored(std::uint64_t generation, const std::string& supervisor_id) const {
#if FMI_ENABLE_REDIS
    run_command(impl->config,
                "HSET " + impl->criu_meta_key() +
                        " state " + state_to_string(CriuJobState::Restored) +
                        " restore_generation " + std::to_string(generation) +
                        " supervisor " + supervisor_id);
#endif
}

std::uint64_t FMI::FT::Coordinator::criu_requested_generation() const {
    return criu_job_info().requested_generation;
}

std::uint64_t FMI::FT::Coordinator::criu_completed_generation() const {
    return criu_job_info().completed_generation;
}

std::uint64_t FMI::FT::Coordinator::criu_restore_generation() const {
    return criu_job_info().restore_generation;
}

FMI::FT::CriuJobInfo FMI::FT::Coordinator::criu_job_info() const {
#if FMI_ENABLE_REDIS
    auto exists = run_command(impl->config, "EXISTS " + impl->criu_meta_key());
    if (exists->integer == 0) {
        run_command(impl->config,
                    "HSET " + impl->criu_meta_key() +
                            " world_size " + std::to_string(impl->num_peers) +
                            " state " + state_to_string(CriuJobState::Running) +
                            " requested_generation 0 completed_generation 0 restore_generation 0 supervisor none");
    } else {
        auto world_size = run_command(impl->config, "HGET " + impl->criu_meta_key() + " world_size");
        if (world_size->type == REDIS_REPLY_NIL) {
            run_command(impl->config, "HSET " + impl->criu_meta_key() + " world_size " + std::to_string(impl->num_peers));
        } else if (std::stoul(world_size->str) != impl->num_peers) {
            throw std::runtime_error("CRIU FT world size does not match the stored communicator metadata");
        }
    }

    FMI::FT::CriuJobInfo info;
    auto state = run_command(impl->config, "HGET " + impl->criu_meta_key() + " state");
    if (state->type == REDIS_REPLY_NIL || state->str == nullptr) {
        info.state = CriuJobState::Running;
    } else {
        info.state = criu_job_state_from_string(state->str);
    }
    auto requested = run_command(impl->config, "HGET " + impl->criu_meta_key() + " requested_generation");
    if (requested->type != REDIS_REPLY_NIL && requested->str != nullptr) {
        info.requested_generation = std::stoull(requested->str);
    }
    auto completed = run_command(impl->config, "HGET " + impl->criu_meta_key() + " completed_generation");
    if (completed->type != REDIS_REPLY_NIL && completed->str != nullptr) {
        info.completed_generation = std::stoull(completed->str);
    }
    auto restore = run_command(impl->config, "HGET " + impl->criu_meta_key() + " restore_generation");
    if (restore->type != REDIS_REPLY_NIL && restore->str != nullptr) {
        info.restore_generation = std::stoull(restore->str);
    }
    auto supervisor = run_command(impl->config, "HGET " + impl->criu_meta_key() + " supervisor");
    if (supervisor->type != REDIS_REPLY_NIL && supervisor->str != nullptr) {
        info.supervisor = supervisor->str;
    }
    return info;
#else
    return {};
#endif
}

std::vector<FMI::FT::CriuRankInfo> FMI::FT::Coordinator::criu_rank_info() const {
#if FMI_ENABLE_REDIS
    std::vector<FMI::FT::CriuRankInfo> ranks;
    auto reply = run_command(impl->config, "SMEMBERS " + impl->criu_ranks_key());
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

        auto pid = run_command(impl->config, "HGET " + impl->criu_rank_key(rank_id) + " pid");
        if (pid->type != REDIS_REPLY_NIL && pid->str != nullptr) {
            info.pid = std::stoi(pid->str);
        }
        auto host = run_command(impl->config, "HGET " + impl->criu_rank_key(rank_id) + " host_id");
        if (host->type != REDIS_REPLY_NIL && host->str != nullptr) {
            info.host_id = host->str;
        }
        auto backend = run_command(impl->config, "HGET " + impl->criu_rank_key(rank_id) + " backend");
        if (backend->type != REDIS_REPLY_NIL && backend->str != nullptr) {
            info.backend = backend->str;
        }
        auto state = run_command(impl->config, "HGET " + impl->criu_rank_key(rank_id) + " state");
        if (state->type != REDIS_REPLY_NIL && state->str != nullptr) {
            info.state = criu_rank_state_from_string(state->str);
        }
        auto quiesced = run_command(impl->config, "HGET " + impl->criu_rank_key(rank_id) + " quiesced_generation");
        if (quiesced->type != REDIS_REPLY_NIL && quiesced->str != nullptr) {
            info.quiesced_generation = std::stoull(quiesced->str);
        }
        auto heartbeat = run_command(impl->config, "HGET " + impl->criu_rank_key(rank_id) + " last_heartbeat_ms");
        if (heartbeat->type != REDIS_REPLY_NIL && heartbeat->str != nullptr) {
            info.last_heartbeat_ms = std::stoull(heartbeat->str);
        }
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
