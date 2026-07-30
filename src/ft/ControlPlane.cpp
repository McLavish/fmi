#include "../../include/ft/ControlPlane.h"
#include "../../include/utils/Configuration.h"
#include "../../include/utils/Signals.h"

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
#include <unordered_map>
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
        // Clear the pointer as soon as it is freed: connect_redis throws when Redis is
        // unreachable, and leaving the freed pointer in place left ~Impl() to free it a
        // second time (double free) and any retried command() to read context->err after
        // free. This is reachable whenever Redis drops mid-run, because command() calls
        // connect() exactly when the context is already broken (context->err) or a command
        // came back with no reply.
        if (context != nullptr) {
            redisFree(context);
            context = nullptr;
        }
        context = connect_redis(config);
    }

    void disconnect() {
        std::lock_guard<std::mutex> lock(command_mutex);
        if (context != nullptr) {
            redisFree(context);
            context = nullptr;
        }
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

    //! HGETALL returning a field->value map (empty when the key is missing). Collapses the
    //! per-field round-trips in directory_snapshot / criu_rank_info into one command per hash.
    std::unordered_map<std::string, std::string> hgetall(const std::string& key) {
        std::unordered_map<std::string, std::string> fields;
        auto reply = command({"HGETALL", key});
        if (reply->type != REDIS_REPLY_ARRAY) {
            return fields;
        }
        for (std::size_t i = 0; i + 1 < reply->elements; i += 2) {
            auto* field = reply->element[i];
            auto* value = reply->element[i + 1];
            if (field == nullptr || field->str == nullptr || value == nullptr || value->str == nullptr) {
                continue;
            }
            fields.emplace(field->str, value->str);
        }
        return fields;
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

    [[nodiscard]] std::string boundaries_key() const {
        return prefix() + "boundaries";
    }

    //! Job-scoped, deliberately NOT epoch-scoped: an incarnation fences a lineage across every
    //! epoch it lives through, and a per-epoch counter would hand a rank's second replacement
    //! the number its first one already used.
    [[nodiscard]] std::string incarnations_key() const {
        return prefix() + "incarnations";
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

    [[nodiscard]] std::string moved_key(std::uint64_t epoch) const {
        return prefix() + "epoch:" + std::to_string(epoch) + ":moved";
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

    //! One hash for all staged image blobs (field "<epoch>:<rank>"), so clear_criu_state's
    //! criu:* pattern sweep reclaims them together with the registry.
    [[nodiscard]] std::string criu_images_key() const {
        return criu_prefix() + "images";
    }

    [[nodiscard]] static std::string criu_image_field(std::uint64_t epoch, Utils::peer_num rank) {
        return std::to_string(epoch) + ":" + std::to_string(rank);
    }
#endif
};

FMI::FT::ControlPlane::ControlPlane(std::string config_path, std::string comm_name, FMI::Utils::peer_num num_peers) {
    // The control-plane socket is dropped by a criu restore like any other, and the first
    // command afterwards is issued on the captured context. Without this the rank dies of
    // SIGPIPE inside hiredis before it can reconnect. See Utils::suppress_sigpipe.
    Utils::suppress_sigpipe();
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
    // No ensure_job() here: the constructor already ensures the job, and epoch() is on the hot path
    // (every Communicator operation reads it via enter_operation). Re-ensuring on each call adds ~3
    // Redis round-trips per operation for no benefit. request_migration(s) still call ensure_job()
    // explicitly, so the lazy-create guarantee for external callers is preserved.
    return impl->hget_u64(impl->meta_key(), "current_epoch");
#else
    return 0;
#endif
}

void FMI::FT::ControlPlane::request_migration(FMI::Utils::peer_num rank) {
    // The single-rank request is just the degenerate batch — one atomic code path for both.
    request_migrations({rank});
}

void FMI::FT::ControlPlane::request_migrations(const std::vector<FMI::Utils::peer_num>& ranks) const {
#if FMI_ENABLE_REDIS
    if (ranks.empty()) {
        return;
    }
    ensure_job();
    // One Lua EVAL so the whole batch flips together (the rank side observes either none or all of
    // it) AND so the request binds atomically to the epoch current at EVAL time: the script reads
    // current_epoch itself and builds the states key in-script, so a concurrent promote_epoch can
    // never make us write state into a stale epoch's hash.
    //
    // One cut at a time: if the pending set already holds a rank OUTSIDE the requested set, a
    // different migration cut is in flight and this request is rejected (error). Epoch promotion
    // is a single global cut — were two cuts allowed to overlap, the first promotion would clear
    // the second cut's pending marks and silently drop its migrations. Re-requesting a subset of
    // ranks that are already pending stays idempotent (migrate_local re-requests ranks the
    // orchestrator may have marked earlier), and a rank that already reached QUIESCED keeps that
    // state — knocking it back to MIGRATION_PENDING would make promote_epoch's quiescence gate
    // wait forever on a rank that is already parked.
    // KEYS[1]=pending, KEYS[2]=meta; ARGV[1]=pending state, ARGV[2]=states-key prefix,
    // ARGV[3]=states-key suffix, ARGV[4]=the QUIESCED wire string, ARGV[5..]=ranks.
    static const std::string script =
            "local requested = {} "
            "for i = 5, #ARGV do requested[ARGV[i]] = true end "
            "local pending = redis.call('SMEMBERS', KEYS[1]) "
            "for i = 1, #pending do "
            "if not requested[pending[i]] then "
            "return redis.error_reply('cannot request migration: rank ' .. pending[i] "
            ".. ' is already pending from another migration cut') "
            "end "
            "end "
            "local epoch = redis.call('HGET', KEYS[2], 'current_epoch') "
            "if not epoch then epoch = '0' end "
            "local states = ARGV[2] .. epoch .. ARGV[3] "
            "for i = 5, #ARGV do "
            "redis.call('SADD', KEYS[1], ARGV[i]) "
            "if redis.call('HGET', states, ARGV[i]) ~= ARGV[4] then "
            "redis.call('HSET', states, ARGV[i], ARGV[1]) "
            "end "
            "end "
            "return #ARGV - 4";
    std::vector<std::string> args = {"EVAL", script, "2", impl->pending_key(), impl->meta_key(),
                                     to_string(RankState::MigrationPending),
                                     impl->prefix() + "epoch:", ":states",
                                     to_string(RankState::Quiesced)};
    for (auto rank : ranks) {
        args.push_back(std::to_string(rank));
    }
    impl->command(args);
#else
    (void) ranks;
#endif
}

void FMI::FT::ControlPlane::set_placement(std::uint64_t epoch, FMI::Utils::peer_num rank, const std::string& placement) const {
#if FMI_ENABLE_REDIS
    impl->command({"HSET", impl->placement_key(epoch), std::to_string(rank), placement});
#endif
}

void FMI::FT::ControlPlane::join_epoch(std::uint64_t epoch, FMI::Utils::peer_num rank,
                                       const std::string& worker_id, const std::string& placement) const {
    register_rank(epoch, rank, worker_id, RankState::Active);
    if (!placement.empty()) {
        set_placement(epoch, rank, placement);
    }
}

std::uint64_t FMI::FT::ControlPlane::claim_incarnation(FMI::Utils::peer_num rank) const {
#if FMI_ENABLE_REDIS
    // HINCRBY, so two processes racing to serve one rank cannot come away with the same
    // number — which is the whole point: the loser must be recognisable as superseded.
    auto reply = impl->command({"HINCRBY", impl->incarnations_key(), std::to_string(rank), "1"});
    if (reply->type != REDIS_REPLY_INTEGER || reply->integer < 1) {
        throw std::runtime_error("Could not claim an incarnation for rank " + std::to_string(rank));
    }
    // The first claimant is incarnation 0, so a job that never migrates carries zeroes and the
    // lineage rules hold trivially.
    return static_cast<std::uint64_t>(reply->integer) - 1;
#else
    (void) rank;
    return 0;
#endif
}

std::uint64_t FMI::FT::ControlPlane::current_incarnation(FMI::Utils::peer_num rank) const {
#if FMI_ENABLE_REDIS
    const std::uint64_t claimed = impl->hget_u64(impl->incarnations_key(), std::to_string(rank), 0);
    return claimed == 0 ? 0 : claimed - 1;
#else
    (void) rank;
    return 0;
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
    // Three HGETALLs (members/states/placement) instead of HKEYS + three HGETs per rank. The
    // members hash defines membership and already carries each rank's worker_id as its value.
    auto members = impl->hgetall(impl->members_key(epoch));
    auto states = impl->hgetall(impl->states_key(epoch));
    auto placement = impl->hgetall(impl->placement_key(epoch));
    for (const auto& [rank_str, worker_id] : members) {
        FMI::FT::RankDirectoryEntry info;
        info.rank = static_cast<FMI::Utils::peer_num>(std::stoul(rank_str));
        info.worker_id = worker_id;
        if (auto it = states.find(rank_str); it != states.end()) {
            info.state = FMI::FT::rank_state_from_string(it->second);
        }
        if (auto it = placement.find(rank_str); it != placement.end()) {
            info.placement = it->second;
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
    impl->command({"DEL", impl->boundaries_key()});
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

FMI::FT::ControlPlane::OperationSnapshot FMI::FT::ControlPlane::observe_operation(FMI::Utils::peer_num rank,
                                                                                 std::uint64_t boundary,
                                                                                 std::uint64_t epoch) const {
#if FMI_ENABLE_REDIS
    // One script = one round-trip on the per-operation hot path, and an atomic pairing of the
    // epoch with the pending set (promotion updates both in a single script on its side).
    // The same script publishes this rank's boundary (telemetry, costs no extra round-trip)
    // and maintains the consensus cut boundary: the FIRST boundary observation that sees a
    // non-empty pending set fixes cut_index.
    //
    // Epoch guard: the boundary is published (and a cut proposed) only when the caller's
    // epoch still is the current epoch. A rank that slept through a promotion would otherwise
    // pollute the NEW epoch's boundary hash with its old-epoch counter — corrupting both the
    // next cut proposal and promote_epoch's cut gate. Such a caller only needs the epoch from
    // the snapshot (it rejoins immediately); the cut fields are returned as "no cut".
    //
    // Proposal rule: cut at the proposer's own boundary B, unless some OTHER rank has
    // published a boundary >= B — then cut one operation later, at max_published + 1.
    // Publishing happens atomically with the pending check, so the hash records exactly who
    // could be inside which operation: a rank that published J and did not park saw an empty
    // pending set in that same script call and may be executing operation J right now. Cutting
    // at C > J keeps everyone — the migration target included — executing until boundary C, so
    // that rank gets the target's participation instead of blocking forever on (or reading EOF
    // from) a target that quiesced at a boundary it had already passed. Ranks whose published
    // boundary is BELOW B are only inside operations the proposer already completed: whatever
    // they still owe or expect involving the target is already in flight, so cutting at B is
    // safe and costs no deferral (this also keeps a lone rank able to quiesce immediately).
    //
    // The cut is returned as -1 for "none": 0 is a legitimate boundary (a cut proposed before
    // any operation ran) and must not be conflated with "no cut proposed".
    static const std::string script =
            "local epoch = redis.call('HGET', KEYS[1], 'current_epoch') "
            "if not epoch then epoch = '0' end "
            "local same_epoch = tonumber(ARGV[3]) == tonumber(epoch) "
            "if same_epoch then redis.call('HSET', KEYS[3], ARGV[1], ARGV[2]) end "
            "local pending = redis.call('SCARD', KEYS[2]) "
            "local cut = -1 "
            "if pending > 0 then "
            "local existing = redis.call('HGET', KEYS[1], 'cut_index') "
            "if existing then cut = tonumber(existing) "
            "elseif same_epoch then "
            "cut = tonumber(ARGV[2]) "
            "local bounds = redis.call('HGETALL', KEYS[3]) "
            "for i = 1, #bounds, 2 do "
            "if bounds[i] ~= ARGV[1] and tonumber(bounds[i + 1]) >= cut then "
            "cut = tonumber(bounds[i + 1]) + 1 "
            "end "
            "end "
            "redis.call('HSET', KEYS[1], 'cut_index', cut) "
            "end "
            "end "
            "return {epoch, pending, redis.call('SISMEMBER', KEYS[2], ARGV[1]), cut}";
    auto reply = impl->command({"EVAL", script, "3", impl->meta_key(), impl->pending_key(),
                                impl->boundaries_key(), std::to_string(rank), std::to_string(boundary),
                                std::to_string(epoch)});
    OperationSnapshot snapshot;
    if (reply->type == REDIS_REPLY_ARRAY && reply->elements == 4) {
        auto* current = reply->element[0];
        if (current != nullptr && current->str != nullptr) {
            snapshot.epoch = std::stoull(current->str);
        }
        snapshot.any_pending = reply->element[1] != nullptr && reply->element[1]->integer > 0;
        snapshot.self_pending = reply->element[2] != nullptr && reply->element[2]->integer == 1;
        if (reply->element[3] != nullptr && reply->element[3]->integer >= 0) {
            snapshot.cut_proposed = true;
            snapshot.cut_index = static_cast<std::uint64_t>(reply->element[3]->integer);
        }
    }
    return snapshot;
#else
    (void) rank;
    (void) boundary;
    (void) epoch;
    return {};
#endif
}

std::vector<FMI::Utils::peer_num> FMI::FT::ControlPlane::moved_ranks(std::uint64_t epoch) const {
#if FMI_ENABLE_REDIS
    std::vector<FMI::Utils::peer_num> moved;
    auto reply = impl->command({"SMEMBERS", impl->moved_key(epoch)});
    if (reply->type == REDIS_REPLY_ARRAY) {
        for (std::size_t i = 0; i < reply->elements; i++) {
            auto* member = reply->element[i];
            if (member != nullptr && member->str != nullptr) {
                moved.push_back(static_cast<FMI::Utils::peer_num>(std::stoul(member->str)));
            }
        }
    }
    std::sort(moved.begin(), moved.end());
    return moved;
#else
    (void) epoch;
    return {};
#endif
}

std::vector<std::pair<FMI::Utils::peer_num, std::uint64_t>> FMI::FT::ControlPlane::operation_boundaries() const {
#if FMI_ENABLE_REDIS
    std::vector<std::pair<FMI::Utils::peer_num, std::uint64_t>> boundaries;
    auto reply = impl->command({"HGETALL", impl->boundaries_key()});
    if (reply->type == REDIS_REPLY_ARRAY) {
        for (std::size_t i = 0; i + 1 < reply->elements; i += 2) {
            auto* field = reply->element[i];
            auto* value = reply->element[i + 1];
            if (field != nullptr && field->str != nullptr && value != nullptr && value->str != nullptr) {
                boundaries.emplace_back(static_cast<FMI::Utils::peer_num>(std::stoul(field->str)),
                                        std::stoull(value->str));
            }
        }
    }
    std::sort(boundaries.begin(), boundaries.end());
    return boundaries;
#else
    return {};
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
    impl->command({"HSET", impl->states_key(epoch), std::to_string(rank), to_string(state)});
#endif
}

void FMI::FT::ControlPlane::set_rank_state(std::uint64_t epoch, FMI::Utils::peer_num rank, FMI::FT::RankState state) const {
#if FMI_ENABLE_REDIS
    impl->command({"HSET", impl->states_key(epoch), std::to_string(rank), to_string(state)});
#endif
}

void FMI::FT::ControlPlane::disconnect() {
#if FMI_ENABLE_REDIS
    impl->disconnect();
#endif
}

bool FMI::FT::ControlPlane::promote_epoch(std::uint64_t next_epoch) const {
#if FMI_ENABLE_REDIS
    // Quiescence gate: refuse to promote (via error_reply -> exception) while any pending rank
    // has not marked itself QUIESCED in the epoch being left. Promotion atomically clears the
    // pending set, so promoting past an un-quiesced target would erase the only signal telling
    // that rank it is a migration target — its next operation would rejoin the new epoch as a
    // survivor next to its replacement (two live processes owning one logical rank).
    //
    // When the promotion advances the epoch, also reclaim the per-epoch hashes of the epoch we
    // are leaving (the in-script `current`): members/states/placement. They are never read after
    // promotion — every backend-visible name is epoch-qualified and survivors join_epoch into the
    // new epoch — so without this they accumulate in Redis for the lifetime of the job. Building
    // the keys from `current` (not next-1) makes the cleanup correct regardless of step size.
    // KEYS[1]=meta, KEYS[2]=pending, KEYS[3]=boundaries; ARGV[1]=next epoch, ARGV[2]=epoch-key
    // prefix, ARGV[3..5]=the three per-epoch suffixes, ARGV[6]=the QUIESCED wire string,
    // ARGV[7]=the ":moved" suffix.
    //
    // Second gate (after quiescence): when a consensus cut was fixed, every MEMBER of the
    // epoch being left must have published a boundary >= the cut. Promoting past a slower
    // rank would strand it: it would rejoin at the new epoch having completed fewer
    // operations than its cohort (the cut state is deleted below, so it could not even
    // finish), permanently misaligning the operation streams. Ranks below the cut can always
    // finish to it — their peers completed those operations, so everything they still need
    // is already in their socket buffers / the object store — so this gate opens by itself;
    // callers retry exactly as for the quiescence gate. An epoch promoted without any cut
    // (no operation observed the pending set — e.g. a bare orchestrator promote) skips this
    // gate: there is no alignment obligation to enforce.
    //
    // The consensus cut state (cut_index + published boundaries) belongs to the epoch being
    // left: every rank resets its boundary counter to 0 when it rejoins, so both are cleared
    // here, atomically with the promotion that releases the cut.
    // The pending set is persisted as the entered epoch's "moved" set before it is cleared:
    // rejoining ranks read it to reconnect only the links that involve a migrated rank and
    // keep their surviving connections (selective re-pair). Moved sets of OLDER epochs are
    // deliberately NOT reclaimed here (only at clear_job_state): a rank that crosses several
    // epochs in one rejoin must union the moved sets of every epoch it skipped, so they must
    // survive later promotions. One small set per cut for the job's lifetime.
    static const std::string script =
            "local current = redis.call('HGET', KEYS[1], 'current_epoch') "
            "if not current then current = '0' end "
            "if tonumber(ARGV[1]) <= tonumber(current) then return 0 end "
            "local states = ARGV[2]..current..ARGV[4] "
            "local pending = redis.call('SMEMBERS', KEYS[2]) "
            "for i = 1, #pending do "
            "if redis.call('HGET', states, pending[i]) ~= ARGV[6] then "
            "return redis.error_reply('cannot promote epoch ' .. ARGV[1] .. ': rank ' .. pending[i] "
            ".. ' is marked for migration but has not quiesced') "
            "end "
            "end "
            "local cutv = redis.call('HGET', KEYS[1], 'cut_index') "
            "if cutv then "
            "local members = redis.call('HKEYS', ARGV[2]..current..ARGV[3]) "
            "for i = 1, #members do "
            "local b = redis.call('HGET', KEYS[3], members[i]) "
            "if (not b) or tonumber(b) < tonumber(cutv) then "
            "return redis.error_reply('cannot promote epoch ' .. ARGV[1] .. ': rank ' .. members[i] "
            ".. ' has not reached the consensus cut boundary') "
            "end "
            "end "
            "end "
            "redis.call('HSET', KEYS[1], 'current_epoch', ARGV[1]) "
            "redis.call('HDEL', KEYS[1], 'cut_index') "
            "local moved = ARGV[2]..ARGV[1]..ARGV[7] "
            "redis.call('DEL', moved) "
            "for i = 1, #pending do redis.call('SADD', moved, pending[i]) end "
            "redis.call('DEL', KEYS[2]) "
            "redis.call('DEL', KEYS[3]) "
            "redis.call('DEL', ARGV[2]..current..ARGV[3], ARGV[2]..current..ARGV[4], "
            "ARGV[2]..current..ARGV[5]) "
            "return 1";
    auto reply = impl->command({"EVAL", script, "3", impl->meta_key(), impl->pending_key(),
                                impl->boundaries_key(), std::to_string(next_epoch),
                                impl->prefix() + "epoch:", ":members",
                                ":states", ":placement", to_string(RankState::Quiesced), ":moved"});
    return reply->integer == 1;
#else
    (void) next_epoch;
    return false;
#endif
}

void FMI::FT::ControlPlane::mark_rank_quiesced(std::uint64_t epoch, FMI::Utils::peer_num rank) const {
    set_rank_state(epoch, rank, RankState::Quiesced);
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

void FMI::FT::ControlPlane::criu_image_put(std::uint64_t epoch, FMI::Utils::peer_num rank,
                                          const std::string& blob) const {
#if FMI_ENABLE_REDIS
    ensure_criu_registry();
    impl->command({"HSET", impl->criu_images_key(), Impl::criu_image_field(epoch, rank), blob});
#else
    (void) epoch; (void) rank; (void) blob;
#endif
}

std::string FMI::FT::ControlPlane::criu_image_get(std::uint64_t epoch, FMI::Utils::peer_num rank) const {
#if FMI_ENABLE_REDIS
    auto reply = impl->command({"HGET", impl->criu_images_key(), Impl::criu_image_field(epoch, rank)});
    if (reply->type == REDIS_REPLY_NIL || reply->str == nullptr) {
        throw std::runtime_error("No staged CRIU image for rank " + std::to_string(rank) +
                                 " at epoch " + std::to_string(epoch));
    }
    // The archive is binary: size via reply->len, never strlen semantics.
    return {reply->str, reply->len};
#else
    (void) epoch; (void) rank;
    return {};
#endif
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

        // One HGETALL per rank instead of six HGETs. Missing fields keep the struct defaults.
        auto fields = impl->hgetall(impl->criu_rank_key(rank_id));
        auto field = [&fields](const std::string& name) -> const std::string* {
            auto it = fields.find(name);
            return it != fields.end() ? &it->second : nullptr;
        };
        if (const auto* pid = field("pid")) {
            info.pid = std::stoi(*pid);
        }
        if (const auto* host = field("host_id")) {
            info.host_id = *host;
        }
        if (const auto* backend = field("backend")) {
            info.backend = *backend;
        }
        if (const auto* state = field("state")) {
            info.state = criu_rank_state_from_string(*state);
        }
        if (const auto* generation = field("quiesced_generation")) {
            info.quiesced_generation = std::stoull(*generation);
        }
        if (const auto* heartbeat = field("last_heartbeat_ms")) {
            info.last_heartbeat_ms = std::stoull(*heartbeat);
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
#endif
