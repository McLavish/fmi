#include "../../include/comm/DrainCoordinator.h"

#include "../../include/comm/PeerRegistry.h"

#include <exception>
#include <stdexcept>
#include <utility>

std::string FMI::Comm::MemberRecord::encode() const {
    return std::to_string(epoch) + ":" + ip + ":" + std::to_string(port) + ":" +
           std::to_string(nonce);
}

bool FMI::Comm::MemberRecord::decode(const std::string& text, MemberRecord& out) {
    // Split from both ends rather than tokenising: an IPv4 literal contains no colon, but the
    // parse must not depend on that being true of whatever a future advertise_host carries.
    const auto first = text.find(':');
    if (first == std::string::npos) {
        return false;
    }
    const auto last = text.rfind(':');
    const auto second_last = last == std::string::npos ? std::string::npos : text.rfind(':', last - 1);
    if (last == first || second_last == std::string::npos || second_last <= first) {
        return false;
    }
    try {
        out.epoch = std::stoull(text.substr(0, first));
        out.ip = text.substr(first + 1, second_last - first - 1);
        out.port = std::stoi(text.substr(second_last + 1, last - second_last - 1));
        out.nonce = std::stoull(text.substr(last + 1));
    } catch (const std::exception&) {
        return false;
    }
    return true;
}

const char* FMI::Comm::to_string(DrainEvent::Type type) {
    switch (type) {
        case DrainEvent::Type::Migrate:
            return "migrate";
        case DrainEvent::Type::Leaving:
            return "leaving";
        case DrainEvent::Type::Sealed:
            return "sealed";
        case DrainEvent::Type::Restored:
            return "restored";
        default:
            return "unknown";
    }
}

FMI::Comm::DrainEvent::Type FMI::Comm::drain_event_type_from(const std::string& text) {
    if (text == "migrate") {
        return DrainEvent::Type::Migrate;
    }
    if (text == "leaving") {
        return DrainEvent::Type::Leaving;
    }
    if (text == "sealed") {
        return DrainEvent::Type::Sealed;
    }
    if (text == "restored") {
        return DrainEvent::Type::Restored;
    }
    // Never a throw: a stream this rank shares with a newer build must stay readable past an
    // event kind this build has never heard of.
    return DrainEvent::Type::Unknown;
}

FMI::Comm::RedisDrainCoordinator::RedisDrainCoordinator(std::string host, int port,
                                                        std::string comm_name, unsigned int ttl_s,
                                                        long timeout_ms)
        : comm_name(std::move(comm_name)), ttl_s(ttl_s),
          timeout_ms(timeout_ms <= 0 ? 1000 : timeout_ms),
          registry(std::make_unique<PeerRegistry>(std::move(host), port)) {
    // Its own connection, never the transport registry's: the trigger thread reads this one
    // while an application thread may be blocked in a peer lookup on the other, and a migration
    // that has to queue behind an unrelated HGETALL is a migration whose bound is someone
    // else's timeout.
}

FMI::Comm::RedisDrainCoordinator::~RedisDrainCoordinator() = default;

std::string FMI::Comm::RedisDrainCoordinator::members_key() const {
    return "fmi:drain:" + comm_name + ":members";
}

std::string FMI::Comm::RedisDrainCoordinator::events_key() const {
    return "fmi:drain:" + comm_name + ":events";
}

std::string FMI::Comm::RedisDrainCoordinator::batch_key() const {
    return "fmi:drain:" + comm_name + ":batch";
}

void FMI::Comm::RedisDrainCoordinator::publish_member(Utils::peer_num rank,
                                                      const MemberRecord& record) {
    registry->publish(members_key(), std::to_string(rank), record.encode(), ttl_s, timeout_ms);
}

std::map<FMI::Utils::peer_num, FMI::Comm::MemberRecord> FMI::Comm::RedisDrainCoordinator::members() {
    std::map<Utils::peer_num, MemberRecord> out;
    for (const auto& [rank, text] : registry->snapshot(members_key(), timeout_ms)) {
        MemberRecord record;
        if (MemberRecord::decode(text, record)) {
            out[rank] = record;
        }
    }
    return out;
}

void FMI::Comm::RedisDrainCoordinator::remove_member(Utils::peer_num rank) {
    registry->hdel(members_key(), std::to_string(rank), timeout_ms);
}

std::string FMI::Comm::RedisDrainCoordinator::emit(const DrainEvent& event) {
    std::vector<std::pair<std::string, std::string>> fields{
            {"type", to_string(event.type)},
            {"rank", std::to_string(event.rank)},
            {"epoch", std::to_string(event.epoch)},
            {"batch", event.batch}};
    for (const auto& [key, value] : event.extra) {
        // The four names above are the schema; an extra that collides with one of them would
        // rewrite the event rather than annotate it.
        if (key == "type" || key == "rank" || key == "epoch" || key == "batch") {
            continue;
        }
        fields.emplace_back(key, value);
    }
    return registry->xadd(events_key(), fields, ttl_s, timeout_ms);
}

std::string FMI::Comm::RedisDrainCoordinator::tail_id() {
    return registry->tail_id(events_key(), timeout_ms);
}

std::vector<std::pair<std::string, FMI::Comm::DrainEvent>>
FMI::Comm::RedisDrainCoordinator::read_after(const std::string& last_id, long block_ms) {
    std::vector<std::pair<std::string, DrainEvent>> out;
    for (auto& [id, fields] : registry->xread_after(events_key(), last_id, block_ms, timeout_ms)) {
        DrainEvent event;
        auto it = fields.find("type");
        event.type = it == fields.end() ? DrainEvent::Type::Unknown
                                        : drain_event_type_from(it->second);
        try {
            it = fields.find("rank");
            if (it != fields.end()) {
                event.rank = static_cast<Utils::peer_num>(std::stoul(it->second));
            }
            it = fields.find("epoch");
            if (it != fields.end()) {
                event.epoch = std::stoull(it->second);
            }
        } catch (const std::exception&) {
            // A malformed entry is skipped, not fatal: the id still advances, so one bad write
            // cannot wedge every reader on the stream forever.
            continue;
        }
        it = fields.find("batch");
        if (it != fields.end()) {
            event.batch = it->second;
        }
        for (auto& [key, value] : fields) {
            if (key != "type" && key != "rank" && key != "epoch" && key != "batch") {
                event.extra[key] = value;
            }
        }
        out.emplace_back(id, std::move(event));
    }
    return out;
}

bool FMI::Comm::RedisDrainCoordinator::try_batch_lock(const std::string& owner, long px_ms) {
    return registry->set_nx_px(batch_key(), owner, px_ms, timeout_ms);
}

void FMI::Comm::RedisDrainCoordinator::release_batch_lock(const std::string& owner) {
    registry->del_if_equal(batch_key(), owner, timeout_ms);
}

void FMI::Comm::RedisDrainCoordinator::disconnect() {
    registry->disconnect();
}
