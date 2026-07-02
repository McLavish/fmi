#ifndef FMI_FT_COMMON_H
#define FMI_FT_COMMON_H

#include "../utils/Common.h"

#include <cstdint>
#include <stdexcept>
#include <string>
#include <vector>

namespace FMI::FT {
    enum class RankState : std::uint8_t {
        Active,
        MigrationPending,
        Quiesced
    };

    //! Wire format of RankState in the Redis control plane. Shared by the C++ control plane and
    //! the Python binding so the strings can never drift apart.
    inline std::string to_string(RankState state) {
        switch (state) {
            case RankState::Active:
                return "ACTIVE";
            case RankState::MigrationPending:
                return "MIGRATION_PENDING";
            case RankState::Quiesced:
                return "QUIESCED";
        }
        throw std::runtime_error("Unknown rank state");
    }

    inline RankState rank_state_from_string(const std::string& state) {
        if (state == "ACTIVE") {
            return RankState::Active;
        }
        if (state == "MIGRATION_PENDING") {
            return RankState::MigrationPending;
        }
        if (state == "QUIESCED") {
            return RankState::Quiesced;
        }
        throw std::runtime_error("Unknown rank state string: " + state);
    }

    struct RankDirectoryEntry {
        FMI::Utils::peer_num rank = 0;
        std::string worker_id;
        std::string placement;
        RankState state = RankState::Active;
    };

    //! Epoch-qualified communicator name. Every backend-visible name is epoch-fenced so stale
    //! messages/objects from an old epoch can never be consumed after reconfiguration. Used by
    //! both the Communicator (initial + reconfigure) and the migration runtime.
    inline std::string epoch_comm_name(const std::string& base, std::uint64_t epoch) {
        return base + "@epoch=" + std::to_string(epoch);
    }

#ifdef FMI_ENABLE_CRIU
    enum class CriuRankState : std::uint8_t {
        Running,
        Quiesced
    };

    struct CriuRankInfo {
        FMI::Utils::peer_num rank = 0;
        int pid = 0;
        std::string host_id;
        std::string backend;
        CriuRankState state = CriuRankState::Running;
        std::uint64_t quiesced_generation = 0;
        std::uint64_t last_heartbeat_ms = 0;
    };
#endif
}

#endif //FMI_FT_COMMON_H
