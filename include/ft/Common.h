#ifndef FMI_FT_COMMON_H
#define FMI_FT_COMMON_H

#include "../utils/Common.h"

#include <cstdint>
#include <string>
#include <vector>

namespace FMI::FT {
    enum class Event : std::uint8_t {
        None,
        MigrateSelf,
        Reconfigured
    };

    enum class RankState : std::uint8_t {
        Active,
        MigrationPending,
        Quiesced,
        Replaced
    };

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
