#ifndef FMI_FT_COMMON_H
#define FMI_FT_COMMON_H

#include "../utils/Common.h"

#include <cstdint>
#include <string>
#include <vector>

namespace FMI::FT {
    enum class Mode : std::uint8_t {
        SafePointRestart,
        CriuCoordinated
    };

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

    enum class CriuJobState : std::uint8_t {
        Running,
        CheckpointRequested,
        Quiesced,
        CheckpointComplete,
        RestoreRequested,
        Restored
    };

    enum class CriuRankState : std::uint8_t {
        Running,
        Quiesced
    };

    struct CriuJobInfo {
        CriuJobState state = CriuJobState::Running;
        std::uint64_t requested_generation = 0;
        std::uint64_t completed_generation = 0;
        std::uint64_t restore_generation = 0;
        std::string supervisor;
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

    struct CriuStatus {
        CriuJobInfo job;
        std::vector<CriuRankInfo> ranks;
    };
}

#endif //FMI_FT_COMMON_H
