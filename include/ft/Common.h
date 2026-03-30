#ifndef FMI_FT_COMMON_H
#define FMI_FT_COMMON_H

#include <cstdint>

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
}

#endif //FMI_FT_COMMON_H
