#ifndef FMI_FT_CRIUREQUIREMENTS_H
#define FMI_FT_CRIUREQUIREMENTS_H

#include "../../utils/Configuration.h"

#include <stdexcept>
#include <string>

namespace FMI::FT {
    //! CRIU can only dump/restore a process whose data plane is Direct — the sole backend that
    //! releases its sockets before checkpoint (Direct::prepare_for_checkpoint). CRIU/S3 channels
    //! would be captured with live sockets, and the image registry would falsely record "Direct".
    //! Both the rank-side runtime and the host-local rank agent enforce this; centralized here so
    //! the rule and its message live in exactly one place.
    inline void require_checkpoint_safe_data_plane(const FMI::Utils::FaultToleranceConfig& config) {
        if (config.preferred_data_backend != "Direct") {
            throw std::runtime_error(
                    "fault_tolerance.state_transfer=\"criu\" requires preferred_data_backend=\"Direct\" "
                    "(the only checkpoint-safe data backend); got \"" + config.preferred_data_backend + "\"");
        }
    }
}

#endif //FMI_FT_CRIUREQUIREMENTS_H
