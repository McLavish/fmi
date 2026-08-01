#ifndef FMI_FT_CRIUREQUIREMENTS_H
#define FMI_FT_CRIUREQUIREMENTS_H

#include "../../utils/Configuration.h"

#include <stdexcept>
#include <string>

namespace FMI::FT {
    //! CRIU can only dump/restore a process whose channels release their transport before the
    //! checkpoint (Channel::prepare_for_checkpoint). Three backends do, each for its own reason:
    //! Direct closes its peer sockets, and its pairing is re-driven through the rendezvous server
    //! afterwards. DirectTCP closes those too and then drops everything that ties the rank to the
    //! machine it was dumped on — the listening socket, the cached advertised address, and the
    //! registry client — so a process restored elsewhere binds a fresh port, re-resolves the
    //! address peers must dial, and re-publishes it rather than advertising an address that no
    //! longer reaches it. One caveat rides on that re-resolution: an explicit advertise_host is
    //! taken verbatim from the (checkpointed) config, so cross-host restore needs advertise_host
    //! left empty — the auto probe picks the address that routes to the registry from whichever
    //! machine the process wakes up on. Same-host restore is fine either way. Redis drops its client connection (object state lives server-side and
    //! survives). S3 is not checkpoint-safe: the AWS SDK holds live sockets and worker threads
    //! that would be captured in the image. Both the rank-side runtime and the host-local rank
    //! agent enforce this; centralized here so the rule and its message live in exactly one place.
    inline bool is_checkpoint_safe_backend(const std::string& name) {
        return name == "Direct" || name == "DirectTCP" || name == "Redis";
    }

    inline void require_checkpoint_safe_data_plane(const FMI::Utils::FaultToleranceConfig& config) {
        if (!is_checkpoint_safe_backend(config.preferred_data_backend)) {
            throw std::runtime_error(
                    "fault_tolerance.state_transfer=\"criu\" requires a checkpoint-safe "
                    "preferred_data_backend (\"Direct\", \"DirectTCP\" or \"Redis\"); got \"" +
                    config.preferred_data_backend + "\"");
        }
    }

    //! The same rule applied to the whole ENABLED channel set: Communicator::build_channels
    //! instantiates every enabled backend, and an extra enabled non-checkpoint-safe channel
    //! would carry live sockets (and, for S3, AWS SDK threads) into the criu image even though
    //! the pinned policy never selects it.
    inline void require_checkpoint_safe_channels(FMI::Utils::Configuration& configuration) {
        for (const auto& [name, params] : configuration.get_active_channels()) {
            if (!is_checkpoint_safe_backend(name)) {
                throw std::runtime_error(
                        "fault_tolerance.state_transfer=\"criu\" requires every enabled channel "
                        "backend to be checkpoint-safe (\"Direct\", \"DirectTCP\" or \"Redis\"), "
                        "but \"" + name + "\" is enabled");
            }
        }
    }
}

#endif //FMI_FT_CRIUREQUIREMENTS_H
