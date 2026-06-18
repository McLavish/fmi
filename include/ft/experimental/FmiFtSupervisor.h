#ifndef FMI_FT_FMIFTSUPERVISOR_H
#define FMI_FT_FMIFTSUPERVISOR_H

#include "../../utils/Common.h"
#include "../../utils/Configuration.h"
#include "../Coordinator.h"

#include <cstdint>
#include <memory>
#include <string>

namespace FMI::FT {
    //! Shared base for the host-local CRIU supervisors (whole-job CriuSupervisor and single-rank
    //! MigrationSupervisor). It owns the identical construction state both need — the config, the
    //! resolved host/supervisor identity, and the Redis Coordinator — and the shared "FT enabled +
    //! Redis control plane" precondition, so the two cannot drift apart. Construction is two-phase
    //! by design: the derived ctor runs validate_base() plus any backend-specific checks and only
    //! then calls connect(), preserving the invariant that configuration is fully validated before
    //! a Redis connection is opened.
    class FmiFtSupervisor {
    public:
        //! Remove this communicator's CRIU image tree and clear its CRIU control-plane state.
        void cleanup();

    protected:
        FmiFtSupervisor(std::string config_path, std::string comm_name, FMI::Utils::peer_num num_peers);
        ~FmiFtSupervisor() = default;

        //! Preconditions common to every FT supervisor: fault tolerance enabled and Redis control.
        void validate_base() const;

        //! Resolve host/supervisor identity and open the control-plane Coordinator. Call after all
        //! configuration validation has passed.
        void connect();

        std::string config_path;
        std::string comm_name;
        FMI::Utils::peer_num num_peers;
        FMI::Utils::FaultToleranceConfig config;
        std::string host_id;
        std::string supervisor_id;
        std::shared_ptr<FMI::FT::Coordinator> coordinator;
    };
}

#endif //FMI_FT_FMIFTSUPERVISOR_H
