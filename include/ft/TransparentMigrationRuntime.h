#ifndef FMI_FT_TRANSPARENTMIGRATIONRUNTIME_H
#define FMI_FT_TRANSPARENTMIGRATIONRUNTIME_H

#include "../utils/Common.h"
#include "../utils/Configuration.h"
#include "ControlPlane.h"
#include "OperationRuntime.h"

#include <cstdint>
#include <functional>
#include <memory>
#include <string>

namespace FMI::FT {
    class TransparentMigrationRuntime : public OperationRuntime {
    public:
        TransparentMigrationRuntime(
            FMI::Utils::peer_num peer_id,
            std::string worker_id,
            std::string placement,
            std::uint64_t active_epoch,
            const FMI::Utils::FaultToleranceConfig& config,
            std::shared_ptr<FMI::FT::ControlPlane> control_plane,
            std::string base_comm_name,
            std::function<void(const std::string&)> reconfigure_callback,
            std::function<void()> prepare_for_checkpoint = {});

        void enter_operation() override;
        void exit_operation() override;

    private:
        FMI::Utils::peer_num peer_id;
        std::string worker_id;
        std::string placement;
        std::uint64_t active_epoch;
        FMI::Utils::FaultToleranceConfig config;
        std::shared_ptr<FMI::FT::ControlPlane> control_plane;
        std::string base_comm_name;
        std::function<void(const std::string&)> reconfigure_callback;
        std::function<void()> prepare_for_checkpoint;

        // How a migrated rank's state is handled, the data backend name, and host identity used by
        // the CRIU path are all read from `config` where needed (see checkpoint_and_wait_for_restore).

        //! Wait for the orchestrator to promote the epoch, then rebuild channels in place.
        //! The wait is unbounded by design: the library never promotes its own epoch — an
        //! external actor (the orchestrator for state_transfer="none", the rank agent for
        //! "criu") always does — so the only honest thing a waiting rank can do is wait. A
        //! stuck migration (dead orchestrator/agent, failed restore) is therefore not a
        //! library-detected error; detecting and resolving it (abort the job, or promote a
        //! replacement) is the orchestrator's responsibility. See docs/fault-tolerance.md.
        void wait_for_promotion_and_reconfigure();

        //! CRIU state-transfer quiesce point for the migration target: release transport,
        //! publish a restorable image entry, and block until restored + promoted. The process
        //! is criu-dumped while blocked here and the restored image resumes at the same point.
        void checkpoint_and_wait_for_restore();
    };
}

#endif //FMI_FT_TRANSPARENTMIGRATIONRUNTIME_H
