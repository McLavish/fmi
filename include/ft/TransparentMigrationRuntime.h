#ifndef FMI_FT_TRANSPARENTMIGRATIONRUNTIME_H
#define FMI_FT_TRANSPARENTMIGRATIONRUNTIME_H

#include "../utils/Common.h"
#include "../utils/Configuration.h"
#include "Coordinator.h"
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
            std::shared_ptr<FMI::FT::Coordinator> coordinator,
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
        std::shared_ptr<FMI::FT::Coordinator> coordinator;
        std::string base_comm_name;
        std::function<void(const std::string&)> reconfigure_callback;
        std::function<void()> prepare_for_checkpoint;

        // Derived from config: how a migrated rank's state is handled, the data backend name
        // recorded in the CRIU image registry, and this host's identity for same-host scoping.
        std::string state_transfer;
        std::string backend_name;
        std::string host_id;

        //! Wait for the orchestrator to promote the epoch, then rebuild channels in place.
        //! timeout_ms == 0 disables the wall-clock deadline (used across a CRIU dump/restore,
        //! where the external supervisor controls completion).
        void wait_for_promotion_and_reconfigure(unsigned int timeout_ms);

        //! CRIU state-transfer quiesce point for the migration target: release transport,
        //! publish a restorable image entry, and block until restored + promoted. The process
        //! is criu-dumped while blocked here and the restored image resumes at the same point.
        void checkpoint_and_wait_for_restore();
    };
}

#endif //FMI_FT_TRANSPARENTMIGRATIONRUNTIME_H
