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
            std::function<void(const std::string&)> reconfigure_callback);

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

        void wait_for_promotion_and_reconfigure();
    };
}

#endif //FMI_FT_TRANSPARENTMIGRATIONRUNTIME_H
