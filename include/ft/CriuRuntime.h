#ifndef FMI_FT_CRIURUNTIME_H
#define FMI_FT_CRIURUNTIME_H

#include "../utils/Common.h"
#include "../utils/Configuration.h"
#include "Coordinator.h"
#include "OperationRuntime.h"

#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>

namespace FMI::FT {
    class CriuRuntime : public OperationRuntime {
    public:
        CriuRuntime(FMI::Utils::peer_num peer_id, FMI::Utils::peer_num num_peers, std::string config_path, std::string comm_name,
                    std::string backend_name, std::function<void()> prepare_for_checkpoint);

        void enter_operation() override;
        void exit_operation() override;
        void shutdown() override;

    private:
        FMI::Utils::peer_num peer_id;
        FMI::Utils::peer_num num_peers;
        std::string config_path;
        std::string comm_name;
        std::string backend_name;
        FMI::Utils::FaultToleranceConfig config;
        std::string host_id;
        std::function<void()> prepare_for_checkpoint;
        std::shared_ptr<FMI::FT::Coordinator> coordinator;
        std::mutex state_mutex;
        std::condition_variable state_cv;
        std::size_t active_operations = 0;
        bool quiescing = false;
        std::uint64_t last_completed_generation = 0;

        void quiesce(std::uint64_t generation);
        [[nodiscard]] int current_pid() const;
    };
}

#endif //FMI_FT_CRIURUNTIME_H
