#ifndef FMI_FT_CRIUSUPERVISOR_H
#define FMI_FT_CRIUSUPERVISOR_H

#include "../utils/Common.h"
#include "../utils/Configuration.h"
#include "Coordinator.h"

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace FMI::FT {
    class CriuSupervisor {
    public:
        CriuSupervisor(std::string config_path, std::string comm_name, FMI::Utils::peer_num num_peers);

        [[nodiscard]] std::uint64_t checkpoint();
        [[nodiscard]] std::uint64_t restore(std::uint64_t generation = 0);
        [[nodiscard]] CriuStatus status() const;
        void cleanup();

    private:
        std::string config_path;
        std::string comm_name;
        FMI::Utils::peer_num num_peers;
        FMI::Utils::FaultToleranceConfig config;
        std::string host_id;
        std::string supervisor_id;
        std::shared_ptr<FMI::FT::Coordinator> coordinator;

        [[nodiscard]] std::string resolve_host_id() const;
        [[nodiscard]] std::string generation_dir(std::uint64_t generation) const;
        [[nodiscard]] std::vector<CriuRankInfo> local_ranks() const;
        void ensure_criu_mode() const;
        void ensure_same_host_scope(const std::vector<CriuRankInfo>& ranks) const;
        void wait_for_quiesce(std::uint64_t generation) const;
        void dump_rank(const CriuRankInfo& rank, std::uint64_t generation) const;
        void restore_rank(const CriuRankInfo& rank, std::uint64_t generation) const;
        static void run_criu(const std::vector<std::string>& args);
    };
}

#endif //FMI_FT_CRIUSUPERVISOR_H
