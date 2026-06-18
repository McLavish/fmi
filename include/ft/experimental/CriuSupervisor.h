#ifndef FMI_FT_CRIUSUPERVISOR_H
#define FMI_FT_CRIUSUPERVISOR_H

#include "../../utils/Common.h"
#include "../Coordinator.h"
#include "FmiFtSupervisor.h"

#include <cstdint>
#include <string>
#include <vector>

namespace FMI::FT {
    class CriuSupervisor : public FmiFtSupervisor {
    public:
        CriuSupervisor(std::string config_path, std::string comm_name, FMI::Utils::peer_num num_peers);

        [[nodiscard]] std::uint64_t checkpoint();
        [[nodiscard]] std::uint64_t restore(std::uint64_t generation = 0);
        [[nodiscard]] CriuStatus status() const;

    private:
        [[nodiscard]] std::string generation_dir(std::uint64_t generation) const;
        [[nodiscard]] std::vector<CriuRankInfo> local_ranks() const;
        void ensure_same_host_scope(const std::vector<CriuRankInfo>& ranks) const;
        void wait_for_quiesce(std::uint64_t generation) const;
        void dump_rank(const CriuRankInfo& rank, std::uint64_t generation) const;
        void restore_rank(const CriuRankInfo& rank, std::uint64_t generation) const;
        static void run_criu(const std::vector<std::string>& args);
    };
}

#endif //FMI_FT_CRIUSUPERVISOR_H
