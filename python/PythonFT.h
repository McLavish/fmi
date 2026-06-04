#ifndef FMI_PYTHONFT_H
#define FMI_PYTHONFT_H

#include <boost/python/list.hpp>

#include <ft/Coordinator.h>
#include <memory>
#include <string>

namespace FMI::Utils {
    struct PythonRankDirectoryEntry {
        FMI::Utils::peer_num rank = 0;
        std::string worker_id;
        std::string placement;
        std::string state;
    };

    class PythonFTCoordinator {
    public:
        PythonFTCoordinator(std::string config_path, std::string comm_name, FMI::Utils::peer_num num_peers);

        void request_migration(FMI::Utils::peer_num rank);
        void promote_epoch();
        void clear_job_state();
        [[nodiscard]] std::uint64_t epoch() const;
        std::string placement_for_rank(std::uint64_t epoch, FMI::Utils::peer_num rank);
        boost::python::list directory_snapshot(std::uint64_t epoch);

    private:
        std::shared_ptr<FMI::FT::Coordinator> coordinator;
    };
}

#endif //FMI_PYTHONFT_H
