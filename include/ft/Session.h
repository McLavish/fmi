#ifndef FMI_FT_SESSION_H
#define FMI_FT_SESSION_H

#include "../Communicator.h"
#include "Common.h"
#include "Coordinator.h"

#include <memory>
#include <string>

namespace FMI::FT {
    //! Fault-tolerant wrapper around FMI::Communicator using epoch-based reconfiguration.
    class Session {
    public:
        Session(FMI::Utils::peer_num peer_id, FMI::Utils::peer_num num_peers, std::string config_path, std::string comm_name,
                std::string worker_id = "", unsigned int faas_memory = 128);

        //! Returns the communicator for the currently active epoch.
        FMI::Communicator& comm();

        //! Safe-point used to observe migration requests and reconfigure the communicator.
        Event safe_point();

        //! Returns the active epoch known to this session.
        [[nodiscard]] std::uint64_t epoch() const { return active_epoch; }

    private:
        FMI::Utils::peer_num peer_id;
        FMI::Utils::peer_num num_peers;
        std::string config_path;
        std::string base_comm_name;
        std::string worker_id;
        unsigned int faas_memory;
        bool ft_enabled = false;
        bool replacement_candidate = false;
        std::uint64_t active_epoch = 0;
        std::unique_ptr<FMI::FT::Coordinator> coordinator;
        std::shared_ptr<FMI::Communicator> communicator;

        void build_communicator(std::uint64_t epoch);
        [[nodiscard]] std::string epoch_comm_name(std::uint64_t epoch) const;
        static std::string default_worker_id(FMI::Utils::peer_num peer_id);
    };
}

#endif //FMI_FT_SESSION_H
