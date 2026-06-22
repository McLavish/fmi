#include "PythonFT.h"

#include <stdexcept>

namespace {
    std::string rank_state_to_string(FMI::FT::RankState state) {
        switch (state) {
            case FMI::FT::RankState::Active:       return "ACTIVE";
            case FMI::FT::RankState::MigrationPending: return "MIGRATION_PENDING";
            case FMI::FT::RankState::Quiesced:     return "QUIESCED";
            case FMI::FT::RankState::Replaced:     return "REPLACED";
        }
        throw std::runtime_error("Unknown rank state");
    }
}

FMI::Utils::PythonFTControlPlane::PythonFTControlPlane(std::string config_path, std::string comm_name, FMI::Utils::peer_num num_peers) {
    control_plane = std::make_shared<FMI::FT::ControlPlane>(std::move(config_path), std::move(comm_name), num_peers);
}

void FMI::Utils::PythonFTControlPlane::request_migration(FMI::Utils::peer_num rank) {
    control_plane->request_migration(rank);
}

void FMI::Utils::PythonFTControlPlane::promote_epoch() {
    control_plane->promote_epoch(control_plane->epoch() + 1);
}

void FMI::Utils::PythonFTControlPlane::clear_job_state() {
    control_plane->clear_job_state();
}

std::uint64_t FMI::Utils::PythonFTControlPlane::epoch() const {
    return control_plane->epoch();
}

std::string FMI::Utils::PythonFTControlPlane::placement_for_rank(std::uint64_t epoch, FMI::Utils::peer_num rank) {
    return control_plane->placement_for_rank(epoch, rank);
}

boost::python::list FMI::Utils::PythonFTControlPlane::directory_snapshot(std::uint64_t epoch) {
    boost::python::list result;
    for (const auto& entry : control_plane->directory_snapshot(epoch)) {
        PythonRankDirectoryEntry python_entry;
        python_entry.rank = entry.rank;
        python_entry.worker_id = entry.worker_id;
        python_entry.placement = entry.placement;
        python_entry.state = rank_state_to_string(entry.state);
        result.append(boost::python::object(python_entry));
    }
    return result;
}
