#include "../../../include/ft/experimental/FmiFtSupervisor.h"
#include "../../../include/ft/experimental/HostId.h"

#include <filesystem>
#include <stdexcept>

#include <unistd.h>

namespace fs = std::filesystem;

FMI::FT::FmiFtSupervisor::FmiFtSupervisor(std::string config_path, std::string comm_name,
                                          FMI::Utils::peer_num num_peers) :
        config_path(std::move(config_path)),
        comm_name(std::move(comm_name)),
        num_peers(num_peers) {
    FMI::Utils::Configuration configuration(this->config_path);
    config = configuration.get_fault_tolerance_config();
}

void FMI::FT::FmiFtSupervisor::validate_base() const {
    if (!config.enabled) {
        throw std::runtime_error("FT supervisor requires fault tolerance to be enabled");
    }
    if (config.control_backend != "Redis") {
        throw std::runtime_error("FT supervisor requires Redis as the control backend");
    }
}

void FMI::FT::FmiFtSupervisor::connect() {
    host_id = FMI::FT::resolve_host_id(config);
    supervisor_id = host_id + ":" + std::to_string(getpid());
    coordinator = std::make_shared<FMI::FT::Coordinator>(config_path, comm_name, num_peers);
}

void FMI::FT::FmiFtSupervisor::cleanup() {
    auto images_path = fs::path(config.images_dir) / comm_name;
    if (fs::exists(images_path)) {
        fs::remove_all(images_path);
    }
    coordinator->clear_criu_job_state();
}
