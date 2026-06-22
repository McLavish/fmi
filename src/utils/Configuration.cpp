#include "../../include/utils/Configuration.h"
#include <boost/property_tree/json_parser.hpp>
#include <stdexcept>
#include <utility>

namespace FMI::Utils {

    Configuration::Configuration(const std::string& config_path) {
        boost::property_tree::read_json(config_path, root);
    }

    std::map< std::string, std::pair< std::map<std::string, std::string>, std::map<std::string, std::string> > > Configuration::get_active_channels() {
        std::map< std::string, std::pair< std::map<std::string, std::string>, std::map<std::string, std::string> > > backends;
        auto backends_tree = root.get_child("backends");
        auto modeldata_tree = root.get_child("model");
        for (const auto& kv : backends_tree) {
            std::string backend_name = kv.first;
            std::map<std::string, std::string> backend_params;
            for (const auto& backend_tree : kv.second) {
                std::string param_name = backend_tree.first;
                std::string param_val = backend_tree.second.data();
                backend_params[param_name] = param_val;
            }
            if (backend_params.count("enabled") && backend_params["enabled"] != "true") {
                continue;
            }
            std::map<std::string, std::string> model_params;
            if (modeldata_tree.count(backend_name)) {
                for (const auto& model_tree : modeldata_tree.get_child(backend_name)) {
                    std::string param_name = model_tree.first;
                    std::string param_val = model_tree.second.data();
                    model_params[param_name] = param_val;
                }
            }
            backends[backend_name] = std::make_pair(backend_params, model_params);
        }
        return backends;
    }

    double Configuration::get_faas_price() {
        return root.get_child("model").get_child("FaaS").get<double>("gib_second_price");
    }

    FaultToleranceConfig Configuration::get_fault_tolerance_config() {
        FaultToleranceConfig config;
        if (!root.count("fault_tolerance")) {
            return config;
        }

        auto ft_tree = root.get_child("fault_tolerance");
        config.enabled = ft_tree.get("enabled", false);
        config.control_backend = ft_tree.get("control_backend", config.control_backend);
        config.control_host = ft_tree.get("control_host", config.control_host);
        config.control_port = ft_tree.get("control_port", config.control_port);
        config.poll_interval_ms = ft_tree.get("poll_interval_ms", config.poll_interval_ms);
        config.preferred_data_backend = ft_tree.get("preferred_data_backend", config.preferred_data_backend);
        config.state_transfer = ft_tree.get("state_transfer", config.state_transfer);
        // CRIU-mechanism knobs live under a nested "criu" object. Absent (non-criu configs) -> defaults.
        if (auto criu_tree = ft_tree.get_child_optional("criu")) {
            config.criu.images_dir = criu_tree->get("images_dir", config.criu.images_dir);
            config.criu.poll_ms = criu_tree->get("poll_ms", config.criu.poll_ms);
            config.criu.quiesce_timeout_ms = criu_tree->get("quiesce_timeout_ms", config.criu.quiesce_timeout_ms);
            config.criu.host_id = criu_tree->get("host_id", config.criu.host_id);
        }
        return config;
    }
}
