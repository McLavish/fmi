#ifndef FMI_CONFIGURATION_H
#define FMI_CONFIGURATION_H


#include <string>
#include <map>
#include <boost/property_tree/ptree.hpp>
#include "../ft/Common.h"

namespace FMI::Utils {
    struct FaultToleranceConfig {
        bool enabled = false;
        FMI::FT::Mode mode = FMI::FT::Mode::TransparentMigration;
        std::string control_backend = "Redis";
        std::string control_host = "127.0.0.1";
        unsigned int control_port = 6379;
        unsigned int poll_interval_ms = 1000;
        unsigned int reconfigure_timeout_ms = 10000;
        std::string preferred_data_backend;
        std::string images_dir = "/tmp/fmi-criu-images";
        unsigned int poll_ms = 100;
        unsigned int quiesce_timeout_ms = 10000;
        std::string host_id;
    };

    //! Configuration parser for the FMI JSON configuration file
    class Configuration {
    public:
        explicit Configuration(const std::string& config_path);

        //! Returns the name and a pair with the configuration and model parameters for all channels that are marked active ("active": True) in the config file.
        std::map< std::string, std::pair< std::map<std::string, std::string>, std::map<std::string, std::string> > > get_active_channels();

        //! Returns the configured GiB Second Price for the FaaS platform.
        double get_faas_price();

        //! Returns fault-tolerance settings. Missing configuration disables fault tolerance.
        FaultToleranceConfig get_fault_tolerance_config();

    private:
        boost::property_tree::ptree root;
    };
}



#endif //FMI_CONFIGURATION_H
