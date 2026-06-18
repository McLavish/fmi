#ifndef FMI_CONFIGURATION_H
#define FMI_CONFIGURATION_H


#include <string>
#include <map>
#include <boost/property_tree/ptree.hpp>
#include "../ft/Common.h"

namespace FMI::Utils {
    struct FaultToleranceConfig {
        bool enabled = false;
        std::string control_backend = "Redis";
        std::string control_host = "127.0.0.1";
        unsigned int control_port = 6379;
        unsigned int poll_interval_ms = 1000;
        unsigned int reconfigure_timeout_ms = 10000;
        std::string preferred_data_backend;

        // Selects how a migrated rank's application state is handled at the migration quiesce
        // point. This is NOT a second FT protocol: transparent migration remains the single
        // protocol and the epoch-cut semantics are unchanged. It only chooses what happens to
        // the targeted rank's in-memory state:
        //   "none" (default) - the rank exits and a fresh replacement recomputes from scratch.
        //   "criu"           - the rank's process image is checkpointed and restored (same-host
        //                      v1), preserving application memory transparently. Requires a
        //                      build with FMI_ENABLE_CRIU=ON.
        std::string state_transfer = "none";

        // CRIU settings. Used by the CRIU-backed state-transfer path (state_transfer="criu")
        // and by the experimental whole-job CRIU code. images_dir/poll_ms/quiesce_timeout_ms
        // govern checkpoint image placement and the quiesce handshake; host_id overrides the
        // detected hostname for same-host scoping.
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
