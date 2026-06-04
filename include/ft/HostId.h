#ifndef FMI_FT_HOSTID_H
#define FMI_FT_HOSTID_H

#include "../utils/Configuration.h"

#include <stdexcept>
#include <string>

#include <unistd.h>

namespace FMI::FT {
    //! Resolve the host identifier for a CRIU rank/supervisor: the configured override if set,
    //! otherwise the system hostname.
    inline std::string resolve_host_id(const FMI::Utils::FaultToleranceConfig& config) {
        if (!config.host_id.empty()) {
            return config.host_id;
        }

        char hostname[256] = {0};
        if (gethostname(hostname, sizeof(hostname)) != 0) {
            throw std::runtime_error("Could not determine host_id");
        }
        return hostname;
    }
}

#endif //FMI_FT_HOSTID_H
