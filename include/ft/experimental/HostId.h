#ifndef FMI_FT_HOSTID_H
#define FMI_FT_HOSTID_H

#include "../../utils/Configuration.h"

#include <stdexcept>
#include <string>

#include <unistd.h>

namespace FMI::FT {
    //! Resolve the host identifier for a CRIU rank/agent: the configured override if set,
    //! otherwise the system hostname.
    inline std::string resolve_host_id(const FMI::Utils::FaultToleranceConfig& config) {
        if (!config.criu.host_id.empty()) {
            return config.criu.host_id;
        }

        // Pass sizeof-1 so the final byte stays NUL even on a libc that truncates a too-long name
        // without terminating it (POSIX leaves that case unspecified); the buffer is zero-init.
        char hostname[256] = {0};
        if (gethostname(hostname, sizeof(hostname) - 1) != 0) {
            throw std::runtime_error("Could not determine host_id");
        }
        return hostname;
    }
}

#endif //FMI_FT_HOSTID_H
