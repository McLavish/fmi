#ifndef FMI_FT_CRIUEXEC_H
#define FMI_FT_CRIUEXEC_H

#include <string>
#include <vector>

namespace FMI::FT {
    //! Run a command via fork/execvp/waitpid. Throws std::runtime_error on a non-zero exit
    //! (or a failed fork/wait). Used by the rank agent for the non-criu legs of a migration
    //! (packing/unpacking image archives).
    void run_process(const std::vector<std::string>& args);

    //! Run a `criu` subcommand via run_process, appending the operator-supplied
    //! FMI_CRIU_EXTRA_ARGS flags. Used by the host-local rank agent.
    void run_criu(const std::vector<std::string>& args);
}

#endif //FMI_FT_CRIUEXEC_H
