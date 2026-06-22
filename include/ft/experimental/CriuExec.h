#ifndef FMI_FT_CRIUEXEC_H
#define FMI_FT_CRIUEXEC_H

#include <string>
#include <vector>

namespace FMI::FT {
    //! Run a `criu` subcommand via fork/execvp/waitpid. Throws std::runtime_error on a
    //! non-zero exit (or a failed fork/wait) so callers can surface checkpoint/restore
    //! failures. Used by the single-rank migration supervisor.
    void run_criu(const std::vector<std::string>& args);
}

#endif //FMI_FT_CRIUEXEC_H
