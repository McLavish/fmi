#include "../../../include/ft/experimental/CriuExec.h"

#include <cerrno>
#include <cstdlib>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#include <sys/wait.h>
#include <unistd.h>

void FMI::FT::run_process(const std::vector<std::string>& args) {
    if (args.empty()) {
        throw std::runtime_error("run_process called with no arguments");
    }

    std::vector<char*> argv;
    argv.reserve(args.size() + 1);
    for (const auto& arg : args) {
        argv.push_back(const_cast<char*>(arg.c_str()));
    }
    argv.push_back(nullptr);

    pid_t pid = fork();
    if (pid < 0) {
        throw std::runtime_error("fork() failed while launching " + args[0]);
    }
    if (pid == 0) {
        execvp(argv[0], argv.data());
        _exit(127);
    }

    int status = 0;
    int wait_rc;
    do {
        wait_rc = waitpid(pid, &status, 0);
    } while (wait_rc < 0 && errno == EINTR);
    if (wait_rc < 0) {
        throw std::runtime_error("waitpid() failed while waiting for " + args[0]);
    }
    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        int code = WIFEXITED(status) ? WEXITSTATUS(status) : -1;
        std::string message = args[0] + " command failed with exit code " + std::to_string(code);
        if (code == 127) {
            // The child returns 127 when execvp could not run the binary — almost always
            // because it is not on PATH (or not executable).
            message += " (is " + args[0] + " installed and on PATH?)";
        }
        throw std::runtime_error(message);
    }
}

void FMI::FT::run_criu(const std::vector<std::string>& args) {
    // Append operator-supplied criu flags (whitespace-split, no shell quoting) from
    // FMI_CRIU_EXTRA_ARGS here, at the single point every criu invocation passes through, so
    // the rank agent and every dump/restore honour per-deployment flags (e.g. --unprivileged
    // for rootless criu) — no call site can forget to wire it in.
    std::vector<std::string> full_args = args;
    if (const char* extra = std::getenv("FMI_CRIU_EXTRA_ARGS")) {
        std::istringstream stream(extra);
        std::string token;
        while (stream >> token) {
            full_args.push_back(token);
        }
    }
    run_process(full_args);
}
