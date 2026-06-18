#include "../../../include/ft/experimental/CriuExec.h"

#include <cerrno>
#include <stdexcept>
#include <string>

#include <sys/wait.h>
#include <unistd.h>

void FMI::FT::run_criu(const std::vector<std::string>& args) {
    std::vector<char*> argv;
    argv.reserve(args.size() + 1);
    for (const auto& arg : args) {
        argv.push_back(const_cast<char*>(arg.c_str()));
    }
    argv.push_back(nullptr);

    pid_t pid = fork();
    if (pid < 0) {
        throw std::runtime_error("fork() failed while launching criu");
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
        throw std::runtime_error("waitpid() failed while waiting for criu");
    }
    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        int code = WIFEXITED(status) ? WEXITSTATUS(status) : -1;
        std::string message = "criu command failed with exit code " + std::to_string(code);
        if (code == 127) {
            // The child returns 127 when execvp could not run the binary — almost always
            // because criu is not on PATH (or not executable).
            message += " (is criu installed and on PATH?)";
        }
        throw std::runtime_error(message);
    }
}
