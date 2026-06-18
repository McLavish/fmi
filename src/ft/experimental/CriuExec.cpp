#include "../../../include/ft/experimental/CriuExec.h"

#include <stdexcept>

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
    if (waitpid(pid, &status, 0) < 0) {
        throw std::runtime_error("waitpid() failed while waiting for criu");
    }
    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        throw std::runtime_error("criu command failed with exit code " +
                                 std::to_string(WIFEXITED(status) ? WEXITSTATUS(status) : -1));
    }
}
