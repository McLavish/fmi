#include "../../include/utils/Signals.h"

#include <csignal>
#include <mutex>

void FMI::Utils::suppress_sigpipe() {
    static std::once_flag once;
    std::call_once(once, []() {
        struct sigaction current {};
        if (::sigaction(SIGPIPE, nullptr, &current) != 0) {
            return;
        }
        // Only SIG_DFL is ours to replace. SIG_IGN means somebody already did this, and a real
        // handler belongs to the application.
        if (current.sa_handler != SIG_DFL) {
            return;
        }
        struct sigaction ignore {};
        ignore.sa_handler = SIG_IGN;
        ::sigemptyset(&ignore.sa_mask);
        ::sigaction(SIGPIPE, &ignore, nullptr);
    });
}
