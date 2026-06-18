#ifndef FMI_COMMON_H
#define FMI_COMMON_H
#include <chrono>
#include <exception>
#include <thread>

//! Contains various utilities that are used in FMI
namespace FMI::Utils {
    //! Type for peer IDs / numbers
    using peer_num = unsigned int;

    //! Custom exception that is thrown on timeouts
    struct Timeout : public std::exception {
        [[nodiscard]] const char * what () const noexcept {
            return "Timeout was reached";
        }
    };

    //! Poll @p predicate every @p poll_interval_ms until it returns true (then return true) or
    //! @p timeout_ms elapses (then return false). @p timeout_ms == 0 waits indefinitely. Shared
    //! by the FT control-plane wait loops (epoch promotion, checkpoint-ready, migration watch).
    template <typename Predicate>
    bool poll_until(Predicate&& predicate, unsigned int timeout_ms, unsigned int poll_interval_ms) {
        auto start = std::chrono::steady_clock::now();
        while (true) {
            if (predicate()) {
                return true;
            }
            if (timeout_ms != 0) {
                auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now() - start).count();
                // Compare in the signed millisecond domain: elapsed is monotonic (>= 0) and the
                // widening cast of timeout_ms is exact, so this avoids the truncating
                // unsigned cast that would wrap past ~49.7 days.
                if (elapsed >= static_cast<decltype(elapsed)>(timeout_ms)) {
                    return false;
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(poll_interval_ms));
        }
    }

    //! Set by the client, controls the optimization goal of the Channel Policy
    enum Hint {
        fast, cheap
    };

    //! List of currently supported collectives
    enum Operation {
        send, bcast, barrier, gather, scatter, reduce, allreduce, scan
    };

    //! All the information about an operation, passed to the Channel Policy for its decision on which channel to use.
    struct OperationInfo {
        Operation op;
        std::size_t data_size;
        bool left_to_right = false;
    };

}

#endif //FMI_COMMON_H
