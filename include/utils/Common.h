#ifndef FMI_COMMON_H
#define FMI_COMMON_H
#include <exception>
#include <stdexcept>

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

    //! The store refused for a reason waiting cannot fix.
    /*!
     * The counterpart of Timeout, which says only that something has not appeared within the
     * patience this job was configured with — a peer may still be on its way. This one says the
     * store answered and the answer was no: credentials that expired while the rank was frozen, a
     * bucket that does not exist, a stored value whose length is not the length its reader
     * expects. None of those get better by polling, and reporting them as patience running out
     * sends whoever reads the log looking at the wrong rank for the wrong reason.
     */
    struct BackendFailure : public std::runtime_error {
        using std::runtime_error::runtime_error;
    };

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
