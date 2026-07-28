#include "../../include/comm/OperationScope.h"

namespace {
    //! See OperationScope.h: thread_local because ranks may be OpenMP threads in one process.
    thread_local FMI::Comm::OperationIdentity current_identity{};
}

const FMI::Comm::OperationIdentity& FMI::Comm::active_operation() {
    return current_identity;
}

FMI::Comm::OperationScope::OperationScope(const OperationIdentity& identity)
        : previous(current_identity) {
    current_identity = identity;
}

FMI::Comm::OperationScope::~OperationScope() {
    // Restore rather than clear: a channel that re-enters the Communicator would otherwise
    // lose the outer operation's identity.
    current_identity = previous;
}
