#ifndef FMI_OPERATIONSCOPE_H
#define FMI_OPERATIONSCOPE_H

#include "LinkFrame.h"

#include <cstdint>

namespace FMI::Comm {

    //! Identity of the FMI operation a rank is currently executing.
    /*!
     * This is what makes protocol v2 transparent. Channel::send and Channel::recv carry no
     * lane argument, and every collective in PeerToPeer reaches the transport through those
     * same two virtuals — so below the Channel interface a barrier fragment and an
     * application send are indistinguishable. Rather than change the Channel interface (which
     * would break user-supplied channels), the Communicator publishes the active operation's
     * identity for the duration of one logical call and channels read it.
     *
     * The application never sees any of this: no new API, no annotation, no instrumentation.
     */
    struct OperationIdentity {
        Lane lane = Lane::P2P;
        OpKind op_kind = OpKind::Send;
        //! Per-Communicator monotonic ordinal, counting user-visible collectives only.
        std::uint64_t collective_index = 0;
        //! Collective root; for point-to-point, the DESTINATION rank.
        /*!
         * Destination rather than "the other end" on purpose: the sender and the receiver must
         * derive the same value, and each naming its peer would make every legal p2p receive
         * an identity mismatch.
         */
        std::uint32_t root = 0;
        //! Reduction flags; they select entirely different collective algorithms, so two ranks
        //! disagreeing on them are in different logical operations.
        bool commutative = false;
        bool associative = false;
        //! False outside any FMI operation.
        bool valid = false;
    };

    //! The identity currently in scope on this thread.
    /*!
     * Thread-local, not global: the test suites run ranks both as forked processes and as
     * OpenMP threads inside one process, and a shared global would let one rank's operation
     * identity leak into another's.
     */
    const OperationIdentity& active_operation();

    //! Publishes an OperationIdentity for the extent of one logical FMI operation.
    /*!
     * Nesting is by design. PeerToPeer::allreduce calls its own reduce() and bcast(), which
     * are Channel virtuals rather than Communicator entry points, so the outermost scope stays
     * active throughout and every fragment of a user-visible allreduce shares one identity.
     * That is what keeps collective_index counting user-visible collectives, which all ranks
     * agree on by the usual same-order rule, instead of counting internal fragments, which
     * they would not.
     */
    class OperationScope {
    public:
        explicit OperationScope(const OperationIdentity& identity);
        ~OperationScope();

        OperationScope(const OperationScope&) = delete;
        OperationScope& operator=(const OperationScope&) = delete;

    private:
        OperationIdentity previous;
    };

    //! Build the identity of a point-to-point send/recv. @p dest is the receiving rank.
    inline OperationIdentity p2p_identity(std::uint32_t dest) {
        OperationIdentity id;
        id.lane = Lane::P2P;
        id.op_kind = OpKind::Send;
        id.collective_index = 0;
        id.root = dest;
        id.valid = true;
        return id;
    }

    //! Build the identity of a collective. @p index is the Communicator's collective ordinal.
    inline OperationIdentity collective_identity(OpKind op, std::uint64_t index,
                                                 std::uint32_t root,
                                                 bool commutative = false,
                                                 bool associative = false) {
        OperationIdentity id;
        id.lane = Lane::Collective;
        id.op_kind = op;
        id.collective_index = index;
        id.root = root;
        id.commutative = commutative;
        id.associative = associative;
        id.valid = true;
        return id;
    }
}

#endif //FMI_OPERATIONSCOPE_H
