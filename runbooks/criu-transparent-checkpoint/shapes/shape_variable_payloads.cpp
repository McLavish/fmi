// App shape: variable_payloads
//
// Every other shape here sends one message width for a whole run. This one changes the width on
// every message, per rank and per step, drawn from a table that spans six orders of magnitude:
// a single int in one message, two mebibytes in the next, from the same rank on the same link.
//
// What that buys, and why it is worth its own shape:
//
//   * A wide message is one frame, but its bytes take many write()/recv() calls to cross the
//     socket, so a freeze can land part way through a *single logical message* rather than only
//     in the gap between two of them. That is exactly the case README point 2 is about — the
//     receive watermark may only advance once the payload is in the application's buffer — and a
//     workload of uniformly small messages never reaches it.
//   * Widths differ per rank, so at any instant different links hold in-flight messages of very
//     different sizes: a rank can be blocked mid-2-MiB-send to one neighbour while a 1-int
//     message on another link was acked long ago.
//   * Widths differ from message to message on one link, so a link never settles into a regime.
//     A repair after a restore has to be correct for whatever width the interrupted message
//     happened to have, not just for the one width that link has always carried.
//
// A round is kStepsPerRound steps; each step is a one-directional ring exchange (width per
// sender) followed by a broadcast from a rotating root (width per step). The rotating root is
// deliberate: bcast builds a binomial tree around its root, so rotating it walks a different
// tree every step, which is where non-power-of-two rank counts differ from powers of two. The
// steps are what give a round enough weight to be worth checkpointing: one exchange per round
// finishes 60 rounds in under a third of a second, well before the sweep freezes anything.
//
// Deterministic by construction: every width and every element is a pure function of
// (rank, round, step, index), and the checksum folds only values that came off the wire, in a
// fixed order, one fold per element. The fold is order-sensitive, so a lost, duplicated,
// reordered or substituted message changes it — and each is also caught inline, before the
// checksum can absorb it.
//
// `ShapeParams::payload_ints` is ignored on purpose: this shape's whole subject is the width
// schedule, so it picks its own widths and nothing on the command line may flatten them. The
// checksum is therefore a function of (rank, num_peers, rounds) alone.
//
// A round moves several mebibytes per rank, three orders of magnitude more than a baseline
// round, so scale `--rounds` accordingly: a few hundred rounds is a long run for this shape
// where baseline wants tens of thousands.

#include "../shapes.h"

#include <chrono>
#include <cstdio>
#include <thread>
#include <vector>

namespace FMI::Runbooks::Checkpoint {
namespace {

//! Message widths, in ints. Deliberately jagged rather than monotonic — consecutive messages
//! jump between a single element and hundreds of thousands instead of ramping. The largest entry
//! is 2 MiB of payload: far past any socket buffer, so it takes many kernel writes to move, and
//! far below the 16 MiB single-frame limit the transport enforces.
constexpr int kWidths[] = {
        1, 3, 9, 27, 1, 128, 4096, 2, 65536, 5, 262144, 1, 16384, 524288, 7, 131072,
};
constexpr int kWidthCount = static_cast<int>(sizeof(kWidths) / sizeof(kWidths[0]));

//! Steps per round. Coprime-with-nothing on purpose: it only has to be large enough that one
//! round is worth freezing, and it is the whole width table, so each rank sends every width in
//! kWidths exactly once per round.
constexpr int kStepsPerRound = kWidthCount;

//! Width of the ring message rank `sender` puts on the wire in (`round`, `step`).
/*!
 * Both the sender and its receiver evaluate this, which is how the two agree on a size without
 * exchanging one. The strides are chosen against kWidthCount = 16, which each is coprime with:
 * 5 makes a rank walk the entire table within a round, 3 shifts where in the table each round
 * starts, and 7 puts ranks 0..15 on sixteen *different* widths in the same step.
 */
int ring_width(FMI::Utils::peer_num sender, int round, int step) {
    return kWidths[(round * 3 + step * 5 + static_cast<int>(sender) * 7) % kWidthCount];
}

//! Width of the broadcast issued in (`round`, `step`). Offset so it rarely coincides with the
//! ring width the same rank is carrying.
int bcast_width(int round, int step) {
    return kWidths[(round * 3 + step * 5 + 11) % kWidthCount];
}

//! Element `index` of the ring message rank `sender` sends in (`round`, `step`).
int ring_element(FMI::Utils::peer_num sender, int round, int step, int index) {
    return static_cast<int>(sender) * 7919 + round * 131 + step * 37 + index;
}

//! Element `index` of the broadcast `root` sends in (`round`, `step`). The constants differ from
//! ring_element's so a ring payload delivered where a broadcast belongs is caught as a mismatch
//! rather than passing as a plausible value.
int bcast_element(FMI::Utils::peer_num root, int round, int step, int index) {
    return static_cast<int>(root) * 104729 + round * 977 + step * 53 + index * 3 + 11;
}

//! Order-sensitive mix (the usual hash_combine step). Folding `a` then `b` differs from folding
//! `b` then `a`, so a reordered or duplicated message cannot land on the same checksum.
void fold(unsigned long long& acc, unsigned long long value) {
    acc ^= value + 0x9e3779b97f4a7c15ULL + (acc << 6) + (acc >> 2);
}

using ExpectedElement = int (*)(FMI::Utils::peer_num source, int round, int step, int index);

//! Checks a received buffer element by element and folds every element into `acc`. Returns false
//! after printing the first disagreeing element, which the caller turns into a ShapeFailure. The
//! width is folded too, so a message that arrives with the right contents at the wrong width —
//! or a right-width message from the wrong step — still moves the checksum.
bool check_and_fold(const std::vector<int>& got, int width, FMI::Utils::peer_num rank,
                    const char* what, int round, int step, ExpectedElement expected,
                    FMI::Utils::peer_num source, unsigned long long& acc) {
    fold(acc, static_cast<unsigned long long>(width));
    for (int j = 0; j < width; j++) {
        const int want = expected(source, round, step, j);
        if (got[j] != want) {
            std::printf("rank %u: %s MISMATCH round %d step %d width %d slot %d: got %d want %d\n",
                        rank, what, round, step, width, j, got[j], want);
            return false;
        }
        fold(acc, static_cast<unsigned long long>(static_cast<unsigned int>(got[j])));
    }
    return true;
}

unsigned long long run_variable_payloads(FMI::Communicator& comm, FMI::Utils::peer_num rank,
                                         FMI::Utils::peer_num num_peers,
                                         const ShapeParams& params) {
    if (num_peers < 2) {
        std::printf("rank %u: variable_payloads needs at least 2 peers\n", rank);
        throw ShapeFailure{9};
    }
    const auto next = (rank + 1) % num_peers;
    const auto prev = (rank + num_peers - 1) % num_peers;

    unsigned long long checksum = 0;

    for (int round = 0; round < params.rounds; round++) {
        int widest = 0;
        for (int step = 0; step < kStepsPerRound; step++) {
            // --- ring: width varies per sender, so each link carries a different size ------
            const int out_width = ring_width(rank, round, step);
            const int in_width = ring_width(prev, round, step);
            std::vector<int> payload(out_width);
            for (int j = 0; j < out_width; j++) {
                payload[j] = ring_element(rank, round, step, j);
            }
            FMI::Comm::Data<std::vector<int>> out(payload);
            FMI::Comm::Data<std::vector<int>> in(std::vector<int>(in_width, -1));

            // Blocking sends, so fix an order: even ranks send first, odd ranks receive first.
            // With an odd rank count both rank 0 and rank num_peers-1 send first; rank 0's own
            // send still completes because rank 1 is receiving, so it reaches its receive and
            // nothing waits in a cycle. Wide payloads make that ordering matter — a 2 MiB send
            // really does block until its peer starts draining.
            if (rank % 2 == 0) {
                comm.send(out, next);
                comm.recv(in, prev);
            } else {
                comm.recv(in, prev);
                comm.send(out, next);
            }
            const auto got_ring = in.get();
            if (!check_and_fold(got_ring, in_width, rank, "RING", round, step, ring_element, prev,
                                checksum)) {
                throw ShapeFailure{3};
            }

            // --- broadcast: width varies per step, root rotates ----------------------------
            const auto root = static_cast<FMI::Utils::peer_num>(
                    (round + step) % static_cast<int>(num_peers));
            const int b_width = bcast_width(round, step);
            std::vector<int> bcast_buf(b_width, -1);
            if (rank == root) {
                for (int j = 0; j < b_width; j++) {
                    bcast_buf[j] = bcast_element(root, round, step, j);
                }
            }
            FMI::Comm::Data<std::vector<int>> btok(bcast_buf);
            comm.bcast(btok, root);
            const auto got_bcast = btok.get();
            // Checked and folded on every rank, the root included: the root's copy is trivially
            // its own, but folding it everywhere keeps one fold order for all ranks.
            if (!check_and_fold(got_bcast, b_width, rank, "BCAST", round, step, bcast_element,
                                root, checksum)) {
                throw ShapeFailure{4};
            }

            widest = out_width > widest ? out_width : widest;
            widest = b_width > widest ? b_width : widest;
        }

        if (params.print_every > 0 && round % params.print_every == 0) {
            std::printf("rank %u: round %d ok checksum=%llu widest=%d\n",
                        rank, round, checksum, widest);
        }
        if (params.ms_per_round > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(params.ms_per_round));
        }
    }

    return checksum;
}

FMI_REGISTER_SHAPE("variable_payloads",
                   "message widths from 1 int to 2 MiB, changing per rank and per message, so a "
                   "freeze lands part way through a single logical message; ring plus a "
                   "rotating-root bcast, ignores --payload-ints",
                   run_variable_payloads)

} // namespace
} // namespace FMI::Runbooks::Checkpoint
