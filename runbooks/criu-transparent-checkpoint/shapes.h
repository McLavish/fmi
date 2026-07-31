#pragma once

// Registration surface for the checkpoint subject's app shapes.
//
// A *shape* is one application-level workload that the sweep can checkpoint. The subject
// program is deliberately an ORDINARY FMI application, so a shape is nothing but a function
// that calls collectives and point-to-point operations and returns a checksum.
//
// Every shape lives in its OWN translation unit under shapes/, defines one run function, and
// registers it with FMI_REGISTER_SHAPE. No shape file ever edits a shared list, and CMake globs
// shapes/*.cpp, so adding a shape means adding exactly one file and nothing else.
//
// Authoring a shape — shapes/shape_<name>.cpp:
//
//     #include "../shapes.h"
//
//     namespace FMI::Runbooks::Checkpoint {
//     namespace {
//
//     unsigned long long run_my_shape(FMI::Communicator& comm, FMI::Utils::peer_num rank,
//                                     FMI::Utils::peer_num num_peers, const ShapeParams& params) {
//         unsigned long long checksum = 0;
//         for (int round = 0; round < params.rounds; round++) {
//             ...call operations, check each result inline, fold results into checksum...
//             if (bad) {
//                 std::printf("rank %u: MISMATCH ...\n", rank);
//                 throw ShapeFailure{3};
//             }
//         }
//         return checksum;
//     }
//
//     FMI_REGISTER_SHAPE("my_shape", "one line: what this shape exercises", run_my_shape)
//
//     } // namespace
//     } // namespace FMI::Runbooks::Checkpoint
//
// THE CHECKSUM CONTRACT — the sweep depends on it entirely.
//
// run() must return a value determined solely by (shape, rank, num_peers, params). It must not
// depend on timing, wall clock, pids, addresses, hash/iteration order over an unordered
// container, how many times the rank was checkpointed, or anything else that varies between two
// runs of the same shape. The sweep runs each shape once cleanly to obtain a per-rank baseline
// checksum, then compares every checkpointed run against it:
//
//   * a checksum that differs between two clean runs makes every trial fail, and
//   * a checksum that does not actually fold in *received* data makes every trial pass
//     vacuously, which proves nothing.
//
// So fold in the values that came off the wire, and verify them inline as well: a lost,
// duplicated or substituted message must fail the run rather than being absorbed into a
// plausible-looking answer.

#include <fmi.h>

#include <string>
#include <vector>

namespace FMI::Runbooks::Checkpoint {

//! Knobs the driver passes through from its command line. A shape may ignore any of them, but
//! its checksum must be a function of the ones it uses.
struct ShapeParams {
    //! Iterations of the shape's main loop.
    int rounds = 1;
    //! Milliseconds to sleep at the end of each round; 0 means no sleep.
    int ms_per_round = 0;
    //! Print a progress line every N rounds; <= 0 means print none. The sweep reads those
    //! lines to prove the checkpointed rank made progress *after* its restore, so a shape that
    //! never prints one can never be shown to have survived anything. The line must begin
    //! "rank <id>: round <n> ok" — that prefix is what the sweep matches; the rest is free.
    int print_every = 1;
    //! Element count for a shape's bulk payload. Anything past a socket buffer makes a single
    //! message span many segments, so a freeze can land part way through a payload rather than
    //! only between whole messages.
    int payload_ints = 1;
};

//! Thrown by a shape that observed a wrong result, after it has printed its own diagnostic.
//! The driver catches it and exits with `exit_code`, so distinct codes name the operation that
//! disagreed. The sweep classifies a trial from the printed MISMATCH line, not from this code.
struct ShapeFailure {
    int exit_code;
};

//! Signature every shape's run function must have. Returns the run's checksum.
using ShapeRun = unsigned long long (*)(FMI::Communicator& comm, FMI::Utils::peer_num rank,
                                        FMI::Utils::peer_num num_peers,
                                        const ShapeParams& params);

//! One named application shape.
struct Shape {
    //! Selector on the command line (--shape / FMI_SHAPE). Unique across all shape files.
    const char* name;
    //! One line, shown by --list-shapes.
    const char* description;
    //! Runs the workload; returns a checksum that must match a clean run.
    ShapeRun run;
};

//! Self-registering entry point. Construct one per translation unit at namespace scope; the
//! FMI_REGISTER_SHAPE macro does exactly that.
struct ShapeRegistrar {
    ShapeRegistrar(const char* name, const char* description, ShapeRun run);
};

//! Every registered shape, ordered by name so --list-shapes does not depend on link order.
//! Calling this freezes the registry: a shape registered afterwards is an error.
const std::vector<Shape>& shapes();

//! The shape called `name`, or nullptr.
const Shape* find_shape(const std::string& name);

//! Empty when the registry is well formed; otherwise why it is not (an empty or duplicate name,
//! a null run function, a registration that arrived after the catalog was built). The driver
//! refuses to run a shape while this is non-empty.
const std::string& registry_error();

} // namespace FMI::Runbooks::Checkpoint

//! Registers `run_function` under `shape_name` with `shape_description`. Place exactly one
//! invocation at namespace scope in each shape translation unit.
#define FMI_REGISTER_SHAPE(shape_name, shape_description, run_function)                       \
    namespace {                                                                               \
    const ::FMI::Runbooks::Checkpoint::ShapeRegistrar                                         \
            fmi_shape_registrar(shape_name, shape_description, &run_function);                \
    }
