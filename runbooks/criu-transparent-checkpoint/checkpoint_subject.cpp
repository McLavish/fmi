// An ORDINARY FMI program. It knows nothing about migration, checkpointing, epochs or the
// link layer: it constructs a Communicator and hands it to one named app shape, which calls
// collectives and point-to-point sends exactly as any FMI application would. Everything the
// checkpoint story needs happens below this file.
//
// argv: <peer_id> <num_peers> <config> <comm_name> <rounds> <ms_per_round>
//       [print_every] [payload_ints] [--shape <name>]
//
// The workload itself lives in shapes/, one file per shape, each self-registering (see
// shapes.h). `--shape NAME`, `--shape=NAME` or the environment variable FMI_SHAPE selects one;
// the default is "baseline", the workload this runbook has always run. `--list-shapes` prints
// what is compiled in.
//
// This file only ever drives a shape and reports its checksum. Every result check, and the
// checksum that lets the sweep compare a checkpointed run against a clean one, belongs to the
// shape — so a lost, duplicated or substituted message fails the run rather than being absorbed
// into a plausible-looking answer.
#include "shapes.h"

#include <fmi.h>

#include <unistd.h>

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <vector>

namespace {

using namespace FMI::Runbooks::Checkpoint;

void print_shapes(std::FILE* out) {
    for (const auto& shape : shapes()) {
        std::fprintf(out, "  %-16s %s\n", shape.name, shape.description);
    }
}

} // namespace

int main(int argc, char** argv) {
    // Split the command line into positional arguments and the options, so that adding
    // --shape does not move any positional argument: the sweep and the runbook's pgrep
    // patterns both depend on the rank being argv[1] and on the positions after it.
    std::vector<std::string> positional;
    std::string shape_name;
    for (int i = 1; i < argc; i++) {
        const std::string arg = argv[i];
        if (arg == "--list-shapes") {
            if (!registry_error().empty()) {
                std::fprintf(stderr, "shape registry is broken: %s\n", registry_error().c_str());
                return 2;
            }
            print_shapes(stdout);
            return 0;
        }
        if (arg == "--shape") {
            if (i + 1 >= argc) {
                std::fprintf(stderr, "--shape needs a shape name\n");
                return 2;
            }
            shape_name = argv[++i];
        } else if (arg.rfind("--shape=", 0) == 0) {
            shape_name = arg.substr(std::string("--shape=").size());
        } else if (arg == "-h" || arg == "--help") {
            std::fprintf(stdout,
                         "usage: %s <peer_id> <num_peers> <config> <comm> <rounds> <ms> "
                         "[print_every] [payload_ints] [--shape <name>]\n\nshapes:\n", argv[0]);
            print_shapes(stdout);
            return 0;
        } else {
            positional.push_back(arg);
        }
    }

    if (positional.size() < 6) {
        std::fprintf(stderr,
                     "usage: %s <peer_id> <num_peers> <config> <comm> <rounds> <ms> "
                     "[print_every] [payload_ints] [--shape <name>]\n", argv[0]);
        return 2;
    }
    const auto peer_id = static_cast<FMI::Utils::peer_num>(std::stoul(positional[0]));
    const auto num_peers = static_cast<FMI::Utils::peer_num>(std::stoul(positional[1]));
    const std::string config = positional[2];
    const std::string comm_name = positional[3];

    ShapeParams params;
    params.rounds = std::stoi(positional[4]);
    params.ms_per_round = std::stoi(positional[5]);
    params.print_every = positional.size() > 6 ? std::stoi(positional[6]) : 1;
    params.payload_ints = positional.size() > 7 ? std::stoi(positional[7]) : 1;

    if (shape_name.empty()) {
        if (const char* from_env = std::getenv("FMI_SHAPE")) {
            shape_name = from_env;
        }
    }
    if (shape_name.empty()) {
        shape_name = "baseline";
    }

    if (!registry_error().empty()) {
        std::fprintf(stderr, "shape registry is broken: %s\n", registry_error().c_str());
        return 2;
    }
    const Shape* shape = find_shape(shape_name);
    if (shape == nullptr) {
        std::fprintf(stderr, "unknown shape: %s\navailable shapes:\n", shape_name.c_str());
        print_shapes(stderr);
        return 2;
    }

    std::setvbuf(stdout, nullptr, _IOLBF, 0);
    std::printf("rank %u: start pid=%d shape=%s\n", peer_id, (int) ::getpid(), shape->name);

    const auto t_start = std::chrono::steady_clock::now();
    FMI::Communicator comm(peer_id, num_peers, config, comm_name, 128);
    comm.hint(FMI::Utils::Hint::fast);
    const auto t_ready = std::chrono::steady_clock::now();

    unsigned long long checksum = 0;
    try {
        checksum = shape->run(comm, peer_id, num_peers, params);
    } catch (const ShapeFailure& failure) {
        // The shape has already printed which operation disagreed and what it expected.
        return failure.exit_code;
    }
    const auto t_done = std::chrono::steady_clock::now();

    // ctor_ms is Communicator construction (for DrainTCP that includes listener bind and
    // arming); run_ms is the shape's whole run, lazy link establishment included. Appended
    // after the checksum so every existing DONE-line regex keeps matching.
    const double ctor_ms = std::chrono::duration<double, std::milli>(t_ready - t_start).count();
    const double run_ms = std::chrono::duration<double, std::milli>(t_done - t_ready).count();
    std::printf("rank %u: DONE rounds=%d checksum=%llu ctor_ms=%.3f run_ms=%.3f\n",
                peer_id, params.rounds, checksum, ctor_ms, run_ms);
    return 0;
}
