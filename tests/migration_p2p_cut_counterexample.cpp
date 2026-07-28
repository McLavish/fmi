#include <fmi.h>

#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>

#include <unistd.h>

// Reproduces a silent cross-epoch FIFO substitution in the current consensus-cut protocol.
//
// Both ranks complete exactly one guarded operation in epoch 0, so both are at numeric
// boundary 1. Those operations are eager Redis sends whose matching receives deliberately
// occur after a common barrier:
//
//   rank 0: send(A -> 1)  | barrier | send(C -> 1)
//   rank 1: send(B -> 0)  | barrier | recv(expected A)
//
// Without migration, rank 1's first receive correctly returns A. With a migration requested
// after the first sends, both ranks park at boundary 1 and promotion succeeds. Reconfiguration
// rebuilds the Redis channels with epoch-1 names and resets their per-link counters. The old A
// is fenced into epoch 0, C becomes epoch 1's send sequence 0, and rank 1 silently receives C
// in the operation that still logically expects A.
//
// state_transfer="criu" is used only to make the target park in this process instead of
// calling exit(0). No criu dump/restore is invoked: external promotion releases the parked
// target exactly as in the existing cut-timing tests.
namespace {
    using namespace std::chrono_literals;

    constexpr FMI::Utils::peer_num world = 2;
    constexpr FMI::Utils::peer_num target_rank = 1;
    constexpr int old_payload_from_rank0 = 0x11111111;
    constexpr int old_payload_from_rank1 = 0x33333333;
    constexpr int new_payload_from_rank0 = 0x22222222;

    struct SharedCaseState {
        std::mutex mutex;
        std::condition_variable cv;
        unsigned int pre_cut_sends_completed = 0;
        bool enter_cut = false;
        bool receive_completed = false;
        int observed_by_rank1 = 0;
        std::array<std::string, world> errors;
    };

    struct CaseResult {
        bool setup_ok = false;
        bool promoted = false;
        bool captured_cut_boundaries = false;
        std::array<std::uint64_t, world> cut_boundaries{};
        int observed_by_rank1 = 0;
        std::string error;
    };

    std::string unique_comm_name(const char* label) {
        auto now = std::chrono::steady_clock::now().time_since_epoch();
        auto nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(now).count();
        return "p2p-cut-counterexample-" + std::string(label) + "-" +
               std::to_string(static_cast<long long>(getpid())) + "-" + std::to_string(nanos);
    }

    bool any_error(const SharedCaseState& state) {
        for (const auto& error : state.errors) {
            if (!error.empty()) {
                return true;
            }
        }
        return false;
    }

    std::string joined_errors(const SharedCaseState& state) {
        std::string result;
        for (FMI::Utils::peer_num rank = 0; rank < world; rank++) {
            if (state.errors[rank].empty()) {
                continue;
            }
            if (!result.empty()) {
                result += "; ";
            }
            result += "rank " + std::to_string(rank) + ": " + state.errors[rank];
        }
        return result;
    }

    void record_error(SharedCaseState& state, FMI::Utils::peer_num rank, const std::string& error) {
        std::lock_guard<std::mutex> lock(state.mutex);
        state.errors[rank] = error;
        state.receive_completed = true;
        state.cv.notify_all();
    }

    CaseResult run_case(const std::string& config_path, bool migrate) {
        const std::string comm_name = unique_comm_name(migrate ? "migration" : "baseline");
        FMI::FT::ControlPlane orchestrator(config_path, comm_name, world);
        orchestrator.clear_job_state();
        orchestrator.clear_criu_state();

        SharedCaseState state;
        std::array<std::thread, world> ranks;

        auto rank_body = [&](FMI::Utils::peer_num rank) {
            try {
                FMI::Communicator comm(rank, world, config_path, comm_name, 128,
                                       "counterexample-rank-" + std::to_string(rank));

                const int pre_cut_payload =
                        rank == 0 ? old_payload_from_rank0 : old_payload_from_rank1;
                FMI::Comm::Data<int> pre_cut(pre_cut_payload);
                comm.send(pre_cut, 1 - rank);

                {
                    std::unique_lock<std::mutex> lock(state.mutex);
                    state.pre_cut_sends_completed++;
                    state.cv.notify_all();
                    state.cv.wait(lock, [&]() { return state.enter_cut || any_error(state); });
                    if (any_error(state)) {
                        return;
                    }
                }

                // Both ranks present the same collective signature at the candidate cut.
                comm.barrier();

                if (rank == 0) {
                    FMI::Comm::Data<int> post_cut(new_payload_from_rank0);
                    comm.send(post_cut, 1);

                    // Keep the channel alive until rank 1 has consumed the payload. Otherwise
                    // ClientServer::finalize could delete C before the receive observes it.
                    std::unique_lock<std::mutex> lock(state.mutex);
                    state.cv.wait(lock, [&]() { return state.receive_completed; });
                } else {
                    FMI::Comm::Data<int> received;
                    comm.recv(received, 0);
                    {
                        std::lock_guard<std::mutex> lock(state.mutex);
                        state.observed_by_rank1 = received.get();
                        state.receive_completed = true;
                    }
                    state.cv.notify_all();
                }
            } catch (const std::exception& error) {
                record_error(state, rank, error.what());
            } catch (const std::string& error) {
                record_error(state, rank, error);
            } catch (...) {
                record_error(state, rank, "unknown exception");
            }
        };

        for (FMI::Utils::peer_num rank = 0; rank < world; rank++) {
            ranks[rank] = std::thread(rank_body, rank);
        }

        CaseResult result;
        {
            std::unique_lock<std::mutex> lock(state.mutex);
            bool ready = state.cv.wait_for(lock, 10s, [&]() {
                return state.pre_cut_sends_completed == world || any_error(state);
            });
            if (!ready || any_error(state)) {
                result.error = ready ? joined_errors(state) : "ranks did not finish the pre-cut sends";
                state.enter_cut = true;
                state.receive_completed = true;
                state.cv.notify_all();
            } else {
                result.setup_ok = true;
            }
        }

        if (result.setup_ok && migrate) {
            try {
                orchestrator.request_migration(target_rank);
            } catch (const std::exception& error) {
                result.error = std::string("request_migration failed: ") + error.what();
                result.setup_ok = false;
            }
        }

        {
            std::lock_guard<std::mutex> lock(state.mutex);
            state.enter_cut = true;
        }
        state.cv.notify_all();

        if (result.setup_ok && migrate) {
            auto deadline = std::chrono::steady_clock::now() + 10s;
            std::string last_error;
            while (std::chrono::steady_clock::now() < deadline) {
                auto boundaries = orchestrator.operation_boundaries();
                if (boundaries.size() == world) {
                    result.captured_cut_boundaries = true;
                    for (const auto& [rank, boundary] : boundaries) {
                        if (rank < world) {
                            result.cut_boundaries[rank] = boundary;
                        }
                    }
                }
                try {
                    result.promoted = orchestrator.promote_epoch(1);
                    if (result.promoted) {
                        break;
                    }
                } catch (const std::exception& error) {
                    last_error = error.what();
                }
                std::this_thread::sleep_for(10ms);
            }
            if (!result.promoted) {
                std::cerr << "promotion did not open: " << last_error << std::endl;
                std::_Exit(3); // Parked Communicators wait indefinitely by protocol contract.
            }
        }

        {
            std::unique_lock<std::mutex> lock(state.mutex);
            bool completed = state.cv.wait_for(lock, 10s, [&]() {
                return state.receive_completed || any_error(state);
            });
            if (!completed) {
                std::cerr << "post-cut receive did not complete" << std::endl;
                std::_Exit(4);
            }
        }

        for (auto& rank : ranks) {
            rank.join();
        }

        {
            std::lock_guard<std::mutex> lock(state.mutex);
            if (any_error(state)) {
                result.error = joined_errors(state);
                result.setup_ok = false;
            }
            result.observed_by_rank1 = state.observed_by_rank1;
        }

        orchestrator.clear_job_state();
        orchestrator.clear_criu_state();
        return result;
    }
}

int main() {
    // A detached watchdog bounds unexpected protocol hangs; the intended counterexample is a
    // silent substitution and normally completes in well under a second.
    std::atomic<bool> finished{false};
    std::thread([&finished]() {
        std::this_thread::sleep_for(30s);
        if (!finished.load()) {
            std::cerr << "counterexample timed out (the protocol hung instead of substituting)"
                      << std::endl;
            std::_Exit(124);
        }
    }).detach();

    const std::string config_path =
            std::string(FMI_SOURCE_DIR) + "/config/fmi_ft_stress_redis_test.json";

    try {
        std::cout << "Schedule: send old payloads; common barrier; send replacement payload; recv"
                  << std::endl;

        CaseResult baseline = run_case(config_path, false);
        if (!baseline.setup_ok) {
            std::cerr << "baseline setup failed: " << baseline.error << std::endl;
            finished.store(true);
            return 1;
        }
        std::cout << "baseline (no migration): expected A=" << old_payload_from_rank0
                  << ", observed=" << baseline.observed_by_rank1 << std::endl;
        if (baseline.observed_by_rank1 != old_payload_from_rank0) {
            std::cerr << "baseline is invalid: FMI did not preserve FIFO even without migration"
                      << std::endl;
            finished.store(true);
            return 1;
        }

        CaseResult migrated = run_case(config_path, true);
        if (!migrated.setup_ok || !migrated.promoted) {
            std::cerr << "migration setup failed: " << migrated.error << std::endl;
            finished.store(true);
            return 1;
        }
        std::cout << "with migration: promoted epoch 0 -> 1 at the shared barrier" << std::endl;
        if (migrated.captured_cut_boundaries) {
            std::cout << "published cut boundaries before promotion: rank 0="
                      << migrated.cut_boundaries[0] << ", rank 1="
                      << migrated.cut_boundaries[1] << std::endl;
        }
        std::cout << "with migration: expected old A=" << old_payload_from_rank0
                  << ", observed=" << migrated.observed_by_rank1
                  << " (new C=" << new_payload_from_rank0 << ")" << std::endl;

        finished.store(true);
        if (migrated.observed_by_rank1 == new_payload_from_rank0) {
            std::cerr << "COUNTEREXAMPLE REPRODUCED: the receive logically matching epoch-0 A "
                         "silently consumed epoch-1 C"
                      << std::endl;
            return 2; // Intentional failure: the library violated the program's FIFO semantics.
        }
        if (migrated.observed_by_rank1 != old_payload_from_rank0) {
            std::cerr << "COUNTEREXAMPLE REPRODUCED with a different wrong payload" << std::endl;
            return 2;
        }

        std::cout << "Counterexample no longer reproduces: A survived the migration cut"
                  << std::endl;
        return 0;
    } catch (const std::exception& error) {
        finished.store(true);
        std::cerr << "counterexample setup error: " << error.what() << std::endl;
        return 1;
    }
}
