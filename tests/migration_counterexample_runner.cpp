#include "migration_counterexample_scenarios.h"

#include <fmi.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <functional>
#include <iostream>
#include <limits>
#include <map>
#include <mutex>
#include <optional>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <signal.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/prctl.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

#include <hiredis/hiredis.h>

namespace FMI::Tests::MigrationCounterexamples {
namespace {

using Clock = std::chrono::steady_clock;

constexpr std::uint32_t observation_magic = 0x464d494f; // "FMIO"
constexpr std::size_t max_pipe_message = 16 * 1024 * 1024;

std::string config_path() {
    return std::string(FMI_SOURCE_DIR) + "/config/fmi_ft_stress_redis_test.json";
}

std::string no_state_transfer_config_path() {
    return std::string(FMI_SOURCE_DIR) +
           "/config/fmi_ft_counterexample_redis_none.json";
}

std::string direct_config_path() {
    return std::string(FMI_SOURCE_DIR) + "/config/fmi_ft_stress_test.json";
}

std::string unique_comm_name(const Scenario& scenario, bool migrate) {
    const auto ticks = std::chrono::duration_cast<std::chrono::nanoseconds>(
            Clock::now().time_since_epoch()).count();
    if (scenario.backend == Backend::Direct) {
        const std::string short_id =
                scenario.id == "direct_moved_link_backlog" ? "moved" : "survivor";
        return "mce-" + short_id + "-" + (migrate ? "m" : "b") + "-p" +
               std::to_string(static_cast<long long>(getpid())) + "-t" +
               std::to_string(ticks);
    }
    return "migration-counterexample-" + scenario.id + "-" +
           (migrate ? "migrated" : "baseline") + "-pid-" +
           std::to_string(static_cast<long long>(getpid())) + "-tick-" +
           std::to_string(ticks);
}

std::string action_name(ActionKind kind) {
    switch (kind) {
    case ActionKind::Send:
        return "send";
    case ActionKind::Receive:
        return "receive";
    case ActionKind::Barrier:
        return "barrier";
    case ActionKind::Synchronize:
        return "synchronize";
    }
    return "unknown";
}

class PhaseGate {
public:
    explicit PhaseGate(std::size_t ranks) : completed(ranks, 0) {}

    void arrive_ready() {
        std::lock_guard<std::mutex> lock(mutex);
        ready++;
        cv.notify_all();
    }

    bool wait_until_ready(std::size_t ranks, Clock::time_point deadline) {
        std::unique_lock<std::mutex> lock(mutex);
        return cv.wait_until(lock, deadline, [&]() { return canceled || ready == ranks; }) &&
               !canceled;
    }

    void release(std::size_t phase) {
        std::lock_guard<std::mutex> lock(mutex);
        released = std::max(released, phase + 1);
        cv.notify_all();
    }

    bool wait_for_release(std::size_t phase) {
        std::unique_lock<std::mutex> lock(mutex);
        cv.wait(lock, [&]() { return canceled || released > phase; });
        return !canceled;
    }

    void complete(FMI::Utils::peer_num rank, std::size_t phase) {
        std::lock_guard<std::mutex> lock(mutex);
        completed.at(rank) = std::max(completed.at(rank), phase + 1);
        cv.notify_all();
    }

    bool wait_until_completed(std::size_t phase_count, Clock::time_point deadline) {
        std::unique_lock<std::mutex> lock(mutex);
        const auto done = [&]() {
            for (const auto count : completed) {
                if (count < phase_count) return false;
            }
            return true;
        };
        return cv.wait_until(lock, deadline, [&]() { return canceled || done(); }) &&
               !canceled && done();
    }

    void release_finish() {
        std::lock_guard<std::mutex> lock(mutex);
        finished = true;
        cv.notify_all();
    }

    bool wait_for_finish() {
        std::unique_lock<std::mutex> lock(mutex);
        cv.wait(lock, [&]() { return canceled || finished; });
        return !canceled;
    }

    void cancel() {
        std::lock_guard<std::mutex> lock(mutex);
        canceled = true;
        cv.notify_all();
    }

    bool is_canceled() const {
        std::lock_guard<std::mutex> lock(mutex);
        return canceled;
    }

private:
    mutable std::mutex mutex;
    std::condition_variable cv;
    std::vector<std::size_t> completed;
    std::size_t ready = 0;
    std::size_t released = 0;
    bool canceled = false;
    bool finished = false;
};

struct RankFailure {
    bool timeout = false;
    FMI::Utils::peer_num rank = 0;
    std::size_t phase = 0;
    std::size_t action = 0;
    ActionKind kind = ActionKind::Synchronize;
    std::string detail;
};

struct ExecutionState {
    explicit ExecutionState(std::size_t ranks) : received(ranks) {}

    void fail(RankFailure value, PhaseGate& gate) {
        {
            std::lock_guard<std::mutex> lock(mutex);
            if (!failure) failure = std::move(value);
        }
        gate.cancel();
    }

    std::optional<RankFailure> get_failure() const {
        std::lock_guard<std::mutex> lock(mutex);
        return failure;
    }

    mutable std::mutex mutex;
    std::optional<RankFailure> failure;
    std::vector<std::vector<std::vector<int>>> received;
};

std::string rank_action_detail(const RankFailure& failure) {
    std::ostringstream output;
    output << "rank " << failure.rank << " phase " << failure.phase << " action "
           << failure.action << " (" << action_name(failure.kind) << "): "
           << failure.detail;
    return output.str();
}

void best_effort_cleanup(const std::string& name, FMI::Utils::peer_num world) {
    try {
        FMI::FT::ControlPlane control_plane(config_path(), name, world);
        control_plane.clear_job_state();
        control_plane.clear_criu_state();
    } catch (...) {
        // The primary observation already records infrastructure errors.  Cleanup must not
        // replace that result or prevent the parent from continuing with the corpus.
    }
}

class ControlPlaneCleanup {
public:
    explicit ControlPlaneCleanup(FMI::FT::ControlPlane& control_plane)
        : control_plane(control_plane) {}

    ~ControlPlaneCleanup() {
        try {
            control_plane.clear_job_state();
            control_plane.clear_criu_state();
        } catch (...) {
            // Parent-side cleanup repeats this after the child exits or is killed.
        }
    }

private:
    FMI::FT::ControlPlane& control_plane;
};

void delete_redis_data_keys(const std::string& comm_name) {
    redisContext* context = redisConnect("127.0.0.1", 6379);
    if (context == nullptr || context->err) {
        if (context != nullptr) redisFree(context);
        return;
    }

    const std::string pattern = comm_name + "@epoch=*";
    auto* keys = static_cast<redisReply*>(
            redisCommand(context, "KEYS %b", pattern.data(), pattern.size()));
    if (keys != nullptr && keys->type == REDIS_REPLY_ARRAY) {
        for (std::size_t index = 0; index < keys->elements; ++index) {
            const auto* key = keys->element[index];
            if (key == nullptr || key->str == nullptr) continue;
            auto* removed = static_cast<redisReply*>(
                    redisCommand(context, "DEL %b", key->str, key->len));
            if (removed != nullptr) freeReplyObject(removed);
        }
    }
    if (keys != nullptr) freeReplyObject(keys);
    redisFree(context);
}

class DataKeyCleanup {
public:
    explicit DataKeyCleanup(std::string comm_name) : comm_name(std::move(comm_name)) {}
    ~DataKeyCleanup() { delete_redis_data_keys(comm_name); }

private:
    std::string comm_name;
};

bool tcpunchd_available() {
    const int socket_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (socket_fd < 0) return false;

    const int original_flags = fcntl(socket_fd, F_GETFL, 0);
    if (original_flags >= 0) fcntl(socket_fd, F_SETFL, original_flags | O_NONBLOCK);
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(10000);
    inet_pton(AF_INET, "127.0.0.1", &address.sin_addr);
    int connected = connect(socket_fd, reinterpret_cast<sockaddr*>(&address), sizeof(address));
    if (connected != 0 && errno == EINPROGRESS) {
        pollfd descriptor{socket_fd, POLLOUT, 0};
        connected = poll(&descriptor, 1, 250) > 0 ? 0 : -1;
        if (connected == 0) {
            int socket_error = 0;
            socklen_t length = sizeof(socket_error);
            if (getsockopt(socket_fd, SOL_SOCKET, SO_ERROR, &socket_error, &length) != 0 ||
                socket_error != 0) {
                connected = -1;
            }
        }
    }
    close(socket_fd);
    return connected == 0;
}

bool write_byte(int fd, char value) {
    while (true) {
        const auto written = write(fd, &value, 1);
        if (written == 1) return true;
        if (written < 0 && errno == EINTR) continue;
        return false;
    }
}

std::optional<char> read_byte_until(int fd, Clock::time_point deadline) {
    while (Clock::now() < deadline) {
        const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
                deadline - Clock::now());
        pollfd descriptor{fd, POLLIN | POLLHUP, 0};
        const int timeout = static_cast<int>(
                std::max<std::int64_t>(1, std::min<std::int64_t>(remaining.count(), 100)));
        const int polled = poll(&descriptor, 1, timeout);
        if (polled < 0 && errno == EINTR) continue;
        if (polled < 0) return std::nullopt;
        if (polled == 0) continue;
        char value = 0;
        const auto count = read(fd, &value, 1);
        if (count == 1) return value;
        if (count < 0 && errno == EINTR) continue;
        return std::nullopt;
    }
    return std::nullopt;
}

class RankChildren {
public:
    struct Child {
        pid_t pid = -1;
        int command_fd = -1;
        int event_fd = -1;
    };

    RankChildren() = default;
    RankChildren(const RankChildren&) = delete;
    RankChildren& operator=(const RankChildren&) = delete;

    ~RankChildren() { stop(); }

    template<typename Function>
    std::size_t spawn(Function function) {
        int commands[2] = {-1, -1};
        int events[2] = {-1, -1};
        if (pipe(commands) != 0 || pipe(events) != 0) {
            const int error = errno;
            if (commands[0] >= 0) close(commands[0]);
            if (commands[1] >= 0) close(commands[1]);
            if (events[0] >= 0) close(events[0]);
            if (events[1] >= 0) close(events[1]);
            throw std::runtime_error(std::string("rank pipe: ") + std::strerror(error));
        }

        const pid_t pid = fork();
        if (pid < 0) {
            const int error = errno;
            close(commands[0]);
            close(commands[1]);
            close(events[0]);
            close(events[1]);
            throw std::runtime_error(std::string("rank fork: ") + std::strerror(error));
        }
        if (pid == 0) {
            close(commands[1]);
            close(events[0]);
            for (const auto& child : children) {
                close(child.command_fd);
                close(child.event_fd);
            }
            prctl(PR_SET_PDEATHSIG, SIGKILL);
            if (getppid() == 1) std::_Exit(125);
            try {
                function(commands[0], events[1]);
                close(commands[0]);
                close(events[1]);
                std::_Exit(0);
            } catch (const std::exception& error) {
                std::cerr << "rank child " << static_cast<long long>(getpid())
                          << ": " << error.what() << '\n';
                write_byte(events[1], 'E');
                close(commands[0]);
                close(events[1]);
                std::_Exit(3);
            } catch (const std::string& error) {
                std::cerr << "rank child " << static_cast<long long>(getpid())
                          << ": " << error << '\n';
                write_byte(events[1], 'E');
                close(commands[0]);
                close(events[1]);
                std::_Exit(3);
            } catch (...) {
                std::cerr << "rank child " << static_cast<long long>(getpid())
                          << ": unknown exception\n";
                write_byte(events[1], 'E');
                close(commands[0]);
                close(events[1]);
                std::_Exit(3);
            }
        }

        close(commands[0]);
        close(events[1]);
        children.push_back({pid, commands[1], events[0]});
        return children.size() - 1;
    }

    bool command(std::size_t child, char value) {
        return child < children.size() && write_byte(children[child].command_fd, value);
    }

    bool expect(std::size_t child, char expected, Clock::time_point deadline,
                std::string& error) {
        const auto event = next(child, deadline, error);
        if (!event) return false;
        if (*event != expected) {
            error = "rank " + std::to_string(child) + " produced event " +
                    std::string(1, *event) + " instead of " + std::string(1, expected);
            return false;
        }
        return true;
    }

    std::optional<char> next(std::size_t child, Clock::time_point deadline,
                             std::string& error) {
        if (child >= children.size()) {
            error = "invalid rank child index";
            return std::nullopt;
        }
        const auto event = read_byte_until(children[child].event_fd, deadline);
        if (!event) {
            error = "rank " + std::to_string(child) + " produced no event before deadline";
            return std::nullopt;
        }
        return event;
    }

    bool command_all(char value) {
        for (std::size_t index = 0; index < children.size(); ++index) {
            if (!command(index, value)) return false;
        }
        return true;
    }

    bool expect_all(char expected, Clock::time_point deadline, std::string& error) {
        for (std::size_t index = 0; index < children.size(); ++index) {
            if (!expect(index, expected, deadline, error)) return false;
        }
        return true;
    }

    void stop() {
        if (stopped) return;
        stopped = true;
        for (auto& child : children) {
            if (child.command_fd >= 0) {
                close(child.command_fd);
                child.command_fd = -1;
            }
            if (child.event_fd >= 0) {
                close(child.event_fd);
                child.event_fd = -1;
            }
            if (child.pid > 0) kill(child.pid, SIGKILL);
        }
        for (auto& child : children) {
            if (child.pid <= 0) continue;
            while (waitpid(child.pid, nullptr, 0) < 0 && errno == EINTR) {}
            child.pid = -1;
        }
    }

private:
    std::vector<Child> children;
    bool stopped = false;
};

bool wait_until(Clock::time_point deadline, const std::function<bool()>& predicate) {
    while (Clock::now() < deadline) {
        if (predicate()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    return predicate();
}

std::map<FMI::Utils::peer_num, std::uint64_t> boundaries(
        const FMI::FT::ControlPlane& orchestrator) {
    std::map<FMI::Utils::peer_num, std::uint64_t> result;
    for (const auto& [rank, boundary] : orchestrator.operation_boundaries()) {
        result[rank] = boundary;
    }
    return result;
}

bool boundary_at_least(const FMI::FT::ControlPlane& orchestrator,
                       FMI::Utils::peer_num rank, std::uint64_t expected) {
    const auto current = boundaries(orchestrator);
    const auto found = current.find(rank);
    return found != current.end() && found->second >= expected;
}

bool rank_has_state(const FMI::FT::ControlPlane& orchestrator, std::uint64_t epoch,
                    FMI::Utils::peer_num rank, FMI::FT::RankState expected) {
    for (const auto& entry : orchestrator.directory_snapshot(epoch)) {
        if (entry.rank == rank) return entry.state == expected;
    }
    return false;
}

Observation setup_error(std::string detail) {
    Observation observation;
    observation.classification = Classification::SetupError;
    observation.detail = std::move(detail);
    return observation;
}

Observation preserved(std::string detail = {}) {
    Observation observation;
    observation.classification = Classification::Preserved;
    observation.baseline_valid = true;
    observation.detail = std::move(detail);
    return observation;
}

Observation counterexample(Classification classification, std::string detail,
                           bool promoted = false) {
    Observation observation;
    observation.classification = classification;
    observation.detail = std::move(detail);
    observation.promoted = promoted;
    return observation;
}

Observation infrastructure_skip(std::string detail) {
    Observation observation;
    observation.classification = Classification::InfrastructureSkip;
    observation.detail = std::move(detail);
    return observation;
}

bool promotion_remains_closed(FMI::FT::ControlPlane& orchestrator,
                              std::uint64_t next_epoch,
                              Clock::time_point deadline,
                              std::string& last_error) {
    while (Clock::now() < deadline) {
        try {
            if (orchestrator.promote_epoch(next_epoch)) return false;
        } catch (const std::exception& error) {
            last_error = error.what();
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    return orchestrator.epoch() < next_epoch;
}

void expect_command(int fd, char expected, Clock::time_point deadline) {
    const auto command = read_byte_until(fd, deadline);
    if (!command || *command != expected) {
        throw std::runtime_error("rank did not receive command " +
                                 std::string(1, expected));
    }
}

Observation run_program_case_named(const Scenario& scenario, bool migrate,
                                   const std::string& comm_name) {
    Observation observation;
    if (scenario.kind != ScenarioKind::Program || scenario.backend != Backend::Redis) {
        observation.detail = "the Task 3 executor supports Redis program scenarios only";
        return observation;
    }
    if (scenario.phases.empty() || scenario.request_before_phase >= scenario.phases.size()) {
        observation.detail = "invalid phase program or migration request point";
        return observation;
    }
    for (std::size_t phase = 0; phase < scenario.phases.size(); ++phase) {
        if (scenario.phases[phase].rank_actions.size() != scenario.world) {
            observation.detail = "phase " + std::to_string(phase) +
                                 " does not contain one action list per rank";
            return observation;
        }
    }

    const auto start = Clock::now();
    const auto margin = std::min(scenario.deadline / 8, std::chrono::milliseconds(500));
    const auto deadline = start + scenario.deadline - margin;
    PhaseGate gate(scenario.world);
    ExecutionState state(scenario.world);
    std::vector<std::thread> ranks;
    ranks.reserve(scenario.world);

    try {
        FMI::FT::ControlPlane orchestrator(config_path(), comm_name, scenario.world);
        orchestrator.clear_job_state();
        orchestrator.clear_criu_state();
        ControlPlaneCleanup cleanup(orchestrator);

        for (FMI::Utils::peer_num rank = 0; rank < scenario.world; ++rank) {
            ranks.emplace_back([&, rank]() {
                std::size_t phase_index = 0;
                std::size_t action_index = 0;
                ActionKind action_kind = ActionKind::Synchronize;
                try {
                    FMI::Communicator comm(rank, scenario.world, config_path(), comm_name, 128,
                                           "counterexample-rank-" + std::to_string(rank));
                    gate.arrive_ready();
                    for (phase_index = 0; phase_index < scenario.phases.size(); ++phase_index) {
                        if (!gate.wait_for_release(phase_index)) return;
                        const auto& actions = scenario.phases[phase_index].rank_actions[rank];
                        for (action_index = 0; action_index < actions.size(); ++action_index) {
                            const auto& action = actions[action_index];
                            action_kind = action.kind;
                            switch (action.kind) {
                            case ActionKind::Send: {
                                FMI::Comm::Data<std::vector<int>> data(action.payload);
                                comm.send(data, action.peer);
                                break;
                            }
                            case ActionKind::Receive: {
                                FMI::Comm::Data<std::vector<int>> data(action.receive_elements);
                                comm.recv(data, action.peer);
                                state.received[rank].push_back(data.get());
                                break;
                            }
                            case ActionKind::Barrier:
                                comm.barrier();
                                break;
                            case ActionKind::Synchronize:
                                break;
                            }
                        }
                        gate.complete(rank, phase_index);
                    }
                    // Keep every Communicator (and therefore every eager Redis object) alive
                    // until all ranks have completed the final phase.
                    gate.wait_for_finish();
                } catch (const FMI::Utils::Timeout& error) {
                    state.fail({true, rank, phase_index, action_index, action_kind, error.what()}, gate);
                } catch (const std::exception& error) {
                    state.fail({false, rank, phase_index, action_index, action_kind, error.what()}, gate);
                } catch (const std::string& error) {
                    state.fail({false, rank, phase_index, action_index, action_kind, error}, gate);
                } catch (...) {
                    state.fail({false, rank, phase_index, action_index, action_kind,
                                "unknown exception"}, gate);
                }
            });
        }

        auto join_ranks = [&]() {
            for (auto& rank : ranks) {
                if (rank.joinable()) rank.join();
            }
        };

        if (!gate.wait_until_ready(scenario.world, deadline)) {
            if (!gate.is_canceled()) {
                observation.classification = migrate ? Classification::OperationStuck
                                                     : Classification::SetupError;
                observation.detail = "ranks did not become ready before the internal deadline";
            }
            gate.cancel();
            join_ranks();
        } else {
            for (std::size_t phase = 0; phase < scenario.phases.size(); ++phase) {
                if (phase != 0 && !gate.wait_until_completed(phase, deadline)) {
                    if (!gate.is_canceled()) {
                        observation.classification = migrate ? Classification::OperationStuck
                                                             : Classification::SetupError;
                        observation.detail = "phase " + std::to_string(phase - 1) +
                                             " did not complete before the internal deadline";
                        gate.cancel();
                    }
                    break;
                }

                const bool request_now = migrate && phase == scenario.request_before_phase;
                std::uint64_t next_epoch = 0;
                if (request_now) {
                    next_epoch = orchestrator.epoch() + 1;
                    orchestrator.request_migrations(scenario.targets);
                }

                gate.release(phase);

                if (request_now) {
                    std::string last_promotion_error;
                    while (Clock::now() < deadline && !gate.is_canceled()) {
                        try {
                            orchestrator.promote_epoch(next_epoch);
                            observation.promoted = true;
                            break;
                        } catch (const std::exception& error) {
                            last_promotion_error = error.what();
                        }
                        std::this_thread::sleep_for(std::chrono::milliseconds(5));
                    }
                    if (!observation.promoted && !gate.is_canceled()) {
                        observation.classification = Classification::PromotionStuck;
                        observation.detail = "promotion did not open before the scenario deadline";
                        if (!last_promotion_error.empty()) {
                            observation.detail += ": " + last_promotion_error;
                        }
                        gate.cancel();
                        break;
                    }
                }
            }

            if (!gate.is_canceled() &&
                !gate.wait_until_completed(scenario.phases.size(), deadline)) {
                observation.classification = migrate ? Classification::OperationStuck
                                                     : Classification::SetupError;
                observation.detail = "final phase did not complete before the internal deadline";
                gate.cancel();
            }
            gate.release_finish();
            join_ranks();
        }

        for (const auto& rank_receives : state.received) {
            observation.received_payloads.insert(observation.received_payloads.end(),
                                                 rank_receives.begin(), rank_receives.end());
        }

        if (const auto failure = state.get_failure()) {
            observation.detail = rank_action_detail(*failure);
            if (failure->timeout) {
                observation.classification = failure->kind == ActionKind::Receive
                        ? Classification::LostMessage
                        : Classification::OperationStuck;
            } else {
                observation.classification = Classification::SetupError;
            }
        } else if (observation.detail.empty() &&
                   observation.classification != Classification::PromotionStuck) {
            observation.classification = Classification::Preserved;
            observation.baseline_valid = !migrate;
        }
    } catch (const std::exception& error) {
        gate.cancel();
        for (auto& rank : ranks) {
            if (rank.joinable()) rank.join();
        }
        observation.classification = Classification::SetupError;
        observation.detail = std::string("orchestrator: ") + error.what();
    } catch (const std::string& error) {
        gate.cancel();
        for (auto& rank : ranks) {
            if (rank.joinable()) rank.join();
        }
        observation.classification = Classification::SetupError;
        observation.detail = "orchestrator: " + error;
    } catch (...) {
        gate.cancel();
        for (auto& rank : ranks) {
            if (rank.joinable()) rank.join();
        }
        observation.classification = Classification::SetupError;
        observation.detail = "orchestrator: unknown exception";
    }
    return observation;
}

Observation run_future_send_after_cut(const Scenario& scenario, bool migrate,
                                      const std::string& comm_name) {
    const auto deadline = Clock::now() + scenario.deadline - std::chrono::milliseconds(750);
    FMI::FT::ControlPlane orchestrator(config_path(), comm_name, scenario.world);
    orchestrator.clear_job_state();
    orchestrator.clear_criu_state();
    ControlPlaneCleanup control_cleanup(orchestrator);
    DataKeyCleanup data_cleanup(comm_name);
    RankChildren ranks;

    ranks.spawn([=](int commands, int events) {
        FMI::Communicator comm(0, 2, config_path(), comm_name, 128, "future-rank-0");
        write_byte(events, 'R');
        expect_command(commands, 'S', deadline);
        FMI::Comm::Data<std::vector<int>> warm(std::vector<int>{101});
        comm.send(warm, 1);
        write_byte(events, 'W');
        expect_command(commands, 'C', deadline);
        FMI::Comm::Data<std::vector<int>> local(std::vector<int>{202});
        comm.send(local, 0);
        FMI::Comm::Data<std::vector<int>> future(std::vector<int>{303});
        comm.send(future, 1);
        write_byte(events, 'D');
        expect_command(commands, 'X', deadline);
    });
    ranks.spawn([=](int commands, int events) {
        FMI::Communicator comm(1, 2, config_path(), comm_name, 128, "future-rank-1");
        write_byte(events, 'R');
        expect_command(commands, 'S', deadline);
        FMI::Comm::Data<std::vector<int>> warm(1);
        comm.recv(warm, 0);
        if (warm.get() != std::vector<int>{101}) throw std::runtime_error("bad warmup");
        write_byte(events, 'W');
        expect_command(commands, 'B', deadline);
        FMI::Comm::Data<std::vector<int>> future(1);
        comm.recv(future, 0);
        if (future.get() != std::vector<int>{303}) throw std::runtime_error("bad future payload");
        write_byte(events, 'D');
        expect_command(commands, 'X', deadline);
    });

    std::string error;
    if (!ranks.expect_all('R', deadline, error) || !ranks.command_all('S') ||
        !ranks.expect_all('W', deadline, error) || !ranks.command(1, 'B')) {
        return setup_error(error.empty() ? "failed to start future-send ranks" : error);
    }
    if (!wait_until(deadline, [&]() { return boundary_at_least(orchestrator, 1, 1); })) {
        return setup_error("receiver did not enter the below-cut receive");
    }

    if (migrate) orchestrator.request_migration(1);
    if (!ranks.command(0, 'C')) return setup_error("failed to release future sender");

    if (!migrate) {
        if (!ranks.expect(0, 'D', deadline, error) ||
            !ranks.expect(1, 'D', deadline, error)) {
            return setup_error(error);
        }
        return preserved("future send satisfies the already-posted receive without migration");
    }

    if (!wait_until(deadline, [&]() {
            return boundary_at_least(orchestrator, 0, 2) &&
                   boundary_at_least(orchestrator, 1, 1);
        })) {
        return setup_error("did not reach the expected sender=2 receiver=1 cut state");
    }
    std::string promotion_error;
    if (!promotion_remains_closed(orchestrator, 1, Clock::now() +
                                  std::chrono::milliseconds(250), promotion_error)) {
        return setup_error("future-send cut unexpectedly promoted");
    }
    return counterexample(
            Classification::OperationStuck,
            "receiver is inside recv at boundary 1 while the matching sender parks at cut 2" +
            (promotion_error.empty() ? std::string{} : "; " + promotion_error));
}

Observation run_pending_set_expansion(const Scenario& scenario, bool migrate,
                                      const std::string& comm_name) {
    const auto deadline = Clock::now() + scenario.deadline - std::chrono::milliseconds(750);
    FMI::FT::ControlPlane orchestrator(config_path(), comm_name, scenario.world);
    orchestrator.clear_job_state();
    orchestrator.clear_criu_state();
    ControlPlaneCleanup control_cleanup(orchestrator);
    DataKeyCleanup data_cleanup(comm_name);
    RankChildren ranks;

    for (FMI::Utils::peer_num rank = 0; rank < 2; ++rank) {
        ranks.spawn([=](int commands, int events) {
            FMI::Communicator comm(rank, 2, config_path(), comm_name, 128,
                                   "expansion-rank-" + std::to_string(rank));
            write_byte(events, 'R');
            expect_command(commands, 'S', deadline);
            comm.barrier();
            write_byte(events, 'D');
            expect_command(commands, 'X', deadline);
        });
    }

    std::string error;
    if (!ranks.expect_all('R', deadline, error)) return setup_error(error);
    if (!migrate) {
        if (!ranks.command_all('S') || !ranks.expect_all('D', deadline, error)) {
            return setup_error(error.empty() ? "failed to run baseline barrier" : error);
        }
        return preserved("the two-rank barrier completes without migration");
    }

    orchestrator.request_migrations({0});
    if (!ranks.command_all('S')) return setup_error("failed to release pending-set ranks");
    if (!wait_until(deadline, [&]() {
            return rank_has_state(orchestrator, 0, 0, FMI::FT::RankState::Quiesced) &&
                   boundary_at_least(orchestrator, 0, 0) &&
                   boundary_at_least(orchestrator, 1, 0);
        })) {
        return setup_error("initial target and survivor did not park at cut 0");
    }

    orchestrator.request_migrations({0, 1});
    if (!rank_has_state(orchestrator, 0, 1, FMI::FT::RankState::MigrationPending)) {
        return setup_error("expanded pending set did not mark the parked survivor pending");
    }
    std::string promotion_error;
    if (!promotion_remains_closed(orchestrator, 1, Clock::now() +
                                  std::chrono::milliseconds(250), promotion_error)) {
        return setup_error("expanded pending set unexpectedly promoted");
    }
    return counterexample(
            Classification::PromotionStuck,
            "rank 1 parked as a survivor before it was added to the pending set and can no "
            "longer publish QUIESCED" +
            (promotion_error.empty() ? std::string{} : "; " + promotion_error));
}

Observation run_failed_operation_boundary(const Scenario& scenario, bool migrate,
                                          const std::string& comm_name) {
    const auto deadline = Clock::now() + scenario.deadline - std::chrono::milliseconds(750);
    FMI::FT::ControlPlane orchestrator(config_path(), comm_name, scenario.world);
    orchestrator.clear_job_state();
    orchestrator.clear_criu_state();
    ControlPlaneCleanup control_cleanup(orchestrator);
    DataKeyCleanup data_cleanup(comm_name);
    RankChildren ranks;

    ranks.spawn([=](int commands, int events) {
        FMI::Communicator comm(0, 2, config_path(), comm_name, 128, "failed-op-rank-0");
        write_byte(events, 'R');
        expect_command(commands, 'S', deadline);
        FMI::Comm::Data<std::vector<int>> reduce_send(std::vector<int>{1});
        FMI::Comm::Data<std::vector<int>> reduce_receive(2);
        FMI::Utils::Function<std::vector<int>> add(
                [](std::vector<int> left, std::vector<int> right) {
                    const auto count = std::min(left.size(), right.size());
                    for (std::size_t index = 0; index < count; ++index) left[index] += right[index];
                    return left;
                }, true, true);
        bool rejected = false;
        try {
            comm.reduce(reduce_send, reduce_receive, 0, add);
        } catch (const std::runtime_error&) {
            rejected = true;
        }
        if (!rejected) throw std::runtime_error("mismatched reduce was not rejected");

        FMI::Comm::Data<std::vector<int>> first(std::vector<int>{101});
        comm.send(first, 1);
        write_byte(events, 'P');
        expect_command(commands, 'C', deadline);
        FMI::Comm::Data<std::vector<int>> second(std::vector<int>{202});
        comm.send(second, 1);
        write_byte(events, 'D');
        expect_command(commands, 'X', deadline);
    });
    ranks.spawn([=](int commands, int events) {
        FMI::Communicator comm(1, 2, config_path(), comm_name, 128, "failed-op-rank-1");
        write_byte(events, 'R');
        expect_command(commands, 'C', deadline);
        FMI::Comm::Data<std::vector<int>> first(1);
        FMI::Comm::Data<std::vector<int>> second(1);
        comm.recv(first, 0);
        comm.recv(second, 0);
        if (first.get() != std::vector<int>{101} || second.get() != std::vector<int>{202}) {
            throw std::runtime_error("wrong payload after failed operation");
        }
        write_byte(events, 'D');
        expect_command(commands, 'X', deadline);
    });

    std::string error;
    if (!ranks.expect_all('R', deadline, error) || !ranks.command(0, 'S') ||
        !ranks.expect(0, 'P', deadline, error)) {
        return setup_error(error.empty() ? "failed operation prelude did not complete" : error);
    }
    if (migrate) orchestrator.request_migration(1);
    if (!ranks.command(0, 'C') || !ranks.command(1, 'C')) {
        return setup_error("failed to release the post-error operations");
    }

    if (!migrate) {
        if (!ranks.expect(0, 'D', deadline, error) ||
            !ranks.expect(1, 'D', deadline, error)) {
            return setup_error(error);
        }
        return preserved("the application catches the rejected reduce and continues normally");
    }

    if (!wait_until(deadline, [&]() {
            return boundary_at_least(orchestrator, 0, 2) &&
                   boundary_at_least(orchestrator, 1, 1);
        })) {
        return setup_error("failed operation did not produce the expected 2-to-1 boundary skew");
    }
    std::string promotion_error;
    if (!promotion_remains_closed(orchestrator, 1, Clock::now() +
                                  std::chrono::milliseconds(250), promotion_error)) {
        return setup_error("failed-operation cut unexpectedly promoted");
    }
    return counterexample(
            Classification::OperationStuck,
            "the rejected reduce advanced rank 0's boundary, so rank 0 parks before send(202) "
            "while rank 1 blocks receiving it" +
            (promotion_error.empty() ? std::string{} : "; " + promotion_error));
}

Observation run_incomplete_membership(const Scenario& scenario, bool migrate,
                                      const std::string& comm_name) {
    const auto deadline = Clock::now() + scenario.deadline - std::chrono::milliseconds(750);
    FMI::FT::ControlPlane orchestrator(config_path(), comm_name, scenario.world);
    orchestrator.clear_job_state();
    orchestrator.clear_criu_state();
    ControlPlaneCleanup control_cleanup(orchestrator);
    DataKeyCleanup data_cleanup(comm_name);
    RankChildren ranks;
    const FMI::Utils::peer_num started_ranks = migrate ? 2 : 3;

    for (FMI::Utils::peer_num rank = 0; rank < started_ranks; ++rank) {
        ranks.spawn([=](int commands, int events) {
            FMI::Communicator comm(rank, 3, config_path(), comm_name, 128,
                                   "membership-rank-" + std::to_string(rank));
            write_byte(events, 'R');
            expect_command(commands, 'S', deadline);
            comm.barrier();
            write_byte(events, 'D');
            expect_command(commands, 'X', deadline);
        });
    }

    std::string error;
    if (!ranks.expect_all('R', deadline, error)) return setup_error(error);
    if (!migrate) {
        if (!ranks.command_all('S') || !ranks.expect_all('D', deadline, error)) {
            return setup_error(error.empty() ? "full-world baseline barrier failed" : error);
        }
        return preserved("all three configured ranks join before the barrier");
    }

    if (orchestrator.directory_snapshot(0).size() != 2) {
        return setup_error("delayed-rank setup did not leave exactly two registered members");
    }
    orchestrator.request_migration(1);
    if (!ranks.command_all('S')) return setup_error("failed to release partial membership");
    if (!wait_until(deadline, [&]() {
            return rank_has_state(orchestrator, 0, 1, FMI::FT::RankState::Quiesced) &&
                   boundary_at_least(orchestrator, 0, 0) &&
                   boundary_at_least(orchestrator, 1, 0);
        })) {
        return setup_error("the two registered ranks did not reach cut 0");
    }

    try {
        if (!orchestrator.promote_epoch(1)) {
            return setup_error("partial-world promotion returned false");
        }
    } catch (const std::exception& error_value) {
        return setup_error(std::string("partial-world promotion was rejected: ") +
                           error_value.what());
    }
    return counterexample(
            Classification::PrematureCollective,
            "epoch 1 was promoted with only ranks 0 and 1 registered in a three-rank world; "
            "rank 2 has never joined",
            true);
}

Observation run_stale_barrier_object(const Scenario& scenario, bool migrate,
                                     const std::string& comm_name) {
    const auto deadline = Clock::now() + scenario.deadline - std::chrono::milliseconds(750);
    const std::string config = no_state_transfer_config_path();
    FMI::FT::ControlPlane orchestrator(config, comm_name, scenario.world);
    orchestrator.clear_job_state();
    ControlPlaneCleanup control_cleanup(orchestrator);
    DataKeyCleanup data_cleanup(comm_name);
    RankChildren ranks;

    for (FMI::Utils::peer_num rank = 0; rank < 2; ++rank) {
        ranks.spawn([=](int commands, int events) {
            FMI::Communicator comm(rank, 2, config, comm_name, 128,
                                   "stale-barrier-rank-" + std::to_string(rank));
            write_byte(events, 'R');
            expect_command(commands, 'S', deadline);
            comm.barrier();
            write_byte(events, 'A');
            expect_command(commands, 'N', deadline);
            comm.barrier();
            write_byte(events, 'D');
            expect_command(commands, 'X', deadline);
        });
    }

    std::string error;
    if (!ranks.expect_all('R', deadline, error) || !ranks.command_all('S') ||
        !ranks.expect_all('A', deadline, error)) {
        return setup_error(error.empty() ? "epoch-zero barrier did not complete" : error);
    }
    if (!migrate) {
        if (!ranks.command_all('N') || !ranks.expect_all('D', deadline, error)) {
            return setup_error(error.empty() ? "baseline second barrier failed" : error);
        }
        return preserved("both ranks participate in both baseline barriers");
    }

    orchestrator.request_migration(1);
    if (!ranks.command_all('N')) return setup_error("failed to release migration barrier");
    if (!wait_until(deadline, [&]() {
            return rank_has_state(orchestrator, 0, 1, FMI::FT::RankState::Quiesced) &&
                   boundary_at_least(orchestrator, 0, 1) &&
                   boundary_at_least(orchestrator, 1, 1);
        })) {
        return setup_error("target did not exit and survivor did not park at cut 1");
    }

    try {
        if (!orchestrator.promote_epoch(1)) {
            return setup_error("stale-barrier migration did not promote");
        }
    } catch (const std::exception& error_value) {
        return setup_error(std::string("stale-barrier promotion failed: ") +
                           error_value.what());
    }
    if (!ranks.expect(0, 'D', deadline, error)) {
        return setup_error("survivor's epoch-one barrier did not return: " + error);
    }
    return counterexample(
            Classification::PrematureCollective,
            "rank 0 completed the epoch-one barrier before any replacement rank 1 existed; "
            "ClientServer counted rank 1's epoch-zero _barrier_0 object",
            true);
}

Observation run_direct_moved_link(const Scenario& scenario, bool migrate,
                                  const std::string& comm_name) {
    if (!tcpunchd_available()) {
        return infrastructure_skip("tcpunchd is not listening on 127.0.0.1:10000");
    }

    const auto deadline = Clock::now() + scenario.deadline - std::chrono::milliseconds(750);
    const std::string config = direct_config_path();
    FMI::FT::ControlPlane orchestrator(config, comm_name, scenario.world);
    orchestrator.clear_job_state();
    orchestrator.clear_criu_state();
    ControlPlaneCleanup control_cleanup(orchestrator);
    RankChildren ranks;

    ranks.spawn([=](int commands, int events) {
        FMI::Communicator comm(0, 2, config, comm_name, 128, "direct-moved-rank-0");
        write_byte(events, 'R');
        expect_command(commands, 'W', deadline);
        FMI::Comm::Data<std::vector<int>> warm(std::vector<int>{1});
        comm.send(warm, 1);
        write_byte(events, 'A');
        expect_command(commands, 'B', deadline);
        FMI::Comm::Data<std::vector<int>> old(std::vector<int>{101});
        comm.send(old, 1);
        write_byte(events, 'B');
        expect_command(commands, 'C', deadline);
        FMI::Comm::Data<std::vector<int>> newer(std::vector<int>{303});
        comm.send(newer, 1);
        write_byte(events, 'D');
        expect_command(commands, 'X', deadline);
    });
    ranks.spawn([=](int commands, int events) {
        FMI::Communicator comm(1, 2, config, comm_name, 128, "direct-moved-rank-1");
        write_byte(events, 'R');
        expect_command(commands, 'W', deadline);
        FMI::Comm::Data<std::vector<int>> warm(1);
        comm.recv(warm, 0);
        if (warm.get() != std::vector<int>{1}) throw std::runtime_error("bad Direct warmup");
        write_byte(events, 'A');
        expect_command(commands, 'B', deadline);
        FMI::Comm::Data<std::vector<int>> align(std::vector<int>{901});
        comm.send(align, 0);
        write_byte(events, 'B');
        expect_command(commands, 'C', deadline);
        FMI::Comm::Data<std::vector<int>> received(1);
        comm.recv(received, 0);
        const auto value = received.get().front();
        if (value == 101) {
            write_byte(events, 'O');
        } else if (value == 303) {
            write_byte(events, 'N');
        } else {
            throw std::runtime_error("unexpected Direct payload " + std::to_string(value));
        }
        expect_command(commands, 'X', deadline);
    });

    std::string error;
    if (!ranks.expect_all('R', deadline, error) || !ranks.command_all('W') ||
        !ranks.expect_all('A', deadline, error) || !ranks.command_all('B') ||
        !ranks.expect_all('B', deadline, error)) {
        return setup_error(error.empty() ? "Direct moved-link prelude failed" : error);
    }

    if (migrate) orchestrator.request_migration(1);
    if (!ranks.command_all('C')) return setup_error("failed to release Direct moved-link cut");
    if (migrate) {
        if (!wait_until(deadline, [&]() {
                return rank_has_state(orchestrator, 0, 1, FMI::FT::RankState::Quiesced) &&
                       boundary_at_least(orchestrator, 0, 2) &&
                       boundary_at_least(orchestrator, 1, 2);
            })) {
            return setup_error("Direct moved-link ranks did not reach cut 2");
        }
        try {
            if (!orchestrator.promote_epoch(1)) {
                return setup_error("Direct moved-link migration did not promote");
            }
        } catch (const std::exception& error_value) {
            return setup_error(std::string("Direct moved-link promotion failed: ") +
                               error_value.what());
        }
    }

    const auto payload_event = ranks.next(1, deadline, error);
    if (!payload_event || !ranks.expect(0, 'D', deadline, error)) {
        return setup_error(error.empty() ? "Direct moved-link result missing" : error);
    }
    if (*payload_event != 'O' && *payload_event != 'N') {
        return setup_error("Direct moved-link rank produced an invalid payload event");
    }

    if (!migrate) {
        if (*payload_event != 'O') {
            return setup_error("Direct moved-link baseline did not preserve FIFO");
        }
        Observation observation = preserved();
        observation.received_payloads = {{101}};
        return observation;
    }
    if (*payload_event == 'O') {
        Observation observation = preserved(
                "the unread moved-link payload survived reconfiguration");
        observation.promoted = true;
        observation.received_payloads = {{101}};
        return observation;
    }
    Observation observation = counterexample(
            Classification::WrongPayload,
            "receive old baseline=[101] migrated=[303]; closing the moved endpoint discarded "
            "the unread epoch-zero bytes",
            true);
    observation.received_payloads = {{303}};
    return observation;
}

Observation run_direct_survivor_link(const Scenario& scenario, bool migrate,
                                     const std::string& comm_name) {
    if (!tcpunchd_available()) {
        return infrastructure_skip("tcpunchd is not listening on 127.0.0.1:10000");
    }

    const auto deadline = Clock::now() + scenario.deadline - std::chrono::milliseconds(750);
    const std::string config = direct_config_path();
    FMI::FT::ControlPlane orchestrator(config, comm_name, scenario.world);
    orchestrator.clear_job_state();
    orchestrator.clear_criu_state();
    ControlPlaneCleanup control_cleanup(orchestrator);
    RankChildren ranks;

    ranks.spawn([=](int commands, int events) {
        FMI::Communicator comm(0, 3, config, comm_name, 128, "direct-survivor-rank-0");
        write_byte(events, 'R');
        expect_command(commands, 'W', deadline);
        FMI::Comm::Data<std::vector<int>> warm(std::vector<int>{1});
        comm.send(warm, 1);
        FMI::Comm::Data<std::vector<int>> from_two(1);
        comm.recv(from_two, 2);
        write_byte(events, 'A');
        expect_command(commands, 'B', deadline);
        FMI::Comm::Data<std::vector<int>> old(std::vector<int>{101});
        comm.send(old, 1);
        write_byte(events, 'B');
        expect_command(commands, 'C', deadline);
        FMI::Comm::Data<std::vector<int>> newer(std::vector<int>{303});
        comm.send(newer, 1);
        FMI::Comm::Data<std::vector<int>> to_two(std::vector<int>{404});
        comm.send(to_two, 2);
        write_byte(events, 'D');
        expect_command(commands, 'X', deadline);
    });
    ranks.spawn([=](int commands, int events) {
        FMI::Communicator comm(1, 3, config, comm_name, 128, "direct-survivor-rank-1");
        write_byte(events, 'R');
        expect_command(commands, 'W', deadline);
        FMI::Comm::Data<std::vector<int>> warm(1);
        comm.recv(warm, 0);
        FMI::Comm::Data<std::vector<int>> to_two(std::vector<int>{2});
        comm.send(to_two, 2);
        write_byte(events, 'A');
        expect_command(commands, 'B', deadline);
        FMI::Comm::Data<std::vector<int>> align(std::vector<int>{201});
        comm.send(align, 2);
        write_byte(events, 'B');
        expect_command(commands, 'C', deadline);
        FMI::Comm::Data<std::vector<int>> received(1);
        comm.recv(received, 0);
        const auto value = received.get().front();
        if (value == 101) {
            write_byte(events, 'O');
        } else if (value == 303) {
            write_byte(events, 'N');
        } else {
            throw std::runtime_error("unexpected survivor-stream payload " +
                                     std::to_string(value));
        }
        expect_command(commands, 'X', deadline);
    });
    ranks.spawn([=](int commands, int events) {
        FMI::Communicator comm(2, 3, config, comm_name, 128, "direct-survivor-rank-2");
        write_byte(events, 'R');
        expect_command(commands, 'W', deadline);
        FMI::Comm::Data<std::vector<int>> from_one(1);
        comm.recv(from_one, 1);
        FMI::Comm::Data<std::vector<int>> to_zero(std::vector<int>{3});
        comm.send(to_zero, 0);
        write_byte(events, 'A');
        expect_command(commands, 'B', deadline);
        FMI::Comm::Data<std::vector<int>> align(std::vector<int>{301});
        comm.send(align, 0);
        write_byte(events, 'B');
        expect_command(commands, 'C', deadline);
        FMI::Comm::Data<std::vector<int>> received(1);
        comm.recv(received, 0);
        if (received.get() != std::vector<int>{404}) {
            throw std::runtime_error("moved link did not receive epoch-current payload");
        }
        write_byte(events, 'D');
        expect_command(commands, 'X', deadline);
    });

    std::string error;
    if (!ranks.expect_all('R', deadline, error) || !ranks.command_all('W') ||
        !ranks.expect_all('A', deadline, error) || !ranks.command_all('B') ||
        !ranks.expect_all('B', deadline, error)) {
        return setup_error(error.empty() ? "Direct survivor-link prelude failed" : error);
    }

    if (migrate) orchestrator.request_migration(2);
    if (!ranks.command_all('C')) return setup_error("failed to release Direct survivor-link cut");
    if (migrate) {
        if (!wait_until(deadline, [&]() {
                return rank_has_state(orchestrator, 0, 2, FMI::FT::RankState::Quiesced) &&
                       boundary_at_least(orchestrator, 0, 3) &&
                       boundary_at_least(orchestrator, 1, 3) &&
                       boundary_at_least(orchestrator, 2, 3);
            })) {
            return setup_error("Direct survivor-link ranks did not reach cut 3");
        }
        try {
            if (!orchestrator.promote_epoch(1)) {
                return setup_error("Direct survivor-link migration did not promote");
            }
        } catch (const std::exception& error_value) {
            return setup_error(std::string("Direct survivor-link promotion failed: ") +
                               error_value.what());
        }
    }

    const auto payload_event = ranks.next(1, deadline, error);
    if (!payload_event || !ranks.expect(0, 'D', deadline, error) ||
        !ranks.expect(2, 'D', deadline, error)) {
        return setup_error(error.empty() ? "Direct survivor-link result missing" : error);
    }
    if (*payload_event != 'O' && *payload_event != 'N') {
        return setup_error("Direct survivor-link rank produced an invalid payload event");
    }
    if (!migrate) {
        if (*payload_event != 'O') {
            return setup_error("Direct survivor-link baseline did not preserve FIFO");
        }
        Observation observation = preserved();
        observation.received_payloads = {{101}};
        return observation;
    }
    if (*payload_event == 'N') {
        Observation observation = counterexample(
                Classification::WrongPayload,
                "closing the survivor link dropped payload 101, so the post-cut payload 303 "
                "occupied its receive slot",
                true);
        observation.received_payloads = {{303}};
        return observation;
    }
    Observation observation = counterexample(
            Classification::ReorderedOrDuplicated,
            "rank 1 received payload 101 from the retained rank-0/rank-1 socket only after "
            "epoch 1 was promoted; epoch-zero bytes crossed the literal epoch fence",
            true);
    observation.received_payloads = {{101}};
    return observation;
}

Observation run_specialized_case_named(const Scenario& scenario, bool migrate,
                                       const std::string& comm_name) {
    try {
        switch (scenario.kind) {
        case ScenarioKind::FutureSendAfterCut:
            return run_future_send_after_cut(scenario, migrate, comm_name);
        case ScenarioKind::StaleBarrierObject:
            return run_stale_barrier_object(scenario, migrate, comm_name);
        case ScenarioKind::PendingSetExpansion:
            return run_pending_set_expansion(scenario, migrate, comm_name);
        case ScenarioKind::FailedOperationBoundary:
            return run_failed_operation_boundary(scenario, migrate, comm_name);
        case ScenarioKind::IncompleteMembership:
            return run_incomplete_membership(scenario, migrate, comm_name);
        case ScenarioKind::Program:
            return setup_error("program scenario dispatched to specialized executor");
        }
    } catch (const std::exception& error) {
        return setup_error(std::string("specialized executor: ") + error.what());
    } catch (const std::string& error) {
        return setup_error("specialized executor: " + error);
    } catch (...) {
        return setup_error("specialized executor: unknown exception");
    }
    return setup_error("unknown specialized scenario kind");
}

Observation run_case_named(const Scenario& scenario, bool migrate,
                           const std::string& comm_name) {
    if (scenario.backend == Backend::Direct) {
        if (scenario.id == "direct_moved_link_backlog") {
            return run_direct_moved_link(scenario, migrate, comm_name);
        }
        if (scenario.id == "direct_survivor_link_crosses_epoch") {
            return run_direct_survivor_link(scenario, migrate, comm_name);
        }
        return setup_error("unknown Direct scenario");
    }
    if (scenario.kind == ScenarioKind::Program) {
        return run_program_case_named(scenario, migrate, comm_name);
    }
    return run_specialized_case_named(scenario, migrate, comm_name);
}

void append_u32(std::vector<std::uint8_t>& bytes, std::uint32_t value) {
    for (unsigned int shift = 0; shift < 32; shift += 8) {
        bytes.push_back(static_cast<std::uint8_t>((value >> shift) & 0xff));
    }
}

bool append_size(std::vector<std::uint8_t>& bytes, std::size_t value) {
    if (value > std::numeric_limits<std::uint32_t>::max()) return false;
    append_u32(bytes, static_cast<std::uint32_t>(value));
    return true;
}

std::optional<std::vector<std::uint8_t>> encode_observation(const Observation& observation) {
    std::vector<std::uint8_t> body;
    append_u32(body, observation_magic);
    append_u32(body, static_cast<std::uint32_t>(observation.classification));
    body.push_back(observation.promoted ? 1 : 0);
    if (!append_size(body, observation.detail.size())) return std::nullopt;
    body.insert(body.end(), observation.detail.begin(), observation.detail.end());
    if (!append_size(body, observation.received_payloads.size())) return std::nullopt;
    for (const auto& payload : observation.received_payloads) {
        if (!append_size(body, payload.size())) return std::nullopt;
        for (const auto value : payload) {
            append_u32(body, static_cast<std::uint32_t>(static_cast<std::int32_t>(value)));
        }
    }
    if (body.size() > std::numeric_limits<std::uint32_t>::max()) return std::nullopt;

    std::vector<std::uint8_t> framed;
    append_u32(framed, static_cast<std::uint32_t>(body.size()));
    framed.insert(framed.end(), body.begin(), body.end());
    return framed;
}

bool write_all(int fd, const std::vector<std::uint8_t>& bytes) {
    std::size_t offset = 0;
    while (offset < bytes.size()) {
        const auto written = write(fd, bytes.data() + offset, bytes.size() - offset);
        if (written > 0) {
            offset += static_cast<std::size_t>(written);
        } else if (written < 0 && errno == EINTR) {
            continue;
        } else {
            return false;
        }
    }
    return true;
}

class Decoder {
public:
    explicit Decoder(const std::vector<std::uint8_t>& bytes) : bytes(bytes) {}

    bool u32(std::uint32_t& value) {
        if (bytes.size() - offset < 4) return false;
        value = 0;
        for (unsigned int shift = 0; shift < 32; shift += 8) {
            value |= static_cast<std::uint32_t>(bytes[offset++]) << shift;
        }
        return true;
    }

    bool string(std::string& value) {
        std::uint32_t size = 0;
        if (!u32(size) || bytes.size() - offset < size) return false;
        value.assign(reinterpret_cast<const char*>(bytes.data() + offset), size);
        offset += size;
        return true;
    }

    bool done() const { return offset == bytes.size(); }

private:
    const std::vector<std::uint8_t>& bytes;
    std::size_t offset = 0;
};

std::optional<Observation> decode_observation(const std::vector<std::uint8_t>& framed) {
    if (framed.size() < 4) return std::nullopt;
    Decoder frame_decoder(framed);
    std::uint32_t body_size = 0;
    if (!frame_decoder.u32(body_size) || body_size > max_pipe_message ||
        framed.size() != static_cast<std::size_t>(body_size) + 4) {
        return std::nullopt;
    }
    std::vector<std::uint8_t> body(framed.begin() + 4, framed.end());
    Decoder decoder(body);
    std::uint32_t magic = 0;
    std::uint32_t classification = 0;
    if (!decoder.u32(magic) || magic != observation_magic ||
        !decoder.u32(classification) ||
        classification > static_cast<std::uint32_t>(Classification::InfrastructureSkip)) {
        return std::nullopt;
    }

    // The promoted byte is intentionally decoded outside Decoder because it is the only
    // single-byte field; reconstruct a cursor-friendly body for the remaining fields.
    if (body.size() < 9) return std::nullopt;
    const bool promoted = body[8] != 0;
    if (body[8] > 1) return std::nullopt;
    std::vector<std::uint8_t> tail(body.begin() + 9, body.end());
    Decoder tail_decoder(tail);

    Observation observation;
    observation.classification = static_cast<Classification>(classification);
    observation.promoted = promoted;
    if (!tail_decoder.string(observation.detail)) return std::nullopt;
    std::uint32_t payload_count = 0;
    if (!tail_decoder.u32(payload_count) || payload_count > 100000) return std::nullopt;
    observation.received_payloads.reserve(payload_count);
    for (std::uint32_t index = 0; index < payload_count; ++index) {
        std::uint32_t element_count = 0;
        if (!tail_decoder.u32(element_count) || element_count > max_pipe_message / sizeof(int)) {
            return std::nullopt;
        }
        std::vector<int> payload;
        payload.reserve(element_count);
        for (std::uint32_t element = 0; element < element_count; ++element) {
            std::uint32_t encoded = 0;
            if (!tail_decoder.u32(encoded)) return std::nullopt;
            payload.push_back(static_cast<std::int32_t>(encoded));
        }
        observation.received_payloads.push_back(std::move(payload));
    }
    if (!tail_decoder.done()) return std::nullopt;
    return observation;
}

void drain_pipe(int fd, std::vector<std::uint8_t>& bytes) {
    std::uint8_t buffer[4096];
    while (bytes.size() <= max_pipe_message + 4) {
        const auto count = read(fd, buffer, sizeof(buffer));
        if (count > 0) {
            bytes.insert(bytes.end(), buffer, buffer + count);
        } else if (count < 0 && errno == EINTR) {
            continue;
        } else {
            break;
        }
    }
}

std::string payload_text(const std::vector<int>& payload) {
    std::ostringstream output;
    output << '[';
    for (std::size_t index = 0; index < payload.size(); ++index) {
        if (index != 0) output << ',';
        output << payload[index];
    }
    output << ']';
    return output.str();
}

std::vector<std::string> receive_tags(const Scenario& scenario) {
    std::vector<std::string> tags;
    for (FMI::Utils::peer_num rank = 0; rank < scenario.world; ++rank) {
        for (const auto& phase : scenario.phases) {
            for (const auto& action : phase.rank_actions[rank]) {
                if (action.kind == ActionKind::Receive) tags.push_back(action.tag);
            }
        }
    }
    return tags;
}

Observation compare_with_baseline(const Scenario& scenario, const Observation& baseline,
                                  Observation migrated) {
    migrated.baseline_valid = baseline.classification == Classification::Preserved;
    if (!migrated.baseline_valid) {
        migrated.classification = Classification::SetupError;
        migrated.detail = "invalid no-migration baseline";
        return migrated;
    }
    if (!migrated.promoted && migrated.classification == Classification::Preserved) {
        migrated.classification = Classification::SetupError;
        migrated.detail = "migrated execution completed without epoch promotion";
        return migrated;
    }
    if (migrated.classification != Classification::Preserved ||
        migrated.received_payloads == baseline.received_payloads) {
        return migrated;
    }

    migrated.classification = scenario.expected_current_failure;
    const auto tags = receive_tags(scenario);
    const auto comparable = std::min(baseline.received_payloads.size(),
                                     migrated.received_payloads.size());
    for (std::size_t index = 0; index < comparable; ++index) {
        if (baseline.received_payloads[index] != migrated.received_payloads[index]) {
            migrated.detail = "receive " +
                    (index < tags.size() ? tags[index] : std::to_string(index)) +
                    " baseline=" + payload_text(baseline.received_payloads[index]) +
                    " migrated=" + payload_text(migrated.received_payloads[index]);
            return migrated;
        }
    }
    migrated.detail = "receive history length changed from " +
                      std::to_string(baseline.received_payloads.size()) + " to " +
                      std::to_string(migrated.received_payloads.size());
    return migrated;
}

} // namespace

Observation run_program_case(const Scenario& scenario, bool migrate) {
    return run_program_case_named(scenario, migrate, unique_comm_name(scenario, migrate));
}

Observation run_isolated(const Scenario& scenario, bool migrate) {
    Observation result;
    int pipe_fds[2] = {-1, -1};
    if (pipe(pipe_fds) != 0) {
        result.detail = std::string("pipe: ") + std::strerror(errno);
        return result;
    }

    const std::string comm_name = unique_comm_name(scenario, migrate);
    const auto started = Clock::now();
    const pid_t child = fork();
    if (child < 0) {
        result.detail = std::string("fork: ") + std::strerror(errno);
        close(pipe_fds[0]);
        close(pipe_fds[1]);
        return result;
    }
    if (child == 0) {
        close(pipe_fds[0]);
        Observation child_result;
        try {
            child_result = run_case_named(scenario, migrate, comm_name);
        } catch (const std::exception& error) {
            child_result.detail = std::string("uncaught child exception: ") + error.what();
        } catch (const std::string& error) {
            child_result.detail = "uncaught child exception: " + error;
        } catch (...) {
            child_result.detail = "uncaught child exception";
        }
        const auto message = encode_observation(child_result);
        const bool written = message && write_all(pipe_fds[1], *message);
        close(pipe_fds[1]);
        std::_Exit(written ? 0 : 2);
    }

    close(pipe_fds[1]);
    const int old_flags = fcntl(pipe_fds[0], F_GETFL, 0);
    if (old_flags >= 0) fcntl(pipe_fds[0], F_SETFL, old_flags | O_NONBLOCK);

    std::vector<std::uint8_t> message;
    int status = 0;
    bool reaped = false;
    bool timed_out = false;
    bool wait_error = false;
    int wait_errno = 0;
    const auto deadline = started + scenario.deadline;
    while (!reaped) {
        drain_pipe(pipe_fds[0], message);
        const pid_t waited = waitpid(child, &status, WNOHANG);
        if (waited == child) {
            reaped = true;
            break;
        }
        if (waited < 0 && errno != EINTR) {
            wait_error = true;
            wait_errno = errno;
            break;
        }
        if (Clock::now() >= deadline) {
            timed_out = true;
            kill(child, SIGKILL);
            while (waitpid(child, &status, 0) < 0 && errno == EINTR) {}
            reaped = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }

    if (wait_error && !reaped) {
        kill(child, SIGKILL);
        while (waitpid(child, &status, 0) < 0 && errno == EINTR) {}
    }
    drain_pipe(pipe_fds[0], message);
    close(pipe_fds[0]);
    best_effort_cleanup(comm_name, scenario.world);

    if (timed_out) {
        result.classification = migrate ? Classification::OperationStuck
                                        : Classification::SetupError;
        result.detail = std::string(migrate ? "migrated" : "baseline") +
                        " child exceeded " + std::to_string(scenario.deadline.count()) +
                        "ms and was killed and reaped";
        return result;
    }
    if (wait_error) {
        result.detail = std::string("waitpid: ") + std::strerror(wait_errno);
        return result;
    }
    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        result.detail = WIFSIGNALED(status)
                ? "child terminated by signal " + std::to_string(WTERMSIG(status))
                : "child exited " + std::to_string(WEXITSTATUS(status));
        return result;
    }
    const auto decoded = decode_observation(message);
    if (!decoded) {
        result.detail = "truncated or invalid observation pipe message";
        return result;
    }
    result = *decoded;
    result.baseline_valid = !migrate && result.classification == Classification::Preserved;
    return result;
}

namespace {

struct Options {
    bool list = false;
    bool explore = false;
    std::optional<std::string> case_id;
    std::optional<Backend> backend;
};

void usage(std::ostream& output) {
    output << "usage: fmi_migration_counterexamples [--list] [--case <id>] "
              "[--backend redis|direct] [--explore]\n";
}

bool parse_backend(const std::string& value, Backend& backend) {
    if (value == "redis") {
        backend = Backend::Redis;
        return true;
    }
    if (value == "direct") {
        backend = Backend::Direct;
        return true;
    }
    return false;
}

std::optional<Options> parse_options(int argc, char* argv[]) {
    Options options;
    for (int index = 1; index < argc; ++index) {
        const std::string argument = argv[index];
        if (argument == "--list") {
            if (options.list) {
                std::cerr << "--list may only be specified once\n";
                return std::nullopt;
            }
            options.list = true;
        } else if (argument == "--explore") {
            if (options.explore) {
                std::cerr << "--explore may only be specified once\n";
                return std::nullopt;
            }
            options.explore = true;
        } else if (argument == "--case") {
            if (options.case_id || ++index == argc) {
                std::cerr << "--case requires one identifier and may only be specified once\n";
                return std::nullopt;
            }
            if (std::string(argv[index]).rfind("--", 0) == 0) {
                std::cerr << "--case requires one identifier and may only be specified once\n";
                return std::nullopt;
            }
            options.case_id = argv[index];
        } else if (argument == "--backend") {
            if (options.backend || ++index == argc) {
                std::cerr << "--backend requires redis or direct and may only be specified once\n";
                return std::nullopt;
            }
            if (std::string(argv[index]).rfind("--", 0) == 0) {
                std::cerr << "--backend requires redis or direct and may only be specified once\n";
                return std::nullopt;
            }
            Backend backend;
            if (!parse_backend(argv[index], backend)) {
                std::cerr << "unknown backend: " << argv[index] << '\n';
                return std::nullopt;
            }
            options.backend = backend;
        } else {
            std::cerr << "unknown option: " << argument << '\n';
            return std::nullopt;
        }
    }

    if (options.explore && (options.list || options.case_id || options.backend)) {
        std::cerr << "--explore cannot be combined with scenario selection options\n";
        return std::nullopt;
    }
    return options;
}

std::vector<const Scenario*> select_scenarios(const Options& options) {
    std::vector<const Scenario*> selected;
    if (options.case_id) {
        const auto* scenario = find_scenario(*options.case_id);
        if (scenario == nullptr) {
            return selected;
        }
        if (!options.backend || scenario->backend == *options.backend) {
            selected.push_back(scenario);
        }
        return selected;
    }
    for (const auto& scenario : scenarios()) {
        if (!options.backend || scenario.backend == *options.backend) {
            selected.push_back(&scenario);
        }
    }
    return selected;
}

int aggregate_exit(const std::vector<Classification>& classifications) {
    bool semantic_counterexample = false;
    for (const auto classification : classifications) {
        if (classification == Classification::SetupError) {
            return 1;
        }
        if (classification != Classification::Preserved &&
            classification != Classification::InfrastructureSkip) {
            semantic_counterexample = true;
        }
    }
    return semantic_counterexample ? 2 : 0;
}

int run_selected(const std::vector<const Scenario*>& selected) {
    std::vector<Classification> classifications;
    classifications.reserve(selected.size());
    for (const auto* scenario : selected) {
        const Observation baseline = run_isolated(*scenario, false);
        std::cout << scenario->id << ": baseline=" << to_string(baseline.classification)
                  << std::flush;
        if (baseline.classification == Classification::InfrastructureSkip) {
            std::cout << " migrated=" << to_string(Classification::InfrastructureSkip)
                      << " detail=" << baseline.detail << '\n';
            classifications.push_back(Classification::InfrastructureSkip);
            continue;
        }
        if (baseline.classification != Classification::Preserved) {
            std::cout << " migrated=" << to_string(Classification::SetupError)
                      << " detail=" << baseline.detail << '\n';
            classifications.push_back(Classification::SetupError);
            continue;
        }

        Observation migrated = compare_with_baseline(*scenario, baseline,
                                                     run_isolated(*scenario, true));
        std::cout << " migrated=" << to_string(migrated.classification)
                  << " promoted=" << (migrated.promoted ? "yes" : "no");
        if (!migrated.detail.empty()) std::cout << " detail=" << migrated.detail;
        std::cout << '\n';
        classifications.push_back(migrated.classification);
    }
    return aggregate_exit(classifications);
}

int run_explorer() {
    const auto counterexamples = explore_bounded_model();
    if (counterexamples.empty()) {
        std::cerr << "explore: setup_error: no bounded counterexamples found\n";
        return aggregate_exit({Classification::SetupError});
    }

    std::vector<Classification> classifications;
    classifications.reserve(counterexamples.size());
    for (std::size_t index = 0; index < counterexamples.size(); ++index) {
        const auto& counterexample = counterexamples[index];
        std::cout << "counterexample " << (index + 1) << '\n'
                  << "  classification: " << to_string(counterexample.classification) << '\n'
                  << "  backend: " << to_string(counterexample.backend) << '\n'
                  << "  matching corpus ID: " << counterexample.matching_scenario_id << '\n'
                  << counterexample.normalized_trace;
        if (index + 1 != counterexamples.size()) std::cout << '\n';
        classifications.push_back(counterexample.classification);
    }
    return aggregate_exit(classifications);
}

} // namespace
} // namespace FMI::Tests::MigrationCounterexamples

int main(int argc, char* argv[]) {
    using namespace FMI::Tests::MigrationCounterexamples;

    const auto options = parse_options(argc, argv);
    if (!options) {
        usage(std::cerr);
        return 1;
    }
    if (!catalog_error().empty()) {
        std::cerr << "scenario catalog setup_error: " << catalog_error() << '\n';
        return 1;
    }
    if (options->explore) {
        return run_explorer();
    }

    const auto selected = select_scenarios(*options);
    if (options->case_id && selected.empty()) {
        if (find_scenario(*options->case_id) == nullptr) {
            std::cerr << "unknown scenario: " << *options->case_id << '\n';
        } else {
            std::cerr << "scenario is not available for the selected backend: "
                      << *options->case_id << '\n';
        }
        return 1;
    }

    if (options->list) {
        for (const auto* scenario : selected) {
            std::cout << scenario->id << '\t' << to_string(scenario->backend) << '\t'
                      << scenario->property << '\n';
        }
        return 0;
    }
    return run_selected(selected);
}
