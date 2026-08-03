#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <functional>
#include <iostream>
#include <map>
#include <new>
#include <stdexcept>
#include <string>
#include <vector>

#include <signal.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <unistd.h>

/*!
 * Local driver for the FMI example programs, modelled on GapRunner's rFaaS driver:
 * per-example hooks (get_context / free_context / initialize_input / check_output / fn),
 * inputs initialized centrally, one process per rank, outputs validated centrally in the
 * parent (some examples share mutable context across ranks during validation).
 */
namespace fmi_examples {

    constexpr int MAX_RANKS = 64;
    constexpr int DEFAULT_RANKS = 4;
    constexpr int DEFAULT_TIMEOUT_SECONDS = 180;
    constexpr unsigned int DEFAULT_MEMORY_MIB = 128;
    constexpr long DEFAULT_TEARDOWN_GRACE_MS = 1000;
    constexpr char DEFAULT_CONFIG_PATH[] = "example_programs/config/fmi_examples.json";

    //! Example-specific command line flags, declared by the example main before parsing.
    class Flags {
    public:
        enum class Kind { Int, Double, String };

        void add_int(const std::string &name, long long default_value, const std::string &help = "") {
            add(name, Kind::Int, help).int_value = default_value;
        }

        void add_double(const std::string &name, double default_value, const std::string &help = "") {
            add(name, Kind::Double, help).double_value = default_value;
        }

        void add_string(const std::string &name, const std::string &default_value, const std::string &help = "") {
            add(name, Kind::String, help).string_value = default_value;
        }

        long long get_long(const std::string &name) const {
            return find(name, Kind::Int).int_value;
        }

        int get_int(const std::string &name) const {
            long long v = get_long(name);
            if (v < INT32_MIN || v > INT32_MAX) {
                throw std::runtime_error("value of --" + normalize(name) + " does not fit into an int");
            }
            return static_cast<int>(v);
        }

        double get_double(const std::string &name) const {
            const Entry &e = find_any(name);
            if (e.kind == Kind::Int) {
                return static_cast<double>(e.int_value);
            }
            if (e.kind != Kind::Double) {
                throw std::runtime_error("flag --" + normalize(name) + " is not numeric");
            }
            return e.double_value;
        }

        const std::string &get_string(const std::string &name) const {
            return find(name, Kind::String).string_value;
        }

        bool has(const std::string &name) const {
            return index.find(normalize(name)) != index.end();
        }

        //! Strips any leading dashes, so "--foo" and "foo" name the same flag.
        static std::string normalize(const std::string &name) {
            size_t pos = name.find_first_not_of('-');
            return pos == std::string::npos ? std::string() : name.substr(pos);
        }

        struct Entry {
            std::string name;
            Kind kind = Kind::Int;
            std::string help;
            long long int_value = 0;
            double double_value = 0.0;
            std::string string_value;
        };

        const std::vector<Entry> &entries() const {
            return order;
        }

        //! Assigns a raw command line token to a declared flag; throws if it does not parse.
        void set_from_string(const std::string &name, const std::string &value) {
            Entry &e = mutable_find(name);
            try {
                size_t consumed = 0;
                if (e.kind == Kind::Int) {
                    e.int_value = std::stoll(value, &consumed);
                } else if (e.kind == Kind::Double) {
                    e.double_value = std::stod(value, &consumed);
                } else {
                    e.string_value = value;
                    consumed = value.size();
                }
                if (consumed != value.size()) {
                    throw std::invalid_argument("trailing characters");
                }
            } catch (const std::exception &) {
                throw std::runtime_error("invalid value for --" + normalize(name) + ": '" + value + "'");
            }
        }

        std::string default_text(const Entry &e) const {
            if (e.kind == Kind::Int) {
                return std::to_string(e.int_value);
            }
            if (e.kind == Kind::Double) {
                return std::to_string(e.double_value);
            }
            return "'" + e.string_value + "'";
        }

    private:
        Entry &add(const std::string &name, Kind kind, const std::string &help) {
            std::string key = normalize(name);
            if (key.empty()) {
                throw std::runtime_error("flag name must not be empty");
            }
            if (index.find(key) != index.end()) {
                throw std::runtime_error("flag --" + key + " declared twice");
            }
            Entry e;
            e.name = key;
            e.kind = kind;
            e.help = help;
            order.push_back(e);
            index[key] = order.size() - 1;
            return order.back();
        }

        const Entry &find_any(const std::string &name) const {
            std::string key = normalize(name);
            auto it = index.find(key);
            if (it == index.end()) {
                throw std::runtime_error("unknown flag --" + key);
            }
            return order[it->second];
        }

        const Entry &find(const std::string &name, Kind kind) const {
            const Entry &e = find_any(name);
            if (e.kind != kind) {
                throw std::runtime_error("flag --" + normalize(name) + " read with the wrong type");
            }
            return e;
        }

        Entry &mutable_find(const std::string &name) {
            return const_cast<Entry &>(find_any(name));
        }

        std::vector<Entry> order;
        std::map<std::string, size_t> index;
    };

    //! Parsed command line, handed to the example's spec builder.
    struct Options {
        std::string example_name;
        int ranks = DEFAULT_RANKS;
        int rank = -1;
        std::string config_path = DEFAULT_CONFIG_PATH;
        std::string comm_name;
        int timeout_seconds = DEFAULT_TIMEOUT_SECONDS;
        unsigned int memory_mib = DEFAULT_MEMORY_MIB;
        long teardown_grace_ms = DEFAULT_TEARDOWN_GRACE_MS;
        Flags flags;

        bool single_rank() const {
            return rank >= 0;
        }
    };

    //! Per-example hooks, mirroring the GapRunner driver contract.
    struct ExampleSpec {
        std::string name;
        size_t input_size = 0;
        size_t output_size = 0;
        std::function<void *()> get_context = [] { return nullptr; };
        std::function<void(void *)> free_context = [](void *) {};
        std::function<void(int, int, void *, char *)> initialize_input = [](int, int, void *, char *) {};
        std::function<bool(int, int, void *, char *)> check_output = [](int, int, void *, char *) { return true; };
        std::function<uint32_t(void *, uint32_t, void *)> fn;
    };

    namespace detail {

        inline std::string &config_path_storage() {
            static std::string value = DEFAULT_CONFIG_PATH;
            return value;
        }

        inline std::string &comm_name_storage() {
            static std::string value;
            return value;
        }

        inline unsigned int &memory_storage() {
            static unsigned int value = DEFAULT_MEMORY_MIB;
            return value;
        }

        inline bool &in_child_storage() {
            static bool value = false;
            return value;
        }

        /*!
         * Cross-process barrier state, living in MAP_SHARED memory allocated before the fork.
         * See fmi_examples::rank_barrier() for why this exists.
         */
        struct RankSync {
            std::atomic<unsigned int> arrived;
            std::atomic<unsigned int> generation;
            std::atomic<unsigned int> ranks;
            std::atomic<long long> deadline_ns;
        };

        // These atomics live in memory shared between separate processes, so they must be
        // lock-free: a lock-based fallback would use a per-process lock table and silently fail
        // to synchronize anything.
        static_assert(ATOMIC_INT_LOCK_FREE == 2, "rank_barrier needs lock-free atomic<unsigned int>");
        static_assert(ATOMIC_LLONG_LOCK_FREE == 2, "rank_barrier needs lock-free atomic<long long>");

        inline RankSync *&sync_storage() {
            static RankSync *value = nullptr;
            return value;
        }

        inline long &teardown_grace_storage() {
            static long value = DEFAULT_TEARDOWN_GRACE_MS;
            return value;
        }

        inline long long steady_now_ns() {
            return std::chrono::duration_cast<std::chrono::nanoseconds>(
                       std::chrono::steady_clock::now().time_since_epoch())
                .count();
        }

        inline void sleep_us(long micros) {
            struct timespec ts;
            ts.tv_sec = micros / 1000000L;
            ts.tv_nsec = (micros % 1000000L) * 1000L;
            ::nanosleep(&ts, nullptr);
        }

        inline void sleep_ms(long millis) {
            sleep_us(millis * 1000L);
        }

    } // namespace detail

    /*!
     * Out-of-band barrier across the forked ranks. NOT an FMI collective: it uses shared memory
     * and never touches a backend.
     *
     * WHY THIS IS NEEDED: FMI::Comm::ClientServer::finalize() (src/comm/ClientServer.cpp), which
     * runs from ~Communicator, deletes every object this peer ever uploaded. The Redis/S3
     * download path is a plain GET that does not consume, so a message must stay in the store
     * until the receiver has polled it. A rank whose last FMI operation is a send therefore
     * deletes that message a few microseconds after writing it, while the receiver polls at
     * millisecond granularity -- the receiver then fails with "Timeout was reached". For `ring`
     * this is deterministic (the last rank's final act is a send to rank 0); for the collectives
     * it is a race that shows up as intermittent failures.
     *
     * Every example that builds a Communicator therefore calls rank_barrier() as its last
     * statement, while the Communicator is still alive, so that no peer tears down its channel
     * until all peers have consumed what they need.
     *
     * The wait is bounded by the harness --timeout deadline: a crashed or hung peer can never
     * make this block past the point where the harness SIGKILLs the run.
     *
     * In single-rank (--rank I) mode the peers are other processes or machines, so there is no
     * shared memory to synchronize through and no barrier is possible. There we instead hold the
     * channels open for --teardown-grace milliseconds, which is enough for peers that are already
     * polling (they poll at millisecond granularity) to pick up a final message before it is
     * deleted. Set --teardown-grace 0 to opt out.
     */
    inline void rank_barrier() {
        detail::RankSync *s = detail::sync_storage();
        if (s == nullptr) {
            const long grace = detail::teardown_grace_storage();
            if (grace > 0) {
                detail::sleep_ms(grace);
            }
            return;
        }
        const unsigned int ranks = s->ranks.load(std::memory_order_relaxed);
        if (ranks <= 1) {
            return;
        }

        const unsigned int gen = s->generation.load(std::memory_order_acquire);
        if (s->arrived.fetch_add(1, std::memory_order_acq_rel) + 1 == ranks) {
            s->arrived.store(0, std::memory_order_relaxed);
            s->generation.fetch_add(1, std::memory_order_release);
            return;
        }

        const long long deadline = s->deadline_ns.load(std::memory_order_relaxed);
        while (s->generation.load(std::memory_order_acquire) == gen) {
            if (detail::steady_now_ns() >= deadline) {
                return; // best effort only; the harness is about to kill this run anyway
            }
            detail::sleep_us(200);
        }
    }

    //! Path of the FMI JSON config to hand to FMI::Communicator (from --config).
    inline const std::string &config_path() {
        return detail::config_path_storage();
    }

    //! Communicator name every rank of this run agrees on (from --comm-name).
    inline const std::string &comm_name() {
        return detail::comm_name_storage();
    }

    //! faas_memory hint in MiB (from --memory); only relevant for the FMI cost model.
    inline unsigned int faas_memory() {
        return detail::memory_storage();
    }

    namespace detail {

        inline std::string unique_suffix() {
            uint64_t pid = static_cast<uint64_t>(::getpid());
            uint64_t now = static_cast<uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count());
            uint64_t mixed = (pid << 32) ^ (now & 0xffffffffull);
            char buf[32];
            std::snprintf(buf, sizeof(buf), "%08x", static_cast<unsigned int>(mixed & 0xffffffffull));
            return std::string(buf);
        }

        inline void print_usage(std::ostream &os, const std::string &example_name, const Flags &flags) {
            os << "Usage: " << example_name << " [options]\n\n"
               << "Common options:\n"
               << "  --ranks N          Number of ranks to fork (default " << DEFAULT_RANKS << ", max " << MAX_RANKS
               << ")\n"
               << "  --rank I           Run only rank I in this process, no fork (cross-machine mode)\n"
               << "  --config PATH      FMI JSON config (default " << DEFAULT_CONFIG_PATH << ")\n"
               << "  --comm-name NAME   FMI communicator name (default " << example_name << "-<hex>)\n"
               << "  --timeout SECONDS  Wall clock limit for the rank processes (default " << DEFAULT_TIMEOUT_SECONDS
               << ")\n"
               << "  --memory MIB       faas_memory hint for the FMI cost model (default " << DEFAULT_MEMORY_MIB
               << ")\n"
               << "  --teardown-grace MS  Hold channels open this long before teardown in --rank mode\n"
               << "                     (default " << DEFAULT_TEARDOWN_GRACE_MS << ", 0 disables)\n"
               << "  --help             Print this message\n";
            if (!flags.entries().empty()) {
                os << "\n" << example_name << " options:\n";
                for (const Flags::Entry &e : flags.entries()) {
                    os << "  --" << e.name << "  " << e.help << " (default " << flags.default_text(e) << ")\n";
                }
            }
            os << "\nFlag values may be given as '--flag value' or '--flag=value'.\n";
        }

        inline long long parse_integer(const std::string &flag, const std::string &value) {
            try {
                size_t consumed = 0;
                long long parsed = std::stoll(value, &consumed);
                if (consumed != value.size()) {
                    throw std::invalid_argument("trailing characters");
                }
                return parsed;
            } catch (const std::exception &) {
                throw std::runtime_error("invalid value for " + flag + ": '" + value + "'");
            }
        }

        inline void reject_reserved_flags(const Flags &declared) {
            static const char *reserved[] = {"ranks",  "rank",   "config", "comm-name",
                                             "timeout", "memory", "help",   "teardown-grace"};
            for (const Flags::Entry &e : declared.entries()) {
                for (const char *name : reserved) {
                    if (e.name == name) {
                        throw std::runtime_error("example flag --" + e.name + " collides with a common flag");
                    }
                }
            }
        }

        //! Returns false if the run should stop after printing usage (--help).
        inline bool parse_args(int argc, char **argv, const std::string &example_name, const Flags &declared,
                               Options &opts) {
            reject_reserved_flags(declared);
            opts.example_name = example_name;
            opts.flags = declared;

            bool comm_name_given = false;
            for (int i = 1; i < argc; i++) {
                std::string arg = argv[i];
                if (arg == "--help" || arg == "-h") {
                    print_usage(std::cout, example_name, declared);
                    return false;
                }
                if (arg.rfind("--", 0) != 0) {
                    throw std::runtime_error("unexpected argument '" + arg + "'");
                }

                std::string name = arg;
                std::string value;
                bool inline_value = false;
                size_t eq = arg.find('=');
                if (eq != std::string::npos) {
                    name = arg.substr(0, eq);
                    value = arg.substr(eq + 1);
                    inline_value = true;
                }

                auto take_value = [&]() -> std::string {
                    if (inline_value) {
                        return value;
                    }
                    if (i + 1 >= argc) {
                        throw std::runtime_error("missing value for " + name);
                    }
                    return std::string(argv[++i]);
                };

                if (name == "--ranks") {
                    opts.ranks = static_cast<int>(parse_integer(name, take_value()));
                } else if (name == "--rank") {
                    opts.rank = static_cast<int>(parse_integer(name, take_value()));
                } else if (name == "--config") {
                    opts.config_path = take_value();
                } else if (name == "--comm-name") {
                    opts.comm_name = take_value();
                    comm_name_given = true;
                } else if (name == "--timeout") {
                    opts.timeout_seconds = static_cast<int>(parse_integer(name, take_value()));
                } else if (name == "--memory") {
                    opts.memory_mib = static_cast<unsigned int>(parse_integer(name, take_value()));
                } else if (name == "--teardown-grace") {
                    opts.teardown_grace_ms = static_cast<long>(parse_integer(name, take_value()));
                } else if (declared.has(name)) {
                    opts.flags.set_from_string(name, take_value());
                } else {
                    throw std::runtime_error("unknown flag '" + name + "'");
                }
            }

            if (opts.ranks < 1) {
                throw std::runtime_error("--ranks must be at least 1");
            }
            if (opts.ranks > MAX_RANKS) {
                throw std::runtime_error("--ranks must not exceed " + std::to_string(MAX_RANKS));
            }
            if (opts.rank >= opts.ranks) {
                throw std::runtime_error("--rank must be in [0, " + std::to_string(opts.ranks) + ")");
            }
            if (opts.timeout_seconds < 1) {
                throw std::runtime_error("--timeout must be at least 1 second");
            }
            if (!comm_name_given) {
                opts.comm_name = example_name + "-" + unique_suffix();
            }
            return true;
        }

        inline size_t stride(size_t size) {
            constexpr size_t alignment = 64;
            size_t rounded = ((size + alignment - 1) / alignment) * alignment;
            return rounded == 0 ? alignment : rounded;
        }

        //! Shared, zero-filled memory so the parent sees what the forked ranks write.
        inline char *shared_alloc(size_t bytes) {
            void *p = ::mmap(nullptr, bytes, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);
            if (p == MAP_FAILED) {
                throw std::runtime_error("mmap of " + std::to_string(bytes) + " bytes failed");
            }
            return static_cast<char *>(p);
        }

        struct RankResult {
            bool started = false;
            bool exited = false;
            bool timed_out = false;
            int exit_code = -1;
            int signal = 0;
            bool check_passed = false;
        };

        inline bool run_check(const ExampleSpec &spec, int rank, int ranks, void *ctx, char *output) {
            try {
                return spec.check_output(rank, ranks, ctx, output);
            } catch (const std::exception &e) {
                std::cout << "Rank " << rank << " check_output threw: " << e.what() << std::endl;
            } catch (...) {
                std::cout << "Rank " << rank << " check_output threw an unknown exception" << std::endl;
            }
            return false;
        }

        inline void report(const ExampleSpec &spec, const std::vector<RankResult> &results, bool ok) {
            for (size_t i = 0; i < results.size(); i++) {
                const RankResult &r = results[i];
                std::cout << "rank " << i << ": ";
                if (!r.started) {
                    std::cout << "NOT STARTED";
                } else if (r.timed_out) {
                    std::cout << "TIMEOUT (killed)";
                } else if (r.signal != 0) {
                    std::cout << "signal=" << r.signal;
                } else {
                    std::cout << "exit=" << r.exit_code;
                }
                std::cout << " check=" << (r.check_passed ? "PASS" : "FAIL") << std::endl;
            }
            std::cout << "EXAMPLE " << spec.name << ": " << (ok ? "PASS" : "FAIL") << std::endl;
        }

        [[noreturn]] inline void run_child(const ExampleSpec &spec, int rank, char *input, char *output) {
            int status = 1;
            try {
                spec.fn(input, static_cast<uint32_t>(spec.input_size), output);
                status = 0;
            } catch (const std::exception &e) {
                std::cout << "Rank " << rank << " crashed: " << e.what() << std::endl;
            } catch (...) {
                std::cout << "Rank " << rank << " crashed: unknown exception" << std::endl;
            }
            std::cout.flush();
            std::cerr.flush();
            ::_exit(status);
        }

        inline int run_multi(const ExampleSpec &spec, const Options &opts) {
            const int ranks = opts.ranks;
            const size_t in_stride = stride(spec.input_size);
            const size_t out_stride = stride(spec.output_size);

            char *inputs = shared_alloc(in_stride * static_cast<size_t>(ranks));
            char *outputs = shared_alloc(out_stride * static_cast<size_t>(ranks));

            // Shared barrier state must exist (and be reachable through sync_storage) before the
            // fork, so every rank inherits the same mapping. See fmi_examples::rank_barrier().
            char *sync_mem = shared_alloc(sizeof(RankSync));
            RankSync *sync = new (sync_mem) RankSync();
            sync->arrived.store(0, std::memory_order_relaxed);
            sync->generation.store(0, std::memory_order_relaxed);
            sync->ranks.store(static_cast<unsigned int>(ranks), std::memory_order_relaxed);
            sync_storage() = sync;

            void *ctx = spec.get_context();
            for (int i = 0; i < ranks; i++) {
                spec.initialize_input(i, ranks, ctx, inputs + in_stride * static_cast<size_t>(i));
            }

            std::vector<RankResult> results(static_cast<size_t>(ranks));
            std::vector<pid_t> pids(static_cast<size_t>(ranks), -1);

            std::cout.flush();
            std::cerr.flush();
            std::fflush(nullptr);

            // Computed before the fork so the children inherit the same absolute deadline and can
            // bound rank_barrier() by it.
            const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(opts.timeout_seconds);
            sync->deadline_ns.store(
                std::chrono::duration_cast<std::chrono::nanoseconds>(deadline.time_since_epoch()).count(),
                std::memory_order_relaxed);

            for (int i = 0; i < ranks; i++) {
                pid_t pid = ::fork();
                if (pid == 0) {
                    in_child_storage() = true;
                    run_child(spec, i, inputs + in_stride * static_cast<size_t>(i),
                              outputs + out_stride * static_cast<size_t>(i));
                }
                if (pid < 0) {
                    std::cout << "Rank " << i << " could not be forked" << std::endl;
                    break;
                }
                pids[static_cast<size_t>(i)] = pid;
                results[static_cast<size_t>(i)].started = true;
            }

            size_t pending = 0;
            for (const RankResult &r : results) {
                pending += r.started ? 1 : 0;
            }

            while (pending > 0) {
                bool progressed = false;
                for (size_t i = 0; i < results.size(); i++) {
                    if (!results[i].started || results[i].exited) {
                        continue;
                    }
                    int status = 0;
                    pid_t r = ::waitpid(pids[i], &status, WNOHANG);
                    if (r == 0) {
                        continue;
                    }
                    results[i].exited = true;
                    pending--;
                    progressed = true;
                    if (r < 0) {
                        results[i].exit_code = -1;
                    } else if (WIFEXITED(status)) {
                        results[i].exit_code = WEXITSTATUS(status);
                    } else if (WIFSIGNALED(status)) {
                        results[i].signal = WTERMSIG(status);
                    }
                }
                if (pending == 0) {
                    break;
                }
                if (std::chrono::steady_clock::now() >= deadline) {
                    for (size_t i = 0; i < results.size(); i++) {
                        if (!results[i].started || results[i].exited) {
                            continue;
                        }
                        std::cout << "Rank " << i << " exceeded the " << opts.timeout_seconds
                                  << "s timeout, sending SIGKILL" << std::endl;
                        ::kill(pids[i], SIGKILL);
                        int status = 0;
                        ::waitpid(pids[i], &status, 0);
                        results[i].exited = true;
                        results[i].timed_out = true;
                        pending--;
                    }
                    break;
                }
                if (!progressed) {
                    sleep_ms(20);
                }
            }

            bool ok = true;
            for (int i = 0; i < ranks; i++) {
                RankResult &r = results[static_cast<size_t>(i)];
                r.check_passed = run_check(spec, i, ranks, ctx, outputs + out_stride * static_cast<size_t>(i));
                ok &= r.check_passed && r.started && !r.timed_out && r.signal == 0 && r.exit_code == 0;
            }

            report(spec, results, ok);

            spec.free_context(ctx);
            sync_storage() = nullptr;
            ::munmap(sync_mem, sizeof(RankSync));
            ::munmap(inputs, in_stride * static_cast<size_t>(ranks));
            ::munmap(outputs, out_stride * static_cast<size_t>(ranks));
            return ok ? 0 : 1;
        }

        inline int run_single(const ExampleSpec &spec, const Options &opts) {
            const int rank = opts.rank;
            std::vector<char> input(stride(spec.input_size), 0);
            std::vector<char> output(stride(spec.output_size), 0);

            void *ctx = spec.get_context();
            spec.initialize_input(rank, opts.ranks, ctx, input.data());

            RankResult r;
            r.started = true;
            r.exit_code = 1;
            try {
                spec.fn(input.data(), static_cast<uint32_t>(spec.input_size), output.data());
                r.exit_code = 0;
            } catch (const std::exception &e) {
                std::cout << "Rank " << rank << " crashed: " << e.what() << std::endl;
            } catch (...) {
                std::cout << "Rank " << rank << " crashed: unknown exception" << std::endl;
            }

            r.check_passed = run_check(spec, rank, opts.ranks, ctx, output.data());
            bool ok = r.exit_code == 0 && r.check_passed;

            std::cout << "rank " << rank << ": exit=" << r.exit_code << " check=" << (r.check_passed ? "PASS" : "FAIL")
                      << std::endl;
            std::cout << "EXAMPLE " << spec.name << ": " << (ok ? "PASS" : "FAIL") << std::endl;

            spec.free_context(ctx);
            return ok ? 0 : 1;
        }

    } // namespace detail

    using SpecBuilder = std::function<ExampleSpec(const Options &)>;

    /*!
     * Parses the command line, builds the example spec and runs it.
     * @return 0 on success, 1 when a rank failed or a check failed, 2 on a usage error.
     */
    inline int run(int argc, char **argv, const std::string &example_name, const Flags &flags,
                   const SpecBuilder &build) {
        if (detail::in_child_storage()) {
            std::cerr << "fmi_examples::run must not be called from a rank process" << std::endl;
            ::_exit(1);
        }

        Options opts;
        try {
            if (!detail::parse_args(argc, argv, example_name, flags, opts)) {
                return 0;
            }
        } catch (const std::exception &e) {
            std::cerr << "error: " << e.what() << "\n" << std::endl;
            detail::print_usage(std::cerr, example_name, flags);
            return 2;
        }

        detail::config_path_storage() = opts.config_path;
        detail::comm_name_storage() = opts.comm_name;
        detail::memory_storage() = opts.memory_mib;
        detail::teardown_grace_storage() = opts.teardown_grace_ms;

        try {
            ExampleSpec spec = build(opts);
            if (spec.name.empty()) {
                spec.name = example_name;
            }
            if (!spec.fn) {
                throw std::runtime_error("example spec has no function");
            }
            std::cout << "Running " << spec.name << " with " << opts.ranks << " ranks, config " << opts.config_path
                      << ", comm_name " << opts.comm_name << std::endl;
            return opts.single_rank() ? detail::run_single(spec, opts) : detail::run_multi(spec, opts);
        } catch (const std::exception &e) {
            std::cerr << "error: " << e.what() << std::endl;
            std::cout << "EXAMPLE " << example_name << ": FAIL" << std::endl;
            return 1;
        }
    }

    //! Overload for examples without extra flags.
    inline int run(int argc, char **argv, const std::string &example_name, const SpecBuilder &build) {
        return run(argc, argv, example_name, Flags(), build);
    }

} // namespace fmi_examples
