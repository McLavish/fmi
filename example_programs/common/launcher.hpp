#pragma once

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <ctime>
#include <iostream>
#include <map>
#include <stdexcept>
#include <string>
#include <vector>

#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include <Communicator.h>

/*!
 * Command line and process plumbing shared by the FMI example programs.
 *
 * Every example is an ordinary FMI program: main() builds an FMI::Communicator and runs its
 * algorithm for exactly one rank. This header only supplies what such a program needs around
 * that: flag parsing (parse_cli), a built-in local launcher that re-execs the same binary once
 * per rank (spawn_local) and the teardown handshake every example ends with (teardown_sync).
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

    //! Parsed command line of one example process.
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

        inline long &teardown_grace_storage() {
            static long value = DEFAULT_TEARDOWN_GRACE_MS;
            return value;
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
               << "  --ranks N          Number of ranks to launch (default " << DEFAULT_RANKS << ", max " << MAX_RANKS
               << ")\n"
               << "  --rank I           Run only rank I in this process, no launcher (cross-machine mode)\n"
               << "  --config PATH      FMI JSON config (default " << DEFAULT_CONFIG_PATH << ")\n"
               << "  --comm-name NAME   FMI communicator name (default " << example_name << "-<hex>)\n"
               << "  --timeout SECONDS  Wall clock limit for the rank processes (default " << DEFAULT_TIMEOUT_SECONDS
               << ")\n"
               << "  --memory MIB       faas_memory hint for the FMI cost model (default " << DEFAULT_MEMORY_MIB
               << ")\n"
               << "  --teardown-grace MS  Hold channels open this long after the final barrier\n"
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
            bool rank_given = false;
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
                    rank_given = true;
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
            // Both bounds matter: opts.rank defaults to -1 to mean "not given" (Options::single_rank()
            // tests rank >= 0), so an explicitly supplied negative value would otherwise silently
            // select the launcher instead of failing as a usage error. rank_given keeps the unset
            // default working while rejecting every explicit out-of-range value, -1 included.
            if (rank_given && (opts.rank < 0 || opts.rank >= opts.ranks)) {
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

        struct RankResult {
            bool started = false;
            bool exited = false;
            bool timed_out = false;
            int exit_code = -1;
            int signal = 0;
        };

    } // namespace detail

    /*!
     * Parses the command line into opts and publishes the values the example needs at
     * Communicator construction time (config_path(), comm_name(), faas_memory()).
     *
     * @return true when the example should continue. On --help usage is printed to stdout and
     *         exit_code is 0; on a usage error the message and usage go to stderr and exit_code
     *         is 2. In both cases false is returned and main should return exit_code.
     */
    inline bool parse_cli(int argc, char **argv, const std::string &example_name, const Flags &declared, Options &opts,
                          int &exit_code) {
        exit_code = 0;
        try {
            if (!detail::parse_args(argc, argv, example_name, declared, opts)) {
                return false;
            }
        } catch (const std::exception &e) {
            std::cerr << "error: " << e.what() << "\n" << std::endl;
            detail::print_usage(std::cerr, example_name, declared);
            exit_code = 2;
            return false;
        }

        detail::config_path_storage() = opts.config_path;
        detail::comm_name_storage() = opts.comm_name;
        detail::memory_storage() = opts.memory_mib;
        detail::teardown_grace_storage() = opts.teardown_grace_ms;
        return true;
    }

    //! Overload for examples without extra flags.
    inline bool parse_cli(int argc, char **argv, const std::string &example_name, Options &opts, int &exit_code) {
        return parse_cli(argc, argv, example_name, Flags(), opts, exit_code);
    }

    /*!
     * Local launcher: runs opts.ranks copies of this very binary, one per rank, and waits for
     * them. Called from main() only when --rank was not given.
     *
     * Each child gets the original argv verbatim plus "--rank I --comm-name <name>" appended.
     * Parsing is last-wins, so the appended values are authoritative; --rank cannot already be
     * present here (its presence is what routes main() past this call), and a user supplied
     * --comm-name round-trips unchanged because opts.comm_name is what was parsed from it.
     *
     * The presence of --rank in the child command line is also the only fork-bomb guard: a child
     * always takes the single-rank path in main() and can therefore never launch further ranks.
     *
     * @return 0 when every rank exited 0, 1 otherwise.
     */
    inline int spawn_local(int argc, char **argv, const Options &opts) {
        const int ranks = opts.ranks;
        std::vector<detail::RankResult> results(static_cast<size_t>(ranks));
        std::vector<pid_t> pids(static_cast<size_t>(ranks), -1);

        std::cout << "Running " << opts.example_name << " with " << ranks << " ranks, config " << opts.config_path
                  << ", comm_name " << opts.comm_name << std::endl;

        // Computed before the first fork so every rank is bounded by the same absolute deadline.
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(opts.timeout_seconds);

        for (int i = 0; i < ranks; i++) {
            std::vector<std::string> appended = {"--rank", std::to_string(i), "--comm-name", opts.comm_name};
            std::vector<char *> child_argv;
            for (int a = 0; a < argc; a++) {
                child_argv.push_back(argv[a]);
            }
            for (std::string &s : appended) {
                child_argv.push_back(const_cast<char *>(s.c_str()));
            }
            child_argv.push_back(nullptr);

            std::cout.flush();
            std::cerr.flush();
            std::fflush(nullptr);

            pid_t pid = ::fork();
            if (pid == 0) {
                ::execv("/proc/self/exe", child_argv.data());
                std::perror("execv");
                ::_exit(127);
            }
            if (pid < 0) {
                std::cout << "rank " << i << ": NOT STARTED (fork failed)" << std::endl;
                continue;
            }
            pids[static_cast<size_t>(i)] = pid;
            results[static_cast<size_t>(i)].started = true;
        }

        size_t pending = 0;
        for (const detail::RankResult &r : results) {
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
                detail::sleep_ms(20);
            }
        }

        bool ok = true;
        for (size_t i = 0; i < results.size(); i++) {
            const detail::RankResult &r = results[i];
            ok &= r.started && !r.timed_out && r.signal == 0 && r.exit_code == 0;
            if (!r.started) {
                continue; // already reported above
            }
            std::cout << "rank " << i << ": ";
            if (r.timed_out) {
                std::cout << "TIMEOUT (killed)";
            } else if (r.signal != 0) {
                std::cout << "signal=" << r.signal;
            } else {
                std::cout << "exit=" << r.exit_code;
            }
            std::cout << std::endl;
        }
        std::cout << "EXAMPLE " << opts.example_name << ": " << (ok ? "PASS" : "FAIL") << std::endl;
        return ok ? 0 : 1;
    }

    /*!
     * Last statements of every example, executed while the Communicator is still alive: an FMI
     * barrier followed by a short grace period.
     *
     * WHY: FMI::Comm::ClientServer::finalize() (src/comm/ClientServer.cpp:67) runs from
     * ~Communicator and deletes every object this peer ever uploaded. Over Redis/S3 a recv is a
     * plain GET that does not consume, so a message has to stay in the store until the receiver
     * has polled it -- a rank whose last operation is a send would otherwise delete that message
     * microseconds after writing it, while the receiver polls at millisecond granularity and then
     * fails with "Timeout was reached". The barrier orders that: nobody starts tearing down before
     * every peer has finished its own operations.
     *
     * The barrier alone is not quite enough over ClientServer: its implementation
     * (src/comm/ClientServer.cpp:44) uploads a per-peer marker object and polls until it sees
     * num_peers of them, so the first rank to observe the full count returns and deletes its own
     * marker in finalize() while slower peers are still counting markers. The grace period covers
     * exactly that window. Over Direct the barrier is a real rendezvous and the grace is merely
     * harmless.
     *
     * The barrier is best effort: a failure here is reported but must not fail a rank whose actual
     * work already succeeded (this runs after validation).
     */
    inline void teardown_sync(FMI::Communicator &comm, const Options &opts) {
        try {
            comm.barrier();
        } catch (const std::exception &e) {
            std::cout << "note: teardown barrier failed: " << e.what() << std::endl;
        } catch (const std::string &s) {
            // TCPunch's error_exit (extern/TCPunch/common/utils.h) throws a bare std::string.
            std::cout << "note: teardown barrier failed: " << s << std::endl;
        } catch (...) {
            std::cout << "note: teardown barrier failed: unknown exception" << std::endl;
        }
        if (opts.teardown_grace_ms > 0) {
            detail::sleep_ms(opts.teardown_grace_ms);
        }
    }

} // namespace fmi_examples
