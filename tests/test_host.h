#ifndef FMI_TESTS_TEST_HOST_H
#define FMI_TESTS_TEST_HOST_H

#include <poll.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

//! A host manager for the tests: the other end of every rank's control link.
/*!
 * It does what the real host manager and the coordinator do together, as far as a rank can
 * tell: it keeps the rank table, answers `where`, passes on every newer row of a rank that was
 * asked for, tells a rank to leave, and collects what the rank reports when it is drained.
 * It also lets a test say things a correct control plane never would (a stale row, a leave for
 * the wrong generation), which is how the rules of rank state are tested.
 *
 * One thread, one poll loop, any number of jobs. Every rank is reachable at 127.0.0.1.
 */
class TestHost {
public:
    struct Drained {
        std::string move;
        std::uint64_t generation = 0;
        std::map<unsigned, std::uint64_t> sent;
        std::map<unsigned, std::uint64_t> received;
    };

    TestHost() {
        static std::atomic<int> serial{0};
        socket_path = "/tmp/fmi-test-host-" + std::to_string(::getpid()) + "-" +
                      std::to_string(serial.fetch_add(1)) + ".sock";
        ::unlink(socket_path.c_str());
        listener = ::socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
        struct sockaddr_un addr{};
        addr.sun_family = AF_UNIX;
        std::strncpy(addr.sun_path, socket_path.c_str(), sizeof(addr.sun_path) - 1);
        if (listener < 0 || ::bind(listener, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) != 0 ||
            ::listen(listener, 256) != 0 || ::pipe(wake) != 0) {
            throw std::runtime_error("TestHost: cannot listen on " + socket_path + ": " + strerror(errno));
        }
        owner = ::getpid();
        thread = std::make_unique<std::thread>(&TestHost::loop, this);
    }

    ~TestHost() {
        if (::getpid() != owner) {
            // A forked child holds a copy of this object and none of its threads. The thread
            // object must not be destroyed there: it still looks joinable.
            (void) thread.release();
            return;
        }
        stopping.store(true);
        poke();
        if (thread->joinable()) {
            thread->join();
        }
        for (auto& [fd, conn] : conns) {
            ::close(fd);
        }
        ::close(listener);
        ::close(wake[0]);
        ::close(wake[1]);
        ::unlink(socket_path.c_str());
    }

    [[nodiscard]] const std::string& path() const { return socket_path; }

    //! Mark @p rank as leaving, tell everyone who asked about it, and tell it to leave.
    //! With @p quietly the row is not written: the peers have only the FIN to go by.
    std::string leave(const std::string& job, unsigned rank, bool quietly = false) {
        std::lock_guard<std::mutex> lock(mu);
        Row& row = table[{job, rank}];
        const std::string move = "m" + std::to_string(++moves);
        row.moving = true;
        if (!quietly) {
            row.leaving = true;
            publish_locked(job, rank);
        }
        const int fd = fd_of_locked(job, rank);
        if (fd < 0) {
            throw std::runtime_error("TestHost: rank " + std::to_string(rank) + " is not connected");
        }
        write_locked(fd, "leave move=" + move + " generation=" + std::to_string(row.generation));
        return move;
    }

    //! Say an arbitrary line to one rank, as a faulty or late control plane would.
    void say_to(const std::string& job, unsigned rank, const std::string& line) {
        std::lock_guard<std::mutex> lock(mu);
        const int fd = fd_of_locked(job, rank);
        if (fd < 0) {
            throw std::runtime_error("TestHost: rank " + std::to_string(rank) + " is not connected");
        }
        write_locked(fd, line);
    }

    //! Wait until @p rank has reported `drained` for @p move.
    bool wait_drained(const std::string& job, unsigned rank, const std::string& move, long timeout_ms,
                      Drained* out = nullptr) {
        std::unique_lock<std::mutex> lock(mu);
        const bool seen = changed.wait_for(lock, std::chrono::milliseconds(timeout_ms), [&]() {
            const auto it = drained.find({job, rank});
            return it != drained.end() && it->second.move == move;
        });
        if (seen && out != nullptr) {
            *out = drained[{job, rank}];
        }
        return seen;
    }

    //! Wait until the table holds @p rank at @p generation, not leaving: the rank is (back) here.
    bool wait_here(const std::string& job, unsigned rank, std::uint64_t generation, long timeout_ms) {
        std::unique_lock<std::mutex> lock(mu);
        return changed.wait_for(lock, std::chrono::milliseconds(timeout_ms), [&]() {
            const auto it = table.find({job, rank});
            return it != table.end() && it->second.known && !it->second.leaving &&
                   it->second.generation == generation;
        });
    }

    //! Wait until @p rank has said `bye`, and return the generation it had then in @p at.
    bool wait_bye(const std::string& job, unsigned rank, long timeout_ms, std::uint64_t* at) {
        std::unique_lock<std::mutex> lock(mu);
        const bool seen = changed.wait_for(lock, std::chrono::milliseconds(timeout_ms),
                                           [&]() { return byes.count({job, rank}) > 0; });
        if (seen && at != nullptr) {
            *at = byes[{job, rank}];
        }
        return seen;
    }

    [[nodiscard]] std::uint64_t generation_of(const std::string& job, unsigned rank) {
        std::lock_guard<std::mutex> lock(mu);
        return table[{job, rank}].generation;
    }

    //! Lines this host could not make sense of; a test expects none.
    [[nodiscard]] int failures() {
        std::lock_guard<std::mutex> lock(mu);
        return failed;
    }

private:
    struct Row {
        bool known = false;
        std::uint64_t generation = 0;
        bool leaving = false;
        //! Told to leave and not back yet, whether or not the row says so (a quiet leave).
        bool moving = false;
        int port = 0;
        std::uint64_t nonce = 0;
    };

    struct Conn {
        std::string buffer;
        std::string job;
        unsigned rank = 0;
        bool registered = false;
        std::set<unsigned> asked;
    };

    static std::map<std::string, std::string> fields_of(const std::string& line, std::string& verb) {
        std::map<std::string, std::string> out;
        std::istringstream words(line);
        std::string word;
        bool first = true;
        while (words >> word) {
            if (first) {
                verb = word;
                first = false;
                continue;
            }
            const auto eq = word.find('=');
            if (eq != std::string::npos) {
                out[word.substr(0, eq)] = word.substr(eq + 1);
            }
        }
        return out;
    }

    void poke() {
        const char byte = 1;
        (void) !::write(wake[1], &byte, 1);
    }

    int fd_of_locked(const std::string& job, unsigned rank) {
        for (auto& [fd, conn] : conns) {
            if (conn.registered && conn.job == job && conn.rank == rank) {
                return fd;
            }
        }
        return -1;
    }

    static void write_locked(int fd, const std::string& line) {
        const std::string wire = line + "\n";
        std::size_t sent = 0;
        while (sent < wire.size()) {
            const ssize_t n = ::send(fd, wire.data() + sent, wire.size() - sent, MSG_NOSIGNAL);
            if (n <= 0) {
                if (n < 0 && errno == EINTR) {
                    continue;
                }
                return;   // the rank closed its link; it will not miss this line
            }
            sent += static_cast<std::size_t>(n);
        }
    }

    std::string row_line_locked(const std::string& job, unsigned rank) {
        const Row& row = table[{job, rank}];
        return "rank rank=" + std::to_string(rank) + " generation=" + std::to_string(row.generation) +
               " state=" + (row.leaving ? "leaving" : "here") + " addr=127.0.0.1:" +
               std::to_string(row.port) + " nonce=" + std::to_string(row.nonce);
    }

    //! Tell every rank of the job that asked about @p rank what its row is now.
    void publish_locked(const std::string& job, unsigned rank) {
        const std::string line = row_line_locked(job, rank);
        for (auto& [fd, conn] : conns) {
            if (conn.registered && conn.job == job && conn.asked.count(rank) > 0) {
                write_locked(fd, line);
            }
        }
    }

    void handle_locked(int fd, Conn& conn, const std::string& line) {
        std::string verb;
        auto fields = fields_of(line, verb);
        try {
            if (verb == "hello") {
                conn.job = fields.at("job");
                conn.rank = static_cast<unsigned>(std::stoul(fields.at("rank")));
                conn.registered = true;
                Row& row = table[{conn.job, conn.rank}];
                const std::uint64_t generation = std::stoull(fields.at("generation"));
                if (!row.known || generation >= row.generation) {
                    row.known = true;
                    row.generation = generation;
                    row.leaving = false;
                    row.moving = false;
                    row.port = std::stoi(fields.at("port"));
                    row.nonce = std::stoull(fields.at("nonce"));
                }
                write_locked(fd, "welcome");
                publish_locked(conn.job, conn.rank);
            } else if (verb == "where") {
                const auto rank = static_cast<unsigned>(std::stoul(fields.at("rank")));
                conn.asked.insert(rank);
                const auto it = table.find({conn.job, rank});
                if (it != table.end() && it->second.known) {
                    write_locked(fd, row_line_locked(conn.job, rank));
                }
            } else if (verb == "sync") {
                write_locked(fd, "synced token=" + fields.at("token"));
            } else if (verb == "drained") {
                Drained report;
                report.move = fields.at("move");
                report.generation = std::stoull(fields.at("generation"));
                for (const auto& [key, value] : fields) {
                    if (key.size() > 1 && (key[0] == 's' || key[0] == 'r') &&
                        key.find_first_not_of("0123456789", 1) == std::string::npos) {
                        const auto peer = static_cast<unsigned>(std::stoul(key.substr(1)));
                        (key[0] == 's' ? report.sent : report.received)[peer] = std::stoull(value);
                    }
                }
                drained[{conn.job, conn.rank}] = report;
            } else if (verb == "bye") {
                // The rank finalized: whoever asked about it is told, so that nobody waits for
                // it. The row itself is forgotten when the link closes.
                const auto it = table.find({conn.job, conn.rank});
                if (it != table.end()) {
                    byes[{conn.job, conn.rank}] = it->second.generation;
                    const std::string line = "rank rank=" + std::to_string(conn.rank) + " generation=" +
                                             std::to_string(it->second.generation) + " state=gone";
                    for (auto& [other_fd, other] : conns) {
                        if (other.registered && other.job == conn.job && other.asked.count(conn.rank) > 0) {
                            write_locked(other_fd, line);
                        }
                    }
                }
            } else if (verb == "failed") {
                failed++;
            } else {
                failed++;
            }
        } catch (const std::exception&) {
            failed++;
        }
    }

    void loop() {
        while (!stopping.load()) {
            std::vector<struct pollfd> fds;
            fds.push_back({wake[0], POLLIN, 0});
            fds.push_back({listener, POLLIN, 0});
            {
                std::lock_guard<std::mutex> lock(mu);
                for (auto& [fd, conn] : conns) {
                    fds.push_back({fd, POLLIN, 0});
                }
            }
            if (::poll(fds.data(), fds.size(), 1000) <= 0) {
                continue;
            }
            if (fds[0].revents != 0) {
                char sink[64];
                (void) !::read(wake[0], sink, sizeof(sink));
            }
            std::lock_guard<std::mutex> lock(mu);
            if (fds[1].revents != 0) {
                const int fd = ::accept4(listener, nullptr, nullptr, SOCK_CLOEXEC);
                if (fd >= 0) {
                    conns[fd] = Conn{};
                }
            }
            for (std::size_t i = 2; i < fds.size(); i++) {
                if (fds[i].revents == 0) {
                    continue;
                }
                const int fd = fds[i].fd;
                auto it = conns.find(fd);
                if (it == conns.end()) {
                    continue;
                }
                char chunk[4096];
                const ssize_t n = ::recv(fd, chunk, sizeof(chunk), MSG_DONTWAIT);
                if (n > 0) {
                    it->second.buffer.append(chunk, static_cast<std::size_t>(n));
                    std::size_t end = 0;
                    while ((end = it->second.buffer.find('\n')) != std::string::npos) {
                        const std::string line = it->second.buffer.substr(0, end);
                        it->second.buffer.erase(0, end + 1);
                        handle_locked(fd, it->second, line);
                    }
                } else if (n == 0 || (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR)) {
                    // A rank closes its link when it is drained, and then it comes back. A link
                    // that closes outside a move belongs to a rank that finalized or died: its
                    // row is forgotten, so that it is never handed out as an address again.
                    const Conn& gone = it->second;
                    if (gone.registered) {
                        const auto row = table.find({gone.job, gone.rank});
                        if (row != table.end() && !row->second.moving) {
                            table.erase(row);
                            drained.erase({gone.job, gone.rank});
                        }
                    }
                    ::close(fd);
                    conns.erase(it);
                }
            }
            changed.notify_all();
        }
    }

    std::string socket_path;
    int listener = -1;
    int wake[2] = {-1, -1};
    pid_t owner = 0;
    std::atomic<bool> stopping{false};
    std::unique_ptr<std::thread> thread;

    std::mutex mu;
    std::condition_variable changed;
    std::map<int, Conn> conns;
    std::map<std::pair<std::string, unsigned>, Row> table;
    std::map<std::pair<std::string, unsigned>, Drained> drained;
    std::map<std::pair<std::string, unsigned>, std::uint64_t> byes;   // the generation at the goodbye
    int moves = 0;
    int failed = 0;
};

#endif // FMI_TESTS_TEST_HOST_H
