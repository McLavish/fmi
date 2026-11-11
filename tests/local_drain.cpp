//! The Local Drain channel against a test host manager, over real sockets.
/*!
 * Every rank is a channel of its own with a listener, a service thread and a control link, so
 * the ranks of a case are threads of this process. `stop=hold` makes a drained rank wait in
 * place for release_hold() where a real rank raises SIGSTOP; the one case that needs the real
 * stop forks a rank and plays the host manager's part with waitpid and SIGCONT.
 *
 * The cases follow runtime/DESIGN.md of fmi-spot-migration: the data path, a rank that leaves and
 * comes back, the FIN as the fast signal, and the rule that rank state only moves forward.
 */
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>

#include "../include/comm/LinkHello.h"
#include "../include/comm/LocalDrain.h"
#include "test_host.h"

#include <dirent.h>
#include <sys/wait.h>

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <map>
#include <memory>
#include <numeric>
#include <string>
#include <thread>
#include <vector>

using FMI::Comm::LocalDrain;

namespace {
    using Rank = std::shared_ptr<LocalDrain>;

    std::map<std::string, std::string> model_params() {
        return {{"bandwidth", "400"},   {"overhead", "0.2"}, {"transfer_price", "0"},
                {"vm_price", "0.0134"}, {"requests_per_hour", "1000"},
                {"include_infrastructure_costs", "false"}};
    }

    //! A job name no other case and no other run has used.
    std::string unique_job(const char* tag) {
        return std::string("ld-") + tag + "-" + std::to_string(::getpid()) + "-" +
               std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    }

    Rank make_rank(const std::string& host_link, unsigned rank, unsigned size, const std::string& job,
                   std::map<std::string, std::string> extra = {}) {
        ::unsetenv("FMI_HOST_LINK");
        std::map<std::string, std::string> params = {{"host_link", host_link},
                                                     {"bind_host", "127.0.0.1"},
                                                     {"max_timeout", "8000"},
                                                     {"connect_retry_interval_ms", "5"},
                                                     {"stop", "hold"}};
        for (auto& [key, value] : extra) {
            params[key] = value;
        }
        auto ch = std::make_shared<LocalDrain>(params, model_params());
        ch->set_peer_id(rank);
        ch->set_num_peers(size);
        ch->set_comm_name(job);
        ch->on_registered();
        return ch;
    }

    void send_int(const Rank& ch, int value, unsigned to) {
        ch->send({reinterpret_cast<char*>(&value), sizeof(value)}, to);
    }

    int recv_int(const Rank& ch, unsigned from) {
        int value = -1;
        ch->recv({reinterpret_cast<char*>(&value), sizeof(value)}, from);
        return value;
    }

    bool wait_until(const std::function<bool()>& done, long timeout_ms) {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
        while (!done()) {
            if (std::chrono::steady_clock::now() >= deadline) {
                return false;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(2));
        }
        return true;
    }

    //! One value from @p a to @p b and one back, each end in a thread of its own.
    void ping_pong(const Rank& a, unsigned a_rank, const Rank& b, unsigned b_rank, int value) {
        int there = -1;
        int back = -1;
        std::thread other([&]() {
            there = recv_int(b, a_rank);
            send_int(b, there + 1, a_rank);
        });
        send_int(a, value, b_rank);
        back = recv_int(a, b_rank);
        other.join();
        BOOST_CHECK_EQUAL(there, value);
        BOOST_CHECK_EQUAL(back, value + 1);
    }

    //! Move @p rank out and wait until it is drained and waits in place.
    TestHost::Drained move_out(TestHost& host, const std::string& job, const Rank& ch, unsigned rank,
                               bool quietly = false) {
        TestHost::Drained report;
        const std::string move = host.leave(job, rank, quietly);
        BOOST_REQUIRE(host.wait_drained(job, rank, move, 10000, &report));
        BOOST_REQUIRE(wait_until([&]() { return ch->is_held(); }, 10000));
        return report;
    }

    //! Let a drained rank continue and wait until the table has it back.
    void move_back(TestHost& host, const std::string& job, const Rank& ch, unsigned rank) {
        const std::uint64_t next = ch->generation() + 1;
        ch->release_hold();
        BOOST_REQUIRE(host.wait_here(job, rank, next, 10000));
        BOOST_REQUIRE(wait_until([&]() { return !ch->is_held() && ch->generation() == next; }, 10000));
    }

    std::vector<char> pattern(std::size_t bytes, unsigned seed) {
        std::vector<char> out(bytes);
        std::uint32_t x = seed * 2654435761u + 1u;
        for (std::size_t i = 0; i < bytes; i++) {
            x = x * 1664525u + 1013904223u;
            out[i] = static_cast<char>(x >> 24);
        }
        return out;
    }

    int sockets_of(pid_t pid) {
        const std::string dir = "/proc/" + std::to_string(pid) + "/fd";
        DIR* d = ::opendir(dir.c_str());
        if (d == nullptr) {
            return -1;
        }
        int count = 0;
        while (struct dirent* entry = ::readdir(d)) {
            // The three standard streams are whatever this test was started with (a socket,
            // under some runners); a host manager gives its ranks /dev/null and a log file.
            if (std::atoi(entry->d_name) < 3) {
                continue;
            }
            char target[256];
            const std::string path = dir + "/" + entry->d_name;
            const ssize_t n = ::readlink(path.c_str(), target, sizeof(target) - 1);
            if (n > 0) {
                target[n] = 0;
                if (std::strncmp(target, "socket:", 7) == 0) {
                    count++;
                    BOOST_TEST_MESSAGE("descriptor " << entry->d_name << " of " << pid << " is " << target);
                }
            }
        }
        ::closedir(d);
        return count;
    }
}

BOOST_AUTO_TEST_SUITE(LocalDrainChannel)

//! The hello is the only thing this channel puts on a link besides payload.
BOOST_AUTO_TEST_CASE(the_hello_survives_a_round_trip) {
    FMI::Comm::LinkHello out;
    out.sender_rank = 3;
    out.receiver_rank = 7;
    out.link_name_hash = 0x0123456789ABCDEFull;
    out.nonce = 0xFEDCBA9876543210ull;
    out.sender_generation = 42;
    out.bytes_sent = 1ull << 40;
    out.bytes_received = (1ull << 40) + 17;

    char wire[FMI::Comm::link_hello_bytes];
    FMI::Comm::encode_hello(out, wire);
    FMI::Comm::LinkHello back;
    BOOST_REQUIRE(FMI::Comm::decode_hello(wire, back));
    BOOST_CHECK_EQUAL(back.sender_rank, out.sender_rank);
    BOOST_CHECK_EQUAL(back.receiver_rank, out.receiver_rank);
    BOOST_CHECK_EQUAL(back.link_name_hash, out.link_name_hash);
    BOOST_CHECK_EQUAL(back.nonce, out.nonce);
    BOOST_CHECK_EQUAL(back.sender_generation, out.sender_generation);
    BOOST_CHECK_EQUAL(back.bytes_sent, out.bytes_sent);
    BOOST_CHECK_EQUAL(back.bytes_received, out.bytes_received);

    char damaged[FMI::Comm::link_hello_bytes];
    std::memcpy(damaged, wire, sizeof(wire));
    damaged[0] = static_cast<char>(damaged[0] ^ 0xFF);
    FMI::Comm::LinkHello ignored;
    BOOST_CHECK(!FMI::Comm::decode_hello(damaged, ignored));
    std::memcpy(damaged, wire, sizeof(wire));
    damaged[5] = static_cast<char>(damaged[5] + 1);   // another version
    BOOST_CHECK(!FMI::Comm::decode_hello(damaged, ignored));
}

//! Rows only move forward, whatever order they arrive in.
BOOST_AUTO_TEST_CASE(the_rank_table_only_moves_forward) {
    FMI::Comm::RankTable table;
    table.resize(4);
    FMI::Comm::RankRow here0;
    here0.known = true;
    here0.generation = 0;
    here0.port = 1000;
    FMI::Comm::RankRow leaving0 = here0;
    leaving0.leaving = true;
    FMI::Comm::RankRow here1 = here0;
    here1.generation = 1;
    here1.port = 2000;

    BOOST_CHECK(table.apply(2, here0));
    BOOST_CHECK(!table.apply(2, here0));      // a repeat
    BOOST_CHECK(table.apply(2, here1));       // back, the leaving row not seen yet
    BOOST_CHECK(!table.apply(2, leaving0));   // which then arrives late and changes nothing
    BOOST_CHECK(!table.apply(2, here0));
    BOOST_CHECK_EQUAL(table.get(2).generation, 1u);
    BOOST_CHECK(!table.get(2).leaving);
    BOOST_CHECK_EQUAL(table.get(2).port, 2000);

    // A hello names a generation without an address; the row that follows may fill it in.
    FMI::Comm::RankRow from_hello;
    from_hello.known = true;
    from_hello.generation = 2;
    BOOST_CHECK(table.apply(2, from_hello));
    FMI::Comm::RankRow here2 = here1;
    here2.generation = 2;
    here2.port = 3000;
    BOOST_CHECK(table.apply(2, here2));
    BOOST_CHECK_EQUAL(table.get(2).port, 3000);
    BOOST_CHECK(!table.apply(9, here0));      // not a rank of this job
}

BOOST_AUTO_TEST_CASE(a_rank_out_of_range_is_refused) {
    TestHost host;
    auto ch = make_rank(host.path(), 0, 2, unique_job("bounds"));
    int value = 1;
    BOOST_CHECK_THROW(ch->send({reinterpret_cast<char*>(&value), sizeof(value)}, 5), std::runtime_error);
    BOOST_CHECK_THROW(ch->send({reinterpret_cast<char*>(&value), sizeof(value)}, 0), std::runtime_error);
    ch->finalize();
}

BOOST_AUTO_TEST_CASE(a_rank_without_a_host_manager_says_so) {
    auto ch = make_rank("/tmp/fmi-no-such-host-manager.sock", 1, 2, unique_job("nohost"),
                        {{"max_timeout", "300"}});
    BOOST_CHECK_THROW(send_int(ch, 1, 0), std::runtime_error);
}

BOOST_AUTO_TEST_CASE(two_ranks_talk_both_ways) {
    TestHost host;
    const std::string job = unique_job("pair");
    auto r0 = make_rank(host.path(), 0, 2, job);
    auto r1 = make_rank(host.path(), 1, 2, job);
    ping_pong(r0, 0, r1, 1, 41);
    ping_pong(r1, 1, r0, 0, 141);
    r0->finalize();
    r1->finalize();
    BOOST_CHECK_EQUAL(host.failures(), 0);
}

//! Far larger than a socket buffer, so the message spans many chunks in both directions.
BOOST_AUTO_TEST_CASE(a_message_larger_than_any_socket_buffer) {
    TestHost host;
    const std::string job = unique_job("large");
    auto r0 = make_rank(host.path(), 0, 2, job);
    auto r1 = make_rank(host.path(), 1, 2, job);
    const std::vector<char> forth = pattern(24u << 20, 1);
    const std::vector<char> back = pattern(24u << 20, 2);
    std::vector<char> got_forth(forth.size());
    std::vector<char> got_back(back.size());
    std::thread other([&]() {
        r1->recv({got_forth.data(), got_forth.size()}, 0);
        std::vector<char> copy = back;
        r1->send({copy.data(), copy.size()}, 0);
    });
    std::vector<char> copy = forth;
    r0->send({copy.data(), copy.size()}, 1);
    r0->recv({got_back.data(), got_back.size()}, 1);
    other.join();
    BOOST_CHECK(got_forth == forth);
    BOOST_CHECK(got_back == back);
    r0->finalize();
    r1->finalize();
}

BOOST_AUTO_TEST_CASE(collectives_at_four_and_eight_ranks) {
    for (unsigned size : {4u, 8u}) {
        TestHost host;
        const std::string job = unique_job("coll");
        std::vector<Rank> ranks;
        for (unsigned r = 0; r < size; r++) {
            ranks.push_back(make_rank(host.path(), r, size, job));
        }
        std::vector<int> heard(size, -1);
        std::vector<int> sums(size, -1);
        std::vector<std::thread> threads;
        for (unsigned r = 0; r < size; r++) {
            threads.emplace_back([&, r]() {
                int word = r == 2 ? 4711 : -1;
                ranks[r]->bcast({reinterpret_cast<char*>(&word), sizeof(word)}, 2);
                heard[r] = word;
                int mine = static_cast<int>(r) + 1;
                int total = 0;
                raw_function sum{[](char* a, char* b) {
                                     *reinterpret_cast<int*>(a) += *reinterpret_cast<int*>(b);
                                 },
                                 true, true};
                ranks[r]->allreduce({reinterpret_cast<char*>(&mine), sizeof(mine)},
                                    {reinterpret_cast<char*>(&total), sizeof(total)}, sum);
                sums[r] = total;
                ranks[r]->barrier();
            });
        }
        for (auto& t : threads) {
            t.join();
        }
        const int expected = static_cast<int>(size * (size + 1) / 2);
        for (unsigned r = 0; r < size; r++) {
            BOOST_CHECK_EQUAL(heard[r], 4711);
            BOOST_CHECK_EQUAL(sums[r], expected);
            ranks[r]->finalize();
        }
        BOOST_CHECK_EQUAL(host.failures(), 0);
    }
}

//! The move itself: drained means no socket at all, the counters say what crossed the link, and
//! the rank is back one generation later on a new listener.
BOOST_AUTO_TEST_CASE(a_rank_leaves_and_comes_back) {
    TestHost host;
    const std::string job = unique_job("move");
    auto r0 = make_rank(host.path(), 0, 2, job);
    auto r1 = make_rank(host.path(), 1, 2, job);
    ping_pong(r1, 1, r0, 0, 5);
    BOOST_CHECK_EQUAL(r0->open_sockets(), 3);   // listener, control link, the link to rank 1

    const TestHost::Drained report = move_out(host, job, r1, 1);
    BOOST_CHECK_EQUAL(r1->open_sockets(), 0);
    BOOST_CHECK_EQUAL(report.generation, 0u);
    BOOST_CHECK_EQUAL(report.sent.at(0), sizeof(int));
    BOOST_CHECK_EQUAL(report.received.at(0), sizeof(int));
    // The peer closed its side when the FIN arrived; it keeps its listener and control link.
    BOOST_CHECK(wait_until([&]() { return r0->open_sockets() == 2; }, 2000));

    move_back(host, job, r1, 1);
    BOOST_CHECK_EQUAL(r1->generation(), 1u);
    ping_pong(r0, 0, r1, 1, 70);
    ping_pong(r1, 1, r0, 0, 80);
    r0->finalize();
    r1->finalize();
    BOOST_CHECK_EQUAL(host.failures(), 0);
}

//! A message cut by a move continues at its byte offset on the new connection, whichever end
//! of it moves.
BOOST_AUTO_TEST_CASE(a_transfer_cut_by_a_move_resumes_at_its_offset) {
    TestHost host;
    const std::string job = unique_job("cut");
    auto r0 = make_rank(host.path(), 0, 2, job);
    auto r1 = make_rank(host.path(), 1, 2, job);
    ping_pong(r1, 1, r0, 0, 1);

    for (int round = 0; round < 2; round++) {
        const bool receiver_moves = round == 0;
        const std::vector<char> payload = pattern(64u << 20, 10 + round);
        std::vector<char> copy = payload;
        std::vector<char> got(payload.size());
        std::thread sender([&]() {
            (receiver_moves ? r0 : r1)->send({copy.data(), copy.size()}, receiver_moves ? 1 : 0);
        });
        std::thread receiver([&]() {
            // Late on purpose: the sender fills the socket buffers and blocks in the middle of
            // the message, so the move below cuts it there and nowhere else.
            std::this_thread::sleep_for(std::chrono::milliseconds(400));
            (receiver_moves ? r1 : r0)->recv({got.data(), got.size()}, receiver_moves ? 0 : 1);
        });
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        const TestHost::Drained report = move_out(host, job, r1, 1);
        BOOST_CHECK_EQUAL(r1->open_sockets(), 0);
        const std::uint64_t crossed = receiver_moves ? report.received.at(0) : report.sent.at(0);
        // More than the few bytes of the greetings, far less than the message: cut inside it.
        BOOST_CHECK_GT(crossed, 64u * 1024u);
        BOOST_CHECK_LT(crossed, payload.size());
        BOOST_TEST_MESSAGE("round " << round << ": the cut came after " << crossed << " of "
                                    << payload.size() << " bytes");
        move_back(host, job, r1, 1);
        sender.join();
        receiver.join();
        BOOST_CHECK(got == payload);
    }
    BOOST_CHECK_EQUAL(r1->generation(), 2u);
    r0->finalize();
    r1->finalize();
    BOOST_CHECK_EQUAL(host.failures(), 0);
}

//! D13: the peer needs no message to close its side. Here no row says that rank 1 leaves.
BOOST_AUTO_TEST_CASE(a_peer_closes_on_the_fin_alone) {
    TestHost host;
    const std::string job = unique_job("fin");
    auto r0 = make_rank(host.path(), 0, 2, job);
    auto r1 = make_rank(host.path(), 1, 2, job);
    ping_pong(r1, 1, r0, 0, 3);

    const auto begun = std::chrono::steady_clock::now();
    move_out(host, job, r1, 1, /*quietly=*/true);
    const auto drained_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - begun).count();
    BOOST_CHECK(wait_until([&]() { return r0->open_sockets() == 2; }, 2000));
    // The drain finished although nobody told rank 0 anything: well inside the grace period.
    BOOST_CHECK_LT(drained_ms, 2000);

    // The row of the next generation is what tells rank 0 that this was a move.
    move_back(host, job, r1, 1);
    ping_pong(r0, 0, r1, 1, 30);
    r0->finalize();
    r1->finalize();
}

//! A rank that is killed says no goodbye and no row explains its FIN: that is a death, and it
//! is reported as one after the grace period.
BOOST_AUTO_TEST_CASE(a_close_without_a_move_is_a_death) {
    TestHost host;
    const std::string job = unique_job("death");
    const std::string link = host.path();
    const pid_t child = ::fork();
    if (child == 0) {
        for (int fd = 3; fd < 1024; fd++) {
            ::close(fd);
        }
        try {
            auto ch = make_rank(link, 1, 2, job);
            send_int(ch, 3, 0);
            recv_int(ch, 0);   // never answered: the parent kills this rank here
        } catch (...) {
        }
        std::_Exit(0);
    }
    BOOST_REQUIRE(child > 0);
    auto r0 = make_rank(link, 0, 2, job, {{"drain_grace_ms", "300"}});
    BOOST_CHECK_EQUAL(recv_int(r0, 1), 3);
    ::kill(child, SIGKILL);
    ::waitpid(child, nullptr, 0);
    const auto begun = std::chrono::steady_clock::now();
    BOOST_CHECK_THROW(recv_int(r0, 1), std::runtime_error);
    const auto waited_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - begun).count();
    BOOST_CHECK_GE(waited_ms, 250);
    BOOST_CHECK_LT(waited_ms, 3000);
    r0->finalize();
}

//! A rank that finalized said goodbye. A peer that still waits for it was left out of the
//! collective, which is a timeout, and it learns that at once.
BOOST_AUTO_TEST_CASE(a_peer_that_finalized_is_a_timeout) {
    TestHost host;
    const std::string job = unique_job("bye");
    auto r0 = make_rank(host.path(), 0, 2, job);
    auto r1 = make_rank(host.path(), 1, 2, job);
    ping_pong(r1, 1, r0, 0, 3);
    send_int(r1, 9, 0);   // its last word, sent before it goes
    r1->finalize();
    BOOST_CHECK_EQUAL(recv_int(r0, 1), 9);   // what it sent is still delivered
    const auto begun = std::chrono::steady_clock::now();
    BOOST_CHECK_THROW(recv_int(r0, 1), FMI::Utils::Timeout);
    BOOST_CHECK_THROW(send_int(r0, 1, 1), FMI::Utils::Timeout);
    const auto waited_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - begun).count();
    BOOST_CHECK_LT(waited_ms, 2000);   // far below max_timeout: told, not timed out
    r0->finalize();
}

//! D12: a row that is older than what a rank knows changes nothing, however it is worded.
BOOST_AUTO_TEST_CASE(a_stale_row_changes_nothing) {
    TestHost host;
    const std::string job = unique_job("stale");
    auto r0 = make_rank(host.path(), 0, 2, job);
    auto r1 = make_rank(host.path(), 1, 2, job);
    ping_pong(r1, 1, r0, 0, 3);
    move_out(host, job, r1, 1);
    move_back(host, job, r1, 1);
    ping_pong(r1, 1, r0, 0, 4);   // connected again, at generation 1

    // What the first system could deliver late: the leave of the generation that is gone.
    host.say_to(job, 0, "rank rank=1 generation=0 state=leaving addr=127.0.0.1:1 nonce=1");
    host.say_to(job, 0, "rank rank=1 generation=0 state=here addr=127.0.0.1:1 nonce=1");
    host.say_to(job, 0, "rank rank=1 generation=1 state=here addr=127.0.0.1:1 nonce=1");
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    BOOST_CHECK_EQUAL(r0->open_sockets(), 3);   // the link is untouched
    const auto begun = std::chrono::steady_clock::now();
    ping_pong(r0, 0, r1, 1, 50);
    const auto took_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - begun).count();
    BOOST_CHECK_LT(took_ms, 1000);
    r0->finalize();
    r1->finalize();
}

BOOST_AUTO_TEST_CASE(a_leave_for_another_generation_is_ignored) {
    TestHost host;
    const std::string job = unique_job("wronggen");
    auto r0 = make_rank(host.path(), 0, 2, job);
    auto r1 = make_rank(host.path(), 1, 2, job);
    ping_pong(r1, 1, r0, 0, 3);
    host.say_to(job, 1, "leave move=late generation=7");
    host.say_to(job, 1, "a verb from a newer host manager");
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    BOOST_CHECK(!r1->is_held());
    BOOST_CHECK_EQUAL(r1->generation(), 0u);
    ping_pong(r0, 0, r1, 1, 9);
    r0->finalize();
    r1->finalize();
}

//! Two ranks of one move are each other's peers. Both drain, both stop without a socket, and
//! what one says it sent the other says it received.
BOOST_AUTO_TEST_CASE(two_ranks_leave_together) {
    TestHost host;
    const std::string job = unique_job("batch");
    const unsigned size = 4;
    std::vector<Rank> ranks;
    for (unsigned r = 0; r < size; r++) {
        ranks.push_back(make_rank(host.path(), r, size, job));
    }
    auto allreduce_everywhere = [&](int base) {
        std::vector<int> sums(size, -1);
        std::vector<std::thread> threads;
        for (unsigned r = 0; r < size; r++) {
            threads.emplace_back([&, r]() {
                int mine = base + static_cast<int>(r);
                int total = 0;
                raw_function sum{[](char* a, char* b) {
                                     *reinterpret_cast<int*>(a) += *reinterpret_cast<int*>(b);
                                 },
                                 true, true};
                ranks[r]->allreduce({reinterpret_cast<char*>(&mine), sizeof(mine)},
                                    {reinterpret_cast<char*>(&total), sizeof(total)}, sum);
                sums[r] = total;
            });
        }
        for (auto& t : threads) {
            t.join();
        }
        for (unsigned r = 0; r < size; r++) {
            BOOST_CHECK_EQUAL(sums[r], static_cast<int>(size) * base + 6);
        }
    };
    allreduce_everywhere(100);
    ping_pong(ranks[2], 2, ranks[1], 1, 8);   // make sure the two that leave share a link

    const std::string move1 = host.leave(job, 1);
    const std::string move2 = host.leave(job, 2);
    TestHost::Drained one, two;
    BOOST_REQUIRE(host.wait_drained(job, 1, move1, 10000, &one));
    BOOST_REQUIRE(host.wait_drained(job, 2, move2, 10000, &two));
    BOOST_REQUIRE(wait_until([&]() { return ranks[1]->is_held() && ranks[2]->is_held(); }, 10000));
    BOOST_CHECK_EQUAL(ranks[1]->open_sockets(), 0);
    BOOST_CHECK_EQUAL(ranks[2]->open_sockets(), 0);
    BOOST_CHECK_EQUAL(one.sent.at(2), two.received.at(1));
    BOOST_CHECK_EQUAL(one.received.at(2), two.sent.at(1));

    // Rank 1 is back while rank 2 is still away: it must wait for rank 2, not dial where it was.
    move_back(host, job, ranks[1], 1);
    std::thread late([&]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(150));
        move_back(host, job, ranks[2], 2);
    });
    allreduce_everywhere(200);
    late.join();
    allreduce_everywhere(300);
    for (auto& ch : ranks) {
        ch->finalize();
    }
    BOOST_CHECK_EQUAL(host.failures(), 0);
}

BOOST_AUTO_TEST_CASE(a_rank_moves_five_times) {
    TestHost host;
    const std::string job = unique_job("five");
    auto r0 = make_rank(host.path(), 0, 3, job);
    auto r1 = make_rank(host.path(), 1, 3, job);
    auto r2 = make_rank(host.path(), 2, 3, job);
    for (int i = 0; i < 5; i++) {
        ping_pong(r1, 1, r0, 0, 10 * i);
        ping_pong(r2, 2, r1, 1, 10 * i + 1);
        move_out(host, job, r1, 1);
        BOOST_CHECK_EQUAL(r1->open_sockets(), 0);
        move_back(host, job, r1, 1);
    }
    BOOST_CHECK_EQUAL(r1->generation(), 5u);
    ping_pong(r0, 0, r1, 1, 77);
    ping_pong(r1, 1, r2, 2, 78);
    r0->finalize();
    r1->finalize();
    r2->finalize();
    BOOST_CHECK_EQUAL(host.failures(), 0);
}

//! A move must not eat the budget of an operation, whether the operation started before the
//! move or in the middle of it. (The first system charged the second kind.)
BOOST_AUTO_TEST_CASE(an_operation_that_waits_out_a_move_keeps_its_budget) {
    TestHost host;
    const std::string job = unique_job("budget");
    auto r0 = make_rank(host.path(), 0, 2, job, {{"max_timeout", "400"}});
    auto r1 = make_rank(host.path(), 1, 2, job);
    ping_pong(r1, 1, r0, 0, 3);

    int before = -1;
    std::thread early([&]() { before = recv_int(r0, 1); });   // waits before the move begins
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    move_out(host, job, r1, 1);
    int during = -1;
    std::exception_ptr failure;
    std::thread late([&]() {                                   // starts while rank 1 is away
        try {
            during = recv_int(r0, 1);
        } catch (...) {
            failure = std::current_exception();
        }
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(900));   // more than twice the budget
    move_back(host, job, r1, 1);
    send_int(r1, 21, 0);
    send_int(r1, 22, 0);
    early.join();
    late.join();
    BOOST_CHECK(failure == nullptr);
    BOOST_CHECK_EQUAL(before + during, 43);
    r0->finalize();
    r1->finalize();
}

//! The real stop. A forked rank raises SIGSTOP when it is drained; at that moment it owns no
//! socket, which is what lets its image be restored on any host. SIGCONT stands in for the
//! restore.
BOOST_AUTO_TEST_CASE(a_stopped_rank_owns_no_socket) {
    TestHost host;
    const std::string job = unique_job("stop");
    const std::string link = host.path();
    const pid_t child = ::fork();
    if (child == 0) {
        int code = 5;
        // The fork copied the sockets of this test process (its host managers); a rank that a
        // host manager starts has none of them.
        for (int fd = 3; fd < 1024; fd++) {
            ::close(fd);
        }
        try {
            auto ch = make_rank(link, 1, 2, job, {{"stop", "sigstop"}});
            send_int(ch, 7, 0);
            const int word = recv_int(ch, 0);   // arrives after the stop and the continue
            send_int(ch, word + 1, 0);
            ch->finalize();
            code = (word == 11 && ch->generation() == 1) ? 0 : 3;
        } catch (...) {
            code = 4;
        }
        std::_Exit(code);
    }
    BOOST_REQUIRE(child > 0);
    auto r0 = make_rank(link, 0, 2, job);
    BOOST_CHECK_EQUAL(recv_int(r0, 1), 7);

    const std::string move = host.leave(job, 1);
    BOOST_REQUIRE(host.wait_drained(job, 1, move, 10000));
    int status = 0;
    BOOST_REQUIRE_EQUAL(::waitpid(child, &status, WUNTRACED), child);
    BOOST_REQUIRE(WIFSTOPPED(status));
    BOOST_CHECK_EQUAL(sockets_of(child), 0);

    ::kill(child, SIGCONT);
    BOOST_REQUIRE(host.wait_here(job, 1, 1, 10000));
    send_int(r0, 11, 1);
    BOOST_CHECK_EQUAL(recv_int(r0, 1), 12);
    BOOST_REQUIRE_EQUAL(::waitpid(child, &status, 0), child);
    BOOST_CHECK(WIFEXITED(status));
    BOOST_CHECK_EQUAL(WEXITSTATUS(status), 0);
    r0->finalize();
    BOOST_CHECK_EQUAL(host.failures(), 0);
}

BOOST_AUTO_TEST_SUITE_END()
