#ifndef FMI_TESTS_FORKED_RANK_GUARD_H
#define FMI_TESTS_FORKED_RANK_GUARD_H

#include <sys/wait.h>

#include <cstdlib>
#include <exception>
#include <vector>

//! Backstop for fork-based test cases: a forked rank child must NEVER outlive the test case
//! (or backend-loop iteration) it was forked in. If it does — e.g. the case body throws and
//! Boost's per-case handler swallows the exception, or the case simply has no child-exit
//! path — the child continues into the NEXT fork-based case and forks again there, and every
//! such survivor multiplies at every subsequent case: 9 four-peer cases turn 1 process into
//! 4^9 ≈ 262k, which exhausts the kernel pid limit and takes the whole host down (this has
//! happened). The guard exits the child on scope exit, on both the normal and the unwind
//! path. std::_Exit on purpose: the child shares the parent's Boost/OpenMP state and must
//! not run destructors or atexit handlers that touch it. The exit code records whether the
//! child was terminating due to an in-flight exception, for parents that care to look.
//!
//! Usage, replacing the bare `int peer_id = 0;` before a fork loop:
//!     ForkedRankGuard rank_guard;
//!     int& peer_id = rank_guard.peer_id;
//!     for (int i = 1; i < num_peers; i++) { ... fork(), child sets peer_id = i ... }
struct ForkedRankGuard {
    int peer_id = 0;
    //! Ranks forked through fork_ranks(), so reap_ranks() can wait for exactly those.
    std::vector<pid_t> children;

    //! Clear any zombie left behind by an earlier case before this one forks anything.
    /*!
     * A case whose rank construction throws — say a config naming a backend the build
     * excluded — unwinds without waiting for the children it already forked. Those children
     * exit immediately (see the destructor) but stay unreaped, and a LATER case that waits
     * with wait(nullptr) then consumes one of them instead of its own rank child. It proceeds
     * to check shared result flags the rank has not written yet and reports a failure that
     * never happened. Draining here means a case's waits can only ever see its own children.
     */
    ForkedRankGuard() {
        while (::waitpid(-1, nullptr, WNOHANG) > 0) {
        }
    }

    //! Fork one child per non-zero rank, recording their pids in the parent.
    int fork_ranks(int num_peers) {
        for (int i = 1; i < num_peers; i++) {
            const pid_t pid = ::fork();
            if (pid == 0) {
                peer_id = i;
                children.clear();   // a child owns none of the parent's other ranks
                break;
            }
            if (pid > 0) {
                children.push_back(pid);
            }
        }
        return peer_id;
    }

    //! Child: leave. Parent: wait for the specific ranks it forked, and only those.
    void reap_ranks() {
        if (peer_id != 0) {
            std::_Exit(0);
        }
        for (const pid_t pid : children) {
            ::waitpid(pid, nullptr, 0);
        }
    }

    ~ForkedRankGuard() {
        if (peer_id != 0) {
            std::_Exit(std::uncaught_exceptions() > 0 ? 1 : 0);
        }
    }
};

#endif // FMI_TESTS_FORKED_RANK_GUARD_H
