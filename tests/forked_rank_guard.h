#ifndef FMI_TESTS_FORKED_RANK_GUARD_H
#define FMI_TESTS_FORKED_RANK_GUARD_H

#include <cstdlib>
#include <exception>

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

    ~ForkedRankGuard() {
        if (peer_id != 0) {
            std::_Exit(std::uncaught_exceptions() > 0 ? 1 : 0);
        }
    }
};

#endif // FMI_TESTS_FORKED_RANK_GUARD_H
