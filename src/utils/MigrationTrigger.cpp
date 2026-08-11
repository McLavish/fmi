#include "../../include/utils/MigrationTrigger.h"

#include <boost/log/trivial.hpp>

#include <pthread.h>
#include <signal.h>
#include <unistd.h>

#if defined(__GLIBC__)
#include <malloc.h>
#endif

#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <exception>
#include <stdexcept>
#include <thread>

namespace {
    //! The safety net for a signal that reaches a thread which does not block it.
    /*!
     * The trigger thread takes the signal with sigtimedwait, which is only possible in a thread
     * that blocks it. Threads that existed before the channel armed do not, and a real-time
     * signal whose default action is "terminate" must never be what a driver's migration
     * request does to a job. So a handler is installed too: it records the request and the
     * trigger thread picks it up on its next tick. Both fields are lock-free atomics, which is
     * all a signal handler is allowed to touch.
     */
    std::atomic<bool> pending_request{false};
    std::atomic<long long> pending_epoch{0};

    extern "C" void drain_request_handler(int, siginfo_t* info, void*) {
        pending_epoch.store(info == nullptr ? 0 : static_cast<long long>(info->si_value.sival_int),
                            std::memory_order_release);
        pending_request.store(true, std::memory_order_release);
    }

    long monotonic_ms() {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::steady_clock::now().time_since_epoch())
                .count();
    }

    //! Forget every drain request this process is holding but nobody asked for.
    /*!
     * Two places a request can be waiting when nothing is armed: the handler's flag, set by a
     * signal that reached a thread which does not block it, and the kernel's pending set, where
     * a signal blocked on every live thread simply sits. Neither is cleared by disarming, and
     * both are consumed by the *next* arming — a migration nobody requested, which seals every
     * link and raises SIGSTOP with no checkpointer on the other end of it. The epoch fence
     * cannot catch it either: a request written for the epoch this process is already at passes.
     *
     * @param blocked_signal  the drain signal, which must be blocked on the calling thread —
     *        sigtimedwait can only dequeue what the caller blocks — or -1 to clear the flag only.
     *
     * A signal made pending on a *different* thread with pthread_kill cannot be dequeued from
     * here; nothing in this protocol sends one that way (drivers address the process).
     */
    void discard_pending_requests(int blocked_signal) {
        if (blocked_signal > 0) {
            sigset_t set;
            sigemptyset(&set);
            sigaddset(&set, blocked_signal);
            struct timespec zero{};
            // Zero timeout: this asks what is already pending and never waits for more.
            while (::sigtimedwait(&set, nullptr, &zero) > 0) {
            }
        }
        pending_request.store(false, std::memory_order_release);
        pending_epoch.store(0, std::memory_order_release);
    }

    bool mode_includes(const std::string& mode, const char* what) {
        if (mode == "none") {
            return false;
        }
        if (mode == "both") {
            return true;
        }
        return mode == what;
    }
}

int FMI::Utils::MigrationTrigger::signal_for_offset(int offset) {
    // SIGRTMIN is a function call under glibc and moves with the threading library's private
    // signals; resolving it at run time is the only correct way to name this signal.
    const int base = SIGRTMIN;
    const int number = base + offset;
    if (number < base || number > SIGRTMAX) {
        throw std::logic_error("MigrationTrigger: drain signal offset " + std::to_string(offset) +
                               " is outside the real-time range [" + std::to_string(base) + ", " +
                               std::to_string(SIGRTMAX) + "]");
    }
    return number;
}

FMI::Utils::MigrationTrigger& FMI::Utils::MigrationTrigger::instance() {
    static MigrationTrigger singleton;
    static std::once_flag once;
    std::call_once(once, []() {
        // Only the child half: the parent keeps its thread and its participants, and the child
        // has neither (fork clones the calling thread alone). Registered once, never removed —
        // pthread_atfork has no unregister, which is another reason this is a singleton.
        ::pthread_atfork(nullptr, nullptr, &MigrationTrigger::forget_in_child);
    });
    return singleton;
}

void FMI::Utils::MigrationTrigger::forget_in_child() noexcept {
    MigrationTrigger& self = instance();
    // No lock: in the child of a fork this is the only thread, and a mutex held by a thread
    // that did not survive the fork can never be unlocked. Assigning over it is safe here for
    // the same reason it would be unsafe anywhere else.
    self.participant = nullptr;
    self.attachments = 0;
    self.signal_number = -1;
    self.signal_armed = false;
    self.running.store(false, std::memory_order_release);
    self.stopping.store(false, std::memory_order_release);
    self.restore_count.store(0, std::memory_order_release);
    if (self.thread.joinable()) {
        // The thread does not exist in this process; detaching the handle is how the child
        // stops owning a join it can never complete.
        self.thread.detach();
    }
    pending_request.store(false, std::memory_order_release);
}

void FMI::Utils::MigrationTrigger::attach(Comm::DrainParticipant* new_participant,
                                          const TriggerConfig& new_config) {
    if (new_participant == nullptr) {
        throw std::logic_error("MigrationTrigger::attach: no participant");
    }
    std::lock_guard<std::mutex> lock(mutex);
    if (participant != nullptr && participant != new_participant) {
        // One armed drain channel per process, enforced rather than assumed: two would have to
        // agree on the signal, the epoch and what a restore means, and the first sign that they
        // do not would be a migration that drains one channel and freezes the other.
        throw std::logic_error(
                "MigrationTrigger: communicator '" + participant->channel_comm_name() +
                "' is already armed for drain migration in this process; '" +
                new_participant->channel_comm_name() +
                "' cannot arm as well (one armed drain channel per process)");
    }
    if (participant == new_participant) {
        attachments++;
        return;
    }

    participant = new_participant;
    config = new_config;
    signal_number = signal_for_offset(config.signal_offset);
    attachments = 1;

    if (!mode_includes(config.mode, "signal") && !mode_includes(config.mode, "control")) {
        // Mode "none": armed for a rehearsal driven by the application or a test, with no
        // thread and no signal policy. Nothing to start.
        return;
    }

    if (mode_includes(config.mode, "signal")) {
        struct sigaction action{};
        action.sa_flags = SA_SIGINFO | SA_RESTART;
        action.sa_sigaction = &drain_request_handler;
        sigemptyset(&action.sa_mask);
        if (::sigaction(signal_number, &action, nullptr) != 0) {
            participant = nullptr;
            attachments = 0;
            throw std::runtime_error("MigrationTrigger: could not install the drain signal "
                                     "handler: " + std::string(std::strerror(errno)));
        }
        // Blocked here, before the trigger thread exists, so the trigger thread inherits the
        // block and every thread the application starts later does too.
        sigset_t set;
        sigemptyset(&set);
        sigaddset(&set, signal_number);
        if (::pthread_sigmask(SIG_BLOCK, &set, nullptr) != 0) {
            participant = nullptr;
            attachments = 0;
            throw std::runtime_error("MigrationTrigger: could not block the drain signal on this "
                                     "thread: " + std::string(std::strerror(errno)));
        }
        signal_armed = true;
    }

    // Before the trigger thread exists, and after the signal is blocked here: whatever request
    // is left over from a window in which nothing was armed belongs to nobody, and the thread
    // started below would otherwise consume it as this participant's own migration order.
    discard_pending_requests(signal_armed ? signal_number : -1);

    stopping.store(false, std::memory_order_release);
    running.store(true, std::memory_order_release);
    thread = std::thread(&MigrationTrigger::trigger_loop, this);
}

void FMI::Utils::MigrationTrigger::detach(Comm::DrainParticipant* leaving) noexcept {
    std::thread to_join;
    {
        std::lock_guard<std::mutex> lock(mutex);
        if (leaving == nullptr || participant != leaving || attachments == 0) {
            return;
        }
        attachments--;
        if (attachments > 0) {
            return;
        }
        participant = nullptr;
        stopping.store(true, std::memory_order_release);
        to_join = std::move(thread);
    }
    // Woken rather than waited out: the loop's own tick is short, but a stop that depends on a
    // timeout expiring is a stop whose bound is a configuration value.
    if (to_join.joinable()) {
        // Only when the signal is genuinely armed. In a control-only configuration nothing
        // installed a handler and nothing blocked it, and a real-time signal whose disposition
        // is still the default terminates the process — which is not a way to stop a thread.
        const int number = signal_armed ? signal_number : -1;
        if (number > 0) {
            // Aimed at the trigger thread rather than the process: the signal is only blocked
            // on threads that existed after arming, and a process-directed wake could land on
            // one that does not block it.
            ::pthread_kill(to_join.native_handle(), number);
        }
        try {
            to_join.join();
        } catch (const std::exception&) {
        }
    }
    running.store(false, std::memory_order_release);
    std::lock_guard<std::mutex> lock(mutex);
    // Nothing is armed from here on, so a request that is still in flight has no addressee. It
    // is dropped at both ends of the window — here, so it does not sit in the process for the
    // life of the job, and again at the next arm, which is the one that would have honoured it.
    discard_pending_requests(signal_armed ? signal_number : -1);
    signal_number = -1;
    signal_armed = false;
}

int FMI::Utils::MigrationTrigger::drain_signal() const {
    std::lock_guard<std::mutex> lock(mutex);
    return signal_number;
}

void FMI::Utils::MigrationTrigger::handle_signal_request(std::int64_t requested_epoch) {
    Comm::DrainParticipant* target = nullptr;
    bool rehearsal = false;
    long hold = 0;
    {
        std::lock_guard<std::mutex> lock(mutex);
        target = participant;
        rehearsal = config.rehearsal_only;
        hold = config.hold_ms;
    }
    if (target == nullptr) {
        return;
    }
    const auto current = static_cast<std::int64_t>(epoch());
    if (requested_epoch < current) {
        // Queued before a restore and delivered after it: whoever asked was addressing the
        // process this one replaced. Logged rather than silently swallowed — a dropped request
        // is exactly as visible as a performed one.
        BOOST_LOG_TRIVIAL(warning)
                << "MigrationTrigger: dropped a migration request for epoch " << requested_epoch
                << "; this process is already at epoch " << current;
        return;
    }
    try {
        run_migration(*target, rehearsal, hold);
    } catch (const std::exception& e) {
        // The channel is broken and says so on every parked application thread; this is the
        // record on the thread that found out first.
        BOOST_LOG_TRIVIAL(error) << "MigrationTrigger: migration failed: " << e.what();
    }
}

void FMI::Utils::MigrationTrigger::trigger_loop() {
    sigset_t set;
    sigemptyset(&set);
    int number = -1;
    long poll_interval = 20;
    bool watch_signal = false;
    bool watch_control = false;
    {
        std::lock_guard<std::mutex> lock(mutex);
        number = signal_number;
        poll_interval = config.control_poll_interval_ms <= 0 ? 20 : config.control_poll_interval_ms;
        watch_signal = mode_includes(config.mode, "signal");
        watch_control = mode_includes(config.mode, "control");
    }
    if (number > 0) {
        sigaddset(&set, number);
        // The block is inherited from the attaching thread, but say it again here: a trigger
        // thread that does not block the signal cannot take it with sigtimedwait.
        ::pthread_sigmask(SIG_BLOCK, &set, nullptr);
    }

    while (!stopping.load(std::memory_order_acquire)) {
        if (watch_signal && number > 0) {
            struct timespec timeout{};
            timeout.tv_sec = poll_interval / 1000;
            timeout.tv_nsec = (poll_interval % 1000) * 1000000L;
            siginfo_t info{};
            const int taken = ::sigtimedwait(&set, &info, &timeout);
            if (stopping.load(std::memory_order_acquire)) {
                break;
            }
            if (taken > 0) {
                handle_signal_request(static_cast<std::int64_t>(info.si_value.sival_int));
                continue;
            }
        } else {
            std::this_thread::sleep_for(std::chrono::milliseconds(poll_interval));
            if (stopping.load(std::memory_order_acquire)) {
                break;
            }
        }

        if (pending_request.exchange(false, std::memory_order_acq_rel)) {
            handle_signal_request(pending_epoch.load(std::memory_order_acquire));
            continue;
        }

        if (!watch_control) {
            continue;
        }
        Comm::DrainParticipant* target = nullptr;
        bool rehearsal = false;
        long hold = 0;
        {
            std::lock_guard<std::mutex> lock(mutex);
            target = participant;
            rehearsal = config.rehearsal_only;
            hold = config.hold_ms;
        }
        if (target == nullptr) {
            continue;
        }
        bool migrate_me = false;
        try {
            // Non-blocking: this thread owes the signal wait its next tick, and a coordinator
            // read that blocks is a migration request that waits for a stream event.
            migrate_me = target->poll_control_events();
        } catch (const std::exception& e) {
            BOOST_LOG_TRIVIAL(error)
                    << "MigrationTrigger: rank " << target->local_rank() << " of "
                    << target->channel_comm_name() << " could not dispatch control events: "
                    << e.what();
            continue;
        }
        if (migrate_me) {
            try {
                run_migration(*target, rehearsal, hold);
            } catch (const std::exception& e) {
                BOOST_LOG_TRIVIAL(error) << "MigrationTrigger: migration failed: " << e.what();
            }
        }
    }
}

void FMI::Utils::MigrationTrigger::run_migration(Comm::DrainParticipant& target,
                                                 bool rehearsal_only, long hold_ms) {
    const long started = monotonic_ms();
    BOOST_LOG_TRIVIAL(info) << "MigrationTrigger: rank " << target.local_rank() << " of "
                            << target.channel_comm_name() << " begins a "
                            << (rehearsal_only ? "rehearsed" : "real") << " migration at epoch "
                            << epoch();

    // Steps 1-5: the gate, the leave notice, the listener, the half-close-all-then-read-all
    // drain, the seal. A failure here throws and leaves the channel broken on purpose.
    target.quiesce_and_drain({});
    // Step 6: after this the process owns no socket at all, which is the whole point — the
    // image a checkpoint takes of it is host-agnostic and needs no --tcp-close on either leg.
    target.release_transport_sockets();

#if defined(__GLIBC__)
    // Step 7: hand back what the drain buffers just made free. The image is about to be
    // written out page by page, and a page of freed heap costs exactly as much as a used one.
    ::malloc_trim(0);
#endif
    std::fflush(nullptr);

    if (rehearsal_only) {
        // The regression net: everything above and below is the real sequence, and only the
        // stop is replaced. hold_ms is how a test keeps a survivor waiting across a window
        // longer than its own configured patience.
        if (hold_ms > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(hold_ms));
        }
    } else {
        // Step 8: still holding every link lock, so no application thread can observe the
        // half-restored state that follows.
        ::raise(SIGSTOP);
    }

    // Execution continues here after a CRIU restore, a SIGCONT, or the rehearsal's hold.
    restore_count.fetch_add(1, std::memory_order_acq_rel);
    target.resume_after_restore();
    BOOST_LOG_TRIVIAL(info) << "MigrationTrigger: rank " << target.local_rank() << " of "
                            << target.channel_comm_name() << " is back at epoch " << epoch()
                            << " after " << (monotonic_ms() - started) << " ms";
}
