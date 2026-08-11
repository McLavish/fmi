#ifndef FMI_MIGRATIONTRIGGER_H
#define FMI_MIGRATIONTRIGGER_H

#include "../comm/DrainProtocol.h"

#include <atomic>
#include <cstdint>
#include <map>
#include <mutex>
#include <string>
#include <thread>

namespace FMI::Utils {

    //! How one drain-armed channel wants to be told to migrate.
    struct TriggerConfig {
        //! Real-time signal this process listens on, as an offset from SIGRTMIN. Resolved at
        //! run time and never at compile time: SIGRTMIN is a glibc function call, not a
        //! constant, and the first few real-time signals belong to the threading library.
        int signal_offset = 3;

        //! How long the trigger thread sleeps in one signal wait, and therefore also how often
        //! it asks the coordinator for new events. The whole latency budget of "a peer began
        //! migrating and this rank has not noticed yet".
        long control_poll_interval_ms = 20;

        //! "both", "signal", "control" or "none".
        /*!
         * A drain-armed job whose ranks do not include "control" cannot survive a migration:
         * the migrating rank's drain finishes only once every peer has half-closed its side,
         * and a peer learns to do that from the coordinator. "signal" alone is for the rank a
         * driver addresses directly; "none" arms nothing and is what the unit tests use to
         * drive the state machine by hand.
         */
        std::string mode = "both";

        //! Test and dry-run mode: the SIGSTOP that ends the migrator sequence is replaced by an
        //! immediate restore leg. Everything before it — the gate, the leave notice, the
        //! half-close, the drain to EOF, the seal, the socket release — is the real thing.
        bool rehearsal_only = false;

        //! How long a rehearsal stays sealed before it restores. Exists so a test can hold a
        //! survivor across a window longer than its own max_timeout and check that it waits.
        long hold_ms = 0;
    };

    //! The process-wide half of the drain protocol: one signal policy, one epoch, one thread.
    /*!
     * It lives in `utils` rather than next to the channel because what it owns is
     * process-global rather than per-channel: a signal disposition, a blocked signal mask, a
     * `pthread_atfork` handler, and the count of times this process has been restored. Two
     * drain-armed channels in one process would have to agree on all four, which is why a
     * second *armed* participant is a `std::logic_error` naming both communicators instead of
     * a race nobody sees until a migration.
     *
     * **Signal delivery and threads.** `attach` blocks the drain signal on the calling thread
     * before the trigger thread exists, so the trigger thread inherits the block and can
     * `sigtimedwait` for it. Threads created *after* that inherit the block too — which is why
     * the supported topology is **one rank per process**, with the Communicator constructed
     * before the application spawns its own threads (OpenMP included). A signal that reaches a
     * thread which does not block it is still not fatal: a handler is installed as well, and
     * records the request for the trigger thread to pick up on its next tick rather than
     * letting the default action kill the job.
     *
     * **Failure policy.** One-phase, assume success. Anything that stops a migration from
     * completing is loud — an exception on the way in, an `unrecoverable` link and a logged
     * error on the way out. Nothing here is ever skipped quietly, because a migration that was
     * silently not performed looks exactly like one that worked until the rank never comes
     * back.
     */
    class MigrationTrigger {
    public:
        static MigrationTrigger& instance();

        //! Arm @p participant. Refcounted: the same participant may attach more than once, and
        //! the thread runs from the first attach to the last detach.
        //! @throws std::logic_error when a different participant is already armed.
        void attach(Comm::DrainParticipant* participant, const TriggerConfig& config);

        //! Drop one reference to @p participant; on the last one, stop and join the thread.
        //! Bounded: the stop flag is set and the wait is woken, never left to a timeout alone.
        void detach(Comm::DrainParticipant* participant) noexcept;

        //! How many times this process has been restored. The staleness fence for a migration
        //! request that was queued before a restore and delivered after it.
        [[nodiscard]] std::uint64_t epoch() const {
            return restore_count.load(std::memory_order_acquire);
        }

        //! Run the migrator sequence on the calling thread, to its end.
        /*!
         * Steps 1-5 (gate, leave notice, stop accepting, half-close and drain to EOF, seal) are
         * the participant's; this owns the process-wide tail: release every remaining socket,
         * return the heap, flush stdio, stop — and, on the far side of the stop, bump the epoch
         * and hand control back for the restore leg.
         *
         * @param rehearsal_only  replace the `SIGSTOP` with an immediate restore leg.
         * @param hold_ms         how long a rehearsal stays sealed before restoring.
         * @throws whatever the drain failed with. The channel is left broken on purpose; a
         *         migration that could not complete must not be papered over.
         */
        void run_migration(Comm::DrainParticipant& participant, bool rehearsal_only, long hold_ms);

        //! The signal number an offset resolves to in this process.
        static int signal_for_offset(int offset);

        //! The signal the armed participant listens on, or -1 when nothing is armed. For tests
        //! and drivers that need to address this process.
        [[nodiscard]] int drain_signal() const;

    private:
        MigrationTrigger() = default;

        void trigger_loop();

        //! Act on a migration request that arrived by signal, or say why it was dropped.
        void handle_signal_request(std::int64_t requested_epoch);

        //! Everything the child of a fork must forget: it owns no trigger thread (fork clones
        //! only the calling one) and no channel of its own until it builds one.
        static void forget_in_child() noexcept;

        mutable std::mutex mutex;
        Comm::DrainParticipant* participant = nullptr;
        TriggerConfig config;
        std::size_t attachments = 0;
        int signal_number = -1;
        //! Whether the handler is installed and the signal blocked. Without both, the signal is
        //! not this runtime's to send: its default action would kill the job.
        bool signal_armed = false;

        std::thread thread;
        std::atomic<bool> running{false};
        std::atomic<bool> stopping{false};
        std::atomic<std::uint64_t> restore_count{0};
    };
}

#endif //FMI_MIGRATIONTRIGGER_H
