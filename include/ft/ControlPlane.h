#ifndef FMI_FT_COORDINATOR_H
#define FMI_FT_COORDINATOR_H

#include "../utils/Common.h"
#include "Common.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace FMI {
    class Communicator;
}

namespace FMI::FT {
    class TransparentMigrationRuntime;
    //! Control-plane client used by FMI::Communicator (transparent migration) and by external daemons that request migrations.
    class ControlPlane {
    public:
        ControlPlane(std::string config_path, std::string comm_name, FMI::Utils::peer_num num_peers);
        ~ControlPlane();

        ControlPlane(const ControlPlane&) = delete;
        ControlPlane& operator=(const ControlPlane&) = delete;
        ControlPlane(ControlPlane&&) noexcept = default;
        ControlPlane& operator=(ControlPlane&&) noexcept = default;

        //! Request a migration for the logical rank.
        void request_migration(FMI::Utils::peer_num rank);

        //! Request migration for a set of ranks in one atomic step, so the whole target set
        //! becomes pending together (no window where some ranks are pending and others are not,
        //! which a watcher/agent could act on mid-batch). Used to drive batch / "migrate all
        //! local" migrations. No-op for an empty set.
        //!
        //! One cut at a time: throws while a rank outside the requested set is already pending —
        //! epoch promotion releases one global cut, so overlapping cuts from two requesters would
        //! silently drop each other at promotion. Re-requesting already-pending ranks (or a
        //! superset of them) is idempotent; retry after the in-flight cut promotes.
        void request_migrations(const std::vector<FMI::Utils::peer_num>& ranks) const;

        [[nodiscard]] std::string placement_for_rank(std::uint64_t epoch, FMI::Utils::peer_num rank) const;
        [[nodiscard]] std::vector<RankDirectoryEntry> directory_snapshot(std::uint64_t epoch) const;

        //! Remove all FT metadata associated with this communicator name.
        void clear_job_state();

        //! Promote the epoch to next_epoch if it advances the current epoch. Returns true when
        //! this call performed the promotion, false when the epoch was already >= next_epoch.
        //!
        //! Promotion is gated on quiescence: it throws (without changing anything) while any
        //! pending-migration rank has not marked itself QUIESCED in the current epoch. This makes
        //! the ordering requirement of the protocol impossible to violate: promoting before the
        //! target rank reached its quiesce point would otherwise let the old process rejoin the
        //! new epoch as a survivor next to its replacement (two processes owning one logical rank).
        //!
        //! When a consensus cut boundary was fixed, promotion is ALSO gated on every member of
        //! the epoch having published a boundary >= the cut. Without this, a slow rank still
        //! below the cut when the epoch advanced would rejoin early — having completed fewer
        //! operations than its cohort — and the ranks would execute different user operations
        //! as the "same" collective forever after. Ranks below the cut can always finish to it
        //! (their peers completed those operations, so everything they still need is already
        //! sent/uploaded), so the gate opening is only a matter of time; callers retry exactly
        //! like they do for the quiescence gate.
        bool promote_epoch(std::uint64_t next_epoch) const;

        //! Mark @p rank QUIESCED in @p epoch's state hash: the rank-side quiesce marker that
        //! promote_epoch's gate waits for. Called by the migration runtime at the quiesce point;
        //! public so orchestrators/tests driving the protocol externally can simulate a rank.
        void mark_rank_quiesced(std::uint64_t epoch, FMI::Utils::peer_num rank) const;

        //! Write placement for a rank in a given epoch.
        void set_placement(std::uint64_t epoch, FMI::Utils::peer_num rank, const std::string& placement) const;

        //! Join @p epoch as an Active member: register (rank -> worker_id), and record placement if
        //! non-empty. The single membership-join used by both the Communicator constructor (initial
        //! join) and the migration runtime (re-join after epoch promotion).
        void join_epoch(std::uint64_t epoch, FMI::Utils::peer_num rank, const std::string& worker_id,
                        const std::string& placement) const;

        //! Return the currently active communicator epoch.
        [[nodiscard]] std::uint64_t epoch() const;

        //! Drop the Redis connection; the next command transparently reconnects. Used by the
        //! CRIU quiesce path to hold the control-plane socket closed between promotion polls,
        //! so the process image captured by `criu dump` contains no established TCP socket —
        //! which removes the need for criu's --tcp-close (and the SIGPIPE hazard of a restored
        //! rank writing to a dead socket: after restore the context is simply null and the
        //! next command opens a fresh connection).
        void disconnect();

        //! Last operation boundary each rank published (rank -> operations completed in the
        //! active epoch). Piggybacked for free on the per-operation snapshot, so an external
        //! orchestrator/agent can see what every rank is doing (progress, who lags, where a
        //! stuck migration is stuck) without any rank-side instrumentation.
        [[nodiscard]] std::vector<std::pair<FMI::Utils::peer_num, std::uint64_t>> operation_boundaries() const;

        //! Ranks migrated by the cut that entered @p epoch (the pending set persisted by
        //! promote_epoch, sorted). Rejoining ranks use it for selective re-pair: only links
        //! involving a moved rank are reconnected; surviving connections are kept. Empty for
        //! an epoch entered without any migration (e.g. a bare promote), where keeping every
        //! connection is exactly right. Moved sets are retained until clear_job_state (one
        //! small set per cut) so a rank that crosses several epochs in one rejoin can union
        //! the moved sets of every epoch it skipped — deleting them at the next promotion
        //! would leave such a rank holding a kept-but-dead socket to an earlier cut's target.
        [[nodiscard]] std::vector<FMI::Utils::peer_num> moved_ranks(std::uint64_t epoch) const;

#ifdef FMI_ENABLE_CRIU
        //! Remove the CRIU rank registry associated with this communicator name.
        void clear_criu_state();

        void criu_register_rank(FMI::Utils::peer_num rank, int pid, const std::string& host_id, const std::string& backend) const;
        void criu_mark_rank_running(FMI::Utils::peer_num rank, int pid, const std::string& host_id, const std::string& backend) const;
        void criu_mark_rank_quiesced(FMI::Utils::peer_num rank, int pid, const std::string& host_id, const std::string& backend,
                                     std::uint64_t generation) const;
        [[nodiscard]] std::vector<CriuRankInfo> criu_rank_info() const;

        //! Stage a rank's packed checkpoint image (an opaque archive blob) for @p epoch, so an
        //! agent on another host can fetch and restore it: dump-side half of a cross-host
        //! migration. Overwrites any previously staged image for the same (epoch, rank).
        void criu_image_put(std::uint64_t epoch, FMI::Utils::peer_num rank, const std::string& blob) const;
        //! Fetch a staged checkpoint image. Throws when no image is staged for (epoch, rank).
        [[nodiscard]] std::string criu_image_get(std::uint64_t epoch, FMI::Utils::peer_num rank) const;
#endif

    private:
        struct Impl;
        std::shared_ptr<Impl> impl;

        friend class FMI::Communicator;
        friend class TransparentMigrationRuntime;

        //! What an operation boundary needs to know, read atomically in one Redis round-trip:
        //! the current epoch, the pending set (its cardinality plus this rank's membership),
        //! and the consensus cut boundary. Atomicity matters — promotion bumps the epoch and
        //! clears the pending set in one script, so a snapshot can never pair a stale epoch
        //! with an already-cleared set.
        struct OperationSnapshot {
            std::uint64_t epoch = 0;
            bool any_pending = false;
            bool self_pending = false;
            //! True when a consensus cut boundary has been fixed for the pending cut. Kept
            //! separate from cut_index because 0 is a legitimate cut boundary (a cut proposed
            //! before any operation ran); conflating "cut at 0" with "no cut" would let ranks
            //! park at differing boundaries in exactly the race the cut exists to prevent.
            bool cut_proposed = false;
            //! Boundary index the pending cut takes effect at (valid iff cut_proposed). Set
            //! atomically by the first same-epoch rank that observes the pending set at a
            //! boundary: the proposer's boundary, bumped past any other rank's published
            //! boundary. Any rank already inside a lower operation still gets the target's
            //! participation, because every rank — the target included — keeps executing
            //! operations while its own boundary is below the cut.
            std::uint64_t cut_index = 0;
        };
        //! Publish @p boundary for @p rank (only when @p epoch still is the current epoch —
        //! a rank that slept through a promotion must not pollute the new epoch's boundary
        //! hash with an old-epoch counter) and read the snapshot, all in one script/round-trip.
        [[nodiscard]] OperationSnapshot observe_operation(FMI::Utils::peer_num rank, std::uint64_t boundary,
                                                          std::uint64_t epoch) const;

        void ensure_job() const;
        [[nodiscard]] bool is_rank_pending(FMI::Utils::peer_num rank) const;
        void register_rank(std::uint64_t epoch, FMI::Utils::peer_num rank, const std::string& worker_id, RankState state) const;
        void set_rank_state(std::uint64_t epoch, FMI::Utils::peer_num rank, RankState state) const;

        void check_world_size(const std::string& meta_key, const char* error_message) const;
#ifdef FMI_ENABLE_CRIU
        void ensure_criu_registry() const;
        void criu_write_rank(FMI::Utils::peer_num rank, int pid, const std::string& host_id, const std::string& backend,
                             CriuRankState state, bool write_generation, std::uint64_t generation) const;
#endif
    };
}

#endif //FMI_FT_COORDINATOR_H
