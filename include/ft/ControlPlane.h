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

#ifdef FMI_ENABLE_CRIU
        //! Remove the CRIU rank registry associated with this communicator name.
        void clear_criu_state();

        void criu_register_rank(FMI::Utils::peer_num rank, int pid, const std::string& host_id, const std::string& backend) const;
        void criu_mark_rank_running(FMI::Utils::peer_num rank, int pid, const std::string& host_id, const std::string& backend) const;
        void criu_mark_rank_quiesced(FMI::Utils::peer_num rank, int pid, const std::string& host_id, const std::string& backend,
                                     std::uint64_t generation) const;
        [[nodiscard]] std::vector<CriuRankInfo> criu_rank_info() const;
#endif

    private:
        struct Impl;
        std::shared_ptr<Impl> impl;

        friend class FMI::Communicator;
        friend class TransparentMigrationRuntime;

        //! What an operation boundary needs to know, read atomically in one Redis round-trip:
        //! the current epoch and the pending set (its cardinality plus this rank's membership).
        //! Atomicity matters — promotion bumps the epoch and clears the pending set in one
        //! script, so a snapshot can never pair a stale epoch with an already-cleared set.
        struct OperationSnapshot {
            std::uint64_t epoch = 0;
            bool any_pending = false;
            bool self_pending = false;
        };
        [[nodiscard]] OperationSnapshot observe_operation(FMI::Utils::peer_num rank) const;

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
