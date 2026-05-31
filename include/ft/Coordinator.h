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
    class Coordinator {
    public:
        Coordinator(std::string config_path, std::string comm_name, FMI::Utils::peer_num num_peers);
        ~Coordinator();

        Coordinator(const Coordinator&) = delete;
        Coordinator& operator=(const Coordinator&) = delete;
        Coordinator(Coordinator&&) noexcept = default;
        Coordinator& operator=(Coordinator&&) noexcept = default;

        //! Request a migration for the logical rank.
        void request_migration(FMI::Utils::peer_num rank);

        [[nodiscard]] std::string placement_for_rank(std::uint64_t epoch, FMI::Utils::peer_num rank) const;
        [[nodiscard]] std::vector<RankDirectoryEntry> directory_snapshot(std::uint64_t epoch) const;

        //! Remove all FT metadata associated with this communicator name.
        void clear_job_state();

        //! Return the number of members with a live lease in the given epoch.
        [[nodiscard]] std::size_t live_member_count(std::uint64_t epoch) const;

        //! Promote the epoch to next_epoch (idempotent).
        void promote_epoch(std::uint64_t next_epoch) const;

        //! Write placement for a rank in a given epoch.
        void set_placement(std::uint64_t epoch, FMI::Utils::peer_num rank, const std::string& placement) const;

        //! Remove all CRIU FT metadata associated with this communicator name.
        void clear_criu_job_state();

        //! Return the currently active communicator epoch.
        [[nodiscard]] std::uint64_t epoch() const;

        void criu_register_rank(FMI::Utils::peer_num rank, int pid, const std::string& host_id, const std::string& backend) const;
        void criu_mark_rank_running(FMI::Utils::peer_num rank, int pid, const std::string& host_id, const std::string& backend) const;
        void criu_mark_rank_quiesced(FMI::Utils::peer_num rank, int pid, const std::string& host_id, const std::string& backend,
                                     std::uint64_t generation) const;
        [[nodiscard]] std::uint64_t criu_request_checkpoint(const std::string& supervisor_id) const;
        [[nodiscard]] bool criu_all_ranks_quiesced(std::uint64_t generation, const std::string& host_id = "") const;
        void criu_mark_job_quiesced(std::uint64_t generation, const std::string& supervisor_id) const;
        void criu_mark_checkpoint_complete(std::uint64_t generation, const std::string& supervisor_id) const;
        [[nodiscard]] std::uint64_t criu_request_restore(std::uint64_t generation, const std::string& supervisor_id) const;
        void criu_mark_job_restored(std::uint64_t generation, const std::string& supervisor_id) const;
        [[nodiscard]] std::uint64_t criu_requested_generation() const;
        [[nodiscard]] std::uint64_t criu_completed_generation() const;
        [[nodiscard]] std::uint64_t criu_restore_generation() const;
        [[nodiscard]] CriuJobInfo criu_job_info() const;
        [[nodiscard]] std::vector<CriuRankInfo> criu_rank_info() const;

    private:
        struct Impl;
        std::shared_ptr<Impl> impl;

        friend class FMI::Communicator;
        friend class TransparentMigrationRuntime;

        void ensure_job() const;
        [[nodiscard]] bool has_pending_migration() const;
        [[nodiscard]] bool is_rank_pending(FMI::Utils::peer_num rank) const;
        void register_rank(std::uint64_t epoch, FMI::Utils::peer_num rank, const std::string& worker_id, RankState state) const;
        void set_rank_state(std::uint64_t epoch, FMI::Utils::peer_num rank, RankState state) const;
        [[nodiscard]] std::string worker_for_rank(std::uint64_t epoch, FMI::Utils::peer_num rank) const;
        void refresh_lease(std::uint64_t epoch, FMI::Utils::peer_num rank, const std::string& worker_id) const;
        [[nodiscard]] unsigned int heartbeat_ms() const;
        [[nodiscard]] unsigned int lease_ms() const;
    };
}

#endif //FMI_FT_COORDINATOR_H
