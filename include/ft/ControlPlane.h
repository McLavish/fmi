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

        [[nodiscard]] std::string placement_for_rank(std::uint64_t epoch, FMI::Utils::peer_num rank) const;
        [[nodiscard]] std::vector<RankDirectoryEntry> directory_snapshot(std::uint64_t epoch) const;

        //! Remove all FT metadata associated with this communicator name.
        void clear_job_state();

        //! Promote the epoch to next_epoch if it advances the current epoch.
        void promote_epoch(std::uint64_t next_epoch) const;

        //! Write placement for a rank in a given epoch.
        void set_placement(std::uint64_t epoch, FMI::Utils::peer_num rank, const std::string& placement) const;

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

        void ensure_job() const;
        [[nodiscard]] bool has_pending_migration() const;
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
