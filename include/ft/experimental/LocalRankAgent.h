#ifndef FMI_FT_LOCALRANKAGENT_H
#define FMI_FT_LOCALRANKAGENT_H

#include "../../utils/Common.h"
#include "../../utils/Configuration.h"
#include "../ControlPlane.h"

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace FMI::FT {
    //! Host-local driver for CRIU-backed single-rank transparent migration (same-host v1).
    //!
    //! It bridges the transparent-migration control plane (Redis) to criu: when a targeted
    //! rank reaches its migration quiesce point it publishes a checkpoint-ready image entry in
    //! the CRIU rank registry (pid + host + QUIESCED). The agent waits for that entry,
    //! `criu dump`s the rank's process image, `criu restore`s it (preserving application
    //! memory), then promotes the communicator epoch so survivors and the restored rank
    //! reconfigure across the epoch cut. The targeted rank stays a plain FMI::Communicator
    //! application; it never invokes criu itself.
    class LocalRankAgent {
    public:
        LocalRankAgent(std::string config_path, std::string comm_name, FMI::Utils::peer_num num_peers);

        //! Migrate a single logical rank: wait for it to publish a checkpoint-ready image on
        //! this host, criu-dump it, criu-restore it, then promote the epoch. Returns the
        //! promoted epoch. Throws FMI::Utils::Timeout if the rank never becomes ready.
        std::uint64_t migrate_rank(FMI::Utils::peer_num rank) const;

        //! Migrate a set of ranks in one epoch cut: wait for ALL of them to reach their quiesce
        //! point on this host (a consistent cut), criu-dump + criu-restore each in parallel, then
        //! promote the epoch exactly once. Returns the promoted epoch. Throws FMI::Utils::Timeout
        //! if any rank never becomes ready, and rethrows the first dump/restore failure; in either
        //! case the epoch is NOT promoted (no half-checkpoint advances the cut).
        std::uint64_t migrate_ranks(const std::vector<FMI::Utils::peer_num>& ranks) const;

        //! Migrate every rank running on this host: discover the host-local ranks from the CRIU
        //! registry (advertised at rank construction), request their migration, then batch-migrate
        //! them via migrate_ranks. Returns the promoted epoch, or 0 if no local ranks are found.
        std::uint64_t migrate_local() const;

        //! Watch the migration request set and migrate the first pending rank that becomes
        //! ready on this host. Returns the promoted epoch, or 0 if no request appears within
        //! the quiesce timeout. Convenience wrapper around migrate_rank for the CLI.
        std::uint64_t watch_once();

        //! Remove this communicator's CRIU image tree and clear its CRIU control-plane state.
        void cleanup();

    private:
        //! Preconditions for driving a CRIU migration: fault tolerance enabled, Redis control
        //! plane, state_transfer="criu", and a checkpoint-safe Direct data plane.
        void ensure_migration_mode() const;
        //! Block until every rank in @p ranks has published a checkpoint-ready image (QUIESCED for
        //! @p target_epoch, pid>0) on this host, returning their registry entries. Throws
        //! FMI::Utils::Timeout if not all become ready within the quiesce timeout.
        [[nodiscard]] std::vector<CriuRankInfo> wait_for_ready_ranks(
                const std::vector<FMI::Utils::peer_num>& ranks, std::uint64_t target_epoch) const;
        [[nodiscard]] std::string rank_image_dir(std::uint64_t target_epoch, FMI::Utils::peer_num rank) const;
        void dump_rank(int pid, const std::string& dir) const;
        void restore_rank(const std::string& dir) const;

        std::string config_path;
        std::string comm_name;
        FMI::Utils::FaultToleranceConfig config;
        std::string host_id;
        std::shared_ptr<FMI::FT::ControlPlane> control_plane;
    };
}

#endif //FMI_FT_LOCALRANKAGENT_H
