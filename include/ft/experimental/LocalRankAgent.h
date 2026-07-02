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
    //! Outcome of staging a host's ranks for cross-host restore: the epoch the images were
    //! staged for (0 when no local ranks were found) and the ranks in the cut.
    struct EvacuationResult {
        std::uint64_t staged_epoch = 0;
        std::vector<FMI::Utils::peer_num> ranks;
    };

    //! Outcome of restoring a staged rank on this host.
    struct RestoreResult {
        int pid = 0;
        std::string host_id;
    };

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

        //! Cross-host dump half: discover this host's ranks, request their migration as one cut,
        //! wait for all of them to quiesce, criu-dump each in parallel, then pack every rank's
        //! image (plus its FMI_CRIU_EXTRA_FILES, "{rank}"-templated paths that criu will reopen
        //! on restore) and stage the archives in the control plane. Does NOT restore and does NOT
        //! promote — restore_remote agents on other hosts consume the staged images, and the
        //! orchestrator promotes once all of them report success. A dump/pack failure aborts the
        //! whole cut without staging a partial set.
        EvacuationResult evacuate_local() const;

        //! Cross-host restore half: fetch the staged image for @p rank (staged for epoch
        //! current+1), unpack it at / so criu finds every dumped path, criu-restore the process
        //! (it resumes parked in its promotion-wait loop), and re-advertise the rank in the CRIU
        //! registry as Running on THIS host. Does NOT promote. Throws when no image is staged.
        RestoreResult restore_remote(FMI::Utils::peer_num rank) const;

        //! Promote the communicator to epoch current+1, releasing every parked rank. The
        //! orchestrator-facing final step of a cross-host migration, gated (in the control
        //! plane) on all pending ranks having quiesced. Returns the target epoch.
        std::uint64_t promote_next() const;

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
        //! Discover the ranks advertised on this host that are eligible for a new cut (skipping
        //! leftovers parked Quiesced for an epoch that has already passed).
        [[nodiscard]] std::vector<FMI::Utils::peer_num> discover_local_ranks() const;
        [[nodiscard]] std::string rank_image_dir(std::uint64_t target_epoch, FMI::Utils::peer_num rank) const;
        void dump_rank(int pid, const std::string& dir) const;
        void restore_rank(const std::string& dir) const;
        //! Pack a dumped rank's image dir plus its FMI_CRIU_EXTRA_FILES into a /-relative
        //! tar.gz archive and return its bytes.
        [[nodiscard]] std::string pack_rank_image(const std::string& dir, FMI::Utils::peer_num rank) const;

        std::string config_path;
        std::string comm_name;
        FMI::Utils::FaultToleranceConfig config;
        std::string host_id;
        std::shared_ptr<FMI::FT::ControlPlane> control_plane;
    };
}

#endif //FMI_FT_LOCALRANKAGENT_H
