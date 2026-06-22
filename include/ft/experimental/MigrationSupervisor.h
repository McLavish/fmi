#ifndef FMI_FT_MIGRATIONSUPERVISOR_H
#define FMI_FT_MIGRATIONSUPERVISOR_H

#include "../../utils/Common.h"
#include "../../utils/Configuration.h"
#include "../Coordinator.h"

#include <cstdint>
#include <memory>
#include <string>

namespace FMI::FT {
    //! Host-local driver for CRIU-backed single-rank transparent migration (same-host v1).
    //!
    //! It bridges the transparent-migration control plane (Redis) to criu: when a targeted
    //! rank reaches its migration quiesce point it publishes a checkpoint-ready image entry in
    //! the CRIU rank registry (pid + host + QUIESCED). The supervisor waits for that entry,
    //! `criu dump`s the rank's process image, `criu restore`s it (preserving application
    //! memory), then promotes the communicator epoch so survivors and the restored rank
    //! reconfigure across the epoch cut. The targeted rank stays a plain FMI::Communicator
    //! application; it never invokes criu itself.
    class MigrationSupervisor {
    public:
        MigrationSupervisor(std::string config_path, std::string comm_name, FMI::Utils::peer_num num_peers);

        //! Migrate a single logical rank: wait for it to publish a checkpoint-ready image on
        //! this host, criu-dump it, criu-restore it, then promote the epoch. Returns the
        //! promoted epoch. Throws FMI::Utils::Timeout if the rank never becomes ready.
        std::uint64_t migrate_rank(FMI::Utils::peer_num rank) const;

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
        [[nodiscard]] CriuRankInfo wait_for_ready_rank(FMI::Utils::peer_num rank, std::uint64_t target_epoch) const;
        [[nodiscard]] std::string rank_image_dir(std::uint64_t target_epoch, FMI::Utils::peer_num rank) const;
        void dump_rank(int pid, const std::string& dir) const;
        void restore_rank(const std::string& dir) const;

        std::string config_path;
        std::string comm_name;
        FMI::Utils::peer_num num_peers;
        FMI::Utils::FaultToleranceConfig config;
        std::string host_id;
        std::shared_ptr<FMI::FT::Coordinator> coordinator;
    };
}

#endif //FMI_FT_MIGRATIONSUPERVISOR_H
