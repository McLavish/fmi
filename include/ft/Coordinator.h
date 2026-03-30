#ifndef FMI_FT_COORDINATOR_H
#define FMI_FT_COORDINATOR_H

#include "../utils/Common.h"
#include "Common.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

namespace FMI::FT {
    class Session;

    //! Control-plane client used both by FMI::FT::Session and by external daemons that request migrations.
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

        //! Remove all FT metadata associated with this communicator name.
        void clear_job_state();

        //! Return the currently active communicator epoch.
        [[nodiscard]] std::uint64_t epoch() const;

    private:
        struct Impl;
        std::shared_ptr<Impl> impl;

        friend class Session;

        void ensure_job() const;
        [[nodiscard]] bool has_pending_migration() const;
        [[nodiscard]] bool is_rank_pending(FMI::Utils::peer_num rank) const;
        void register_rank(std::uint64_t epoch, FMI::Utils::peer_num rank, const std::string& worker_id, RankState state) const;
        void set_rank_state(std::uint64_t epoch, FMI::Utils::peer_num rank, RankState state) const;
        [[nodiscard]] std::string worker_for_rank(std::uint64_t epoch, FMI::Utils::peer_num rank) const;
        void refresh_lease(std::uint64_t epoch, FMI::Utils::peer_num rank, const std::string& worker_id) const;
        [[nodiscard]] std::size_t live_member_count(std::uint64_t epoch) const;
        void promote_epoch(std::uint64_t next_epoch) const;
        [[nodiscard]] unsigned int heartbeat_ms() const;
        [[nodiscard]] unsigned int lease_ms() const;
    };
}

#endif //FMI_FT_COORDINATOR_H
