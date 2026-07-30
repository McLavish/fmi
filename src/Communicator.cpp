#include "../include/Communicator.h"
#include "../include/ft/TransparentMigrationRuntime.h"
#include "../include/ft/ControlPlane.h"
#include "../include/ft/experimental/CriuRequirements.h"

#include <chrono>
#include <utility>

namespace {
    std::string make_worker_id(FMI::Utils::peer_num peer_id) {
        auto now = std::chrono::system_clock::now().time_since_epoch();
        auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(now).count();
        return "rank-" + std::to_string(peer_id) + "-pid-" + std::to_string(getpid()) + "-ts-" + std::to_string(millis);
    }
}

namespace FMI {
    Communicator::Communicator(FMI::Utils::peer_num peer_id, FMI::Utils::peer_num num_peers, std::string config_path,
                               std::string comm_name, unsigned int faas_memory,
                               std::string worker_id, std::string placement) {
        Utils::Configuration config(config_path);
        auto ft_config = config.get_fault_tolerance_config();
        this->peer_id = peer_id;
        this->num_peers = num_peers;
        this->config_path = config_path;
        this->faas_memory = faas_memory;

        if (ft_config.enabled && !ft_config.preferred_data_backend.empty()) {
            auto backends = config.get_active_channels();
            if (backends.find(ft_config.preferred_data_backend) == backends.end()) {
                throw std::runtime_error("Preferred data backend " + ft_config.preferred_data_backend + " is not enabled");
            }
        }
        if (ft_config.enabled && ft_config.state_transfer == "criu") {
            // Checkpoint safety covers the whole enabled channel set, not just the preferred
            // backend: every enabled backend is instantiated below and would be captured in the
            // criu image. Checked here, before the control plane connects, so a bad config fails
            // fast on both the rank side and the agent side (LocalRankAgent enforces the same).
            FMI::FT::require_checkpoint_safe_channels(config);
        }

        if (ft_config.enabled) {
            // Resolve worker_id (auto-generate if not provided)
            std::string resolved_worker_id = worker_id.empty() ? make_worker_id(peer_id) : std::move(worker_id);

            auto control_plane = std::make_shared<FMI::FT::ControlPlane>(config_path, comm_name, num_peers);
            std::uint64_t current_epoch = control_plane->epoch();

            if (control_plane->is_rank_pending(peer_id)) {
                // A replacement rank waits for the orchestrator to promote and clear pending.
                // Unbounded by the same contract as the survivor reconfigure wait: only the
                // orchestrator can clear pending / promote the epoch, so a migration that never
                // completes is its responsibility to detect and resolve, not a library timeout.
                // See docs/fault-tolerance.md.
                FMI::Utils::poll_until(
                        [&]() { return !control_plane->is_rank_pending(peer_id); },
                        0, ft_config.poll_interval_ms);
                current_epoch = control_plane->epoch();
            }

            // Contract 3, and deliberately here rather than in join_epoch: this is the one
            // place that runs exactly once per *process*. A survivor rejoining a later epoch
            // does not run it and keeps its lineage, which is what makes its link state still
            // reconcilable; a criu-restored process does not run it either, for the same
            // reason. Only a genuinely new process serving this rank takes a new incarnation,
            // and that is precisely when its peers must start the stream again.
            incarnation = control_plane->claim_incarnation(peer_id);

            std::uint64_t active_epoch = current_epoch;
            // PLANS.md step 4 seam: a future CRIU-backed state restore for replacement ranks
            // belongs here, after the rank joins epoch N+1 and before it resumes user work.
            control_plane->join_epoch(active_epoch, peer_id, resolved_worker_id, placement);

            // Keep the un-qualified name for the data plane: only Direct's pairing names are
            // epoch-qualified, ClientServer keys must stay stable across epochs.
            this->data_comm_name = comm_name;
            this->comm_name = FMI::FT::epoch_comm_name(comm_name, active_epoch);
            build_channels(config);

            operation_runtime = std::make_shared<FMI::FT::TransparentMigrationRuntime>(
                    peer_id, resolved_worker_id, placement, active_epoch,
                    ft_config, control_plane, comm_name,
                    [this](const std::string& new_name, const std::vector<FMI::Utils::peer_num>& moved) {
                        reconfigure_to_epoch(new_name, moved);
                    },
                    [this]() { prepare_channels_for_checkpoint(); },
                    [this]() { finalize_channels(); });
        } else {
            this->comm_name = comm_name;
            this->data_comm_name = comm_name;
            build_channels(config);
        }

        double faas_price = (double) faas_memory / 1024. * config.get_faas_price();
        set_channel_policy(std::make_shared<FMI::Utils::ChannelPolicy>(
                channels, faas_price, channel_hint,
                ft_config.enabled ? ft_config.preferred_data_backend : ""));
    }

    void Communicator::build_channels(Utils::Configuration& config) {
        // this->comm_name must be set before calling this; register_channel propagates it to each
        // channel. get_active_channels() already filters out disabled backends.
        for (auto const& [backend_name, params] : config.get_active_channels()) {
            register_channel(backend_name,
                Comm::Channel::get_channel(backend_name, params.first, params.second));
        }
    }

    void Communicator::reconfigure_to_epoch(const std::string& new_comm_name,
                                            const std::vector<FMI::Utils::peer_num>& moved_ranks) {
        this->comm_name = new_comm_name;
        // Selective re-pair: channels that can reconfigure in place (Direct) keep their
        // surviving peer connections and only drop links to migrated ranks — one migration no
        // longer forces every survivor pair back through the TCPunch rendezvous. Channels that
        // cannot carry state across epochs (ClientServer: per-epoch object names + operation
        // counters) decline, get finalized (releasing their epoch-N objects), and are rebuilt.
        for (auto it = channels.begin(); it != channels.end();) {
            if (it->second->reconfigure_for_epoch(new_comm_name, moved_ranks)) {
                ++it;
            } else {
                it->second->finalize();
                it = channels.erase(it);
            }
        }
        Utils::Configuration config(config_path);
        for (auto const& [backend_name, params] : config.get_active_channels()) {
            if (channels.find(backend_name) == channels.end()) {
                register_channel(backend_name,
                    Comm::Channel::get_channel(backend_name, params.first, params.second));
            }
        }
        // PLANS.md step 4 seam: if CRIU state transfer is integrated into transparent
        // migration, restored rank state must be available before rebuilt channels are used.
        // ChannelPolicy holds a reference to the channels map; it sees the rebuilt entries automatically
    }

    void Communicator::register_channel(std::string name, std::shared_ptr<FMI::Comm::Channel> c) {
        c->set_peer_id(peer_id);
        c->set_num_peers(num_peers);
        c->set_comm_name(comm_name);
        c->set_data_comm_name(data_comm_name);
        c->set_incarnation(incarnation);
        channels[name] = c;
    }

    Communicator::~Communicator() {
        if (operation_runtime != nullptr) {
            operation_runtime->shutdown();
        }
        finalize_channels();
    }

    void Communicator::finalize_channels() {
        for (auto const& [name, channel] : channels) {
            channel->finalize();
        }
    }

    void Communicator::set_channel_policy(std::shared_ptr<FMI::Utils::ChannelPolicy> policy) {
        this->policy = std::move(policy);
    }

    void Communicator::hint(FMI::Utils::Hint hint) {
        this->channel_hint = hint;
        policy->set_hint(hint);
    }

    void Communicator::enter_operation() {
        if (operation_runtime != nullptr) {
            operation_runtime->enter_operation();
        }
    }

    void Communicator::exit_operation() {
        if (operation_runtime != nullptr) {
            operation_runtime->exit_operation();
        }
    }

    void Communicator::prepare_channels_for_checkpoint() {
        // Future CRIU integration point for transparent migration: release transport resources
        // before checkpointing application state on the outgoing rank.
        for (const auto& [name, channel] : channels) {
            channel->prepare_for_checkpoint();
        }
    }
}
