#include "../include/Communicator.h"
#include "../include/ft/TransparentMigrationRuntime.h"
#include "../include/ft/ControlPlane.h"

#include <chrono>
#include <utility>

namespace {
    std::string make_worker_id(FMI::Utils::peer_num peer_id) {
        auto now = std::chrono::system_clock::now().time_since_epoch();
        auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(now).count();
        return "rank-" + std::to_string(peer_id) + "-pid-" + std::to_string(getpid()) + "-ts-" + std::to_string(millis);
    }

    std::string epoch_comm_name(const std::string& base, std::uint64_t epoch) {
        return base + "@epoch=" + std::to_string(epoch);
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

            std::uint64_t active_epoch = current_epoch;
            control_plane->register_rank(active_epoch, peer_id, resolved_worker_id, FMI::FT::RankState::Active);
            // PLANS.md step 4 seam: a future CRIU-backed state restore for replacement ranks
            // belongs here, after the rank joins epoch N+1 and before it resumes user work.
            if (!placement.empty()) {
                control_plane->set_placement(active_epoch, peer_id, placement);
            }

            this->comm_name = epoch_comm_name(comm_name, active_epoch);
            build_channels(this->comm_name);

            double gib_second_price = config.get_faas_price();
            double faas_price = (double) faas_memory / 1024. * gib_second_price;
            std::string preferred = ft_config.preferred_data_backend;
            set_channel_policy(std::make_shared<FMI::Utils::ChannelPolicy>(
                    channels, faas_price, channel_hint, preferred));

            operation_runtime = std::make_shared<FMI::FT::TransparentMigrationRuntime>(
                    peer_id, resolved_worker_id, placement, active_epoch,
                    ft_config, control_plane, comm_name,
                    [this](const std::string& new_name) { reconfigure_to_epoch(new_name); },
                    [this]() { prepare_channels_for_checkpoint(); });
        } else {
            this->comm_name = comm_name;
            build_channels(this->comm_name);

            double gib_second_price = config.get_faas_price();
            double faas_price = (double) faas_memory / 1024. * gib_second_price;
            set_channel_policy(std::make_shared<FMI::Utils::ChannelPolicy>(
                    channels, faas_price, channel_hint));
        }
    }

    void Communicator::build_channels(const std::string&) {
        // this->comm_name must be set before calling this; register_channel propagates it to each channel
        Utils::Configuration config(config_path);
        for (auto const& [backend_name, params] : config.get_active_channels()) {
            auto backend_params = params.first;
            auto model_params = params.second;
            if (backend_params.find("enabled")->second == "true") {
                register_channel(backend_name,
                    Comm::Channel::get_channel(backend_name, backend_params, model_params));
            }
        }
    }

    void Communicator::reconfigure_to_epoch(const std::string& new_comm_name) {
        for (auto const& [name, channel] : channels) {
            channel->finalize();
        }
        channels.clear();
        this->comm_name = new_comm_name;
        build_channels(new_comm_name);
        // PLANS.md step 4 seam: if CRIU state transfer is integrated into transparent
        // migration, restored rank state must be available before rebuilt channels are used.
        // ChannelPolicy holds a reference to the channels map; it sees the rebuilt entries automatically
    }

    void Communicator::register_channel(std::string name, std::shared_ptr<FMI::Comm::Channel> c) {
        c->set_peer_id(peer_id);
        c->set_num_peers(num_peers);
        c->set_comm_name(comm_name);
        channels[name] = c;
    }

    Communicator::~Communicator() {
        if (operation_runtime != nullptr) {
            operation_runtime->shutdown();
        }
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
