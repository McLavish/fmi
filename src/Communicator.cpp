#include "../include/Communicator.h"
#ifdef FMI_ENABLE_CRIU
#include "../include/ft/CriuRuntime.h"
#endif
#include "../include/ft/TransparentMigrationRuntime.h"
#include "../include/ft/Coordinator.h"

#include <chrono>
#include <thread>
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

        if (ft_config.enabled && ft_config.mode == FMI::FT::Mode::CriuCoordinated) {
#ifndef FMI_ENABLE_CRIU
            throw std::runtime_error("FMI built without CRIU support");
#else
            auto backends = config.get_active_channels();
            if (backends.find("Direct") == backends.end()) {
                throw std::runtime_error("CRIU-coordinated fault tolerance requires the Direct backend");
            }
            if (backends.size() != 1 || !ft_config.preferred_data_backend.empty() && ft_config.preferred_data_backend != "Direct") {
                throw std::runtime_error("CRIU-coordinated fault tolerance only supports the Direct backend");
            }
#endif
        }
        if (ft_config.enabled && !ft_config.preferred_data_backend.empty()) {
            auto backends = config.get_active_channels();
            if (backends.find(ft_config.preferred_data_backend) == backends.end()) {
                throw std::runtime_error("Preferred data backend " + ft_config.preferred_data_backend + " is not enabled");
            }
        }

        if (ft_config.enabled && ft_config.mode == FMI::FT::Mode::TransparentMigration) {
            // Resolve worker_id (auto-generate if not provided)
            std::string resolved_worker_id = worker_id.empty() ? make_worker_id(peer_id) : std::move(worker_id);

            auto coordinator = std::make_shared<FMI::FT::Coordinator>(config_path, comm_name, num_peers);
            std::uint64_t current_epoch = coordinator->epoch();

            // Detect replacement: pending for this rank AND a different worker already holds it
            std::string existing_worker = coordinator->worker_for_rank(current_epoch, peer_id);
            bool is_replacement = coordinator->is_rank_pending(peer_id) &&
                                  !existing_worker.empty() &&
                                  existing_worker != resolved_worker_id;

            std::uint64_t active_epoch;
            if (is_replacement) {
                // Join the next epoch and block until it is promoted
                active_epoch = current_epoch + 1;
                auto start = std::chrono::steady_clock::now();
                while (true) {
                    auto observed = coordinator->epoch();
                    if (observed > current_epoch) {
                        active_epoch = observed;
                        coordinator->register_rank(active_epoch, peer_id, resolved_worker_id, FMI::FT::RankState::Active);
                        if (!placement.empty()) {
                            coordinator->set_placement(active_epoch, peer_id, placement);
                        }
                        coordinator->refresh_lease(active_epoch, peer_id, resolved_worker_id);
                        break;
                    }
                    coordinator->register_rank(active_epoch, peer_id, resolved_worker_id, FMI::FT::RankState::Replaced);
                    if (!placement.empty()) {
                        coordinator->set_placement(active_epoch, peer_id, placement);
                    }
                    coordinator->refresh_lease(active_epoch, peer_id, resolved_worker_id);
                    if (coordinator->live_member_count(active_epoch) >= num_peers) {
                        coordinator->promote_epoch(active_epoch);
                    }
                    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now() - start).count();
                    if (static_cast<unsigned int>(elapsed) >= coordinator->lease_ms()) {
                        throw FMI::Utils::Timeout();
                    }
                    std::this_thread::sleep_for(std::chrono::milliseconds(coordinator->heartbeat_ms()));
                }
            } else {
                active_epoch = current_epoch;
                coordinator->register_rank(active_epoch, peer_id, resolved_worker_id, FMI::FT::RankState::Active);
                if (!placement.empty()) {
                    coordinator->set_placement(active_epoch, peer_id, placement);
                }
                coordinator->refresh_lease(active_epoch, peer_id, resolved_worker_id);
            }

            this->comm_name = epoch_comm_name(comm_name, active_epoch);
            build_channels(this->comm_name);

            double gib_second_price = config.get_faas_price();
            double faas_price = (double) faas_memory / 1024. * gib_second_price;
            std::string preferred = ft_config.preferred_data_backend;
            set_channel_policy(std::make_shared<FMI::Utils::ChannelPolicy>(
                    channels, num_peers, faas_price, channel_hint, preferred));

            operation_runtime = std::make_shared<FMI::FT::TransparentMigrationRuntime>(
                    peer_id, num_peers, resolved_worker_id, placement, active_epoch,
                    ft_config, coordinator, comm_name,
                    [this](const std::string& new_name) { reconfigure_to_epoch(new_name); });
        } else {
            // Non-FT or CriuCoordinated path
            this->comm_name = comm_name;
            build_channels(this->comm_name);

            double gib_second_price = config.get_faas_price();
            double faas_price = (double) faas_memory / 1024. * gib_second_price;
            std::string preferred_backend;
            if (ft_config.enabled && ft_config.mode == FMI::FT::Mode::CriuCoordinated) {
                preferred_backend = ft_config.preferred_data_backend.empty() ? "Direct" : ft_config.preferred_data_backend;
            } else if (ft_config.enabled) {
                preferred_backend = ft_config.preferred_data_backend;
            }
            set_channel_policy(std::make_shared<FMI::Utils::ChannelPolicy>(
                    channels, num_peers, faas_price, channel_hint, preferred_backend));

            if (ft_config.enabled && ft_config.mode == FMI::FT::Mode::CriuCoordinated) {
#ifdef FMI_ENABLE_CRIU
                operation_runtime = std::make_shared<FMI::FT::CriuRuntime>(
                        peer_id, num_peers, std::move(config_path), this->comm_name,
                        "Direct",
                        [this]() { prepare_channels_for_checkpoint(); });
#endif
            }
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
        for (const auto& [name, channel] : channels) {
            channel->prepare_for_checkpoint();
        }
    }
}
