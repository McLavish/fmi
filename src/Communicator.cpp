#include "../include/Communicator.h"
#include "../include/ft/CriuRuntime.h"

#include <utility>
namespace FMI {
    Communicator::Communicator(FMI::Utils::peer_num peer_id, FMI::Utils::peer_num num_peers, std::string config_path, std::string comm_name,
                               unsigned int faas_memory) {
        Utils::Configuration config(config_path);
        auto ft_config = config.get_fault_tolerance_config();
        this->peer_id = peer_id;
        this->num_peers = num_peers;
        this->comm_name = comm_name;
        auto backends = config.get_active_channels();
        if (ft_config.enabled && ft_config.mode == FMI::FT::Mode::CriuCoordinated) {
            if (backends.find("Direct") == backends.end()) {
                throw std::runtime_error("CRIU-coordinated fault tolerance requires the Direct backend");
            }
            if (backends.size() != 1 || !ft_config.preferred_data_backend.empty() && ft_config.preferred_data_backend != "Direct") {
                throw std::runtime_error("CRIU-coordinated fault tolerance only supports the Direct backend");
            }
        }
        if (ft_config.enabled && !ft_config.preferred_data_backend.empty() &&
            backends.find(ft_config.preferred_data_backend) == backends.end()) {
            throw std::runtime_error("Preferred data backend " + ft_config.preferred_data_backend + " is not enabled");
        }
        for (auto const& [backend_name, params] : backends) {
            auto backend_params = params.first;
            auto model_params = params.second;
            if (backend_params.find("enabled")->second == "true") {
                register_channel(backend_name, Comm::Channel::get_channel(backend_name, backend_params, model_params));
            }
        }
        if (ft_config.enabled && !ft_config.preferred_data_backend.empty() &&
            channels.find(ft_config.preferred_data_backend) == channels.end()) {
            throw std::runtime_error("Preferred data backend " + ft_config.preferred_data_backend + " is not available");
        }
        double gib_second_price = config.get_faas_price();
        double faas_price = (double) faas_memory / 1024. * gib_second_price;
        std::string preferred_backend;
        if (ft_config.enabled) {
            preferred_backend = ft_config.preferred_data_backend;
            if (ft_config.mode == FMI::FT::Mode::CriuCoordinated && preferred_backend.empty()) {
                preferred_backend = "Direct";
            }
        }
        set_channel_policy(std::make_shared<FMI::Utils::ChannelPolicy>(
                channels,
                num_peers,
                faas_price,
                channel_hint,
                preferred_backend));

        if (ft_config.enabled && ft_config.mode == FMI::FT::Mode::CriuCoordinated) {
            criu_runtime = std::make_shared<FMI::FT::CriuRuntime>(
                    peer_id,
                    num_peers,
                    std::move(config_path),
                    this->comm_name,
                    "Direct",
                    [this]() { prepare_channels_for_checkpoint(); });
        }
    }

    void Communicator::register_channel(std::string name, std::shared_ptr<FMI::Comm::Channel> c) {
        c->set_peer_id(peer_id);
        c->set_num_peers(num_peers);
        c->set_comm_name(comm_name);
        channels[name] = c;
    }

    Communicator::~Communicator() {
        if (criu_runtime != nullptr) {
            criu_runtime->shutdown();
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
        if (criu_runtime != nullptr) {
            criu_runtime->enter_operation();
        }
    }

    void Communicator::exit_operation() {
        if (criu_runtime != nullptr) {
            criu_runtime->exit_operation();
        }
    }

    void Communicator::prepare_channels_for_checkpoint() {
        for (const auto& [name, channel] : channels) {
            channel->prepare_for_checkpoint();
        }
    }

}
