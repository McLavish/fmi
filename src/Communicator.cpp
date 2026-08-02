#include "../include/Communicator.h"

#include <utility>

namespace FMI {
    Communicator::Communicator(FMI::Utils::peer_num peer_id, FMI::Utils::peer_num num_peers, std::string config_path,
                               std::string comm_name, unsigned int faas_memory) {
        Utils::Configuration config(config_path);
        this->peer_id = peer_id;
        this->num_peers = num_peers;
        this->comm_name = comm_name;
        build_channels(config);

        double faas_price = (double) faas_memory / 1024. * config.get_faas_price();
        set_channel_policy(std::make_shared<FMI::Utils::ChannelPolicy>(channels, faas_price, channel_hint));
    }

    void Communicator::build_channels(Utils::Configuration& config) {
        // this->comm_name must be set before calling this; register_channel propagates it to each
        // channel. get_active_channels() already filters out disabled backends.
        for (auto const& [backend_name, params] : config.get_active_channels()) {
            register_channel(backend_name,
                Comm::Channel::get_channel(backend_name, params.first, params.second));
        }
    }

    void Communicator::register_channel(std::string name, std::shared_ptr<FMI::Comm::Channel> c) {
        c->set_peer_id(peer_id);
        c->set_num_peers(num_peers);
        c->set_comm_name(comm_name);
        c->set_incarnation(incarnation);
        channels[name] = c;
    }

    Communicator::~Communicator() {
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
}
