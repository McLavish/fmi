#include "../../include/utils/ChannelPolicy.h"
#include "../../include/comm/Channel.h"

#include <stdexcept>
#include <utility>

FMI::Utils::ChannelPolicy::ChannelPolicy(std::map<std::string, std::shared_ptr<FMI::Comm::Channel>>& channels,
                                         double faas_price, Hint hint) :
        channels(channels),
        faas_price(faas_price),
        hint(hint) {}

std::string FMI::Utils::ChannelPolicy::get_channel(const OperationInfo& op_info) {
    if (channels.empty()) {
        throw std::runtime_error("No channels are registered in the communicator");
    }

    // Pick the channel minimizing the metric the hint cares about. For "fast" that is latency
    // alone, so the (potentially non-trivial) price computation is skipped entirely.
    const std::string* best_name = nullptr;
    double best_metric = 0.0;
    for (const auto& [channel_name, channel] : channels) {
        double latency = channel->get_operation_latency(op_info);
        double metric = (hint == fast)
                ? latency
                : channel->get_operation_price(op_info) + get_faas_price(latency);
        if (best_name == nullptr || metric < best_metric) {
            best_name = &channel_name;
            best_metric = metric;
        }
    }
    return *best_name;
}

double FMI::Utils::ChannelPolicy::get_faas_price(double execution_time) {
    return execution_time * faas_price;
}

void FMI::Utils::ChannelPolicy::set_hint(FMI::Utils::Hint hint) {
    this->hint = hint;
}
