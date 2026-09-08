#include "../../include/utils/Clock.h"

#include <atomic>
#include <chrono>

namespace {
    std::atomic<long> offset_ms{0};
}

long FMI::Utils::monotonic_raw_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::steady_clock::now().time_since_epoch())
            .count();
}

long FMI::Utils::realtime_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::system_clock::now().time_since_epoch())
            .count();
}

long FMI::Utils::monotonic_ms() {
    return monotonic_raw_ms() + offset_ms.load(std::memory_order_acquire);
}

long FMI::Utils::monotonic_offset_ms() {
    return offset_ms.load(std::memory_order_acquire);
}

long FMI::Utils::rebase_monotonic_after_restore(long mono_before_raw, long real_before) {
    const long real_elapsed = realtime_ms() - real_before;
    const long mono_elapsed = monotonic_raw_ms() - mono_before_raw;
    const long correction = real_elapsed - mono_elapsed;
    offset_ms.fetch_add(correction, std::memory_order_acq_rel);
    return correction;
}
