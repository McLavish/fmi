#ifndef FMI_UTILS_CLOCK_H
#define FMI_UTILS_CLOCK_H

namespace FMI::Utils {
    //! Milliseconds of a monotonic clock that stays continuous across a restore on another host.
    /*!
     * Every deadline, budget and window in the migration protocol is a difference of two
     * readings of this clock. CLOCK_MONOTONIC counts from the boot of the host, so a process
     * restored elsewhere sees it jump by the difference of the two hosts' uptimes — hours
     * either way — unless the restorer recreated a time namespace, which needs a privilege a
     * function sandbox does not grant. This clock adds a process-wide offset to the raw reading;
     * `rebase_monotonic_after_restore` sets the offset once, right after a restore, from the
     * wall-clock time that really elapsed (both hosts are NTP-disciplined), so that the readings
     * before and after the stop differ by the migration's true duration and nothing else.
     */
    long monotonic_ms();

    //! The raw CLOCK_MONOTONIC reading, in milliseconds, without the offset.
    long monotonic_raw_ms();

    //! CLOCK_REALTIME in milliseconds.
    long realtime_ms();

    //! Fold the clock jump of a restore into the offset.
    /*!
     * @param mono_before_raw  `monotonic_raw_ms()` taken right before the process stopped
     * @param real_before      `realtime_ms()` taken at the same instant
     * @return the correction applied, in ms: zero when the clock did not jump (a restore on
     *         the same host, a rehearsal, or a restore under a time namespace)
     */
    long rebase_monotonic_after_restore(long mono_before_raw, long real_before);

    //! The offset currently applied by `monotonic_ms()`.
    long monotonic_offset_ms();
}

#endif  // FMI_UTILS_CLOCK_H
