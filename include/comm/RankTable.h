#ifndef FMI_RANKTABLE_H
#define FMI_RANKTABLE_H

#include "../utils/Common.h"

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <string>
#include <vector>

namespace FMI::Comm {

    //! What is known about one rank: where it listens, which generation it is, whether it is away.
    struct RankRow {
        bool known = false;
        std::uint64_t generation = 0;
        //! True from the moment the coordinator decides to move the rank until it is back.
        bool leaving = false;
        //! The rank finalized its channel. Nothing follows this row.
        bool gone = false;
        std::string ip;
        int port = 0;
        std::uint64_t nonce = 0;

        //! Orders every row a rank can ever have: here(g) < leaving(g) < here(g + 1) < gone.
        [[nodiscard]] std::uint64_t version() const {
            if (!known) {
                return 0;
            }
            return gone ? UINT64_MAX : generation * 2 + (leaving ? 1 : 0) + 1;
        }

        [[nodiscard]] bool dialable() const { return known && !leaving && !gone && port != 0; }
    };

    //! A rank's copy of the rank table. Rows only move forward.
    /*!
     * A row is applied only when it is newer than the one held. A repeated row, a late row and
     * rows arriving in any order therefore leave the table where the newest one put it; there is
     * no log to replay and no cursor to keep. The one exception lets an address fill in a row of
     * the same version that was learned from a hello, which names a generation but no address.
     */
    class RankTable {
    public:
        void resize(Utils::peer_num ranks) {
            std::lock_guard<std::mutex> lock(mu);
            rows.assign(ranks, RankRow{});
        }

        //! True when @p row replaced the row held for @p rank.
        bool apply(Utils::peer_num rank, const RankRow& row) {
            {
                std::lock_guard<std::mutex> lock(mu);
                if (rank >= rows.size() || !row.known) {
                    return false;
                }
                RankRow& held = rows[rank];
                const bool newer = row.version() > held.version();
                const bool fills_address =
                        row.version() == held.version() && held.port == 0 && row.port != 0;
                if (!newer && !fills_address) {
                    return false;
                }
                held = row;
            }
            changed.notify_all();
            return true;
        }

        [[nodiscard]] RankRow get(Utils::peer_num rank) const {
            std::lock_guard<std::mutex> lock(mu);
            return rank < rows.size() ? rows[rank] : RankRow{};
        }

        //! Wait until the row of @p rank is newer than @p version, at most @p wait_ms.
        void wait_newer(Utils::peer_num rank, std::uint64_t version, long wait_ms) const {
            std::unique_lock<std::mutex> lock(mu);
            changed.wait_for(lock, std::chrono::milliseconds(wait_ms > 0 ? wait_ms : 0),
                             [&]() { return rank < rows.size() && rows[rank].version() > version; });
        }

        //! Wake every waiter, for a channel that is shutting down or has lost its host manager.
        void wake_all() { changed.notify_all(); }

    private:
        mutable std::mutex mu;
        mutable std::condition_variable changed;
        std::vector<RankRow> rows;
    };
}

#endif //FMI_RANKTABLE_H
