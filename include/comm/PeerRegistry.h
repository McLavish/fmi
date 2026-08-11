#ifndef FMI_PEERREGISTRY_H
#define FMI_PEERREGISTRY_H

#include "../utils/Common.h"

#include <hiredis/hiredis.h>

#include <sys/time.h>
#include <sys/types.h>

#include <map>
#include <mutex>
#include <string>
#include <vector>

namespace FMI::Comm {
    //! Minimal Redis client for the peer registry.
    /*!
     * Commands go out through redisCommandArgv with explicit argument lengths, and a dead context
     * is reconnected and the batch retried once. Deliberately NOT built like the Redis channel,
     * which concatenates commands into a string and passes it as a printf format — a comm_name
     * containing '%' or a space corrupts those.
     */
    class PeerRegistry {
    public:
        PeerRegistry(std::string host, int port);

        ~PeerRegistry();

        void disconnect();

        //! HSET + EXPIRE pipelined into a single round trip.
        void publish(const std::string& key, const std::string& field, const std::string& value,
                     unsigned int ttl_s, long timeout_ms);

        //! Whole rank -> "ip:port:nonce" map in one round trip. Never one HGET per peer.
        std::map<FMI::Utils::peer_num, std::string> snapshot(const std::string& key, long timeout_ms);

    private:
        void close_locked();

        //! Clamp every subsequent connect and command to what is left of the caller's deadline.
        /*!
         * Without this, redisConnect and redisGetReply block on the socket with no timeout at all,
         * so an unreachable or wedged registry hangs establishment indefinitely — the mesh deadline
         * is only ever checked between calls, never inside one.
         */
        void set_timeout_locked(long timeout_ms);

        void connect_locked();

        //! Issue a batch and collect its replies. Retries the whole batch once on a dead context —
        //! including after a fork, where the inherited connection must not be reused (two processes
        //! reading one RESP stream steal each other's replies).
        std::vector<redisReply*> pipeline_locked(const std::vector<std::vector<std::string>>& batch);

        std::string host;
        int port;
        redisContext* context = nullptr;
        //! Connect and command timeout; reset from the caller's remaining deadline before each op.
        struct timeval io_timeout{1, 0};
        pid_t owner_pid = -1;
        std::vector<redisReply*> owned;
        std::mutex mutex;
    };
}

#endif //FMI_PEERREGISTRY_H
