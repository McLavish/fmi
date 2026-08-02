#ifndef FMI_REDIS_H
#define FMI_REDIS_H

#include "RecoverableClientServer.h"
#include <sys/types.h>
#include <chrono>
#include <map>
#include <memory>
#include <string>
#include <hiredis/hiredis.h>

namespace FMI::Comm {
    //! Channel that uses Redis with the Hiredis client library as storage backend.
    class Redis : public RecoverableClientServer {
    public:
        explicit Redis(std::map<std::string, std::string> params, std::map<std::string, std::string> model_params);

        ~Redis();

        void upload_object(channel_data buf, std::string name) override;

        bool download_object(channel_data buf, std::string name) override;

        void delete_object(std::string name) override;

        std::vector<std::string> get_object_names() override;

        double get_latency(Utils::peer_num producer, Utils::peer_num consumer, std::size_t size_in_bytes) override;

        double get_price(Utils::peer_num producer, Utils::peer_num consumer, std::size_t size_in_bytes) override;

    protected:
        //! The descriptor the current connection is on, or -1 when there is no connection.
        /*!
         * A test seam, not part of the channel contract: it exists so that a case can
         * shutdown() the descriptor and leave this channel holding exactly what a
         * `criu restore --tcp-close` hands a restored rank — a valid fd whose connection is
         * gone, with the context none the wiser (context->err is still 0). Nothing in the
         * library reads it, and no caller may close it: the channel owns the number.
         */
        int connection_fd() const;

    private:
        //! Reply ownership for the command helper; freeReplyObject is NULL-safe.
        struct ReplyDeleter {
            void operator()(redisReply* reply) const { freeReplyObject(reply); }
        };

        //! An owned reply, or empty when the command could not be completed at all.
        using ReplyPtr = std::unique_ptr<redisReply, ReplyDeleter>;

        //! Issue one command, argument by argument. Empty return means "could not complete".
        ReplyPtr command(int argc, const char** argv, const std::size_t* argvlen);

        //! Drop whatever context exists and dial a fresh one. Logs on failure, never throws.
        void connect();

        //! Free the context and forget it, remembering why it died.
        void drop_connection();

        //! Why the last command could not be completed, for messages built after the context is gone.
        std::string last_failure() const;

        //! How many times a poll-loop-shaped retry may run before the caller's budget is spent.
        /*!
         * The ClientServer download loops give up after ceil(max_timeout / timeout) passes; an upload,
         * which has nobody polling on its behalf, retries under the same budget so that a rank
         * writing and a rank waiting for what it writes give up at roughly the same moment.
         */
        unsigned int poll_attempts() const;

        std::string hostname;
        int port;
        redisContext* context;
        //! Connect and per-command timeouts, and whether they are applied at all.
        /*!
         * Without them hiredis blocks on the socket indefinitely, so a store that accepts the
         * connection and then stops answering — the shape a frozen or partitioned server has —
         * hangs the rank inside one command, where no poll budget can see it.
         *
         * Deliberately NOT derived from the poll `timeout`: that is the sleep between polls and
         * is 1 ms in every shipped config, which as a socket timeout would abort healthy commands
         * on any loaded machine. They are separate knobs because they measure different things.
         */
        long connect_timeout_ms = 1000;
        long io_timeout_ms = 1000;
        bool apply_timeouts = false;
        //! The process that dialled the current connection; see command().
        pid_t owner_pid = -1;
        //! No dial before this instant, under recover; set by connect() after one fails.
        /*!
         * A dial against a store that answers nothing costs connect_timeout_ms, while the poll
         * loop that asked for it charges its budget one `timeout` — 1 ms in the shipped configs.
         * Without this the two diverge by that ratio and max_timeout stops bounding anything.
         * See connect().
         */
        std::chrono::steady_clock::time_point dial_not_before {};
        //! One log line per outage, not one per operation: the poll loops call this thousands of times.
        bool connection_warned = false;
        //! The same latch for downloads, which swallow a connection failure under recover.
        bool download_warned = false;
        //! Error text of the context that carried the last failure; it is freed before the caller reports.
        std::string last_error;
        // Model params
        double bandwidth_single;
        double bandwidth_multiple;
        double overhead;
        double transfer_price;
        double instance_price;
        unsigned int requests_per_hour;
        bool include_infrastructure_costs;

    };
}

#endif //FMI_REDIS_H
