#ifndef FMI_REDIS_H
#define FMI_REDIS_H

#include "ClientServer.h"
#include <map>
#include <memory>
#include <string>
#include <hiredis/hiredis.h>

namespace FMI::Comm {
    //! Channel that uses Redis with the Hiredis client library as storage backend.
    class Redis : public ClientServer {
    public:
        explicit Redis(std::map<std::string, std::string> params, std::map<std::string, std::string> model_params);

        ~Redis();

        void upload_object(channel_data buf, std::string name) override;

        bool download_object(channel_data buf, std::string name) override;

        void delete_object(std::string name) override;

        std::vector<std::string> get_object_names() override;

        double get_latency(Utils::peer_num producer, Utils::peer_num consumer, std::size_t size_in_bytes) override;

        double get_price(Utils::peer_num producer, Utils::peer_num consumer, std::size_t size_in_bytes) override;

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

        std::string hostname;
        int port;
        redisContext* context;
        //! One log line per outage, not one per operation: the poll loops call this thousands of times.
        bool connection_warned = false;
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
