#ifndef FMI_S3_H
#define FMI_S3_H

#include "RecoverableClientServer.h"
#include <chrono>
#include <map>
#include <string>
#include <aws/s3/S3Client.h>
#include <aws/core/Aws.h>
#include <boost/interprocess/streams/bufferstream.hpp>

namespace FMI::Comm {
    //! Channel that uses AWS S3 as backend and uses the AWS SDK for C++ to access S3.
    class S3 : public RecoverableClientServer {
    public:
        explicit S3(std::map<std::string, std::string> params, std::map<std::string, std::string> model_params);

        ~S3();

        void upload_object(channel_data buf, std::string name) override;

        bool download_object(channel_data buf, std::string name) override;

        void delete_object(std::string name) override;

        //! One DeleteObjects request per thousand names instead of one request per name.
        void delete_objects(const std::vector<std::string>& names) override;

        std::vector<std::string> get_object_names() override;

        double get_latency(Utils::peer_num producer, Utils::peer_num consumer, std::size_t size_in_bytes) override;

        double get_price(Utils::peer_num producer, Utils::peer_num consumer, std::size_t size_in_bytes) override;

    private:
        //! Turn one failed GetObject into the answer download_object owes its caller, or an error.
        bool report_download_failure(const Aws::S3::S3Error& error, const std::string& name,
                                     std::chrono::steady_clock::time_point started);

        //! Count one failed request that might pass, and give up once it has gone on too long.
        void note_transient_failure(const Aws::S3::S3Error& error, const std::string& name,
                                    std::chrono::steady_clock::time_point started);

        //! The store answered a request: nothing is outstanding against it any more.
        void store_answered();

        //! bucket, key, HTTP status and exception name of a failed request, for a log or a throw.
        std::string describe(const Aws::S3::S3Error& error, const std::string& name) const;

        std::string bucket_name;
        std::unique_ptr<Aws::S3::S3Client, Aws::Deleter<Aws::S3::S3Client>> client;
        //! How many failures in a row that might pass may happen before this channel gives up.
        unsigned int transient_failure_limit = 200;
        //! Failures in a row that might pass; reset whenever the store answers, absence included.
        unsigned int consecutive_transient_failures = 0;
        //! When the request that opened the current run of failures was issued.
        /*!
         * The other half of the same guard: the count alone bounds a store that fails quickly,
         * this bounds one that fails slowly. Only meaningful while the count above is non-zero.
         */
        std::chrono::steady_clock::time_point failing_since{};
        //! Latches so that a condition lasting for thousands of polls is reported once, not once per poll.
        bool refusal_warned = false;
        bool transient_warned = false;
        // Model params
        double bandwidth;
        double overhead;
        double transfer_price;
        double download_price;
        double upload_price;

    };
}


#endif //FMI_S3_H
