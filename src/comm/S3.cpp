#include "../../include/comm/S3.h"
#include "../../include/utils/Signals.h"

#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/client/RetryStrategy.h>
#include <aws/core/http/Scheme.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/ListObjectsRequest.h>

#include <boost/log/trivial.hpp>

#include <cmath>
#include <mutex>
#include <stdexcept>
#include <string>

namespace {

    const char TAG[] = "S3Client";

    //! Bring the AWS SDK up once per process, and deliberately never take it back down.
    /*!
     * Aws::InitAPI / Aws::ShutdownAPI are process-global and, per aws-sdk-cpp issue #456, may be
     * called at most once each: a Shutdown followed by another Init is not supported and crashes
     * in the CRT's static state. The instance counter this replaces did exactly that as soon as a
     * job dropped its last S3 channel and built another one — a communicator destroyed and
     * re-created, which is what every test binary and the checkpoint sweep's subject do — and it
     * counted without synchronisation while claiming to guard against multiple communicators.
     *
     * So the SDK is initialised behind a std::call_once and shut down never. The global state it
     * holds (curl, the CRT allocators, the HTTP client factory) is leaked on purpose: it is
     * bounded, it is a per-process constant, and the process is exiting anyway. The options object
     * is a function-local static for the same reason — ShutdownAPI is the only thing that would
     * ever read it again.
     */
    void initialise_sdk_once() {
        static std::once_flag sdk_initialised;
        std::call_once(sdk_initialised, [] {
            static Aws::SDKOptions options;
            // The SDK sets CURLOPT_NOSIGNAL(1) on every handle it hands out
            // (aws-cpp-sdk-core CurlHandleContainer.cpp:167), which is precisely the option that
            // stops libcurl from installing its own SIGPIPE guard around a transfer: with it set,
            // a write to a socket the peer has closed reaches the process as a signal. That is the
            // first thing a restored rank does — criu --tcp-close leaves every connection the
            // image captured already dead — and it is the same killer the runbook README documents
            // for hiredis. This flag makes the SDK install a handler that logs and swallows.
            // It replaces an application's own SIGPIPE handler, on the same terms
            // Utils::suppress_sigpipe already states: whatever is installed when the first FMI
            // channel is built does not survive.
            options.httpOptions.installSigPipeHandler = true;
            Aws::InitAPI(options);
        });
    }

    //! A required configuration value, or an error naming the key.
    std::string required(const std::map<std::string, std::string>& params, const std::string& key) {
        auto it = params.find(key);
        if (it == params.end() || it->second.empty()) {
            throw std::runtime_error("S3: " + key + " is required in the S3 configuration block");
        }
        return it->second;
    }

    //! A configured number, clamped to a sane range, or the default when the key is absent.
    long numeric_param(const std::map<std::string, std::string>& params, const std::string& key,
                       long fallback, long low, long high) {
        auto it = params.find(key);
        if (it == params.end() || it->second.empty()) {
            return fallback;
        }
        long value = 0;
        try {
            value = std::stol(it->second);
        } catch (const std::exception&) {
            // std::stol's own complaint names neither the key nor the backend, and configuration
            // reaches a channel as untyped strings: this is the only place that knows which value
            // was not a number.
            throw std::runtime_error("S3: " + key + " must be a number, got \"" + it->second + "\"");
        }
        return std::min(high, std::max(low, value));
    }

}

//! Build the client explicitly. Every bound this channel has is set here or it does not exist.
/*!
 * The defaults this constructor overrides are not neutral. An SDK-default client waits for the
 * operating system's connect timeout on an unreachable endpoint, has no request timeout at all
 * (a stalled transfer hangs the rank until something else notices), opens up to 25 connections
 * per client, and picks its retry policy from the SDK version plus whatever AWS_RETRY_MODE
 * happens to be in the environment — so the same code retries three times on one machine and not
 * at all on another. Under recover those are exactly the knobs the poll budget's arithmetic
 * assumes: an operation has to fail inside the budget for the budget to be the failure detector.
 */
FMI::Comm::S3::S3(std::map<std::string, std::string> params, std::map<std::string, std::string> model_params) : RecoverableClientServer(params) {
    // The recovery contract is split between this class and the family above it, and only the
    // family's half exists here so far. Accepting the flag would turn on exactly the halves that
    // are dangerous without the other: finalize would stop deleting and upload would stop
    // recording what it wrote, while this backend still writes plain PutObjects with no expiry —
    // FMI configures no bucket lifecycle rule — so every object of every run would stay in the
    // bucket, and be billed for, forever. It would also promise what upload_object below does not
    // do (a failed write is logged and dropped, under a flag whose whole point is that a write is
    // a delivery obligation) and what download_object does not check (a short object is copied and
    // reported as a complete read).
    //
    // Refused rather than ignored: an operator who wrote it down asked for those guarantees, and a
    // channel that quietly gives them a different set is worse than one that says no. Remove this
    // when the S3 half lands — the TTL by lifecycle rule, the upload escalation, the length check.
    if (recover) {
        throw std::runtime_error("S3: \"recover\" is not implemented by this backend "
                                 "(no object expiry, no upload escalation, no length check); "
                                 "remove it from the S3 configuration block");
    }
    // See Utils::suppress_sigpipe. Called before the SDK is initialised so that a process which
    // has no SIGPIPE disposition of its own gets one either way, whichever of the two runs first.
    Utils::suppress_sigpipe();
    initialise_sdk_once();

    bucket_name = required(params, "bucket_name");
    bandwidth = std::stod(model_params["bandwidth"]);
    overhead = std::stod(model_params["overhead"]);
    transfer_price = std::stod(model_params["transfer_price"]);
    download_price = std::stod(model_params["download_price"]);
    upload_price = std::stod(model_params["upload_price"]);

    // IMDS off. On anything that is not an EC2 instance — a laptop, a container, a Lambda — the
    // instance-metadata probe is a connect to a link-local address that nothing answers, paid for
    // in the credential chain and again in region resolution, before the first request goes out.
    Aws::Client::ClientConfigurationInitValues init_values;
    init_values.shouldDisableIMDS = true;
    Aws::S3::S3ClientConfiguration config(init_values);
    // Required, as before: an empty region makes the endpoint resolver build a URL nobody serves,
    // and the failure surfaces as an unexplained request error much later.
    config.region = required(params, "s3_region");
    config.connectTimeoutMs = numeric_param(params, "connect_timeout_ms", 3000, 1, 3600000);
    // A request that never returns is worse for a recovered rank than one that fails: the poll
    // loop above can retry a failure, but it cannot interrupt a transfer.
    config.requestTimeoutMs = numeric_param(params, "request_timeout_ms", 15000, 1, 3600000);
    // One rank is one thread issuing one request at a time; the pool only has to cover the
    // handles a retry leaves behind. 25 (the SDK default) is 25 sockets per rank of the job.
    config.maxConnections = static_cast<unsigned>(numeric_param(params, "max_connections", 4, 1, 1024));
    // Set explicitly, never inherited: the SDK's default strategy differs between versions and
    // reads AWS_RETRY_MODE / AWS_MAX_ATTEMPTS from the environment, so an unset retry policy is
    // whatever the machine happens to say. These retries are the transparent half of the recovery
    // story — a restored rank's first request fails on a connection the checkpoint captured, and
    // the SDK destroys a failed curl handle rather than returning it to the pool, so the next
    // attempt dials a fresh TCP and TLS session by itself.
    const long max_attempts = numeric_param(params, "max_attempts", 4, 1, 100);
    config.retryStrategy = Aws::MakeShared<Aws::Client::StandardRetryStrategy>(TAG, max_attempts);

    // LocalStack, MinIO and any other S3-compatible endpoint. The scheme has to be set alongside
    // the override because the configuration carries it separately and defaults to HTTPS, which a
    // plain-HTTP test endpoint answers with a TLS handshake error rather than a redirect.
    auto endpoint = params.find("endpoint_url");
    if (endpoint != params.end() && !endpoint->second.empty()) {
        config.endpointOverride = endpoint->second;
        config.scheme = endpoint->second.rfind("http://", 0) == 0 ? Aws::Http::Scheme::HTTP
                                                                  : Aws::Http::Scheme::HTTPS;
    }
    // Polarity, since the key and the field are opposites: useVirtualAddressing is the SDK's
    // "bucket goes in the host name" switch (S3ClientConfiguration.h:51, and the bool of the same
    // name in the legacy S3Client constructors), default true — https://<bucket>.s3.<region>...
    // Path style is its negation, https://<endpoint>/<bucket>/<key>, which is what an endpoint
    // without wildcard DNS can serve.
    auto path_style = params.find("use_path_style");
    if (path_style != params.end() && path_style->second == "true") {
        config.useVirtualAddressing = false;
    }

    // The default chain rather than the environment provider alone. Environment variables are its
    // first source, so a Lambda or any job that exports credentials behaves exactly as before, but
    // the chain also carries the profile and credential_process providers — which re-read their
    // file when the credentials they handed out expire. That is the only route to fresh
    // credentials for a rank that was frozen for longer than its session token lives: a restored
    // process's environment is whatever the image captured, forever.
    std::shared_ptr<Aws::Auth::AWSCredentialsProvider> credentials_provider;
    auto configured_provider = params.find("credentials_provider");
    const std::string provider = configured_provider == params.end() || configured_provider->second.empty()
                                 ? "default" : configured_provider->second;
    if (provider == "default") {
        credentials_provider = Aws::MakeShared<Aws::Auth::DefaultAWSCredentialsProviderChain>(TAG);
    } else if (provider == "environment") {
        credentials_provider = Aws::MakeShared<Aws::Auth::EnvironmentAWSCredentialsProvider>(TAG);
    } else {
        throw std::runtime_error("S3: credentials_provider must be \"default\" or \"environment\", got \""
                                 + provider + "\"");
    }

    // aws-sdk-cpp 1.11 expects the S3-specific configuration type and endpoint-provider signature.
    client = Aws::MakeUnique<Aws::S3::S3Client>(
        TAG,
        credentials_provider,
        nullptr,
        config
    );
}

//! Drops this channel's client. The SDK itself stays up; see initialise_sdk_once.
FMI::Comm::S3::~S3() = default;

bool FMI::Comm::S3::download_object(channel_data buf, std::string name) {
    Aws::S3::Model::GetObjectRequest request;
    request.WithBucket(bucket_name).WithKey(name);
    auto outcome = client->GetObject(request);
    if (outcome.IsSuccess()) {
        auto& s = outcome.GetResult().GetBody();
        s.read(buf.buf, buf.len);
        return true;
    } else {
        return false;
    }
}

void FMI::Comm::S3::upload_object(channel_data buf, std::string name) {
    Aws::S3::Model::PutObjectRequest request;
    request.WithBucket(bucket_name).WithKey(name);

    const std::shared_ptr<Aws::IOStream> data = Aws::MakeShared<boost::interprocess::bufferstream>(TAG, buf.buf, buf.len);

    request.SetBody(data);
    auto outcome = client->PutObject(request);
    if (!outcome.IsSuccess()) {
        BOOST_LOG_TRIVIAL(error) << "Error when uploading to S3: " << outcome.GetError();
    }
}

void FMI::Comm::S3::delete_object(std::string name) {
    Aws::S3::Model::DeleteObjectRequest request;
    request.WithBucket(bucket_name).WithKey(name);
    auto outcome = client->DeleteObject(request);
    if (!outcome.IsSuccess()) {
        BOOST_LOG_TRIVIAL(error) << "Error when deleting from S3: " << outcome.GetError();
    }
}

std::vector<std::string> FMI::Comm::S3::get_object_names() {
    std::vector<std::string> object_names;
    Aws::S3::Model::ListObjectsRequest request;
    request.WithBucket(bucket_name);
    auto outcome = client->ListObjects(request);
    if (outcome.IsSuccess()) {
        auto objects = outcome.GetResult().GetContents();
        for (auto& object : objects) {
            object_names.push_back(object.GetKey());
        }
    } else {
        BOOST_LOG_TRIVIAL(error) << "Error when listing objects from S3: " << outcome.GetError();
    }
    return object_names;
}

double FMI::Comm::S3::get_latency(Utils::peer_num producer, Utils::peer_num consumer, std::size_t size_in_bytes) {
    double fixed_overhead = overhead;
    double waiting_time = (double) timeout / 2.;
    double comm_overhead = fixed_overhead + waiting_time;
    double agg_bandwidth = producer * consumer * bandwidth;
    double trans_time = producer * consumer * ((double) size_in_bytes / 1000000.) / agg_bandwidth;
    return log2(producer + consumer) * comm_overhead + trans_time;
}

double FMI::Comm::S3::get_price(Utils::peer_num producer, Utils::peer_num consumer, std::size_t size_in_bytes) {
    double upload_costs = producer * upload_price + producer * ((double) size_in_bytes / 1000000000.) * transfer_price;
    double expected_polls = (max_timeout / timeout) / 2;
    double download_costs = producer * consumer * expected_polls * download_price + producer * consumer * ((double) size_in_bytes / 1000000000.) * transfer_price;
    return upload_costs + download_costs;
}
