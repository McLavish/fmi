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
#include <aws/s3/model/ListObjectsV2Request.h>

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

    //! What a failed S3 request means to a caller that can only ask again or give up.
    enum class Failure {
        absent,     //!< No such object. To a poll loop that is "not yet", and it is the hot path.
        no_bucket,  //!< The container itself is not there.
        refused,    //!< Rejected for who is asking, not for what was asked.
        transient   //!< 5xx, throttling, a broken connection: the same request may work later.
    };

    //! Classify by HTTP status and exception name, never by the SDK's error enum alone.
    /*!
     * Measured against aws-sdk-cpp 1.11.861: an expired session token arrives as error type 100
     * (CoreErrors::UNKNOWN) with HTTP 400 and the message "Unable to parse ExceptionName:
     * ExpiredToken". The SDK has no enumerator for it, so a switch over GetErrorType() would file
     * one of the likeliest failures a checkpointed rank meets — a session token that expired while
     * the process was frozen — under "unknown, worth retrying", and the job would poll until its
     * budget ran out and then report a timeout. What the service actually said survives in the
     * exception name (AWSErrorMarshaller.cpp:247 keeps it even when it cannot map it) and in the
     * status code, and those do not move between SDK versions.
     *
     * THE 403-VS-404 TRAP: S3 answers a GET for a key that does not exist with 403 AccessDenied,
     * not 404 NoSuchKey, when the caller has no s3:ListBucket permission on the bucket — the point
     * of that behaviour being not to leak whether the key exists, so there is nothing in the
     * response to tell the two apart. A job whose role grants only GetObject and PutObject
     * therefore sees every not-yet-written object as a refusal, which under recover is a throw on
     * the first poll of the first collective rather than a wait. Grant s3:ListBucket on the bucket
     * to the role the ranks run as.
     */
    Failure classify(const Aws::S3::S3Error& error) {
        const int status = static_cast<int>(error.GetResponseCode());
        const Aws::String& exception = error.GetExceptionName();
        if (exception == "NoSuchBucket") {
            // Also a 404, so it has to be recognised before absence is.
            return Failure::no_bucket;
        }
        if (status == 404 || exception == "NoSuchKey") {
            return Failure::absent;
        }
        if (status == 401 || status == 403) {
            return Failure::refused;
        }
        if (status == 400) {
            // 400 is otherwise the SDK's catch-all for a request the service would not take, so
            // only these names promote it to a refusal. ExpiredToken is the one that matters after
            // a long freeze; the rest are the same class of "this rank cannot authenticate".
            for (const char* refusal : {"AccessDenied", "InvalidAccessKeyId", "SignatureDoesNotMatch",
                                        "ExpiredToken", "InvalidToken", "TokenRefreshRequired",
                                        "RequestTimeTooSkewed"}) {
                if (exception == refusal) {
                    return Failure::refused;
                }
            }
        }
        return Failure::transient;
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
    transient_failure_limit = static_cast<unsigned int>(
            numeric_param(params, "transient_failure_limit", 200, 1, 1000000));

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

std::string FMI::Comm::S3::describe(const Aws::S3::S3Error& error, const std::string& name) const {
    const Aws::String& exception = error.GetExceptionName();
    return "bucket " + bucket_name + ", key " + name + ", HTTP " +
           std::to_string(static_cast<int>(error.GetResponseCode())) + ", " +
           (exception.empty() ? std::string("no exception name") : std::string(exception.c_str())) +
           ": " + std::string(error.GetMessage().c_str());
}

//! Say what a failed download was, and decide whether the caller may keep waiting for it.
/*!
 * The whole point of the classification is that "the object is not there yet" and "this rank
 * cannot talk to S3" arrive here identically today — both as a false, both indistinguishable from
 * a peer being slow — and the caller's only reaction to a false is to poll again until its budget
 * runs out. So a job with expired credentials spends its entire max_timeout before reporting a
 * timeout on the wrong rank, and one pointed at a bucket that does not exist does the same.
 *
 * A refusal is therefore raised under recover, where the flag promises that a failure the store
 * has already given a reason for is not waited out. With the flag off nothing may change shape —
 * the callers have no handler — so it stays a false, plus a log line the first time.
 */
bool FMI::Comm::S3::report_download_failure(const Aws::S3::S3Error& error, const std::string& name) {
    switch (classify(error)) {
        case Failure::absent:
            // The hot path: one of these per rank per poll interval for as long as a peer is late.
            // Nothing is built and nothing is logged. The store answering at all is also what says
            // the connection is fine, so the transient count starts over.
            consecutive_transient_failures = 0;
            transient_warned = false;
            refusal_warned = false;
            return false;
        case Failure::no_bucket: {
            const std::string what = "S3: no such bucket (" + describe(error, name) + ")";
            if (recover) {
                throw Utils::BackendFailure(what);
            }
            if (!refusal_warned) {
                refusal_warned = true;
                BOOST_LOG_TRIVIAL(error) << what << " (waiting out the poll budget)";
            }
            return false;
        }
        case Failure::refused: {
            const std::string what = "S3: request refused (" + describe(error, name) +
                                     "); credentials or bucket policy — and note that without "
                                     "s3:ListBucket a missing key is answered 403, not 404";
            if (recover) {
                throw Utils::BackendFailure(what);
            }
            if (!refusal_warned) {
                refusal_warned = true;
                BOOST_LOG_TRIVIAL(error) << what << " (waiting out the poll budget)";
            }
            return false;
        }
        case Failure::transient:
            consecutive_transient_failures++;
            if (recover && consecutive_transient_failures > transient_failure_limit) {
                // Deliberately far above what any poll budget spends: each of these is a request
                // the SDK has already retried and backed off on, so reaching the limit means
                // minutes of a store that is reachable and not working. Redis's download never
                // throws for a connection reason because a poll there costs a millisecond and the
                // budget is a fine failure detector; here a poll costs a request, a round trip and
                // money, and a budget wide enough to survive a checkpoint is wide enough to spend
                // an hour on a bucket that will not answer.
                throw Utils::BackendFailure("S3: " + std::to_string(consecutive_transient_failures) +
                                            " failed requests in a row (" + describe(error, name) + ")");
            }
            if (!transient_warned) {
                transient_warned = true;
                BOOST_LOG_TRIVIAL(warning) << "S3: request failed (" << describe(error, name)
                                           << "), retrying while the poll budget lasts";
            }
            return false;
    }
    return false;
}

bool FMI::Comm::S3::download_object(channel_data buf, std::string name) {
    Aws::S3::Model::GetObjectRequest request;
    request.WithBucket(bucket_name).WithKey(name);
    auto outcome = client->GetObject(request);
    if (!outcome.IsSuccess()) {
        return report_download_failure(outcome.GetError(), name);
    }
    consecutive_transient_failures = 0;
    transient_warned = false;
    refusal_warned = false;

    auto& body = outcome.GetResult().GetBody();
    body.read(buf.buf, buf.len);
    if (recover) {
        // A PutObject is atomic — an object exists whole or not at all — so a body that is not the
        // length the reader expects is never a half-written value. It is two ranks disagreeing
        // about the message size, or two jobs sharing a key, and the flag-off behaviour (copy what
        // there is, report a complete read) leaves the rest of the buffer as whatever it was and
        // tells nobody. Both directions are checked: a short object shows up in gcount, a longer
        // one only in the length the service reported, because the read stops at the buffer.
        const std::streamsize got = body.gcount();
        const long long stored = outcome.GetResult().GetContentLength();
        if (got != static_cast<std::streamsize>(buf.len) ||
            (stored > 0 && stored != static_cast<long long>(buf.len))) {
            throw Utils::BackendFailure("S3: object is " + std::to_string(stored) + " bytes (" +
                                        std::to_string(got) + " read), expected " +
                                        std::to_string(buf.len) + " — bucket " + bucket_name +
                                        ", key " + name);
        }
    }
    return true;
}

//! Write one object. Under recover this call carries a delivery obligation.
/*!
 * The peer that will read this object has no other way of learning that it was written, and its
 * only reaction to an absent object is to poll until its budget runs out. Logging the failure and
 * returning — which is what happens with the flag off, and what the S3 channel has always done —
 * therefore surfaces as an unexplained Timeout, on a different rank, a minute later. Under recover
 * the failure is raised here, where the reason is known, after the SDK has already spent its
 * retries on it.
 */
void FMI::Comm::S3::upload_object(channel_data buf, std::string name) {
    Aws::S3::Model::PutObjectRequest request;
    request.WithBucket(bucket_name).WithKey(name);

    const std::shared_ptr<Aws::IOStream> data = Aws::MakeShared<boost::interprocess::bufferstream>(TAG, buf.buf, buf.len);

    request.SetBody(data);
    auto outcome = client->PutObject(request);
    if (outcome.IsSuccess()) {
        consecutive_transient_failures = 0;
        transient_warned = false;
        refusal_warned = false;
        return;
    }
    if (!recover) {
        BOOST_LOG_TRIVIAL(error) << "Error when uploading to S3: " << outcome.GetError();
        return;
    }
    throw Utils::BackendFailure("S3: could not write object (" + describe(outcome.GetError(), name) + ")");
}

void FMI::Comm::S3::delete_object(std::string name) {
    Aws::S3::Model::DeleteObjectRequest request;
    request.WithBucket(bucket_name).WithKey(name);
    auto outcome = client->DeleteObject(request);
    if (!outcome.IsSuccess()) {
        BOOST_LOG_TRIVIAL(error) << "Error when deleting from S3: " << outcome.GetError();
    }
}

//! Every object this communicator has written, however many pages that takes.
/*!
 * Two things were wrong with asking for one unfiltered page. S3 returns at most a thousand keys
 * per response and says so in IsTruncated, which was ignored: past a thousand objects — one
 * barrier at 1000 ranks, or any job that has been running for a while, since nothing is deleted
 * before finalize — the caller silently saw a prefix of the bucket. And the request had no prefix
 * at all, so every job sharing the bucket listed every other job's objects, which the barrier
 * above counts by suffix: another communicator's markers arrive as this one's.
 *
 * Under recover this is not on any hot path — RecoverableClientServer::barrier asks for the N
 * marker keys by name instead — but the flag-off barrier calls it once per poll, so the prefix is
 * also what keeps that request proportional to the job rather than to the bucket.
 */
std::vector<std::string> FMI::Comm::S3::get_object_names() {
    std::vector<std::string> object_names;
    Aws::S3::Model::ListObjectsV2Request request;
    request.WithBucket(bucket_name).WithPrefix(object_key_prefix());
    while (true) {
        auto outcome = client->ListObjectsV2(request);
        if (!outcome.IsSuccess()) {
            // Never thrown: this runs inside the flag-off barrier's poll loop, which has no
            // handler and re-enters on an empty result, and from nothing that could act on it.
            // What was collected so far is returned, which is what a partial listing means.
            BOOST_LOG_TRIVIAL(error) << "Error when listing objects from S3: " << outcome.GetError();
            break;
        }
        const auto& result = outcome.GetResult();
        for (const auto& object : result.GetContents()) {
            object_names.emplace_back(object.GetKey().c_str(), object.GetKey().size());
        }
        if (!result.GetIsTruncated()) {
            break;
        }
        const Aws::String& token = result.GetNextContinuationToken();
        if (token.empty()) {
            // A truncated listing without a token is a service that contradicts itself; asking
            // again with the same request would repeat the same page forever.
            BOOST_LOG_TRIVIAL(error) << "S3: truncated listing without a continuation token, "
                                        "returning the " << object_names.size() << " names read so far";
            break;
        }
        request.SetContinuationToken(token);
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
