#include "../../include/comm/S3.h"
#include "../../include/utils/Signals.h"

#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/client/RetryStrategy.h>
#include <aws/core/http/Scheme.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/Delete.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/DeleteObjectsRequest.h>
#include <aws/s3/model/ObjectIdentifier.h>
#include <aws/s3/model/ListObjectsV2Request.h>

#include <boost/log/trivial.hpp>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>

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
        absent,        //!< No such object. To a poll loop "not yet", and it is the hot path.
        no_bucket,     //!< The container itself is not there.
        refused,       //!< Rejected for who is asking, not for what was asked.
        misconfigured, //!< The request cannot be routed or signed as this channel is configured.
        transient      //!< 5xx, throttling, a broken connection: the same request may work later.
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
        if (status == 301 || exception == "PermanentRedirect") {
            // The bucket is in another region, or endpoint_url points somewhere that is not
            // serving it. S3 answers a GET with a 301 and a redirect nobody follows; before this
            // it fell through to "might work later" and the operator was told, two hundred failed
            // requests later, that the store had been unreliable.
            return Failure::misconfigured;
        }
        if (status == 404 || exception == "NoSuchKey") {
            return Failure::absent;
        }
        if (status == 401 || status == 403) {
            return Failure::refused;
        }
        if (status == 400) {
            // 400 is otherwise the SDK's catch-all for a request the service would not take, so
            // only these names promote it out of "transient". ExpiredToken is the one that matters
            // after a long freeze — ExpiredTokenException is how STS spells the same thing, kept
            // as a hedge against the two drifting apart — and the rest are the same class of "this
            // rank cannot authenticate".
            for (const char* refusal : {"AccessDenied", "InvalidAccessKeyId", "SignatureDoesNotMatch",
                                        "ExpiredToken", "ExpiredTokenException", "InvalidToken",
                                        "TokenRefreshRequired", "RequestTimeTooSkewed"}) {
                if (exception == refusal) {
                    return Failure::refused;
                }
            }
            if (exception == "AuthorizationHeaderMalformed") {
                // "the region us-east-1 is wrong; expecting eu-central-1": a signature the service
                // will refuse identically every time s3_region says what it says.
                return Failure::misconfigured;
            }
        }
        return Failure::transient;
    }

    //! What to say about a failure that asking again cannot change.
    /*!
     * Every one of these means the store answered and the answer will be the same next time, so
     * the text has to point at the configuration or the credentials rather than at the peer whose
     * object never appeared. Shared by the read and the write paths, which classify identically
     * and differ only in what they do afterwards.
     */
    std::string permanent_failure_text(Failure kind, const std::string& description) {
        switch (kind) {
            case Failure::no_bucket:
                return "S3: no such bucket (" + description + ")";
            case Failure::absent:
                // Not reachable from a read, where absence is the hot path and never an error.
                // A write answered "not found" is a bucket problem the service did not name.
                return "S3: the store answered a write with 404 (" + description + ")";
            case Failure::misconfigured:
                return "S3: the request cannot be routed or signed as configured (" + description +
                       "); s3_region and endpoint_url have to name the region and the endpoint "
                       "the bucket actually lives in";
            default:
                return "S3: request refused (" + description + "); credentials or bucket policy — "
                       "and note that without s3:ListBucket a missing key is answered 403, not 404";
        }
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
        if (value < low || value > high) {
            // Not clamped. A number outside the range this channel can act on is a typo far more
            // often than a request, and silently turning max_attempts: -5 into 1, or
            // request_timeout_ms: 0 into one millisecond, hands back a working channel configured
            // as nobody asked.
            throw std::runtime_error("S3: " + key + " must be between " + std::to_string(low) +
                                     " and " + std::to_string(high) + ", got " + it->second);
        }
        return value;
    }

    //! How many passes a poll loop of this channel makes before it gives up.
    /*!
     * The same arithmetic ClientServer::download does — one `timeout` charged per pass until
     * max_timeout is reached — rounded up, because a budget that is not a multiple of the interval
     * buys one more pass and not one fewer.
     */
    unsigned int poll_attempts(unsigned int max_timeout, unsigned int timeout) {
        if (timeout == 0) {
            // Only reachable with the flag off: RecoverableClientServer refuses a zero interval
            // under recover, and nothing on the flag-off path calls this.
            return 1;
        }
        return std::max(1u, (max_timeout + timeout - 1) / timeout);
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
    // See Utils::suppress_sigpipe. Called before the SDK is initialised so that a process which
    // has no SIGPIPE disposition of its own gets one either way, whichever of the two runs first.
    Utils::suppress_sigpipe();
    initialise_sdk_once();

    bucket_name = required(params, "bucket_name");
    if (recover) {
        // The one obligation of the contract this backend cannot meet by itself. Redis writes
        // every object with SET ... EX and each one expires on its own; S3 has no per-object
        // expiry at all, only a bucket-level lifecycle rule, and a channel cannot install one
        // (it is bucket configuration, and a job's credentials generally may not change it).
        // So object_ttl_s is accepted here — a config that sets it is not wrong — and does
        // nothing: on S3 the mechanism is the rule.
        //
        // Which makes this the whole cleanup story under recover, since finalize deletes nothing:
        // without a rule every object of every run stays in the bucket, and is billed, forever.
        // Said once, at construction, where the operator can still connect it to what they wrote.
        BOOST_LOG_TRIVIAL(warning) << "S3 recover: nothing is deleted on the way out, and S3 has "
                                      "no per-object expiry (object_ttl_s does not apply here) — "
                                      "bucket " << bucket_name << " must carry a lifecycle "
                                      "expiration rule, or this job's objects are kept and billed "
                                      "indefinitely";
    }
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
    //
    // Both knobs, because only one of them is the cap the key's name promises. The SDK's
    // requestTimeoutMs is, on the curl client every Linux build uses, CURLOPT_LOW_SPEED_TIME:
    // how long the transfer may stay below lowSpeedLimit — one byte per second — before it is
    // abandoned (ClientConfiguration.h:203-210,226-230). A store that trickles a byte a second
    // therefore satisfies it forever. CURLOPT_TIMEOUT_MS, the wall clock of the whole request
    // including DNS, connect, TLS and transfer, is httpRequestTimeoutMs, and it defaults to 0,
    // meaning no cap at all. Setting the two together is what makes the number an operator
    // writes here the number of milliseconds a request may take. Note that it bounds one HTTP
    // request: with the retry strategy below, a failing call costs up to max_attempts of these.
    const long request_timeout_ms = numeric_param(params, "request_timeout_ms", 15000, 1, 3600000);
    config.httpRequestTimeoutMs = request_timeout_ms;
    config.requestTimeoutMs = request_timeout_ms;
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

//! The store answered. Whatever it said, nothing is outstanding against it any more.
void FMI::Comm::S3::store_answered() {
    consecutive_transient_failures = 0;
    transient_warned = false;
    refusal_warned = false;
}

//! Count one failed request that might pass, and end the operation once waiting stops being sane.
/*!
 * Two bounds, because either one alone leaves a rank waiting far past the patience it was
 * configured with. A count catches a store that fails quickly and forever; a clock catches one
 * that fails slowly, which is precisely what an unreachable store does.
 *
 * The clock is the one that matters here, and it is not the poll loop's. ClientServer::download
 * charges its budget one `timeout` — 100 ms in the shipped S3 config — for each pass, whatever
 * that pass actually cost. A request to an endpoint that drops packets costs the connect timeout
 * plus every retry the SDK makes, measured at 15 s against a blackholed address and 65 s against
 * one that accepts the connection and then says nothing. So max_timeout, which reads as a minute
 * of patience, is 600 of those passes: hours before the arithmetic budget runs out, on a rank
 * whose peers gave up after their own minute. That is the gap this closes, and it is the same
 * claim the constructor makes about its timeouts — an operation has to fail inside the budget for
 * the budget to be the failure detector.
 *
 * The clock runs across CONSECUTIVE failures only, and any answer from the store restarts it —
 * including "no such object", which is what a peer that has not written yet looks like. A rank
 * frozen for an hour therefore costs its peers nothing here: their requests keep being answered.
 */
void FMI::Comm::S3::note_transient_failure(const Aws::S3::S3Error& error, const std::string& name,
                                           std::chrono::steady_clock::time_point started) {
    if (consecutive_transient_failures == 0) {
        // From when the request went out, not from when its failure was noticed: on this path
        // those are tens of seconds apart, and that difference is the whole problem.
        failing_since = started;
    }
    consecutive_transient_failures++;
    if (!recover) {
        // Flag off there is nobody to raise this to — the callers are poll loops with no handler
        // — so the count is kept for the log line and nothing else happens.
        return;
    }
    const auto failing_for = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - failing_since).count();
    if (consecutive_transient_failures <= transient_failure_limit &&
        failing_for < static_cast<long long>(max_timeout)) {
        return;
    }
    throw Utils::BackendFailure("S3: " + std::to_string(consecutive_transient_failures) +
                                " failed requests in a row over " + std::to_string(failing_for) +
                                " ms (budget " + std::to_string(max_timeout) + " ms, limit " +
                                std::to_string(transient_failure_limit) + " requests): " +
                                describe(error, name));
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
bool FMI::Comm::S3::report_download_failure(const Aws::S3::S3Error& error, const std::string& name,
                                            std::chrono::steady_clock::time_point started) {
    const Failure kind = classify(error);
    if (kind == Failure::absent) {
        // The hot path: one of these per rank per poll interval for as long as a peer is late.
        // Nothing is built and nothing is logged. The store answering at all is also what says the
        // connection is fine, so the transient count starts over.
        store_answered();
        return false;
    }
    if (kind == Failure::transient) {
        // Counts the failure and, under recover, ends the operation once the failures have
        // outlasted either bound. Redis's download never throws for a connection reason because a
        // poll there costs a millisecond and the budget is a fine failure detector; here a poll
        // costs a request, a round trip and money, and a request that fails can cost more wall
        // clock than the whole budget it is charged against.
        note_transient_failure(error, name, started);
        if (!transient_warned) {
            transient_warned = true;
            BOOST_LOG_TRIVIAL(warning) << "S3: request failed (" << describe(error, name)
                                       << "), retrying while the poll budget lasts";
        }
        return false;
    }
    // A refusal, a missing bucket, a region that does not match. Nothing here gets better by
    // asking again, so under recover it is raised; with the flag off the callers have no handler,
    // so it stays a false and a log line the first time.
    //
    // The description is built inside the branches that use it and not before them. This is not
    // the hot path everywhere: a role without s3:ListBucket has S3 answer 403 for every key that
    // does not exist yet (see classify), and in that configuration a poll loop arrives here once
    // per rank per interval — where formatting an error message that the latch above then throws
    // away is the whole cost of the pass.
    if (recover) {
        throw Utils::BackendFailure(permanent_failure_text(kind, describe(error, name)));
    }
    if (!refusal_warned) {
        refusal_warned = true;
        BOOST_LOG_TRIVIAL(error) << permanent_failure_text(kind, describe(error, name))
                                 << " (waiting out the poll budget)";
    }
    return false;
}

bool FMI::Comm::S3::download_object(channel_data buf, std::string name) {
    Aws::S3::Model::GetObjectRequest request;
    request.WithBucket(bucket_name).WithKey(name);
    // Taken before the request goes out: what a failure costs in wall clock is exactly what the
    // poll loop above cannot see, and it is what the failure bound is measured in.
    const auto started = std::chrono::steady_clock::now();
    auto outcome = client->GetObject(request);
    if (!outcome.IsSuccess()) {
        return report_download_failure(outcome.GetError(), name, started);
    }
    store_answered();

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
 * therefore surfaces as an unexplained Timeout, on a different rank, a minute later.
 *
 * So under recover the write is retried under the same budget the reader is spending on it, at the
 * same interval, and only then raised. The SDK's own retries are not that budget: four attempts
 * over three seconds is the right answer to a packet that went missing and the wrong one to a
 * prefix S3 has decided to throttle, which is a routine few seconds of 503 SlowDown and exactly
 * what a collective produces when every rank writes under one prefix at once. Giving up there
 * would make the flag whose purpose is surviving disruption strictly less robust than not setting
 * it: the reader tolerates two hundred of the identical error.
 *
 * A refusal is different and is raised at once. A bucket that does not exist, credentials that
 * expired while this rank was frozen, a region that does not match — the store has answered, and
 * no amount of asking again changes what it said. This is the same split download_object makes,
 * and the same one Redis makes between a command that could not be completed and a store that
 * completed it and said no.
 */
void FMI::Comm::S3::upload_object(channel_data buf, std::string name) {
    auto put = [&] () {
        Aws::S3::Model::PutObjectRequest request;
        request.WithBucket(bucket_name).WithKey(name);
        // Rebuilt per attempt: the request owns the body stream, and a stream that has been read
        // once is at its end. Re-sending the same one would write an empty object.
        const std::shared_ptr<Aws::IOStream> data =
                Aws::MakeShared<boost::interprocess::bufferstream>(TAG, buf.buf, buf.len);
        request.SetBody(data);
        return client->PutObject(request);
    };

    if (!recover) {
        auto outcome = put();
        if (outcome.IsSuccess()) {
            store_answered();
            return;
        }
        BOOST_LOG_TRIVIAL(error) << "Error when uploading to S3: " << outcome.GetError();
        return;
    }

    // The reader's budget, spent the same way: one attempt per poll interval, for as long as the
    // reader would keep polling for what this call owes it. note_transient_failure ends it earlier
    // when the failures have outlasted the wall clock or the consecutive-failure limit, which is
    // what keeps a store that fails slowly from turning this loop into hours of retrying.
    const unsigned int attempts = poll_attempts(max_timeout, timeout);
    std::string last_failure;
    for (unsigned int attempt = 0; attempt < attempts; attempt++) {
        const auto started = std::chrono::steady_clock::now();
        auto outcome = put();
        if (outcome.IsSuccess()) {
            store_answered();
            return;
        }
        const Failure kind = classify(outcome.GetError());
        last_failure = describe(outcome.GetError(), name);
        if (kind != Failure::transient) {
            throw Utils::BackendFailure(permanent_failure_text(kind, last_failure));
        }
        note_transient_failure(outcome.GetError(), name, started);
        if (attempt + 1 < attempts) {
            // The interval the readers sleep between polls, so a store that is coming back is
            // waited for at the same rate on both sides.
            std::this_thread::sleep_for(std::chrono::milliseconds(timeout));
        }
    }
    throw Utils::BackendFailure("S3: could not write " + name + " within the poll budget (" +
                                std::to_string(attempts) + " attempts): " + last_failure);
}

void FMI::Comm::S3::delete_object(std::string name) {
    Aws::S3::Model::DeleteObjectRequest request;
    request.WithBucket(bucket_name).WithKey(name);
    auto outcome = client->DeleteObject(request);
    if (!outcome.IsSuccess()) {
        BOOST_LOG_TRIVIAL(error) << "Error when deleting from S3: " << outcome.GetError();
    }
}

//! Delete in batches of a thousand, the most one DeleteObjects request takes.
/*!
 * The default implementation is a request per object, which at finalize is a round trip per
 * message the rank ever sent: at sweep scale that is tens of seconds of teardown per rank, all of
 * it inside ~Communicator. Batching makes it one request per thousand.
 *
 * Nothing here throws. This runs from a destructor, and every outcome — a whole batch that failed,
 * an individual key the service refused — is reported and stepped over, because the objects
 * outlive the process either way and an exception here would not.
 */
void FMI::Comm::S3::delete_objects(const std::vector<std::string>& names) {
    const std::size_t batch = 1000;
    for (std::size_t start = 0; start < names.size(); start += batch) {
        Aws::S3::Model::Delete payload;
        for (std::size_t i = start; i < names.size() && i < start + batch; i++) {
            Aws::S3::Model::ObjectIdentifier object;
            object.SetKey(Aws::String(names[i].c_str(), names[i].size()));
            payload.AddObjects(std::move(object));
        }
        // Quiet: without it the response repeats every key that was deleted, which is the whole
        // job's traffic echoed back for nobody to read.
        payload.SetQuiet(true);
        Aws::S3::Model::DeleteObjectsRequest request;
        request.WithBucket(bucket_name).WithDelete(std::move(payload));
        auto outcome = client->DeleteObjects(request);
        if (!outcome.IsSuccess()) {
            BOOST_LOG_TRIVIAL(error) << "Error when deleting from S3: " << outcome.GetError();
            continue;
        }
        // The store answered, which is what the failure count is counting the absence of.
        store_answered();
        for (const auto& error : outcome.GetResult().GetErrors()) {
            BOOST_LOG_TRIVIAL(error) << "Error when deleting from S3: key " << error.GetKey()
                                     << ": " << error.GetCode() << " " << error.GetMessage();
        }
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
        store_answered();
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
