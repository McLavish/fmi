#include <boost/test/unit_test.hpp>

// Without the backend compiled in there is nothing here to test.
#if FMI_ENABLE_S3

#include "forked_rank_guard.h"

#include "../include/comm/S3.h"
#include "../include/utils/Common.h"

#include <sys/mman.h>
#include <unistd.h>

#include <chrono>
#include <cstdlib>
#include <cstring>
#include <map>
#include <memory>
#include <new>
#include <string>
#include <vector>

//! The S3 channel against a real object store.
/*!
 * Every case here writes to a bucket, so the suite runs only when one is named in the environment
 * and skips green otherwise — the precedent being tests/protocol_fuzz.cpp, which reads its seed
 * count from FMI_FUZZ_SEEDS. That keeps the default build's test run free of credentials, network
 * and cost while leaving the cases one variable away from running.
 *
 *   FMI_S3_TEST_BUCKET     bucket to use; nothing runs without it
 *   FMI_S3_TEST_REGION     default eu-central-1
 *   FMI_S3_TEST_ENDPOINT   S3-compatible endpoint (MinIO, LocalStack); unset means AWS
 *   FMI_S3_TEST_PATH_STYLE "1" for path-style addressing, which most such endpoints need
 *   FMI_S3_TEST_SLOW       "1" to also run the thousand-object cases
 *
 * Every case names its objects with its pid and its own tag and deletes what it wrote, because the
 * bucket is shared with whatever else the operator points at it — including another copy of this
 * suite.
 */
BOOST_AUTO_TEST_SUITE(S3Backend)

namespace {

    std::string env_or(const char* name, const std::string& fallback) {
        const char* value = std::getenv(name);
        return value != nullptr && value[0] != '\0' ? std::string(value) : fallback;
    }

    //! Whether a bucket was named, saying so once per case when it was not.
    bool store_configured(const char* what) {
        if (!env_or("FMI_S3_TEST_BUCKET", "").empty()) {
            return true;
        }
        BOOST_TEST_MESSAGE("S3Backend: FMI_S3_TEST_BUCKET is unset, skipping " << what);
        return false;
    }

    //! Whether the cases that write a thousand objects were asked for.
    bool slow_enabled(const char* what) {
        const std::string slow = env_or("FMI_S3_TEST_SLOW", "");
        if (slow == "1" || slow == "true") {
            return true;
        }
        BOOST_TEST_MESSAGE("S3Backend: FMI_S3_TEST_SLOW is unset, skipping " << what);
        return false;
    }

    const std::map<std::string, std::string> model_params = {
            {"bandwidth",      "50.0"},
            {"overhead",       "40.4"},
            {"transfer_price", "0.0"},
            {"download_price", "0.00000043"},
            {"upload_price",   "0.0000054"}
    };

    //! The channel parameters this environment describes, plus whatever the case adds.
    std::map<std::string, std::string> s3_params(std::map<std::string, std::string> extra = {}) {
        std::map<std::string, std::string> params = {
                {"bucket_name", env_or("FMI_S3_TEST_BUCKET", "")},
                {"s3_region",   env_or("FMI_S3_TEST_REGION", "eu-central-1")},
                // A poll every 100 ms for at most ten seconds: long enough that a slow request is
                // not mistaken for a missing object, short enough that a case which is going to
                // fail says so while someone is still watching.
                {"timeout",     "100"},
                {"max_timeout", "10000"}
        };
        const std::string endpoint = env_or("FMI_S3_TEST_ENDPOINT", "");
        if (!endpoint.empty()) {
            params["endpoint_url"] = endpoint;
        }
        const std::string path_style = env_or("FMI_S3_TEST_PATH_STYLE", "");
        if (path_style == "1" || path_style == "true") {
            params["use_path_style"] = "true";
        }
        for (const auto& entry : extra) {
            params[entry.first] = entry.second;
        }
        return params;
    }

    //! A prefix no other process and no other case shares.
    std::string key_prefix(const std::string& tag) {
        return "s3test-" + std::to_string(::getpid()) + "-" + tag + "-";
    }

    std::shared_ptr<FMI::Comm::S3> make_s3(const std::string& comm_name,
                                           std::map<std::string, std::string> params = s3_params()) {
        auto channel = std::make_shared<FMI::Comm::S3>(params, model_params);
        channel->set_peer_id(0);
        channel->set_num_peers(1);
        channel->set_comm_name(comm_name);
        return channel;
    }

    double seconds_since(std::chrono::steady_clock::time_point start) {
        return std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
    }

    //! What a forked child reports back to the case that forked it.
    struct ChildResult {
        int outcome;        //!< 0 nothing, 1 BackendFailure, 2 Timeout, 3 another exception, 4 no failure at all
        double seconds;
        char message[512];
    };

    ChildResult* shared_result() {
        void* memory = mmap(nullptr, sizeof(ChildResult), PROT_READ | PROT_WRITE,
                            MAP_SHARED | MAP_ANONYMOUS, -1, 0);
        BOOST_REQUIRE(memory != MAP_FAILED);
        return new (memory) ChildResult{0, 0., {}};
    }

    void record(ChildResult* result, int outcome, double seconds, const std::string& message) {
        result->outcome = outcome;
        result->seconds = seconds;
        std::strncpy(result->message, message.c_str(), sizeof(result->message) - 1);
        result->message[sizeof(result->message) - 1] = '\0';
    }

    bool mentions(const std::string& haystack, const std::string& needle) {
        return haystack.find(needle) != std::string::npos;
    }

}

//! A value written is a value read back, byte for byte, and a deleted one is gone.
BOOST_AUTO_TEST_CASE(round_trip) {
    if (!store_configured("round_trip")) {
        return;
    }
    const std::string prefix = key_prefix("roundtrip");
    auto channel = make_s3(prefix, s3_params({{"recover", "true"}}));

    char payload[64];
    for (std::size_t i = 0; i < sizeof(payload); i++) {
        payload[i] = static_cast<char>('a' + (i % 26));
    }
    const std::string key = prefix + "value";
    channel->upload_object({payload, sizeof(payload)}, key);

    char received[sizeof(payload)] = {};
    BOOST_REQUIRE(channel->download_object({received, sizeof(received)}, key));
    BOOST_TEST(std::memcmp(payload, received, sizeof(payload)) == 0,
               "the object read back is not the object written");

    channel->delete_objects({key});
    BOOST_TEST(!channel->download_object({received, sizeof(received)}, key),
               "the object is still there after being deleted");
}

//! A key that is not there is "not yet", answered immediately and without an exception.
/*!
 * This is the hot path of every poll loop in the family — one of these per rank per poll interval
 * for as long as a peer is late — and under recover it is also the one thing that must NOT become
 * a BackendFailure: a channel that raised on absence would fail at the first collective of every
 * job. Timed because the classification lives next to the retry policy: a missing key that went
 * through the SDK's retries and backoff would still return false, just seconds later, and turn
 * every wait into a crawl.
 */
BOOST_AUTO_TEST_CASE(missing_key_is_not_an_error) {
    if (!store_configured("missing_key_is_not_an_error")) {
        return;
    }
    const std::string prefix = key_prefix("missing");
    auto channel = make_s3(prefix, s3_params({{"recover", "true"}}));

    char received = 0;
    const auto started = std::chrono::steady_clock::now();
    bool found = true;
    BOOST_REQUIRE_NO_THROW(found = channel->download_object({&received, sizeof(received)},
                                                            prefix + "never-written"));
    const double elapsed = seconds_since(started);
    BOOST_TEST(!found, "a key that was never written was reported as present");
    BOOST_TEST(elapsed < 2., "absence took " << elapsed << " s to establish; a poll loop cannot "
                             "afford that per pass");
}

//! An object of the wrong length is a disagreement, and under recover it is raised rather than copied.
/*!
 * PutObject is atomic, so a stored value is never half-written: a body that is not the length the
 * reader expects means two ranks disagreeing about the message size, or two jobs sharing a key.
 * With the flag off the channel copies what there is and reports a complete read, which is pinned
 * here so that the difference between the two is a decision and not an accident.
 */
BOOST_AUTO_TEST_CASE(short_object_is_detected) {
    if (!store_configured("short_object_is_detected")) {
        return;
    }
    const std::string prefix = key_prefix("short");
    const std::string key = prefix + "one-byte";

    auto legacy = make_s3(prefix);
    char one = 'x';
    legacy->upload_object({&one, sizeof(one)}, key);

    char eight[8];
    std::memset(eight, 0, sizeof(eight));
    BOOST_TEST(legacy->download_object({eight, sizeof(eight)}, key),
               "flag off, a short object used to read as a complete one and still must");
    BOOST_TEST(eight[0] == 'x');

    auto recovering = make_s3(prefix, s3_params({{"recover", "true"}}));
    BOOST_CHECK_THROW(recovering->download_object({eight, sizeof(eight)}, key),
                      FMI::Utils::BackendFailure);

    legacy->delete_objects({key});
}

//! Credentials that do not work fail as credentials, promptly, not as a peer that is late.
/*!
 * The failure a checkpointed rank meets: a session token that expired while the process was
 * frozen. Before the classification every S3 error was the same false a missing object is, so the
 * rank polled its whole budget and then reported a Utils::Timeout — on the wrong rank, for the
 * wrong reason, and only after max_timeout. Under recover it must be a BackendFailure naming what
 * the service said, within seconds.
 *
 * Forked because the environment provider reads the process environment, and poisoning it in the
 * test binary would leak into every case that runs afterwards. ForkedRankGuard is what guarantees
 * the child cannot outlive this case (see the header — a survivor forks again in the next case).
 */
BOOST_AUTO_TEST_CASE(bad_credentials_fail_promptly) {
    if (!store_configured("bad_credentials_fail_promptly")) {
        return;
    }
    const std::string prefix = key_prefix("badcreds");
    ChildResult* result = shared_result();

    ForkedRankGuard rank_guard;
    int& peer_id = rank_guard.peer_id;
    rank_guard.fork_ranks(2);

    if (peer_id == 1) {
        // Syntactically valid and certainly not accepted: the example key from the AWS
        // documentation, which no account has ever been issued.
        ::setenv("AWS_ACCESS_KEY_ID", "AKIAIOSFODNN7EXAMPLE", 1);
        ::setenv("AWS_SECRET_ACCESS_KEY", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", 1);
        ::unsetenv("AWS_SESSION_TOKEN");
        const auto started = std::chrono::steady_clock::now();
        try {
            auto channel = make_s3(prefix, s3_params({{"recover", "true"},
                                                      {"credentials_provider", "environment"}}));
            char received = 0;
            const bool found = channel->download_object({&received, sizeof(received)},
                                                        prefix + "never-written");
            record(result, 4, seconds_since(started),
                   std::string("no failure at all, download returned ") + (found ? "true" : "false"));
        } catch (const FMI::Utils::BackendFailure& e) {
            record(result, 1, seconds_since(started), e.what());
        } catch (const FMI::Utils::Timeout& e) {
            record(result, 2, seconds_since(started), e.what());
        } catch (const std::exception& e) {
            record(result, 3, seconds_since(started), e.what());
        }
    }

    rank_guard.reap_ranks();

    const std::string message(result->message);
    BOOST_TEST(result->outcome == 1,
               "expected a BackendFailure, got outcome " << result->outcome << ": " << message);
    if (result->outcome == 1) {
        BOOST_TEST(result->seconds < 30., "the refusal took " << result->seconds
                                          << " s, which is a poll budget, not a classification");
        // The surface the classification actually keys on: a status the service refused with, and
        // the name it gave, which for an expired token the SDK cannot map to an enumerator at all.
        const bool named = mentions(message, "HTTP 400") || mentions(message, "HTTP 401") ||
                           mentions(message, "HTTP 403") || mentions(message, "InvalidAccessKeyId") ||
                           mentions(message, "SignatureDoesNotMatch") || mentions(message, "ExpiredToken") ||
                           mentions(message, "InvalidToken") || mentions(message, "AccessDenied");
        BOOST_TEST(named, "the failure does not name what the store refused with: " << message);
    }
    munmap(result, sizeof(ChildResult));
}

//! Under recover a rank that leaves takes nothing with it; with the flag off it still cleans up.
/*!
 * The rule the whole store-family recovery leans on: a rank that has finished cannot know whether
 * its peers still need what it wrote, and deleting on the way out is what strands the ranks still
 * polling. The objects go away by bucket lifecycle rule instead — which is why a recovering S3
 * channel says so at construction, and why this case deletes explicitly at the end rather than
 * expecting the channel to.
 */
BOOST_AUTO_TEST_CASE(recover_finalize_leaves_objects) {
    if (!store_configured("recover_finalize_leaves_objects")) {
        return;
    }
    const std::string prefix = key_prefix("finalize");
    const std::string kept = prefix + "kept";
    const std::string swept = prefix + "swept";
    char payload = 'k';
    char received = 0;

    {
        auto recovering = make_s3(prefix, s3_params({{"recover", "true"}}));
        recovering->upload({&payload, sizeof(payload)}, kept);
        recovering->finalize();
    }
    auto reader = make_s3(prefix, s3_params({{"recover", "true"}}));
    BOOST_TEST(reader->download_object({&received, sizeof(received)}, kept),
               "recover deleted an object at finalize; a peer still polling for it would strand");

    {
        auto legacy = make_s3(prefix);
        legacy->upload({&payload, sizeof(payload)}, swept);
        legacy->finalize();
    }
    BOOST_TEST(!reader->download_object({&received, sizeof(received)}, swept),
               "with the flag off finalize must delete what the channel wrote, as it always has");

    reader->delete_objects({kept});
}

//! A listing longer than one page comes back whole.
/*!
 * S3 returns at most a thousand keys per response. The unpaginated listing this replaces stopped
 * there silently, so a barrier at scale — or any job that has been running long enough, since
 * nothing is deleted before finalize — counted a prefix of the store and waited for ranks whose
 * markers it could not see.
 */
BOOST_AUTO_TEST_CASE(listing_crosses_page_boundaries) {
    if (!store_configured("listing_crosses_page_boundaries") ||
        !slow_enabled("listing_crosses_page_boundaries")) {
        return;
    }
    // Five past a page: enough to need a second request, few enough to be cheap.
    const int objects = 1005;
    const std::string prefix = key_prefix("pages");
    auto channel = make_s3(prefix);

    char one = '1';
    std::vector<std::string> written;
    written.reserve(objects);
    for (int i = 0; i < objects; i++) {
        written.push_back(prefix + std::to_string(i));
        channel->upload_object({&one, sizeof(one)}, written.back());
    }

    const auto names = channel->get_object_names();
    BOOST_TEST(names.size() == static_cast<std::size_t>(objects),
               "listed " << names.size() << " of " << objects << " objects");

    channel->delete_objects(written);
    BOOST_TEST(channel->get_object_names().empty(), "the listing is not empty after cleanup");
}

//! Deleting a thousand objects is a couple of requests, not a thousand round trips.
/*!
 * finalize runs inside ~Communicator, so a request per object is teardown time every rank of every
 * job pays. The comparison rather than a wall-clock bound: what matters is that the batch does not
 * scale with the number of objects, and that holds on a loopback endpoint as much as on a real
 * one.
 */
BOOST_AUTO_TEST_CASE(batched_delete_beats_one_request_per_object) {
    if (!store_configured("batched_delete_beats_one_request_per_object") ||
        !slow_enabled("batched_delete_beats_one_request_per_object")) {
        return;
    }
    const int objects = 300;
    const std::string prefix = key_prefix("batch");
    auto channel = make_s3(prefix);

    char one = '1';
    std::vector<std::string> one_by_one;
    std::vector<std::string> batched;
    for (int i = 0; i < objects; i++) {
        one_by_one.push_back(prefix + "single-" + std::to_string(i));
        batched.push_back(prefix + "batch-" + std::to_string(i));
        channel->upload_object({&one, sizeof(one)}, one_by_one.back());
        channel->upload_object({&one, sizeof(one)}, batched.back());
    }

    const auto single_started = std::chrono::steady_clock::now();
    for (const auto& name : one_by_one) {
        channel->delete_object(name);
    }
    const double single_seconds = seconds_since(single_started);

    const auto batch_started = std::chrono::steady_clock::now();
    channel->delete_objects(batched);
    const double batch_seconds = seconds_since(batch_started);

    BOOST_TEST_MESSAGE("delete of " << objects << " objects: " << single_seconds
                       << " s one by one, " << batch_seconds << " s batched");
    BOOST_TEST(batch_seconds < single_seconds,
               "the batched delete (" << batch_seconds << " s) was not faster than "
               << objects << " individual ones (" << single_seconds << " s)");
    BOOST_TEST(channel->get_object_names().empty(), "objects were left behind");
}

BOOST_AUTO_TEST_SUITE_END();

#endif // FMI_ENABLE_S3
