#include <boost/test/unit_test.hpp>

// Without the backend compiled in there is nothing here to test.
#if FMI_ENABLE_S3

#include "forked_rank_guard.h"

#include "../include/comm/S3.h"
#include "../include/utils/Common.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <map>
#include <memory>
#include <new>
#include <string>
#include <thread>
#include <vector>

//! The S3 channel, against a real object store and against a fake one.
/*!
 * Two halves. The cases that need a store to work write to a bucket, so they run only when one is
 * named in the environment and skip green otherwise — the precedent being tests/protocol_fuzz.cpp,
 * which reads its seed count from FMI_FUZZ_SEEDS. That keeps the default build's test run free of
 * credentials, network and cost while leaving them one variable away from running.
 *
 * The cases about failure — how long a request that fails takes, how many of them a channel sits
 * through, which of them it refuses to wait out — need a store that does not work, which no
 * bucket provides on demand. Those run always, against a fake endpoint on loopback (see
 * FakeStore), and they are the regression tests for the two bounds that make this backend usable
 * under recover.
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

    //! Deletes what a case wrote even when an assertion between here and there threw.
    /*!
     * Under recover a great deal more is raised than returned — a refusal, a length that does not
     * match, a store that stops answering — and a case whose cleanup sits after the assertion that
     * raised leaves its objects in a bucket somebody else is also using.
     */
    struct Cleanup {
        FMI::Comm::S3* channel;
        std::vector<std::string> names;
        ~Cleanup() {
            try {
                channel->delete_objects(names);
            } catch (...) {
                // delete_objects does not throw; this is only so that the destructor cannot.
            }
        }
    };

    //! An environment variable set for as long as a case needs it, then put back as it was.
    struct EnvGuard {
        std::string name;
        std::string previous;
        bool had_one;

        EnvGuard(const std::string& variable, const std::string& value) : name(variable) {
            const char* current = ::getenv(variable.c_str());
            had_one = current != nullptr;
            if (had_one) {
                previous = current;
            }
            ::setenv(variable.c_str(), value.c_str(), 1);
        }
        ~EnvGuard() {
            if (had_one) {
                ::setenv(name.c_str(), previous.c_str(), 1);
            } else {
                ::unsetenv(name.c_str());
            }
        }
    };

    //! An S3-shaped endpoint on loopback that answers however a case needs it to.
    /*!
     * What the failure paths are made of — how long a request that fails takes, how many of them a
     * channel will sit through — cannot be tested against a store that works, and testing it
     * against a real one that does not means either breaking a bucket or waiting for AWS to have a
     * bad day. So this answers with the status and the error code the case asks for, on loopback,
     * in microseconds.
     *
     * Every loop in here is bounded: the thread stops after a fixed number of requests whatever
     * the test does, each read waits with a timeout, and the destructor joins.
     */
    class FakeStore {
    public:
        //! healthy_after < 0 means never; otherwise the failures stop after that many requests.
        /*!
         * Counted rather than timed on purpose: how long a request to this endpoint takes is the
         * SDK's business and varies by half a second, so a case that flipped after a wall-clock
         * interval could see its first request already answered normally.
         */
        FakeStore(int status, std::string error_code, int healthy_after = -1)
                : status(status), error_code(std::move(error_code)), healthy_after(healthy_after) {
            listener = ::socket(AF_INET, SOCK_STREAM, 0);
            BOOST_REQUIRE(listener >= 0);
            int reuse = 1;
            ::setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
            sockaddr_in address {};
            address.sin_family = AF_INET;
            address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
            address.sin_port = 0;
            BOOST_REQUIRE(::bind(listener, reinterpret_cast<sockaddr*>(&address), sizeof(address)) == 0);
            socklen_t length = sizeof(address);
            BOOST_REQUIRE(::getsockname(listener, reinterpret_cast<sockaddr*>(&address), &length) == 0);
            port = ntohs(address.sin_port);
            BOOST_REQUIRE(::listen(listener, 16) == 0);
            worker = std::thread([this] { serve(); });
        }

        ~FakeStore() {
            stop = true;
            worker.join();
            ::close(listener);
        }

        std::string endpoint() const { return "http://127.0.0.1:" + std::to_string(port); }

        int answered() const { return served.load(); }

    private:
        void serve() {
            // The hard cap. A case that goes wrong stops the endpoint rather than leaving a
            // thread serving requests for as long as the test binary runs.
            const int max_requests = 4096;
            while (!stop && served.load() < max_requests) {
                pollfd waiting {listener, POLLIN, 0};
                if (::poll(&waiting, 1, 100) <= 0) {
                    continue;
                }
                const int connection = ::accept(listener, nullptr, nullptr);
                if (connection < 0) {
                    continue;
                }
                answer(connection);
                ::close(connection);
                served++;
            }
        }

        //! Read one request far enough that the client is not writing into a closed socket, answer it.
        void answer(int connection) {
            std::string request;
            std::size_t body_expected = 0;
            std::size_t header_end = std::string::npos;
            // Bounded twice: by the number of reads and by the bytes kept.
            for (int read_number = 0; read_number < 256 && request.size() < (1u << 20); read_number++) {
                if (header_end != std::string::npos &&
                    request.size() >= header_end + 4 + body_expected) {
                    break;
                }
                pollfd waiting {connection, POLLIN, 0};
                if (::poll(&waiting, 1, 2000) <= 0) {
                    break;
                }
                char chunk[4096];
                const ssize_t got = ::read(connection, chunk, sizeof(chunk));
                if (got <= 0) {
                    break;
                }
                request.append(chunk, static_cast<std::size_t>(got));
                if (header_end == std::string::npos) {
                    header_end = request.find("\r\n\r\n");
                    if (header_end != std::string::npos) {
                        body_expected = content_length(request.substr(0, header_end));
                    }
                }
            }

            const bool healthy = healthy_after >= 0 && served.load() >= healthy_after;
            std::string response;
            if (healthy) {
                response = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n"
                           "ETag: \"d41d8cd98f00b204e9800998ecf8427e\"\r\nConnection: close\r\n\r\n";
            } else {
                const std::string body = "<?xml version=\"1.0\"?><Error><Code>" + error_code +
                                         "</Code><Message>" + error_code + "</Message></Error>";
                response = "HTTP/1.1 " + std::to_string(status) + " Error\r\n"
                           "Content-Type: application/xml\r\nContent-Length: " +
                           std::to_string(body.size()) + "\r\nConnection: close\r\n\r\n" + body;
            }
            std::size_t written = 0;
            for (int write_number = 0; write_number < 64 && written < response.size(); write_number++) {
                const ssize_t put = ::write(connection, response.data() + written,
                                            response.size() - written);
                if (put <= 0) {
                    break;
                }
                written += static_cast<std::size_t>(put);
            }
        }

        static std::size_t content_length(const std::string& headers) {
            std::string lowered = headers;
            for (char& c : lowered) {
                c = static_cast<char>(::tolower(static_cast<unsigned char>(c)));
            }
            const std::size_t at = lowered.find("content-length:");
            if (at == std::string::npos) {
                return 0;
            }
            try {
                return static_cast<std::size_t>(std::stoul(headers.substr(at + 15)));
            } catch (const std::exception&) {
                return 0;
            }
        }

        int listener = -1;
        unsigned short port = 0;
        int status;
        std::string error_code;
        int healthy_after;
        std::atomic<bool> stop {false};
        std::atomic<int> served {0};
        std::thread worker;
    };

    //! Channel parameters pointing at a fake store, taking nothing from the environment.
    std::map<std::string, std::string> fake_params(const FakeStore& store,
                                                   std::map<std::string, std::string> extra = {}) {
        std::map<std::string, std::string> params = {
                {"bucket_name",          "fmi-fake-bucket"},
                {"s3_region",            "eu-central-1"},
                {"timeout",              "100"},
                {"max_timeout",          "3000"},
                {"endpoint_url",         store.endpoint()},
                {"use_path_style",       "true"},
                {"credentials_provider", "environment"}
        };
        for (const auto& entry : extra) {
            params[entry.first] = entry.second;
        }
        return params;
    }

    //! Credentials that are syntactically valid and belong to nobody; the fake store checks none.
    struct FakeCredentials {
        EnvGuard key {"AWS_ACCESS_KEY_ID", "AKIAIOSFODNN7EXAMPLE"};
        EnvGuard secret {"AWS_SECRET_ACCESS_KEY", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"};
        EnvGuard token {"AWS_SESSION_TOKEN", ""};
    };

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
    // The assertions below raise under recover — that is what they are checking — so the cleanup
    // cannot sit after them.
    Cleanup cleanup {legacy.get(), {key}};

    char eight[8];
    std::memset(eight, 0, sizeof(eight));
    BOOST_TEST(legacy->download_object({eight, sizeof(eight)}, key),
               "flag off, a short object used to read as a complete one and still must");
    BOOST_TEST(eight[0] == 'x');

    auto recovering = make_s3(prefix, s3_params({{"recover", "true"}}));
    BOOST_CHECK_THROW(recovering->download_object({eight, sizeof(eight)}, key),
                      FMI::Utils::BackendFailure);
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
    // Under recover a download raises rather than returns for several reasons, and the object
    // this case deliberately leaves behind has to go whichever way the assertions end.
    Cleanup cleanup {reader.get(), {kept}};
    BOOST_TEST(reader->download_object({&received, sizeof(received)}, kept),
               "recover deleted an object at finalize; a peer still polling for it would strand");

    {
        auto legacy = make_s3(prefix);
        legacy->upload({&payload, sizeof(payload)}, swept);
        legacy->finalize();
    }
    BOOST_TEST(!reader->download_object({&received, sizeof(received)}, swept),
               "with the flag off finalize must delete what the channel wrote, as it always has");
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
    // A warning rather than a failure: what the batching has to get right is that a thousand
    // objects go away, and that is asserted below. The comparison is two wall clocks on a host
    // this case does not own, and a scheduler that stalls the batch for a second would fail a
    // channel that is behaving perfectly.
    BOOST_WARN(batch_seconds < single_seconds);
    BOOST_TEST(channel->get_object_names().empty(), "objects were left behind");
}

//! The flag is accepted, and the family's configuration rules apply to this backend too.
/*!
 * S3 refused "recover" at construction until it implemented its half, and the refusal was not
 * pedantry: the family's half on its own — finalize deletes nothing, upload stops recording what
 * it wrote — is every object of every run kept and billed forever, while a failed write is still
 * logged and dropped and a short object still read as complete. All three backend obligations
 * exist now, so the flag is taken; the one the channel cannot install itself, the expiry, is a
 * bucket lifecycle rule the constructor asks for out loud.
 *
 * Nothing here touches a network: a channel is built from configuration alone, and these
 * parameters point at a port nothing listens on, so a regression that dialled at construction
 * would fail here rather than reach a bucket.
 */
BOOST_AUTO_TEST_CASE(takes_the_recover_flag) {
    std::map<std::string, std::string> params = {
            {"bucket_name",          "fmi-s3-backend-no-such-bucket"},
            {"s3_region",            "eu-central-1"},
            {"timeout",              "100"},
            {"max_timeout",          "1000"},
            // Nothing listens on port 1, and no request is made here anyway.
            {"endpoint_url",         "http://127.0.0.1:1"},
            {"use_path_style",       "true"},
            {"credentials_provider", "environment"}
    };

    BOOST_CHECK_NO_THROW(FMI::Comm::S3(params, model_params));

    auto recovering = params;
    recovering["recover"] = "true";
    recovering["object_ttl_s"] = "60";
    BOOST_CHECK_NO_THROW(FMI::Comm::S3(recovering, model_params));

    // The family's validation covers this backend too: a zero poll interval is a rank that would
    // never notice a store that has gone away, and under recover that is refused before the client
    // is built.
    auto degenerate = recovering;
    degenerate["timeout"] = "0";
    BOOST_CHECK_EXCEPTION(FMI::Comm::S3(degenerate, model_params), std::runtime_error,
                          [] (const std::runtime_error& e) {
                              return mentions(e.what(), "timeout");
                          });

    // Configuration errors name the key that is wrong, because a channel receives its parameters
    // as untyped strings and nothing below it knows which one the operator wrote.
    auto no_bucket = recovering;
    no_bucket.erase("bucket_name");
    BOOST_CHECK_EXCEPTION(FMI::Comm::S3(no_bucket, model_params), std::runtime_error,
                          [] (const std::runtime_error& e) {
                              return mentions(e.what(), "bucket_name");
                          });

    auto no_region = recovering;
    no_region.erase("s3_region");
    BOOST_CHECK_EXCEPTION(FMI::Comm::S3(no_region, model_params), std::runtime_error,
                          [] (const std::runtime_error& e) {
                              return mentions(e.what(), "s3_region");
                          });

    auto unknown_provider = recovering;
    unknown_provider["credentials_provider"] = "instance-profile";
    BOOST_CHECK_EXCEPTION(FMI::Comm::S3(unknown_provider, model_params), std::runtime_error,
                          [] (const std::runtime_error& e) {
                              return mentions(e.what(), "credentials_provider");
                          });

    // A bound outside what the channel can act on is a typo far more often than a request, and it
    // used to be clamped into something nobody asked for.
    auto absurd = recovering;
    absurd["max_attempts"] = "-5";
    BOOST_CHECK_EXCEPTION(FMI::Comm::S3(absurd, model_params), std::runtime_error,
                          [] (const std::runtime_error& e) {
                              return mentions(e.what(), "max_attempts");
                          });
}

//! A store that throttles for a few seconds is waited out, not treated as the end of the job.
/*!
 * 503 SlowDown on a prefix is routine at collective scale, where every rank writes under one
 * prefix at once, and it outlasts the four attempts the SDK makes on its own. Under recover the
 * first failed PutObject used to end the job — while the reader of that same object tolerates two
 * hundred of the identical error — which made the flag whose purpose is surviving disruption
 * strictly worse than not setting it, on the one path that carries a delivery obligation.
 */
BOOST_AUTO_TEST_CASE(a_throttled_upload_is_retried_not_abandoned) {
    FakeCredentials credentials;
    FakeStore store(503, "SlowDown", 3);
    // One attempt per request, so that the SDK's own retries cannot be what carries this: every
    // request after the first is one the channel decided to make.
    auto channel = make_s3("throttled", fake_params(store, {{"recover", "true"},
                                                            {"max_timeout", "20000"},
                                                            {"max_attempts", "1"}}));

    char payload[8] = {'t', 'h', 'r', 'o', 't', 't', 'l', 'e'};
    const auto started = std::chrono::steady_clock::now();
    BOOST_CHECK_NO_THROW(channel->upload_object({payload, sizeof(payload)}, "throttled|value"));
    const double elapsed = seconds_since(started);
    BOOST_TEST(store.answered() == 4, "the store answered " << store.answered()
                                      << " requests; three of them had to be refusals the channel "
                                         "came back from, and the fourth the write landing");
    BOOST_TEST(elapsed > 0.2, "the upload landed in " << elapsed << " s, which is not three poll "
                              "intervals apart — the retries cannot have waited");
}

//! A store that never works ends the operation inside the configured patience, saying why.
/*!
 * The poll budget counts intervals, not seconds, and a request that fails on this backend costs
 * far more than the interval it is charged: 100 ms of budget for a pass that took seconds. So
 * max_timeout, read as three seconds of patience, was minutes of real waiting — and against an
 * endpoint that drops packets rather than answering, the measurement that started this was
 * fifty-one minutes for a minute of configured patience.
 *
 * The transient-failure limit is raised out of the way here so that only the clock can end this.
 */
BOOST_AUTO_TEST_CASE(a_failing_store_gives_up_inside_the_budget) {
    FakeCredentials credentials;
    FakeStore store(503, "SlowDown");
    auto channel = make_s3("failing", fake_params(store, {{"recover", "true"},
                                                          {"transient_failure_limit", "100000"}}));

    char received[8] = {};
    const auto started = std::chrono::steady_clock::now();
    std::string message;
    bool threw_backend_failure = false;
    try {
        // The real caller: the family's bounded poll loop, which re-enters only on false.
        channel->download({received, sizeof(received)}, "failing|never-written");
    } catch (const FMI::Utils::BackendFailure& e) {
        threw_backend_failure = true;
        message = e.what();
    } catch (const FMI::Utils::Timeout&) {
        message = "Timeout";
    }
    const double elapsed = seconds_since(started);
    BOOST_TEST(threw_backend_failure,
               "a store that answers every request with 503 ended the operation as " << message);
    BOOST_TEST(elapsed < 40., "giving up took " << elapsed << " s against a 3 s budget");
    BOOST_TEST(mentions(message, "budget"), "the failure does not say what it ran out of: " << message);

    // With the flag off nothing may change shape: the callers have no handler, so the same store
    // is a false and the poll loop's own budget is what ends the operation.
    auto legacy = make_s3("failing", fake_params(store));
    bool found = true;
    BOOST_CHECK_NO_THROW(found = legacy->download_object({received, sizeof(received)},
                                                          "failing|never-written"));
    BOOST_TEST(!found);
}

//! A refusal is raised where it happens, on both paths, without spending the budget on it.
BOOST_AUTO_TEST_CASE(a_refused_request_is_not_waited_out) {
    FakeCredentials credentials;
    FakeStore store(403, "AccessDenied");
    auto channel = make_s3("refused", fake_params(store, {{"recover", "true"}}));

    char payload[4] = {'n', 'o', 'p', 'e'};
    const auto write_started = std::chrono::steady_clock::now();
    BOOST_CHECK_THROW(channel->upload_object({payload, sizeof(payload)}, "refused|value"),
                      FMI::Utils::BackendFailure);
    const double write_elapsed = seconds_since(write_started);
    BOOST_TEST(write_elapsed < 5., "the refused write took " << write_elapsed
                                   << " s; a refusal is not something to retry");

    BOOST_CHECK_THROW(channel->download_object({payload, sizeof(payload)}, "refused|value"),
                      FMI::Utils::BackendFailure);
}

//! A bucket in another region is reported as a region, not as an unreliable store.
BOOST_AUTO_TEST_CASE(a_region_that_does_not_match_is_named) {
    FakeCredentials credentials;
    FakeStore store(400, "AuthorizationHeaderMalformed");
    auto channel = make_s3("region", fake_params(store, {{"recover", "true"}}));

    char received[4] = {};
    std::string message;
    try {
        channel->download_object({received, sizeof(received)}, "region|value");
    } catch (const FMI::Utils::BackendFailure& e) {
        message = e.what();
    }
    BOOST_TEST(mentions(message, "s3_region"),
               "a request the store cannot sign as configured was reported as: " << message);

    FakeStore redirected(301, "PermanentRedirect");
    auto elsewhere = make_s3("region", fake_params(redirected, {{"recover", "true"}}));
    BOOST_CHECK_THROW(elsewhere->download_object({received, sizeof(received)}, "region|value"),
                      FMI::Utils::BackendFailure);
}

BOOST_AUTO_TEST_SUITE_END();

#endif // FMI_ENABLE_S3
