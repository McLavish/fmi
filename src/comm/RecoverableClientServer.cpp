#include "../../include/comm/RecoverableClientServer.h"

#include <boost/log/trivial.hpp>
#include <chrono>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace {

    //! The value a key was configured with, signed, or an error naming the key.
    /*!
     * Signed on purpose: timeout and max_timeout are unsigned members, so a configured "-1" has
     * already become 4294967295 by the time it is one, and a check on the member could not tell
     * that from a deliberate value. The raw string is also what any complaint should quote back.
     */
    long configured(const std::map<std::string, std::string>& params, const std::string& key) {
        auto it = params.find(key);
        if (it == params.end() || it->second.empty()) {
            // Unreachable via the ClientServer constructor, which runs first and whose std::stoi
            // rejects a missing or empty value already; here so that this function has an answer
            // for every input rather than an undefined one.
            throw std::runtime_error("ClientServer recover: " + key + " is required");
        }
        return std::stol(it->second);
    }

}

FMI::Comm::RecoverableClientServer::RecoverableClientServer(std::map<std::string, std::string> params)
        : ClientServer(params) {
    // Exact-string "true", the convention TcpChannelBase::parse_tcp_params uses for framed and
    // recover_links (src/comm/TcpChannelBase.cpp:21-22). Config values reach a channel as an
    // untyped map<string, string>, so anything that is not literally "true" — "1", "yes", "True"
    // — leaves the flag off, in both places, rather than each backend inventing its own spelling.
    recover = params.count("recover") > 0 && params.at("recover") == "true";
    if (params.count("object_ttl_s") > 0 && !params.at("object_ttl_s").empty()) {
        try {
            object_ttl_s = static_cast<unsigned int>(std::stoul(params.at("object_ttl_s")));
        } catch (const std::exception&) {
            // std::stoul's own complaint names neither the key nor the value; configuration
            // arrives as untyped strings and this is the only place that knows which one it was.
            throw std::runtime_error("ClientServer: object_ttl_s must be a number of seconds, got \""
                                     + params.at("object_ttl_s") + "\"");
        }
    }

    if (!recover) {
        // Flag off: this class is transparent, and that includes not rejecting a configuration
        // the family accepts today.
        return;
    }

    if (object_ttl_s == 0) {
        // Not refused — "no expiry" is a legitimate thing to ask a store for, and a job whose
        // objects are cleaned up by something else entirely may well want it. But under recover it
        // is also the one setting under which nothing at all reclaims them: finalize deletes
        // nothing by design, so every object of every run stays in the store until somebody
        // notices. Said once, at construction, where the operator can still connect it to what
        // they wrote.
        BOOST_LOG_TRIVIAL(warning) << "ClientServer recover: object_ttl_s is 0, so nothing this "
                                      "channel writes will ever expire — and a recovered job "
                                      "deletes nothing on the way out, so its objects are kept "
                                      "until something outside the job removes them";
    }

    // The poll budget is the failure detector under recover, so it has to be a budget at all.
    // timeout is the sleep between polls and also the amount elapsed_time advances by, so at 0
    // the ClientServer loops neither sleep nor make progress towards max_timeout: a download of
    // an object that will never appear spins on the store at full speed, forever. That is simply
    // what "timeout": 0 does today, but a channel that offers to survive a broken connection
    // cannot also offer never to notice one, so it is refused here instead of shipped.
    const long poll_interval = configured(params, "timeout");
    const long poll_budget = configured(params, "max_timeout");
    if (poll_interval < 1) {
        throw std::runtime_error("ClientServer recover: timeout must be >= 1, got "
                                 + params.at("timeout"));
    }
    if (poll_budget < poll_interval) {
        // A budget below one interval is a single poll at best and zero at worst; either way the
        // configured patience is not what the operator wrote down.
        throw std::runtime_error("ClientServer recover: max_timeout must be >= timeout ("
                                 + std::to_string(poll_interval) + "), got "
                                 + params.at("max_timeout"));
    }
}

//! Wait for the ranks of this communicator, and only those, by asking for their markers by name.
/*!
 * The base implementation lists the whole store and counts the names ending in "_barrier_<n>".
 * That suffix carries no communicator name, so markers written by another communicator — or by an
 * unrelated earlier run — are counted as arrivals of this one, and a barrier can be satisfied
 * while half its ranks have not reached it. It is also the most expensive thing the family does:
 * one whole-keyspace listing per rank per poll, at poll intervals of a millisecond, which is a
 * KEYS * (O(keyspace), and single-threaded Redis serves nothing else while it runs) or an
 * unpaginated S3 ListObjectsV2 that stops at a thousand objects.
 *
 * Asking for the N names this communicator's ranks will write replaces all of that with N GETs of
 * one byte, none of which can be answered by anything but the rank it names. The markers are
 * per-(rank, generation) and, under recover, never deleted, so a rank that arrives late still
 * finds the ones written before it — which is exactly what the base implementation's listing could
 * not promise once finalize started removing objects.
 *
 * Everything else is the base's: the counter is read once and advanced at entry, the marker goes
 * out through upload(), and the poll budget is spent the same way, one timeout per pass.
 */
void FMI::Comm::RecoverableClientServer::barrier() {
    if (!recover) {
        ClientServer::barrier();
        return;
    }

    const auto barrier_num = num_operations["barrier"];
    num_operations["barrier"]++;
    const std::string suffix = "_barrier_" + std::to_string(barrier_num);
    // One byte, the same marker the base writes — and the size every probe below expects, since
    // under recover a download of the wrong length is a collision rather than a marker.
    char marker = '1';
    upload({&marker, sizeof(marker)}, object_key_prefix() + std::to_string(peer_id) + suffix);

    std::vector<bool> arrived(num_peers, false);
    // Ours is written; nobody has to tell us about it.
    arrived[peer_id] = true;
    Utils::peer_num remaining = num_peers - 1;

    unsigned int elapsed_time = 0;
    while (elapsed_time < max_timeout) {
        for (Utils::peer_num i = 0; i < num_peers; i++) {
            if (arrived[i]) {
                continue;
            }
            char probe = 0;
            if (download_object({&probe, sizeof(probe)},
                                object_key_prefix() + std::to_string(i) + suffix)) {
                // Once seen, always seen: a rank does not un-arrive, and remembering it across
                // passes is what keeps the cost of a barrier proportional to the ranks still
                // missing rather than to num_peers every millisecond.
                arrived[i] = true;
                remaining--;
            }
        }
        if (remaining == 0) {
            return;
        }
        elapsed_time += timeout;
        std::this_thread::sleep_for(std::chrono::milliseconds(timeout));
    }
    throw Utils::Timeout();
}

//! Shutdown, which under recover means leaving the store to it.
/*!
 * A rank that has finished cannot know whether its peers still need what it wrote. Deleting on
 * the way out is what makes a staggered shutdown lose messages today: the first rank out of a
 * collective takes its objects with it, and the ranks still polling wait for something that no
 * longer exists — the deadlock tests/channels.cpp:836-840 works around with a shared-memory
 * rendezvous, about seven runs in ten. So under recover nothing is deleted here. Every object was
 * written with a store-side expiry (Redis SET ... EX; an S3 lifecycle rule) and goes away on its
 * own, which is also the only cleanup that still works when a rank never reaches finalize at all —
 * killed, evicted, or checkpointed and not restored.
 *
 * The obvious alternative, a done-marker barrier before deleting, fails twice over. Its wait can
 * end in Utils::Timeout, and finalize runs from ~Communicator, which is noexcept — a timeout on
 * the way out would be std::terminate. And the marker set is unsound whichever way it is cleaned
 * up: left behind, a later run reusing the communicator name passes the gate instantly and re-arms
 * exactly the deadlock it was meant to close; deleted, a peer still polling strands for its whole
 * budget.
 *
 * The precondition this leans on is that a communicator name is unique per job run — which
 * everything in the tree already assumes and provides (the sweep, the runbook, these tests all
 * build pid- or clock-stamped names). Two runs sharing a name inside the expiry window would read
 * each other's objects; that hazard predates this flag and is not made better or worse by it.
 *
 * The catch-all is not defensive decoration. This is called from a destructor, and an exception
 * that leaves it does not fail an operation, it ends the process. Which is also why the reporting
 * is wrapped in turn: BOOST_LOG_TRIVIAL formats and allocates, so a handler that only logs is
 * still a handler that can throw, and it would throw from inside the one place that must not.
 */
void FMI::Comm::RecoverableClientServer::finalize() {
    try {
        if (!recover) {
            ClientServer::finalize();
        }
    } catch (const std::exception& e) {
        try {
            BOOST_LOG_TRIVIAL(error) << "ClientServer: finalize failed, objects may be left behind: "
                                     << e.what();
        } catch (...) {
            // Nothing left to say it with. Leaving quietly beats ending the process.
        }
    } catch (...) {
        try {
            BOOST_LOG_TRIVIAL(error) << "ClientServer: finalize failed, objects may be left behind";
        } catch (...) {
        }
    }
}

//! Write an object, recording it for deletion only when something will ever delete it.
/*!
 * The base class remembers every name it writes so that finalize can remove them. Under recover
 * finalize removes nothing, so the list would only grow: one entry per message for the whole run,
 * in a process whose memory is about to be written to a checkpoint image. Skipping it keeps the
 * image proportional to what the rank is doing rather than to how long it has been doing it.
 */
void FMI::Comm::RecoverableClientServer::upload(channel_data buf, std::string name) {
    if (!recover) {
        ClientServer::upload(buf, name);
        return;
    }
    upload_object(buf, name);
}

std::string FMI::Comm::RecoverableClientServer::object_key_prefix() const {
    // The separator exists only under recover because it renames every object this channel
    // touches, and a flag-off channel has to keep writing the names it always wrote — including
    // for a peer built from an older revision.
    //
    // Without it the prefix runs straight into a rank number, so communicator "job" rank 11 and
    // communicator "job1" rank 1 both name their first send to rank 2 "job11_2_0": two unrelated
    // jobs read each other's messages, silently and only when the names happen to line up.
    // '|' can appear in neither a rank number nor an operation tag, and is ordinary in both a
    // Redis key and an S3 object name.
    return recover ? comm_name + "|" : comm_name;
}
