#ifndef FMI_DRAINCOORDINATOR_H
#define FMI_DRAINCOORDINATOR_H

#include "../utils/Common.h"

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace FMI::Comm {
    class PeerRegistry;

    //! Where a rank says it can be dialled, and which lineage of it said so.
    /*!
     * The same three fields the transport registry carries, plus the epoch. The epoch is what
     * makes a member record answerable: a survivor that reconnects to a stale address learns
     * nothing from the address itself, but an epoch below the one it last saw restored says
     * plainly that this record predates the migration.
     */
    struct MemberRecord {
        std::uint64_t epoch = 0;
        std::string ip;
        int port = 0;
        std::uint64_t nonce = 0;

        //! "epoch:ip:port:nonce".
        [[nodiscard]] std::string encode() const;

        static bool decode(const std::string& text, MemberRecord& out);
    };

    //! One thing that happened to one rank, ordered against everything else that happened.
    /*!
     * Deliberately flat and stringly-typed in `extra`: the events cross a Redis stream, whose
     * entries are field/value pairs, and every consumer of an event this protocol does not yet
     * define must be able to ignore it rather than fail to parse the stream.
     */
    struct DrainEvent {
        enum class Type {
            Unknown,    //!< something a newer version emitted; ignored, never fatal
            Migrate,    //!< a driver asks `rank` to migrate
            Leaving,    //!< `rank` has begun its migration and is about to seal its links
            Sealed,     //!< `rank` owns no sockets and is ready to be dumped
            Restored    //!< `rank` is back, at `epoch`, listening on a fresh address
        };

        Type type = Type::Unknown;
        Utils::peer_num rank = 0;
        std::uint64_t epoch = 0;
        std::string batch;
        std::map<std::string, std::string> extra;
    };

    const char* to_string(DrainEvent::Type type);

    DrainEvent::Type drain_event_type_from(const std::string& text);

    //! The control plane of the drain protocol, as narrowly as it can be stated.
    /*!
     * An interface rather than the Redis implementation directly, for one reason that matters:
     * the orderings this protocol has to survive — an event delivered twice, an event from a
     * superseded epoch, a leave notice that overtakes the FIN it describes — are orderings a
     * healthy Redis will not produce on demand. A test fake produces them in a millisecond.
     *
     * Every method may throw. A control plane that cannot be reached during a migration is a
     * loud failure by design: this protocol is one-phase and assumes the migration succeeds, so
     * the alternative to failing loudly is a rank that seals its links and is never told to
     * come back.
     */
    class DrainCoordinator {
    public:
        virtual ~DrainCoordinator() = default;

        //! Advertise where this rank now listens, under its current epoch.
        virtual void publish_member(Utils::peer_num rank, const MemberRecord& record) = 0;

        virtual std::map<Utils::peer_num, MemberRecord> members() = 0;

        virtual void remove_member(Utils::peer_num rank) = 0;

        //! Append an event. Returns its id, which orders it against every other event.
        virtual std::string emit(const DrainEvent& event) = 0;

        //! Id of the last event, or "0-0" for an empty stream. The resumption point a rank
        //! records before it is frozen.
        virtual std::string tail_id() = 0;

        //! Every event after @p last_id, blocking at most @p block_ms for the first one.
        virtual std::vector<std::pair<std::string, DrainEvent>> read_after(
                const std::string& last_id, long block_ms) = 0;

        //! Take the one batch lease, for at most @p px_ms. False when someone else holds it.
        virtual bool try_batch_lock(const std::string& owner, long px_ms) = 0;

        //! Release the lease, but only if this owner still holds it.
        virtual void release_batch_lock(const std::string& owner) = 0;

        //! Drop every socket this coordinator owns. Must leave it usable: the next call
        //! reconnects, which is what a restored rank depends on.
        virtual void disconnect() = 0;
    };

    //! The coordinator over Redis: a members hash, an events stream, and a SET NX PX lease.
    /*!
     * Keys are `fmi:drain:<comm>:members`, `:events` and `:batch`, disjoint from the transport
     * registry's `fmi:drain:<comm>` and from DirectTCP's `fmi:direct:*`. All three carry the
     * registry TTL, refreshed on every write, so a job that dies without finalizing expires
     * rather than poisoning the next run under the same name.
     */
    class RedisDrainCoordinator : public DrainCoordinator {
    public:
        RedisDrainCoordinator(std::string host, int port, std::string comm_name,
                              unsigned int ttl_s, long timeout_ms);

        ~RedisDrainCoordinator() override;

        void publish_member(Utils::peer_num rank, const MemberRecord& record) override;

        std::map<Utils::peer_num, MemberRecord> members() override;

        void remove_member(Utils::peer_num rank) override;

        std::string emit(const DrainEvent& event) override;

        std::string tail_id() override;

        std::vector<std::pair<std::string, DrainEvent>> read_after(const std::string& last_id,
                                                                   long block_ms) override;

        bool try_batch_lock(const std::string& owner, long px_ms) override;

        void release_batch_lock(const std::string& owner) override;

        void disconnect() override;

    private:
        [[nodiscard]] std::string members_key() const;
        [[nodiscard]] std::string events_key() const;
        [[nodiscard]] std::string batch_key() const;

        std::string comm_name;
        unsigned int ttl_s;
        //! Deadline every command is clamped to. One number, not a policy: a coordinator call
        //! that outlives the migration it belongs to is the same failure as one that never
        //! answers.
        long timeout_ms;
        std::unique_ptr<PeerRegistry> registry;
    };
}

#endif //FMI_DRAINCOORDINATOR_H
