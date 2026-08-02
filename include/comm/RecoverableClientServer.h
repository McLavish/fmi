#ifndef FMI_RECOVERABLECLIENTSERVER_H
#define FMI_RECOVERABLECLIENTSERVER_H

#include "ClientServer.h"
#include <map>
#include <string>

namespace FMI::Comm {
    //! Recovery layer for the store-backed channel family.
    /*!
     * Sits between ClientServer, which owns the collective algorithms, and the concrete backends,
     * exactly as TcpChannelBase sits between PeerToPeer and Direct / DirectTCP. Everything that
     * has to change so that a rank can lose its connection to the store — and be criu-frozen and
     * restored — mid-job lives here, so that neither the algorithms above nor the client code
     * below carries it.
     *
     * All of it is opt-in behind `"recover": "true"` on the backend's config block. With the flag
     * unset this class is transparent: every override delegates to ClientServer and the key names
     * are the ones the family has always written.
     */
    class RecoverableClientServer : public ClientServer {
    public:
        explicit RecoverableClientServer(std::map<std::string, std::string> params);

        //! Under recover, wait for the N markers this communicator's ranks write, by name.
        void barrier() override;

        //! Under recover, delete nothing and let the objects expire. See the definition.
        void finalize() override;

        //! Under recover, write the object without recording it for a deletion that never comes.
        void upload(channel_data buf, std::string name) override;

    protected:
        //! comm_name, plus a separator once recovery semantics are on. See the definition.
        std::string object_key_prefix() const override;

        //! Whether the recovery semantics are enabled for this channel.
        bool recover = false;

        //! How long the store should keep an object written under recover, in seconds.
        /*!
         * Recovered jobs do not delete their objects — a rank that leaves cannot know whether a
         * peer still needs them — so the store expires them instead. Mirrors DirectTCP's
         * registry_ttl_s, default included (src/comm/DirectTCP.cpp:311).
         */
        unsigned int object_ttl_s = 3600;
    };
}

#endif //FMI_RECOVERABLECLIENTSERVER_H
