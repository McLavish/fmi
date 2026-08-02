#ifndef FMI_DIRECT_H
#define FMI_DIRECT_H

#include "TcpChannelBase.h"

namespace FMI::Comm {
    //! Channel that uses the TCPunch TCP NAT Hole Punching Library for connection establishment.
    /*!
     * Everything about moving bytes once a socket exists lives in TcpChannelBase; this class
     * only turns a pair name into a connected fd via the rendezvous server.
     *
     * Note on error reporting: TCPunch signals a failed pairing two different ways. A pairing
     * that times out throws TCPunch's own global ::Timeout, which establish() translates to
     * FMI::Utils::Timeout. Anything else goes through TCPunch's error_exit(), which throws a
     * bare std::string — that is deliberately NOT caught here. It propagates out of the channel
     * as the documented "the TCPunch client could not reach the rendezvous server" signal, and
     * the test suite catches it explicitly to distinguish that case from a real timeout.
     */
    class Direct : public TcpChannelBase {
    public:
        explicit Direct(std::map<std::string, std::string> params, std::map<std::string, std::string> model_params);

    protected:
        int establish(Utils::peer_num partner_id, const std::string& link_name) override;

        //! TCPunch pairing names, in the original direction-dependent form.
        /*!
         * Kept exactly as it was rather than adopting the base class's rank-ordered name:
         * changing how a link is named changes which registrations pair up on the rendezvous
         * server, so every deployment already running against this backend would have to be
         * upgraded in lockstep.
         */
        std::string link_name(Utils::peer_num partner_id, bool outbound) const override;

    private:
        std::string hostname;
        int port;
    };
}



#endif //FMI_DIRECT_H
