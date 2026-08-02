#ifndef FMI_SIGNALS_H
#define FMI_SIGNALS_H

namespace FMI::Utils {
    //! Make a write to a broken connection fail with EPIPE instead of killing the process.
    /*!
     * FMI's own socket writes all pass MSG_NOSIGNAL, but hiredis — which carries DirectTCP's
     * peer registry and the Redis channel — does not, and a library user cannot reach those
     * sockets to fix it.
     *
     * This is not a nicety. A checkpointed rank is restored with every TCP connection it
     * owned already dropped (criu --tcp-close), so the very first registry
     * command after a restore writes to a dead socket. With the default disposition that
     * delivers SIGPIPE and the restored rank dies instantly, before it can print anything or
     * re-establish a single link — which is exactly what happened the first time an FMI rank
     * was checkpointed for real. The recovery machinery is irrelevant if the process is not
     * alive to run it.
     *
     * Idempotent, and deliberately conservative: the disposition is only changed when it is
     * still SIG_DFL, so an application that installed its own SIGPIPE handler keeps it. An
     * application that installs one *after* the first FMI channel is built also keeps it, and
     * accepts the consequence.
     */
    void suppress_sigpipe();
}

#endif //FMI_SIGNALS_H
