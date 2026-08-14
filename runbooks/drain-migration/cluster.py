#!/usr/bin/env python3
"""The cluster layer of the multi-host drain campaign: ssh, criu, and the control plane.

Reusable on purpose -- `multihost_drain.py` drives the campaign with it, and the LULESH driver
of phase D imports the same functions. Nothing in here knows what a scenario is.

Three things it owns:

  * **ssh plumbing** (`Cluster`), copied from `../criu-transparent-checkpoint/multihost_sweep.py`
    down to the launch string, because every rule in it was paid for by a failure.
  * **the criu legs** (`criu_dump`, `criu_restore`), with the flag set that IS this runbook's
    acceptance criterion: privileged criu (see the CRIU note below — the time namespace),
    `--manage-cgroups=ignore -v4`, and **no `--tcp-close`, no `--tcp-established`, no
    `--shell-job`**.
  * **the control-plane producers** the library deliberately has none of: `emit_migrate`, the
    `migrate` event nothing in FMI writes, and the batch lease a *driver* holds so that several
    ranks may drain in one cut (`DrainTCP::quiesce_and_drain` skips self-leasing when the
    migrate event carried a non-empty batch, so the lease must be held by whoever asked).

Every ssh call carries a timeout, at both layers: `-o ConnectTimeout=10` for the handshake and
a `subprocess` timeout for the whole call. A call that times out comes back as a
`CompletedProcess` with returncode 124 rather than an exception, so a caller's error handling is
one shape and not two.

**No polling over ssh.** `wait_pid_gone` here does not loop over `test -d /proc/<pid>` the way
multihost_sweep does; it makes ONE call to `node_helper.py wait-gone` and the loop runs inside
the node. At an ssh RTT of ~0.15 s the difference is the whole budget of a migration. The
semantics (bounded wait, True iff the pid went away) are unchanged.
"""
import json
import os
import re
import shlex
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

# Single-sourced from the published single-host driver: the registry a config names is addressed
# in exactly one place, and the evidence in README.md was produced through this function.
from drain_driver import redis_cli  # noqa: E402

NODE_HELPER = os.path.join(HERE, "node_helper.py")

# criu, both legs. The omissions are the claim:
#   no --tcp-close / --tcp-established -- the drain is supposed to have left the process owning
#     zero sockets, so criu must be able to image it without being told to drop connections. A
#     dump that criu accepts without them IS the assertion this runbook measures.
#   no --shell-job -- the ranks are launched with setsid, stdin from /dev/null and stdout to a
#     regular file, so there is no controlling terminal to reattach. Leaving it off keeps the
#     invocation honest about what the image contains.
#   --manage-cgroups=ignore -- ranks land in per-session systemd cgroups whose paths exist only
#     on the host that created them; without this, criu tries to re-enter the dump host's
#     session cgroup on the restore host. (multihost_sweep.py's lesson, unchanged.)
# Privileged criu, deliberately: --unprivileged cannot create a time namespace, so a
# cross-host restore lands with the DESTINATION's CLOCK_MONOTONIC and every absolute
# steady_clock deadline the image carries is off by the uptime difference — forward jumps
# expire them all instantly (measured: a 74h-uptime gap threw Timeout right after a
# successful migration; the same cut under sudo criu, which builds the timens and
# preserves the clock, passed with a 756 ms window). The acceptance criterion is
# unchanged: no --tcp-close, no --tcp-established, no --shell-job on either leg.
CRIU = "sudo criu"
CRIU_FLAGS = "--manage-cgroups=ignore -v4"

# Phase A7's bands, keyed by the IP the driver addresses the node as. criu restores a pid
# verbatim, so a pid taken on the destination is a restore that fails with "File exists";
# disjoint per-node bands are what make four machines safe for each other.
DEFAULT_PID_BANDS = {
    "10.164.0.3": 1000000,   # criu-testing
    "10.164.0.4": 1500000,   # criu-node-2
    "10.164.0.5": 2000000,   # criu-node-1
    "10.164.0.6": 2500000,   # criu-node-3
}
# The plan says "512k bands"; 512*1024 = 524288 is wider than the 500000 spacing between the
# bases above, so bands specified that way would overlap and the assertion would admit a pid
# from the neighbouring node. 500000 is the spacing, is strictly tighter, and is what the
# assertion uses. Override with --pid-band-span if the bases are ever re-seeded further apart.
PID_BAND_SPAN = 500000

LEASE_RELEASE_LUA = ("if redis.call('GET', KEYS[1]) == ARGV[1] then "
                     "return redis.call('DEL', KEYS[1]) else return 0 end")


class SetupError(RuntimeError):
    """The environment is not what the campaign requires.

    Kept apart from every protocol verdict on purpose (the plan's failure policy): a pid out of
    band, a HEAD or subject sha that differs between nodes, a node that cannot be reached --
    none of these say anything about the drain protocol, and scoring them as a protocol failure
    would poison the evidence they were meant to protect.
    """


class Cluster:
    """ssh plumbing. Every node is reached over ssh, including the driver's own host --
    one code path, and it keeps rank processes out of the driver's session/cgroup."""

    def __init__(self, nodes, ssh_key="", ssh_user=None, helper=NODE_HELPER):
        self.nodes = list(nodes)
        # Deliberately NO ControlMaster multiplexing: a mux master that wedges (observed
        # here with a detached-launch session) silently hangs every later call to that
        # node. One plain handshake per call costs ~0.2 s and has no shared failure state.
        self.base = ["ssh", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=accept-new",
                     "-o", "ConnectTimeout=10"]
        if ssh_key:
            self.base += ["-i", ssh_key]
        self.user = ssh_user or os.environ.get("FMI_SSH_USER", os.environ.get("USER", "luca"))
        self.helper = helper

    def run(self, node, cmd, timeout=60):
        """Run cmd (a shell string) on node; returns CompletedProcess.

        A call that outruns `timeout` returns returncode 124 (timeout(1)'s convention) instead
        of raising: a driver that has to distinguish "the node said no" from "the node said
        nothing" needs one result shape, not an exception path around every call site.
        """
        try:
            return subprocess.run(self.base + ["%s@%s" % (self.user, node), cmd],
                                  capture_output=True, text=True, timeout=timeout)
        except subprocess.TimeoutExpired:
            return subprocess.CompletedProcess(
                args=cmd, returncode=124, stdout="",
                stderr="ssh to %s timed out after %ss" % (node, timeout))

    def all_nodes(self, cmd, timeout=60):
        return [(n, self.run(n, cmd, timeout)) for n in self.nodes]

    def helper_json(self, node, verb, *args, **kwargs):
        """Run one node_helper verb on `node`; returns (returncode, parsed-json-or-{}).

        The helper lives in the shared tree at the same absolute path on every node, so there is
        nothing to copy and nothing to keep in sync: `ssh node python3 <abspath> <verb>`.
        """
        timeout = kwargs.pop("timeout", 60)
        if kwargs:
            raise TypeError("unexpected keyword arguments: %s" % sorted(kwargs))
        cmd = "python3 " + shlex.quote(self.helper) + " " + verb
        for a in args:
            cmd += " " + shlex.quote(str(a))
        out = self.run(node, cmd, timeout=timeout)
        payload = {}
        line = out.stdout.strip().splitlines()
        if line:
            try:
                payload = json.loads(line[-1])
            except ValueError:
                payload = {"error": "unparseable helper output: %r" % out.stdout[-300:]}
        if not payload and out.stderr.strip():
            payload = {"error": out.stderr.strip()[:300]}
        return out.returncode, payload


# ----------------------------------------------------------------------------- pid bands

def pid_in_band(bands, node, pid, span=PID_BAND_SPAN):
    """Is `pid` inside `node`'s band? Unknown nodes are not asserted (returns True)."""
    base = bands.get(node) if bands else None
    if base is None:
        return True
    return base <= pid < base + span


def assert_launched_pid(bands, node, rank, pid, span=PID_BAND_SPAN):
    """A pid captured at launch must be in the launching node's band -- checked immediately.

    Immediately, because the alternative is finding out at the first restore: a node that was
    rebooted and lost its `ns_last_pid` seed hands out pids from 1 again, they collide with
    another node's, and the campaign fails hours later as a criu error that looks like a
    protocol problem. Here it is a refusal at second zero.
    """
    if not pid_in_band(bands, node, pid, span):
        base = bands.get(node)
        raise SetupError("rank %d launched on %s got pid %d, outside its band [%d, %d) -- "
                         "re-seed /proc/sys/kernel/ns_last_pid on that node (phase A7)"
                         % (rank, node, pid, base, base + span))


def assert_restored_pid(bands, home, dest, rank, pid, span=PID_BAND_SPAN):
    """A restored pid keeps the band of the node the rank was LAUNCHED on, and must not be in
    any OTHER node's band.

    criu restores the dumped pid verbatim -- that is the whole reason the bands exist -- so a
    migrated rank does not acquire the destination's band and never could. What the bands
    promise is the other statement, and it is the one worth asserting after a move: the pid this
    process carries cannot collide with a pid the destination hands out locally.

    `home` is the node the rank was **launched** on, and it has to be: the pid was allocated
    there once and then travels unchanged through every later move, so after a chained migration
    the machine a move starts FROM is no longer the machine whose band the pid belongs to.
    Passing the move's source instead reads a rank that has legitimately moved once as a cluster
    whose bands have gone wrong -- which is what B2's second leg (launched on N2, N3 -> N1) did.

    The destination check is skipped when the destination IS the home node, for the same reason
    stated positively: a rank coming BACK to the machine it was launched on carries a pid that is
    inside that machine's band by construction, and that is the one case where "inside the
    destination's band" is correct rather than alarming. (B2's third leg, N1 -> N2, is exactly
    it.) Everywhere else the bands are disjoint, so being in home's band already means being in
    no other node's -- the check is a guard against a mis-seeded or overlapping band, not a
    restatement of the first.
    """
    if bands and home in bands and not pid_in_band(bands, home, pid, span):
        raise SetupError("rank %d restored on %s with pid %d, which is outside its home band "
                         "on %s -- the bands no longer describe this cluster"
                         % (rank, dest, pid, home))
    if bands and dest in bands and dest != home and pid_in_band(bands, dest, pid, span):
        raise SetupError("rank %d moved to %s keeping pid %d, which is INSIDE %s's own band "
                         "(its home is %s): the destination can hand that pid to a local process "
                         "and the next restore there collides" % (rank, dest, pid, dest, home))


# ----------------------------------------------------------------------------- the job

def launch_ranks(cluster, comm, placement_nodes, subject, config, outdir, rounds, ms,
                 print_every=25, payload_ints=1, shape="baseline", cwd=HERE, bands=None,
                 span=PID_BAND_SPAN, trace=False, timeout=60):
    """Launch every rank on the node its placement names, detached. Returns rank -> (node, pid).

    The launch hygiene is sweep.py's and multihost_sweep.py's, verbatim, and every clause of it
    is load-bearing:

      * `;` NOT `&&` before setsid: with `cd X && cmd &` the WHOLE `cd && cmd` list is
        backgrounded as a subshell, so `$!` names the subshell (criu would dump the wrong
        process) and that subshell inherits the ssh session's stdout/stderr and holds the
        channel open forever (observed: every launch hung for its full timeout). With `;` the
        background job is the bare setsid command.
      * stdin from /dev/null, never inherited: a rank holding one end of whatever socketpair
        drives the launching ssh session makes criu refuse the dump with "External socket is
        used" -- which under THIS runbook's acceptance rule would read as a socket surviving
        the drain.
      * stdout to a regular file, setsid for its own session: with those, neither criu leg needs
        `--shell-job`.
      * `--shape` LAST: the kill patterns match on the positional arguments, so no option may
        ever move one of them.
      * no rebuilt environment handed to the child.

    The pid comes from `LAUNCHED:$!` at the instant of the launch and is asserted in-band right
    there. It is never re-derived with pgrep afterwards: a pgrep pattern is a guess about a
    command line, and this is the kernel's answer.
    """
    npeers = len(placement_nodes)
    placement = {}
    for r in sorted(placement_nodes):
        node = placement_nodes[r]
        log = os.path.join(outdir, "r%d.log" % r)
        trace_prefix = "FMI_LINK_TRACE=1 " if trace else ""
        inner = ("cd %s; %ssetsid %s %d %d %s %s %d %d %d %d --shape %s "
                 "< /dev/null > %s 2>&1 & echo LAUNCHED:$!"
                 % (shlex.quote(cwd), trace_prefix, shlex.quote(subject), r, npeers,
                    shlex.quote(config), shlex.quote(comm), rounds, ms, print_every,
                    payload_ints, shlex.quote(shape), shlex.quote(log)))
        out = cluster.run(node, inner, timeout=timeout)
        m = re.search(r"LAUNCHED:(\d+)", out.stdout)
        if out.returncode != 0 or not m:
            raise SetupError("failed to launch rank %d on %s: %s %s"
                             % (r, node, out.stdout, out.stderr))
        pid = int(m.group(1))
        assert_launched_pid(bands, node, r, pid, span)
        placement[r] = (node, pid)
    return placement


def pid_alive(cluster, node, pid):
    """One call, no loop: `test -d /proc/<pid>` on the node."""
    return cluster.run(node, "test -d /proc/%d" % pid, timeout=30).returncode == 0


def wait_pid_gone(cluster, node, pid, timeout_s=30):
    """True once `pid` is gone from `node`, False if it is still there at the deadline.

    One ssh call -- the loop is `node_helper.py wait-gone`, inside the node. multihost_sweep's
    version polled `test -d /proc/<pid>` over ssh every 0.3 s; at ~0.15 s per handshake that
    spends most of the wait in ssh and cannot resolve anything finer.
    """
    rc, _ = cluster.helper_json(node, "wait-gone", pid, timeout_s, timeout=timeout_s + 30)
    return rc == 0


def sigqueue_remote(cluster, node, pid, sig, value, timeout=30):
    """Queue the drain request at a rank on another machine. Returns (ok, payload).

    The value is the target's current epoch and is mandatory -- see node_helper's sigqueue
    docstring: without it the request arrives as epoch 0 and any rank that has migrated once
    drops it as stale.
    """
    rc, payload = cluster.helper_json(node, "sigqueue", pid, sig, value, timeout=timeout)
    return rc == 0, payload


def cont_remote(cluster, node, pid, timeout=30):
    """SIGCONT a rank criu restored in group-stop. Returns (ok, payload)."""
    rc, payload = cluster.helper_json(node, "cont-if-stopped", pid, timeout=timeout)
    return rc == 0, payload


def seal_wait_remote(cluster, node, pid, timeout_s, state="T", timeout=None):
    """(rc, payload) of one seal-wait: the stop and the fd scan in a single round trip.

    rc 0 = stopped and owns no socket; 1 = never reached the state; 4 = stopped WITH sockets,
    which is the protocol verdict this whole runbook exists to detect.
    """
    return cluster.helper_json(node, "seal-wait", pid, timeout_s, state,
                               timeout=timeout if timeout is not None else timeout_s + 30)


def criu_dump(cluster, node, pid, imgdir, timeout=180):
    """`criu dump` on the node that holds the rank. No socket flags -- see CRIU_FLAGS.

    The log is chmodded readable afterwards and criu's own exit status is preserved across it.
    That is not tidiness: criu creates `dump.log` mode 0600 as **root** (it runs under sudo, see
    CRIU), and on the node where the shared tree is a LOCAL filesystem -- criu-testing, which is
    the NFS server -- root stays root, so the driver cannot read the log it is about to search
    for socket lines. On the other three nodes the export's `all_squash anonuid=1000` turns that
    same root into `luca` and the log is readable, which is exactly what made the hole invisible:
    it opened only for ranks dumped on one machine of four. `drain_driver.socket_lines` returns
    `([], 0)` for a log it cannot open, which is indistinguishable from a clean one -- so the
    runbook's acceptance criterion 2 passed VACUOUSLY for those. `dump_all` now refuses an
    unreadable log outright; this makes it readable in the first place.
    """
    quoted = shlex.quote(imgdir)
    return cluster.run(node, "%s dump %s -t %d -D %s -o dump.log; rc=$?; "
                             "%s chmod 0644 %s/dump.log 2>/dev/null; exit $rc"
                             % (CRIU, CRIU_FLAGS, pid, quoted, CRIU.split()[0], quoted),
                       timeout=timeout)


def criu_restore(cluster, node, imgdir, cwd=HERE, timeout=180):
    """`criu restore` on the DESTINATION node, from the shared image directory.

    `cd <run cwd>` first, and with `&&` rather than `;`: the `;` rule is about *backgrounding*
    (`cd X && cmd &` backgrounds the whole list), and nothing here is backgrounded -- criu's own
    `-d` is what detaches the restored tree. If the cd fails there is nothing to restore into
    and the call should not proceed.

    `-d` and nothing else about state: the restored process comes back in group-stop because
    that is what the image contains, and the caller SIGCONTs it.
    """
    quoted = shlex.quote(imgdir)
    return cluster.run(node, "cd %s && %s restore %s -D %s -o restore.log -d; rc=$?; "
                             "%s chmod 0644 %s/restore.log 2>/dev/null; exit $rc"
                             % (shlex.quote(cwd), CRIU, CRIU_FLAGS, quoted,
                                CRIU.split()[0], quoted),
                       timeout=timeout)


def kill_all(cluster, subject, comm, timeout=30):
    """pkill every rank of `comm` on every node.

    The pattern's first character is BRACKETED -- `[f]mi_checkpoint_subject` -- because
    `pkill -f` also reads the command line of the shell that is running it: the remote shell's
    own argv contains the pattern verbatim, so an unbracketed pattern kills the shell doing the
    killing (and the ssh call comes back looking like it worked). `[f]mi…` matches
    `fmi…` as a regex while the literal text in the shell's argv reads `[f]mi…` and does not
    match itself.
    """
    base = os.path.basename(subject)
    pattern = "[" + base[0] + "]" + base[1:] + " .* " + comm + " "
    cluster.all_nodes("pkill -9 -f %s || true" % shlex.quote(pattern), timeout=timeout)


# --------------------------------------------------------------- the control-plane producers

def events_key(comm):
    return "fmi:drain:%s:events" % comm


def batch_key(comm):
    return "fmi:drain:%s:batch" % comm


def emit_migrate(params, comm, rank, epoch, batch, ttl_s=3600, extra=None):
    """XADD the `migrate` event. **The producer the library does not have.**

    `poll_control_events` consumes it (src/comm/DrainTCP.cpp): a rank that reads a migrate
    addressed to itself with a NON-EMPTY batch adopts that batch id and sets
    `batch_lease_external`, which is what makes it skip self-leasing in `quiesce_and_drain` --
    and that is what makes a batch of several ranks legal at all, since the one-batch-per-
    communicator lease is held by the driver for the whole cut.

    The field names and their order are `RedisDrainCoordinator::emit`'s, byte for byte:
    `type rank epoch batch`, then EXPIRE (which `PeerRegistry::xadd` issues when ttl_s > 0).
    An extra that collides with one of the four is dropped rather than allowed to rewrite the
    event, exactly as `emit` does.

    Note the asymmetry, deliberately not papered over here: `poll_control_events` applies no
    epoch fence to a migrate addressed to self (the signal path's `handle_signal_request` does).
    The epoch field is still written -- it is what the event trail is checked against, and a
    later build that fences on it must find it there.

    Returns the entry id, or None if the XADD failed.
    """
    fields = ["type", "migrate", "rank", str(rank), "epoch", str(epoch), "batch", str(batch)]
    for key, value in (extra or {}).items():
        if key in ("type", "rank", "epoch", "batch"):
            continue
        fields += [str(key), str(value)]
    out = redis_cli(params, "XADD", events_key(comm), "*", *fields)
    if out.returncode != 0:
        return None
    entry = out.stdout.strip()
    if ttl_s > 0:
        redis_cli(params, "EXPIRE", events_key(comm), int(ttl_s))
    return entry or None


def batch_lease_take(params, comm, owner, px_ms):
    """SET <batch key> <owner> NX PX <ms> -- `PeerRegistry::set_nx_px`, from outside.

    A driver-held lease is what makes a batch legal: the ranks it addresses find
    `batch_lease_external` set and do not try to take what they are running under, while any
    OTHER migration -- a stray signal, a second driver -- hits a held lease and refuses loudly.
    Only "OK" means this caller took it; a held lease answers nil (empty output).
    """
    out = redis_cli(params, "SET", batch_key(comm), owner, "NX", "PX", max(int(px_ms), 1))
    return out.returncode == 0 and out.stdout.strip() == "OK"


def batch_lease_release(params, comm, owner):
    """Compare-and-delete, `PeerRegistry::del_if_equal`'s script verbatim.

    Never a plain DEL: a lease that expired and was retaken by someone else must not be deleted
    by the previous holder's release.
    """
    out = redis_cli(params, "EVAL", LEASE_RELEASE_LUA, 1, batch_key(comm), owner)
    return out.returncode == 0 and out.stdout.strip() == "1"


def batch_lease_owner(params, comm):
    """Whoever holds the lease right now, or "" if it is free."""
    out = redis_cli(params, "GET", batch_key(comm))
    return out.stdout.strip() if out.returncode == 0 else ""


def batch_lease_wait_free(params, comm, timeout_s=30.0, poll_s=0.05):
    """Wait until the batch lease is free. **The emit/release race guard.**

    A rank that leased for itself releases inside `resume_after_restore`, one Redis round trip
    AFTER it emitted `restored`. A driver that fires the next migration the instant it sees
    `restored` therefore races that release: its own SET NX lands first, the rank's
    compare-and-delete then finds its own value and removes the lease the driver is holding, and
    the next batch runs leaseless. Waiting for the key to disappear costs one poll and closes it.

    Polls Redis directly (the driver's own host, sub-millisecond); nothing here goes over ssh.
    """
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        out = redis_cli(params, "EXISTS", batch_key(comm))
        if out.returncode == 0 and out.stdout.strip() == "0":
            return True
        time.sleep(poll_s)
    return False


# ----------------------------------------------------------------------------- preflight

def preflight(cluster, params, subject, tree, config_path, bands=None, span=PID_BAND_SPAN,
              probe_port=22, timeout=60):
    """Verify the cluster is what the campaign assumes. Returns (problems, facts).

    `problems` is a list of strings; empty means go. `facts` is what the run log records, so
    that a result can be attributed to a cluster state months later.

    What is checked, and why each one is here:

      1. **ssh to every node.** Everything else is an ssh call.
      2. **the registry is not loopback.** A restored rank re-resolves its advertise address
         against the registry it is configured with; if that is 127.0.0.1 it publishes the
         loopback of whatever machine it woke up on, and the job silently wires itself to
         nothing. This is a hard refusal, not a warning.
      3. **redis PING from every node**, to the address in the config -- the registry AND the
         drain coordinator live there, and a node that cannot reach it cannot migrate.
      4. **TCP reachability between every ordered pair of nodes.** The probe uses the ssh port
         because it is the one port guaranteed to have a listener on every node; on this cluster
         firewalld puts eth0 in the `trusted` zone, so reachability is all-or-nothing and one
         port answers for all of them. Override with probe_port if that ever stops being true.
      5. **pid bands**, per node (phase A7): a rebooted node is a setup error now, not a restore
         failure in hour three.
      6. **one tree, one binary**: `git rev-parse HEAD` of the shared checkout and the subject's
         sha256 must be identical on every node -- and the runtime closure (`ldd` -> `rpm -qf`),
         criu's version and the kernel release with them. criu remaps libraries by path and
         content; a single differing NVRA is a restore that fails for a reason nobody will find.
    """
    problems, facts = [], {"nodes": {}}

    for node, res in cluster.all_nodes("echo ok", timeout=timeout):
        if res.returncode != 0 or "ok" not in res.stdout:
            problems.append("cannot ssh to %s: %s" % (node, res.stderr.strip()[:200]))

    host = str(params.get("registry_host", ""))
    port = str(params.get("registry_port", 6379))
    facts["registry"] = "%s:%s" % (host, port)
    if host in ("127.0.0.1", "localhost", "::1", ""):
        problems.append("registry_host is %r -- a restored rank re-resolves its advertise "
                        "address against the registry, and a loopback registry means every "
                        "rank publishes the loopback of whatever machine it woke up on" % host)
        return problems, facts

    for node, res in cluster.all_nodes("redis-cli -h %s -p %s ping" % (shlex.quote(host),
                                                                      shlex.quote(port)),
                                       timeout=timeout):
        if "PONG" not in res.stdout:
            problems.append("%s cannot PING the registry at %s:%s: %s"
                            % (node, host, port, (res.stdout + res.stderr).strip()[:200]))

    for source in cluster.nodes:
        targets = [n for n in cluster.nodes if n != source]
        if not targets:
            continue
        # bash's /dev/tcp, one call per target, no nested quoting: the host and the port arrive
        # as $0 and $1 of the inner shell rather than being interpolated into it.
        probe = "; ".join(
            "timeout 5 bash -c 'exec 3<>/dev/tcp/\"$0\"/\"$1\"' %s %d || echo UNREACHABLE %s"
            % (shlex.quote(t), probe_port, shlex.quote(t)) for t in targets)
        res = cluster.run(source, probe, timeout=timeout)
        if res.returncode != 0:
            problems.append("reachability probe failed on %s: %s"
                            % (source, (res.stdout + res.stderr).strip()[:200]))
        for line in res.stdout.splitlines():
            if line.startswith("UNREACHABLE"):
                problems.append("%s -> %s: %s (firewalld: eth0 must be in the trusted zone)"
                                % (source, line.split()[1], line))

    if bands:
        for node in cluster.nodes:
            base = bands.get(node)
            if base is None:
                problems.append("no pid band configured for %s" % node)
                continue
            rc, payload = cluster.helper_json(node, "pidband", base, span, timeout=timeout)
            facts["nodes"].setdefault(node, {})["pidband"] = payload
            if rc != 0:
                problems.append("%s allocates pids outside its band [%d, %d): %s"
                                % (node, base, base + span, payload))

    heads, shas, closures, crius, kernels = {}, {}, {}, {}, {}
    for node in cluster.nodes:
        # Through the helper, not `git rev-parse`: git is installed on the development host and
        # on none of the worker nodes, and adding it to them would put an unpinned package on the
        # machines the identity gate is about. The helper reads `.git/HEAD` and the ref it names
        # off the shared filesystem, from inside the node -- which is the claim the gate makes.
        rc, payload = cluster.helper_json(node, "head", tree, timeout=timeout)
        heads[node] = payload.get("head") if rc == 0 and payload.get("head") else \
            "ERROR: " + str(payload.get("error") or payload)[:120]
        rc, payload = cluster.helper_json(node, "nvra", subject, timeout=timeout)
        facts["nodes"].setdefault(node, {})["identity"] = payload
        shas[node] = payload.get("sha256")
        closures[node] = tuple(payload.get("nvra") or ())
        crius[node] = payload.get("criu")
        kernels[node] = payload.get("kernel")
        if rc != 0:
            problems.append("%s: cannot identify the subject at %s: %s"
                            % (node, subject, payload.get("error", payload)))
        res = cluster.run(node, "test -f %s" % shlex.quote(config_path), timeout=timeout)
        if res.returncode != 0:
            problems.append("%s: the config %s does not exist there (the tree must be shared at "
                            "the SAME absolute path on every node)" % (node, config_path))

    facts["head"] = heads
    facts["subject_sha256"] = shas
    for label, values in (("HEAD of " + tree, heads), ("subject sha256", shas),
                          ("runtime closure", closures), ("criu version", crius),
                          ("kernel release", kernels)):
        distinct = set(values.values())
        if len(distinct) > 1:
            problems.append("%s differs between nodes: %s" % (label, values))
    return problems, facts
