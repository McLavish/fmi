#!/usr/bin/env python3
"""Per-node agent for the multi-host drain campaign: every wait loop, inside the node.

Invoked over ssh from the SHARED tree, never copied anywhere:

    ssh <node> python3 /scratch/fmi/runbooks/drain-migration/node_helper.py <verb> [args...]

The shared checkout *is* the deployment. Nothing here imports anything outside the standard
library and nothing here is newer than python3.9 (Rocky 9's system interpreter), so a node
needs no packages, no venv and no uv.

**Why an agent at all.** An ssh round trip on this cluster costs ~0.15 s. A driver that polls
a remote `/proc` over ssh spends its entire budget in handshakes and cannot observe a state
that lasts a millisecond -- and the state this campaign has to observe (a rank that has sealed,
holds no socket and has stopped) is exactly that short. So every poll loop the protocol needs
lives HERE, inside the node, and the driver pays one round trip per *decision*: about six per
migration (sigqueue, seal-wait, dump, wait-gone, restore, cont).

Every verb prints **one line of JSON** on stdout with everything it observed, and says with its
exit code whether the condition the caller asked about held:

  0  the condition held (and the JSON says what was seen)
  1  the condition did not hold within the timeout -- a verdict about the run, not an error
  2  usage error (bad verb, bad arguments)
  3  an OS error the caller could not have prevented; the JSON carries "error"
  4  seal-wait only: the process reached the requested state but still owns socket fds.
     Separated from 1 because it is the campaign's central protocol verdict and must never be
     confused with "it never stopped".

Verbs
-----
  sigqueue <pid> <signal> <value>        queue a real-time signal carrying si_value.sival_int
  wait-state <pid> <state> <timeout_s>   wait for /proc/<pid>/stat's state letter
  seal-wait <pid> <timeout_s> [state]    wait for the state, then scan /proc/<pid>/fd
  wait-gone <pid> <timeout_s>            wait until the pid no longer exists
  cont-if-stopped <pid>                  SIGCONT a process that criu restored in group-stop
  pidband [base span]                    what this node is allocating pids from
  nvra <binary>                          the runtime closure identity gate reads
  head <tree>                            the shared checkout's HEAD, resolved without git
"""
import ctypes
import hashlib
import json
import os
import signal
import subprocess
import sys
import time

# The state poll interval. 20 ms is the library's own control tick
# (`control_poll_interval_ms` in the shipped configs), and the whole reason this loop is on the
# node: at an ssh RTT of ~150 ms the driver could not sample this often from outside.
POLL_S = 0.02

OK, TIMED_OUT, USAGE, OS_ERROR, SOCKETS_SURVIVED = 0, 1, 2, 3, 4


def emit(payload, code=OK):
    """One line of JSON on stdout, and the exit code the caller reads."""
    sys.stdout.write(json.dumps(payload, separators=(",", ":")) + "\n")
    sys.stdout.flush()
    return code


# ------------------------------------------------------------------------------ /proc reading

def proc_state(pid):
    """The single-letter state from /proc/<pid>/stat, or None if the process is gone.

    Read past the LAST ')': the second field is the executable name in brackets and a name
    containing a space or a bracket would otherwise shift every field after it. (This is
    drain_driver.proc_state, verbatim -- the two must agree on what "state T" means.)
    """
    try:
        with open("/proc/%d/stat" % pid) as f:
            text = f.read()
    except OSError:
        return None
    tail = text[text.rfind(")") + 1:].split()
    return tail[0] if tail else None


def fd_scan(pid):
    """(fds, sockets) for a process: every fd as "<n>:<target>", and the socket targets alone.

    The claim this campaign makes is that a drained rank owns ZERO sockets at the instant criu
    looks at it. This is that claim read from the process's own side, one moment before the
    dump; the dump log and the image contents are the other two readings, and the driver checks
    all three.
    """
    fds, sockets = [], []
    try:
        names = sorted(os.listdir("/proc/%d/fd" % pid), key=lambda n: int(n) if n.isdigit() else n)
    except OSError:
        return fds, sockets
    for name in names:
        try:
            target = os.readlink("/proc/%d/fd/%s" % (pid, name))
        except OSError:
            # The fd closed between listdir and readlink. Not evidence of anything.
            continue
        fds.append("%s:%s" % (name, target))
        if target.startswith("socket:"):
            sockets.append(target)
    return fds, sockets


# ----------------------------------------------------------------------------------- the verbs

class _Sigval(ctypes.Union):
    # Pointer-sized on purpose: the kernel's sigval is a union of an int and a void*, and a
    # one-int structure would pass 4 bytes where libc expects 8, leaving the top half of the
    # argument register whatever the ABI happened to have in it.
    _fields_ = [("sival_int", ctypes.c_int), ("sival_ptr", ctypes.c_void_p)]


def verb_sigqueue(argv):
    """sigqueue <pid> <signal> <value> -- queue `signal` at `pid` with `value` as sival_int.

    `os.kill` cannot carry a payload, and the payload is not decoration.
    `MigrationTrigger::handle_signal_request` (src/utils/MigrationTrigger.cpp) reads
    `info.si_value.sival_int` as **the epoch the request was addressed to** and DROPS a request
    whose epoch is behind the process's own restore count -- so that a request queued before a
    restore cannot be acted on after it. A request sent without a payload arrives as epoch 0 and
    is silently dropped by any rank that has already migrated once, which is precisely the
    chained-migration scenario this campaign runs (B2: epochs 0, 1, 2).

    The signal number is resolved by the caller, not here: glibc's SIGRTMIN is a function call
    that moves with the threading library's private signals, and the library resolves it the
    same way at run time (`MigrationTrigger::signal_for_offset`). Hard-coding 34 or 37 would
    agree with one build of one libc and no other.

    Testing it without a cluster and without FMI -- delivery:

        python3 - <<'PY' &
        import os, signal
        s = int(signal.SIGRTMIN) + 3
        signal.pthread_sigmask(signal.SIG_BLOCK, {s})
        print(os.getpid(), flush=True)
        print("got", signal.sigwaitinfo({s}).si_signo, flush=True)
        PY
        # then, with the printed pid:
        python3 node_helper.py sigqueue <pid> "$(python3 -c 'import signal;print(int(signal.SIGRTMIN)+3)')" 0

    Python's `signal.sigwaitinfo` exposes no `si_value`, so the *payload* can only be observed
    from C. The end-to-end check is the rank itself: signal a rank that has already migrated
    once with value 0 and its log must carry
    `MigrationTrigger: dropped a migration request for epoch 0; this process is already at
    epoch 1`; signal it with 1 and it migrates. That asymmetry is the payload, proved.
    """
    if len(argv) != 3:
        return emit({"error": "usage: sigqueue <pid> <signal> <value>"}, USAGE)
    try:
        pid, sig, value = (int(a) for a in argv)
    except ValueError:
        return emit({"error": "pid, signal and value must be integers"}, USAGE)
    libc = ctypes.CDLL("libc.so.6", use_errno=True)
    libc.sigqueue.argtypes = [ctypes.c_int, ctypes.c_int, _Sigval]
    if libc.sigqueue(pid, sig, _Sigval(sival_int=value)) != 0:
        err = ctypes.get_errno()
        return emit({"pid": pid, "signal": sig, "value": value, "errno": err,
                     "error": os.strerror(err)}, OS_ERROR)
    return emit({"pid": pid, "signal": sig, "value": value, "sent": True,
                 "sigrtmin": int(signal.SIGRTMIN)})


def verb_wait_state(argv):
    """wait-state <pid> <state> <timeout_s> -- poll /proc/<pid>/stat until it reads `state`."""
    if len(argv) != 3:
        return emit({"error": "usage: wait-state <pid> <state> <timeout_s>"}, USAGE)
    try:
        pid, want, timeout = int(argv[0]), argv[1], float(argv[2])
    except ValueError:
        return emit({"error": "pid must be an integer and timeout_s a number"}, USAGE)
    started = time.time()
    deadline = started + timeout
    state = proc_state(pid)
    while state != want and time.time() < deadline:
        time.sleep(POLL_S)
        state = proc_state(pid)
    payload = {"pid": pid, "want": want, "state": state, "gone": state is None,
               "waited_s": round(time.time() - started, 3)}
    return emit(payload, OK if state == want else TIMED_OUT)


def verb_seal_wait(argv):
    """seal-wait <pid> <timeout_s> [state=T] -- the stop and the fd scan in ONE round trip.

    The two halves belong together and in this order. `sealed` is emitted at the end of step 5
    of the migrator sequence; the registry and coordinator connections are dropped, the heap is
    trimmed and stdio is flushed *after* it, and only then does the process `raise(SIGSTOP)`.
    A driver that scans fds on the event alone races the very steps that close the last two
    sockets. Waiting for state T first is what makes the scan mean "what criu is about to see".

    Exit 4, not 1, when the state was reached but socket fds remain: that is the campaign's
    central protocol verdict (a socket survived the drain) and must never be reported as "the
    rank never stopped".
    """
    if len(argv) not in (2, 3):
        return emit({"error": "usage: seal-wait <pid> <timeout_s> [state]"}, USAGE)
    try:
        pid, timeout = int(argv[0]), float(argv[1])
    except ValueError:
        return emit({"error": "pid must be an integer and timeout_s a number"}, USAGE)
    want = argv[2] if len(argv) == 3 else "T"
    started = time.time()
    deadline = started + timeout
    state = proc_state(pid)
    while state != want and time.time() < deadline:
        time.sleep(POLL_S)
        state = proc_state(pid)
    payload = {"pid": pid, "want": want, "state": state, "gone": state is None,
               "waited_s": round(time.time() - started, 3), "fds": [], "sockets": []}
    if state != want:
        return emit(payload, TIMED_OUT)
    fds, sockets = fd_scan(pid)
    payload["fds"] = fds
    payload["sockets"] = sockets
    return emit(payload, SOCKETS_SURVIVED if sockets else OK)


def verb_wait_gone(argv):
    """wait-gone <pid> <timeout_s> -- criu kills the dumped process; wait for the pid to free.

    A restore cannot reclaim a pid that is still taken ("Can't fork for <pid>: File exists"),
    and on a same-host restore the pid it wants is the one that was just dumped. The ranks are
    launched with setsid and reparented to init, so init reaps them; this only waits.
    """
    if len(argv) != 2:
        return emit({"error": "usage: wait-gone <pid> <timeout_s>"}, USAGE)
    try:
        pid, timeout = int(argv[0]), float(argv[1])
    except ValueError:
        return emit({"error": "pid must be an integer and timeout_s a number"}, USAGE)
    started = time.time()
    deadline = started + timeout
    gone = not os.path.isdir("/proc/%d" % pid)
    while not gone and time.time() < deadline:
        time.sleep(POLL_S)
        gone = not os.path.isdir("/proc/%d" % pid)
    payload = {"pid": pid, "gone": gone, "state": proc_state(pid),
               "waited_s": round(time.time() - started, 3)}
    return emit(payload, OK if gone else TIMED_OUT)


def verb_cont_if_stopped(argv):
    """cont-if-stopped <pid> -- resume a process criu restored in group-stop.

    The migrator sequence ends in `raise(SIGSTOP)`; criu records the task as TASK_STOPPED and
    faithfully restores it that way, so `/proc/<pid>/stat` reads `T` after a successful
    `criu restore -d`. Nothing inside the library can undo that -- the thread that would call
    SIGCONT is the stopped one. Whoever asked for the migration is who resumes it.
    """
    if len(argv) != 1:
        return emit({"error": "usage: cont-if-stopped <pid>"}, USAGE)
    try:
        pid = int(argv[0])
    except ValueError:
        return emit({"error": "pid must be an integer"}, USAGE)
    state = proc_state(pid)
    if state is None:
        return emit({"pid": pid, "state": None, "gone": True, "sent": False}, TIMED_OUT)
    sent = False
    if state == "T":
        try:
            os.kill(pid, signal.SIGCONT)
            sent = True
        except OSError as exc:
            return emit({"pid": pid, "state": state, "sent": False, "error": str(exc)}, OS_ERROR)
    return emit({"pid": pid, "state": state, "sent": sent, "gone": False})


def verb_pidband(argv):
    """pidband [base span] -- what this node is currently allocating pids from.

    Read-only by design. Seeding `/proc/sys/kernel/ns_last_pid` needs root and is setup's job
    (phase A7); the driver's job is to REFUSE to run when a node is outside its band, so that a
    rebooted node becomes a setup error at launch instead of a restore failure in the middle of
    a campaign. Disjoint bands are what make criu's restore-the-pid-verbatim contract safe
    across four machines.

    `in_band` compares this helper process's OWN pid, which is the freshest sample of the
    node's allocator there is.
    """
    payload = {"self_pid": os.getpid(), "hostname": os.uname().nodename}
    for path, key in (("/proc/sys/kernel/ns_last_pid", "ns_last_pid"),
                      ("/proc/sys/kernel/pid_max", "pid_max")):
        try:
            with open(path) as f:
                payload[key] = int(f.read().strip())
        except (OSError, ValueError):
            payload[key] = None
    if not argv:
        payload["in_band"] = None
        return emit(payload)
    if len(argv) != 2:
        return emit({"error": "usage: pidband [base span]"}, USAGE)
    try:
        base, span = int(argv[0]), int(argv[1])
    except ValueError:
        return emit({"error": "base and span must be integers"}, USAGE)
    payload["base"] = base
    payload["span"] = span
    payload["in_band"] = base <= payload["self_pid"] < base + span
    return emit(payload, OK if payload["in_band"] else TIMED_OUT)


def verb_nvra(argv):
    """nvra <binary> -- the runtime closure, which is the identity gate. Not `rpm -qa`.

    criu remaps shared libraries by path and content, so what has to be identical across the
    four machines is what the subject actually loads, not what happens to be installed. `ldd`
    gives the closure, `rpm -qf` names each member, and the sorted unique list is the thing to
    compare. Boost is linked statically here and correctly does not appear.

    criu's own version and the kernel release ride along because they are the other two halves
    of "the same machine, four times", and asking for them separately would cost three more ssh
    round trips.
    """
    if len(argv) != 1:
        return emit({"error": "usage: nvra <binary>"}, USAGE)
    path = argv[0]
    payload = {"binary": path, "hostname": os.uname().nodename,
               "kernel": os.uname().release, "libs": [], "nvra": [], "criu": None,
               "sha256": None}
    try:
        with open(path, "rb") as f:
            digest = hashlib.sha256()
            for chunk in iter(lambda: f.read(1 << 20), b""):
                digest.update(chunk)
            payload["sha256"] = digest.hexdigest()
    except OSError as exc:
        payload["error"] = "cannot read %s: %s" % (path, exc)
        return emit(payload, OS_ERROR)

    try:
        ldd = subprocess.run(["ldd", path], capture_output=True, text=True, timeout=60)
    except (OSError, subprocess.SubprocessError) as exc:
        payload["error"] = "ldd failed: %s" % exc
        return emit(payload, OS_ERROR)
    libs = []
    for line in ldd.stdout.splitlines():
        line = line.strip()
        if "=>" in line:
            right = line.split("=>", 1)[1].strip()
            candidate = right.split(" (")[0].strip()
        else:
            candidate = line.split(" (")[0].strip()
        if candidate.startswith("/") and os.path.exists(candidate):
            libs.append(candidate)
    payload["libs"] = sorted(set(libs))

    owners = set()
    for lib in payload["libs"]:
        try:
            q = subprocess.run(["rpm", "-qf", "--queryformat",
                                "%{NAME}-%{VERSION}-%{RELEASE}.%{ARCH}", lib],
                               capture_output=True, text=True, timeout=60)
        except (OSError, subprocess.SubprocessError):
            owners.add("unqueryable:" + lib)
            continue
        owners.add(q.stdout.strip() if q.returncode == 0 and q.stdout.strip()
                   else "unowned:" + lib)
    payload["nvra"] = sorted(owners)

    try:
        version = subprocess.run(["criu", "-V"], capture_output=True, text=True, timeout=30)
        payload["criu"] = version.stdout.strip() or version.stderr.strip()
    except (OSError, subprocess.SubprocessError) as exc:
        payload["criu"] = "unavailable: %s" % exc
    return emit(payload)


def verb_head(argv):
    """head <tree> -- the checkout's HEAD commit, resolved by READING the ref files.

    Not `git rev-parse`: git is not installed on the worker nodes of this cluster (only the
    development host has it), and installing it would add an unpinned package to the identity
    gate's own machines for no gain. What the gate actually asserts is that every node sees the
    SAME tree at the SAME absolute path -- and reading `.git/HEAD` and the ref it names off the
    shared filesystem, from inside each node, asserts exactly that and nothing weaker.

    Resolves the three forms a checkout's HEAD can take: a detached sha, a symbolic ref whose
    target is a loose ref file, and a symbolic ref that lives only in `packed-refs`. `.git` may
    itself be a file (`gitdir: <path>`, the worktree/submodule form), which is followed once.
    """
    if len(argv) != 1:
        return emit({"error": "usage: head <tree>"}, USAGE)
    tree = argv[0]
    payload = {"tree": tree, "hostname": os.uname().nodename, "head": None, "ref": None}
    gitdir = os.path.join(tree, ".git")
    try:
        if os.path.isfile(gitdir):
            with open(gitdir) as f:
                pointer = f.read().strip()
            if not pointer.startswith("gitdir:"):
                payload["error"] = "%s is a file but not a gitdir pointer" % gitdir
                return emit(payload, OS_ERROR)
            gitdir = pointer.split(":", 1)[1].strip()
            if not os.path.isabs(gitdir):
                gitdir = os.path.join(tree, gitdir)
        with open(os.path.join(gitdir, "HEAD")) as f:
            text = f.read().strip()
    except OSError as exc:
        payload["error"] = "cannot read HEAD under %s: %s" % (gitdir, exc)
        return emit(payload, OS_ERROR)
    if not text.startswith("ref:"):
        payload["head"] = text
        return emit(payload)
    ref = text.split(":", 1)[1].strip()
    payload["ref"] = ref
    try:
        with open(os.path.join(gitdir, ref)) as f:
            payload["head"] = f.read().strip()
        return emit(payload)
    except OSError:
        pass
    try:
        with open(os.path.join(gitdir, "packed-refs")) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith(("#", "^")):
                    continue
                sha, _, name = line.partition(" ")
                if name.strip() == ref:
                    payload["head"] = sha
                    payload["packed"] = True
                    return emit(payload)
    except OSError as exc:
        payload["error"] = "cannot resolve %s: %s" % (ref, exc)
        return emit(payload, OS_ERROR)
    payload["error"] = "%s is not a loose ref and is not in packed-refs" % ref
    return emit(payload, OS_ERROR)


VERBS = {
    "sigqueue": verb_sigqueue,
    "wait-state": verb_wait_state,
    "seal-wait": verb_seal_wait,
    "wait-gone": verb_wait_gone,
    "cont-if-stopped": verb_cont_if_stopped,
    "pidband": verb_pidband,
    "nvra": verb_nvra,
    "head": verb_head,
}


def main(argv):
    if len(argv) < 2 or argv[1] in ("-h", "--help"):
        sys.stderr.write(__doc__ + "\n")
        return USAGE if len(argv) < 2 else OK
    verb = VERBS.get(argv[1])
    if verb is None:
        return emit({"error": "unknown verb %r; known: %s"
                              % (argv[1], ", ".join(sorted(VERBS)))}, USAGE)
    try:
        return verb(argv[2:])
    except Exception as exc:  # never a traceback on stdout: the caller parses this line
        return emit({"error": "%s: %s" % (type(exc).__name__, exc)}, OS_ERROR)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
