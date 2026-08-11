#!/usr/bin/env python3
"""Drive a real CRIU migration of one DrainTCP rank in the middle of a running job.

See README.md. This is the live counterpart of the `DrainMigration` suite's in-place
rehearsal: the same migrator sequence, ended by a real `SIGSTOP`, a real `criu dump` and a
real `criu restore` instead of by an immediate restore leg.

The application is unmodified — `fmi_checkpoint_subject` is the same binary the
criu-transparent-checkpoint runbook drives over DirectTCP, with the same app shapes. Nothing
in it knows about migration. The driver only ever:

  1. launches the job (sweep.py's launch hygiene, verbatim),
  2. `sigqueue`s SIGRTMIN+offset at one rank, carrying that rank's current epoch,
  3. waits for the rank's own `sealed` event on `fmi:drain:<comm>:events` **and** for the
     process to reach state `T`,
  4. dumps it with **no `--tcp-close`**, which is the whole acceptance criterion: the drain is
     supposed to have left the process owning zero sockets, so criu must be able to take a
     host-agnostic image of it without being told to drop connections,
  5. reaps, restores (again with no `--tcp-close`), continues it, and
  6. scores the run exactly as sweep.py does — every rank must reach DONE with the checksum a
     clean baseline of the same shape produced, and the migrated rank must log a round past
     the one it was frozen on.

A dump that fails, or a dump log that mentions a connected socket, is a **FAIL of the
protocol**, not a skip: a socket in the image means the drain did not do what it claims.
Environmental criu failures are reported as failures too, but tagged separately, because they
are triaged differently.
"""
import argparse
import ctypes
import json
import os
import random
import re
import shutil
import signal
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", ".."))
# Overridable so a differently-named build tree works without editing this file. The subject is
# the criu-transparent-checkpoint runbook's, unchanged: reusing it IS the transparency claim.
SUBJECT = os.environ.get("FMI_CHECKPOINT_SUBJECT", os.path.join(
    REPO, "build", "runbooks", "criu-transparent-checkpoint", "fmi_checkpoint_subject"))
CONFIG = os.environ.get("FMI_DRAIN_CONFIG", os.path.join(HERE, "fmi_drain.json"))

DONE_RE = re.compile(r"rank (\d+): DONE rounds=(\d+) checksum=(-?\d+)")
ROUND_RE = re.compile(r"rank \d+: round (\d+) ok")

PRINT_EVERY = 25
PAYLOAD_INTS = 1
SHAPE = "baseline"
COMM_PREFIX = f"fmidr{os.getpid()}-"
# Set from the config in main(): the backend block and the signal the ranks listen on.
PLANE = ("DrainTCP", {})
DRAIN_SIGNAL = 0

# Field/record separators for the Lua serialisation below. redis-cli 6.2 has no --json, and an
# XRANGE printed flat cannot be parsed: a `last_stream_id` value looks exactly like an entry id.
FS, RS = "\x1e", "\x1d"
STREAM_LUA = """local res = redis.call('XRANGE', KEYS[1], ARGV[1], '+')
local out = {}
for i = 1, #res do
  local parts = {res[i][1]}
  local fields = res[i][2]
  for j = 1, #fields do parts[#parts + 1] = fields[j] end
  out[#out + 1] = table.concat(parts, '\\30')
end
return table.concat(out, '\\29')"""


# --------------------------------------------------------------------------- the control plane

class _Sigval(ctypes.Union):
    _fields_ = [("sival_int", ctypes.c_int), ("sival_ptr", ctypes.c_void_p)]


_libc = ctypes.CDLL("libc.so.6", use_errno=True)
_libc.sigqueue.argtypes = [ctypes.c_int, ctypes.c_int, _Sigval]


def sigqueue(pid, sig, value):
    """Queue `sig` at `pid` with `value` as si_value.sival_int.

    os.kill cannot do this and the payload is not decoration: MigrationTrigger reads it as the
    epoch the request was addressed to, and drops a request whose epoch is behind the process's
    own restore count. A request sent with no payload would arrive as epoch 0 and be dropped by
    any rank that has already been migrated once.
    """
    if _libc.sigqueue(pid, sig, _Sigval(sival_int=value)) != 0:
        err = ctypes.get_errno()
        raise OSError(err, os.strerror(err), f"sigqueue({pid}, {sig}, {value})")


def drain_signal_number(offset):
    """SIGRTMIN+offset, resolved the same way the library resolves it.

    `MigrationTrigger::signal_for_offset` adds the offset to glibc's SIGRTMIN, which is a
    function call rather than a constant because the threading library reserves the first few
    real-time signals. CPython's signal.SIGRTMIN comes from the same libc, so the two agree by
    construction; hard-coding 34 or 32 would not.
    """
    base, top = int(signal.SIGRTMIN), int(signal.SIGRTMAX)
    number = base + offset
    if number < base or number > top:
        sys.exit(f"drain_signal_offset {offset} is outside the real-time range [{base}, {top}]")
    return number


def redis_cli(params, *args):
    """One redis-cli call against the registry this config names."""
    host = str(params.get("registry_host", "127.0.0.1"))
    port = str(params.get("registry_port", 6379))
    return subprocess.run(["redis-cli", "-h", host, "-p", port] + [str(a) for a in args],
                          capture_output=True, text=True)


def read_events(params, comm, after="-"):
    """[(id, {field: value})] for every event after `after`, oldest first.

    `after` is an exclusive-ish bound in the XRANGE sense: pass "-" for the whole stream, or
    "(<id>" to resume strictly after one already seen.
    """
    out = redis_cli(params, "EVAL", STREAM_LUA, 1, f"fmi:drain:{comm}:events", after)
    if out.returncode != 0:
        return []
    events = []
    for record in out.stdout.strip("\n").split(RS):
        if not record:
            continue
        parts = record.split(FS)
        fields = {parts[i]: parts[i + 1] for i in range(1, len(parts) - 1, 2)}
        events.append((parts[0], fields))
    return events


def wait_for_event(params, comm, kind, rank, epoch, timeout_s):
    """The first `kind` event for (rank, epoch), or None once the deadline passes."""
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        for _, fields in read_events(params, comm):
            if (fields.get("type") == kind and fields.get("rank") == str(rank)
                    and fields.get("epoch") == str(epoch)):
                return fields
        time.sleep(0.02)
    return None


def save_events(params, comm, outdir):
    """Write the trial's whole event stream beside its rank logs, before the keys are removed."""
    try:
        with open(os.path.join(outdir, "events.log"), "w") as f:
            for eid, fields in read_events(params, comm):
                f.write(f"{eid} {fields}\n")
    except OSError:
        pass


def clean_comm(params, comm):
    """Remove everything a drain-armed job under this name leaves in Redis.

    Four keys, not one: the transport registry hash the ranks advertise into, the coordinator's
    member hash, its event stream and its batch lease. A stale event stream is the dangerous one
    — a rank starts reading from the stream's tail, but a driver that reads from "-" would find
    the previous run's `sealed` and dump a rank that has not been asked to migrate yet.
    """
    redis_cli(params, "DEL", f"fmi:drain:{comm}", f"fmi:drain:{comm}:members",
              f"fmi:drain:{comm}:events", f"fmi:drain:{comm}:batch")


# ------------------------------------------------------------------------------------ the job

def data_plane(config_path):
    """(backend name, config block) of the one backend the config enables.

    Exactly one, for the reason sweep.py gives: with a second backend enabled the cost model
    routes this subject's every operation to it, and a migration sweep against such a config
    would exercise no DrainTCP code at all and report green.
    """
    try:
        with open(config_path) as f:
            backends = json.load(f)["backends"]
    except (OSError, ValueError, KeyError) as exc:
        sys.exit(f"cannot read the backends block of {config_path}: {exc}")
    enabled = {name: block for name, block in backends.items()
               if str(block.get("enabled", True)).lower() == "true"}
    if len(enabled) != 1:
        sys.exit(f"{config_path} enables {sorted(enabled) or ['no backend']}; this driver needs "
                 "exactly one, and it must be DrainTCP")
    name, block = next(iter(enabled.items()))
    if name != "DrainTCP":
        sys.exit(f"{config_path} enables {name}; this driver migrates DrainTCP ranks")
    if str(block.get("drain", "false")).lower() != "true":
        sys.exit(f"{config_path}: DrainTCP has drain={block.get('drain')!r}; without drain:true "
                 "nothing arms the migration trigger and the signal would kill the rank")
    if str(block.get("trigger", "both")) not in ("both", "signal"):
        sys.exit(f"{config_path}: trigger={block.get('trigger')!r} does not listen for a signal; "
                 "this driver asks by signal, and the peers need 'both' to hear the leave notice")
    if str(block.get("drain_rehearsal_only", "false")).lower() == "true":
        sys.exit(f"{config_path}: drain_rehearsal_only is on, so the sequence ends in an in-place "
                 "restore and never stops — there would be nothing for criu to dump")
    return name, block


def known_shapes():
    """Shape names the built subject registered, or None if it could not be asked."""
    try:
        out = subprocess.run([SUBJECT, "--list-shapes"], capture_output=True, text=True,
                             timeout=30)
    except (OSError, subprocess.SubprocessError):
        return None
    if out.returncode != 0:
        return None
    return [line.split()[0] for line in out.stdout.splitlines() if line.strip()]


def start_job(comm, npeers, rounds, ms, outdir):
    """Launch every rank. The hygiene here is copied from sweep.py and is load-bearing."""
    procs = []
    for r in range(npeers):
        log = open(os.path.join(outdir, f"r{r}.log"), "w")
        # Deliberately does NOT pass env=: handing the child a rebuilt environment makes
        # criu's dump fail with "External socket is used", every time. Inherit instead.
        # --shape goes last on purpose: the pgrep patterns below match on the positional
        # arguments, so no option may ever move one of them.
        # stdin from /dev/null, NOT inherited: a rank that inherits the launcher's stdin can end
        # up holding one end of whatever socketpair drives the launching terminal or CI harness,
        # and criu then refuses the dump with "External socket is used" — which under THIS
        # runbook's acceptance rule would read as a socket surviving the drain.
        p = subprocess.Popen(
            [SUBJECT, str(r), str(npeers), CONFIG, comm, str(rounds), str(ms),
             str(PRINT_EVERY), str(PAYLOAD_INTS), "--shape", SHAPE],
            stdin=subprocess.DEVNULL,
            stdout=log, stderr=subprocess.STDOUT, preexec_fn=os.setsid, cwd=HERE)
        procs.append((p, log))
    return procs


def last_round(outdir, rank):
    """Highest round the rank has logged so far, or -1."""
    try:
        text = open(os.path.join(outdir, f"r{rank}.log")).read()
    except OSError:
        return -1
    hits = ROUND_RE.findall(text)
    return int(hits[-1]) if hits else -1


def collect(outdir, npeers):
    """(checksums, failures) -- checksums is rank -> checksum for ranks that finished."""
    sums, failures = {}, []
    for r in range(npeers):
        try:
            text = open(os.path.join(outdir, f"r{r}.log")).read()
        except OSError:
            failures.append(f"rank {r}: no log")
            continue
        m = DONE_RE.search(text)
        if m:
            sums[r] = int(m.group(3))
        for bad in ("MISMATCH", "terminate called", "Segmentation", "stack smashing"):
            if bad in text:
                failures.append(f"rank {r}: {bad}")
    return sums, failures


def wait_for_finish(outdir, npeers, timeout_s):
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        finished = 0
        for r in range(npeers):
            try:
                text = open(os.path.join(outdir, f"r{r}.log")).read()
            except OSError:
                continue
            if "DONE" in text or "MISMATCH" in text or "terminate called" in text:
                finished += 1
        if finished == npeers:
            return True
        time.sleep(0.1)
    return False


def wait_until_warm(outdir, npeers, min_round, timeout_s):
    """Every rank has logged a round past min_round. False if one never does."""
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if all(last_round(outdir, r) >= min_round for r in range(npeers)):
            return True
        time.sleep(0.05)
    return False


def find_rank_pid(comm, rank):
    """This rank's pid, by its command line.

    The pattern is sweep.py's and it matters: a `pgrep -f` pattern that appears in the
    driver's OWN command line matches the driver. This one is the binary's basename plus the
    positional arguments, which is why the subject takes `--shape` last and never in between.
    """
    out = subprocess.run(["pgrep", "-f", os.path.basename(SUBJECT) + f" {rank} .* {comm} "],
                         capture_output=True, text=True).stdout.split()
    return int(out[0]) if out else None


def proc_state(pid):
    """The single-letter state from /proc/<pid>/stat, or None if the process is gone.

    Read past the last ')': a comm field containing a space or a bracket would otherwise shift
    every field after it.
    """
    try:
        with open(f"/proc/{pid}/stat") as f:
            text = f.read()
    except OSError:
        return None
    tail = text[text.rfind(")") + 1:].split()
    return tail[0] if tail else None


def wait_for_state(pid, want, timeout_s):
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if proc_state(pid) == want:
            return True
        time.sleep(0.02)
    return False


def kill_all(comm):
    subprocess.run(["pkill", "-9", "-f", os.path.basename(SUBJECT) + f" .* {comm} "],
                   capture_output=True)


# --------------------------------------------------------------------------------- the migration

# What a socket in the image looks like in a criu dump log. "Connected TCP socket" is the error
# criu fails a dump with when a live connection is in the tree and neither --tcp-established nor
# --tcp-close was given — the exact refusal this protocol exists to avoid earning; "External
# socket is used" is the one for a socket whose peer is outside the tree; `inetsk` is the tag
# other criu versions log INET socket lines under (3.19 uses `inet:`, see below).
#
# Matched case-sensitively, and deliberately NOT on the two strings that look like they belong
# here:
#   * "unix: Dumping external sockets" is an unconditional stage header every dump prints;
#   * "inet: Collected ..." lines enumerate the whole HOST's TCP table, read out of /proc/net,
#     and a clean drained dump.log here still carries 29 of them.
# Both were tried; each scored a clean migration as a protocol failure. The image itself is the
# stronger evidence and is checked separately — see SOCKET_IMAGES.
SOCKET_EVIDENCE = re.compile(r"inetsk|Connected TCP socket|External socket is used")

# Image files criu only writes when the tree owned a socket of that kind. Their absence is the
# claim stated the other way round, and it does not depend on how a criu version words its log:
# `crit show <img>/files.img` on a drained rank lists REG and PIPE entries and nothing else.
SOCKET_IMAGES = ("inetsk.img", "unixsk.img", "tcp-stream.img", "sk-queues.img",
                 "packetsk.img", "netlinksk.img")


def socket_lines(path, limit=8):
    """The dump log lines that mention a socket, and how many there are in total."""
    try:
        text = open(path, errors="replace").read()
    except OSError:
        return [], 0
    hits = [line for line in text.splitlines() if SOCKET_EVIDENCE.search(line)]
    return hits[:limit], len(hits)


def migrate(params, comm, target, epoch, pid, imgdir, reap, log, args):
    """Seal, dump, restore and resume one rank. Returns (verdict, note).

    verdict is "ok", "socket" (a socket survived the drain — a protocol failure), "criu" (criu
    itself refused, with nothing in its log about sockets) or "drain" (the rank never sealed).
    """
    os.makedirs(imgdir, exist_ok=True)
    sigqueue(pid, DRAIN_SIGNAL, epoch)
    log.append(f"sigqueue SIGRTMIN+{args.signal_offset}({DRAIN_SIGNAL}) epoch={epoch} -> {pid}")

    sealed = wait_for_event(params, comm, "sealed", target, epoch, args.seal_timeout)
    if sealed is None:
        return "drain", f"no sealed event for rank {target} within {args.seal_timeout}s"
    counters = " ".join(f"{k}={v}" for k, v in sorted(sealed.items())
                        if k.startswith(("sent.", "received.")))
    log.append(f"sealed {counters or '(no links)'}")
    # Both, and in this order: the seal is emitted before the process releases its last two
    # sockets and stops, so a dump taken on the event alone can land before either happened.
    if not wait_for_state(pid, "T", args.seal_timeout):
        return "drain", f"rank {target} sealed but never reached state T (now {proc_state(pid)})"

    # What the process holds at the instant criu is about to look at it. Not the verdict — the
    # dump log is — but it is the same claim from the other side, and it says *which* fds
    # survived when one does.
    fds, sockets = [], []
    try:
        for fd in sorted(os.listdir(f"/proc/{pid}/fd")):
            fds.append(fd)
            try:
                if os.readlink(f"/proc/{pid}/fd/{fd}").startswith("socket:"):
                    sockets.append(fd)
            except OSError:
                pass
    except OSError:
        pass
    log.append(f"fds={len(fds)} sockets={len(sockets)}")
    if sockets:
        return "socket", f"rank {target} still holds {len(sockets)} socket fd(s) after its seal"

    # No --tcp-close, on either leg. That is the acceptance criterion, not an omission: the
    # drain is supposed to have left this process owning no socket at all, so criu must be able
    # to image it without being told to drop connections. --shell-job is not needed either —
    # start_job gives every rank its own session and a regular file for stdout.
    dumped = subprocess.run(["criu", "dump", "--unprivileged", "-t", str(pid), "-D", imgdir,
                             "-v4", "-o", "dump.log"], capture_output=True, text=True)
    hits, n_hits = socket_lines(os.path.join(imgdir, "dump.log"))
    log.append(f"dump rc={dumped.returncode} socket-lines={n_hits}")
    if n_hits:
        return "socket", (f"dump.log mentions a socket {n_hits}x: " + " / ".join(hits[:3]))
    if dumped.returncode != 0:
        return "criu", f"dump rc={dumped.returncode}: {dumped.stderr.strip()[:300]}"
    in_image = [name for name in SOCKET_IMAGES if os.path.exists(os.path.join(imgdir, name))]
    if in_image:
        return "socket", f"the image contains {', '.join(in_image)} — a socket was checkpointed"

    if reap is not None:
        # criu kills the dumped process; reap it before restoring, or its pid is still taken and
        # the restore fails with "Can't fork for <pid>: File exists".
        try:
            reap.wait(timeout=30)
        except Exception:
            pass

    restored = subprocess.run(["criu", "restore", "--unprivileged", "-D", imgdir, "-d",
                               "-v4", "-o", "restore.log"], capture_output=True, text=True)
    log.append(f"restore rc={restored.returncode}")
    if restored.returncode != 0:
        return "criu", f"restore rc={restored.returncode}: {restored.stderr.strip()[:300]}"

    # The image was taken of a process in group-stop — the migrator sequence ends in
    # raise(SIGSTOP) — and criu faithfully restores that state. Nothing in the library can undo
    # it from the inside: the thread that would call SIGCONT is the stopped one. Whoever asked
    # for the migration is who resumes it.
    if wait_for_state(pid, "T", 2.0):
        os.kill(pid, signal.SIGCONT)
        log.append("SIGCONT (restored in group-stop, as expected)")
    else:
        log.append(f"restored running (state {proc_state(pid)}), no SIGCONT needed")

    back = wait_for_event(params, comm, "restored", target, epoch + 1, args.restore_timeout)
    if back is None:
        return "drain", (f"rank {target} never emitted restored at epoch {epoch + 1} within "
                         f"{args.restore_timeout}s")
    log.append(f"restored epoch={back.get('epoch')} incarnation={back.get('incarnation')}")
    return "ok", ""


# --------------------------------------------------------------------------------------- run

def baseline(params, npeers, args, root):
    """A clean run's checksums and wall clock. Nothing is migrated."""
    comm = f"{COMM_PREFIX}base{npeers}x{args.rounds}"
    outdir = os.path.join(root, comm)
    os.makedirs(outdir, exist_ok=True)
    clean_comm(params, comm)
    started = time.monotonic()
    procs = start_job(comm, npeers, args.rounds, args.ms, outdir)
    for p, log in procs:
        p.wait(timeout=1200)
        log.close()
    elapsed = time.monotonic() - started
    sums, failures = collect(outdir, npeers)
    clean_comm(params, comm)
    if failures or len(sums) != npeers:
        print(f"BASELINE FAILED for shape {SHAPE} at {npeers} peers: {failures} {sums}",
              file=sys.stderr)
        sys.exit(1)
    return sums, elapsed


def run(args, root):
    params = PLANE[1]
    expected, finish_timeout = {}, {}
    for n in args.peers:
        sums, elapsed = baseline(params, n, args, root)
        expected[n] = sums
        # A migrated run legitimately costs the seal, the dump, the restore and a round of
        # re-establishment on top of a clean one.
        finish_timeout[n] = max(180.0, 3.0 * elapsed + 120.0)
        print(f"baseline shape={SHAPE} {n} peers: {sums} ({elapsed:.1f}s)")

    passed = failed = skipped = 0
    criu_failures = 0
    for trial in range(args.trials):
        npeers = random.choice(args.peers)
        comm = f"{COMM_PREFIX}t{trial}p{npeers}"
        outdir = os.path.join(root, comm)
        os.makedirs(outdir, exist_ok=True)
        clean_comm(params, comm)
        log = [f"peers={npeers} shape={SHAPE}"]
        started = time.monotonic()
        procs = start_job(comm, npeers, args.rounds, args.ms, outdir)

        verdict, note = "ok", ""
        migrations = []
        epochs = {}
        for k in range(args.migrations):
            if not wait_until_warm(outdir, npeers, args.warmup_round, args.warmup_timeout):
                verdict, note = "void", ("not every rank got past round "
                                         f"{args.warmup_round} before the migration was due")
                break
            time.sleep(random.uniform(*args.delay_range))
            target = args.target if args.target is not None else random.randrange(npeers)
            pid = find_rank_pid(comm, target)
            if pid is None:
                # The job outran the migration. That trial proves nothing about the protocol, so
                # it must not be able to report a pass.
                verdict, note = "void", f"rank {target} had already finished"
                break
            before = last_round(outdir, target)
            # A rank's epoch is its own restore count, not the trial's migration number: with
            # --migrations 2 the second one may pick a different rank, and addressing it at
            # epoch 1 would be a request MigrationTrigger drops as coming from the future.
            epoch = epochs.get(target, 0)
            epochs[target] = epoch + 1
            log.append(f"migration {k}: rank {target} pid {pid} at round {before} epoch {epoch}")
            verdict, note = migrate(params, comm, target, epoch, pid,
                                    os.path.join(outdir, f"img{k}"),
                                    next((p for p, _ in procs if p.pid == pid), None),
                                    log, args)
            if verdict != "ok":
                break
            migrations.append((target, before))

        if verdict in ("socket", "criu", "drain"):
            kill_all(comm)
            for _, handle in procs:
                handle.close()
            save_events(params, comm, outdir)
            clean_comm(params, comm)
            failed += 1
            if verdict == "criu":
                criu_failures += 1
            label = {"socket": "FAIL (A SOCKET SURVIVED THE DRAIN)",
                     "criu": "FAIL (criu itself, no socket evidence — triage the environment)",
                     "drain": "FAIL (the drain protocol)"}[verdict]
            print(f"trial {trial}: {label} {' | '.join(log)} :: {note}")
            print(f"    evidence kept in {outdir}")
            continue

        finished = wait_for_finish(outdir, npeers, timeout_s=finish_timeout[npeers])
        sums, failures = collect(outdir, npeers)
        for target, before in migrations:
            after = last_round(outdir, target)
            if after <= before:
                failures.append(f"rank {target} logged no round past {before} after its restore")
        elapsed = time.monotonic() - started
        kill_all(comm)
        for _, handle in procs:
            handle.close()

        if verdict == "void" and not migrations:
            # No migration landed at all. If the job nevertheless finished clean the trial proves
            # nothing and is a SKIP; if the job is broken too, that is a failure in its own right
            # and skipping it would let a build that crashes outright report a clean run.
            clean = finished and not failures and all(
                sums.get(r) == want for r, want in expected[npeers].items())
            if clean:
                clean_comm(params, comm)
                skipped += 1
                print(f"trial {trial}: SKIP {' | '.join(log)} :: {note} — the run proves "
                      "nothing; give it more --rounds or a lower --delay-range")
                continue
            failures.append(f"no migration landed ({note}) AND the job did not finish clean")
        elif verdict == "void":
            # A later migration could not be set up, but earlier ones did land and are scored
            # below on their own evidence.
            log.append(f"note: {note}")

        if not finished:
            failures.append("timed out before every rank reached DONE")
        for r, want in expected[npeers].items():
            if r not in sums:
                failures.append(f"rank {r} never reached DONE")
            elif sums[r] != want:
                failures.append(f"rank {r} checksum {sums[r]} != baseline {want}")

        if failures:
            failed += 1
            save_events(params, comm, outdir)
            clean_comm(params, comm)
            print(f"trial {trial}: FAIL {' | '.join(log)} :: {'; '.join(failures)}")
            print(f"    evidence kept in {outdir}")
        else:
            if args.keep:
                save_events(params, comm, outdir)
            clean_comm(params, comm)
            passed += 1
            print(f"trial {trial}: pass {' | '.join(log)} :: {elapsed:.1f}s wall")
            if not args.keep:
                shutil.rmtree(outdir, ignore_errors=True)

    tail = f" ({criu_failures} of them criu-environmental)" if criu_failures else ""
    print(f"\n== {passed} passed, {failed} failed{tail}, {skipped} skipped ==")
    return 1 if failed else 0


def main():
    global PRINT_EVERY, PAYLOAD_INTS, SHAPE, CONFIG, PLANE, DRAIN_SIGNAL
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--peers", type=int, nargs="+", default=[2])
    ap.add_argument("--rounds", type=int, default=20000)
    ap.add_argument("--ms", type=int, default=1,
                    help="milliseconds of application work per round; a round that costs nothing "
                         "makes the job outrun the migration")
    ap.add_argument("--trials", type=int, default=3)
    ap.add_argument("--migrations", type=int, default=1,
                    help="migrations per trial; the Nth carries epoch N-1, which is what the "
                         "trigger's staleness fence checks")
    ap.add_argument("--shape", default=os.environ.get("FMI_SHAPE", "baseline"),
                    help="app shape the subject runs; see '<subject> --list-shapes'")
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--target", type=int, default=None,
                    help="rank to migrate; the default picks one at random per migration")
    ap.add_argument("--config", default=CONFIG)
    ap.add_argument("--print-every", type=int, default=25)
    ap.add_argument("--payload-ints", type=int, default=1,
                    help="size the vector collective; >1 makes single messages span segments, so "
                         "a drain can land part way through a payload")
    ap.add_argument("--delay-range", type=float, nargs=2, default=[0.2, 1.0],
                    help="extra seconds after warm-up before the signal goes out")
    ap.add_argument("--warmup-round", type=int, default=1,
                    help="every rank must have logged this round before anything is migrated")
    ap.add_argument("--warmup-timeout", type=float, default=60.0)
    ap.add_argument("--seal-timeout", type=float, default=30.0)
    ap.add_argument("--restore-timeout", type=float, default=60.0)
    ap.add_argument("--signal-offset", type=int, default=None,
                    help="override the config's drain_signal_offset")
    ap.add_argument("--keep", action="store_true",
                    help="keep the images and logs of passing trials too")
    args = ap.parse_args()

    PRINT_EVERY = args.print_every
    PAYLOAD_INTS = args.payload_ints
    SHAPE = args.shape
    # Absolute, and resolved before the ranks see it: they are started with cwd=HERE, so a
    # relative --config given from anywhere else would name one file to this script and a
    # different one to the job it launches.
    CONFIG = os.path.abspath(args.config) if os.path.exists(args.config) \
        else os.path.join(HERE, args.config)
    PLANE = data_plane(CONFIG)
    if args.signal_offset is None:
        args.signal_offset = int(PLANE[1].get("drain_signal_offset", 3))
    DRAIN_SIGNAL = drain_signal_number(args.signal_offset)
    print(f"data plane: {PLANE[0]} (from {CONFIG}); drain signal SIGRTMIN+{args.signal_offset} "
          f"= {DRAIN_SIGNAL}")

    available = known_shapes()
    if available is None:
        sys.exit(f"cannot run the subject at {SUBJECT} — build it first, or set "
                 f"FMI_CHECKPOINT_SUBJECT")
    if SHAPE not in available:
        sys.exit(f"unknown shape {SHAPE!r}; the subject registered: {', '.join(available)}")

    random.seed(args.seed)
    # Per-invocation subdirectory, wiping only itself: a shared root cleared at startup destroys
    # the previous run's kept failure specimens the moment any later run starts.
    root = os.path.join(HERE, "runs", f"run{os.getpid()}")
    shutil.rmtree(root, ignore_errors=True)
    os.makedirs(root)
    print(f"logs and images under {root}")
    return run(args, root)


if __name__ == "__main__":
    sys.exit(main())
