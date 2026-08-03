#!/usr/bin/env python3
"""Randomized checkpoint/restore sweep over an unmodified FMI program.

See README.md. Build the subject first:
    cmake -S . -B build -DFMI_ENABLE_S3=OFF -DFMI_ENABLE_TCPUNCH=OFF
    cmake --build build -j"$(nproc)"

Each trial starts a whole job, then checkpoints and restores one or more ranks at randomly
chosen instants. A trial passes only when every rank reaches DONE with the checksum a clean
run of the same shape produced -- so a lost, duplicated or substituted message fails the
trial rather than being absorbed.

--shape selects which app shape the subject runs (default "baseline"); the subject's
--list-shapes says which ones are compiled in. Baselines are taken with the same shape, so
checksums are only ever compared within one shape and rank count.

--config selects the FMI configuration, and with it the data plane under test: fmi.json runs
the job over DirectTCP, fmi_redis.json over Redis, fmi_s3.json over S3. The config must enable
exactly one backend (see data_plane), and the sweep cleans up whatever that backend leaves
behind between trials. An S3 run also costs money, so it prints what it expects to spend and
stops above --max-cost; see s3_cost_guard.
"""
import argparse
import json
import os
import random
import re
import shlex
import shutil
import signal
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", ".."))
# Overridable so a differently-named build tree works without editing this file.
SUBJECT = os.environ.get("FMI_CHECKPOINT_SUBJECT", os.path.join(
    REPO, "build", "runbooks", "criu-transparent-checkpoint", "fmi_checkpoint_subject"))
CONFIG = os.environ.get("FMI_CHECKPOINT_CONFIG", os.path.join(HERE, "fmi.json"))

DONE_RE = re.compile(r"rank (\d+): DONE rounds=(\d+) checksum=(-?\d+)")


PRINT_EVERY = 25
PAYLOAD_INTS = 1
SHAPE = "baseline"
# (backend name, its config block); set from --config in main().
PLANE = ("DirectTCP", {})
# Every name this invocation puts into a store starts with this, so one pattern cleans a run.
RUN_PREFIX = f"fmisw{os.getpid()}-"


def data_plane(config_path):
    """(backend name, config block) of the one backend the config enables.

    Which store has to be cleaned between trials, and under which key names, follows from
    which backend actually carries the messages -- so the sweep has to know. Requiring
    *exactly one* is not just convenience: with several enabled, FMI's cost model chooses per
    operation, and with DirectTCP and Redis both on it picks DirectTCP for every operation
    this subject issues. A sweep against such a config would exercise no Redis code at all
    and report green.
    """
    try:
        with open(config_path) as f:
            backends = json.load(f)["backends"]
    except (OSError, ValueError, KeyError) as exc:
        sys.exit(f"cannot read the backends block of {config_path}: {exc}")
    # A backend with no "enabled" key is enabled, which is how Configuration reads it too
    # (src/utils/Configuration.cpp: it skips a backend only when the key is present and not
    # "true"). JSON true and the string "true" both occur in the shipped configs.
    enabled = {name: params for name, params in backends.items()
               if str(params.get("enabled", True)).lower() == "true"}
    if len(enabled) != 1:
        sys.exit(f"{config_path} enables {sorted(enabled) or ['no backend']}; the sweep needs "
                 "exactly one, so that every operation is carried by the backend under test")
    return next(iter(enabled.items()))


def _redis_delete_pattern(host, port, pattern):
    """DEL every key matching pattern, without KEYS: a live sweep's store is not small."""
    cli = f"redis-cli -h {shlex.quote(str(host))} -p {shlex.quote(str(port))}"
    subprocess.run(f"{cli} --scan --pattern {shlex.quote(pattern)} | xargs -r {cli} DEL",
                   shell=True, capture_output=True)


def clean_comm(comm):
    """Remove what a job under this name left in the data plane. Safe to call before it runs."""
    name, params = PLANE
    if name == "DirectTCP":
        # The peer registry hash, exactly as this sweep has always cleaned it.
        subprocess.run(["redis-cli", "DEL", f"fmi:direct:{comm}"], capture_output=True)
    elif name == "Redis":
        host, port = params.get("host", "127.0.0.1"), params.get("port", 6379)
        # The registry key first: a Redis-plane job writes none, but the same comm_name may
        # have been used by a DirectTCP run against the same server, and a stale entry there
        # is exactly the kind of thing that makes a later run hang on a dead address.
        subprocess.run(["redis-cli", "-h", str(host), "-p", str(port), "DEL",
                        f"fmi:direct:{comm}"], capture_output=True)
        # Under "recover" the channel deletes nothing and lets the objects expire, so a trial
        # leaves its whole message history behind for up to object_ttl_s. Removing it here
        # keeps the store from growing across a long sweep -- and keeps a repeated comm_name
        # from ever reading a previous trial's values as live data.
        _redis_delete_pattern(host, port, f"{comm}*")
    elif name == "S3":
        # Under "recover" the channel deletes nothing, and S3 has no per-object expiry, so a
        # trial's whole message history stays in the bucket until the lifecycle rule or this
        # line removes it. The cost guard in main() is what stands in front of a sweep large
        # enough for that to matter.
        subprocess.run(["aws", "s3", "rm", f"s3://{params.get('bucket_name')}/{comm}",
                        "--recursive", "--only-show-errors"], capture_output=True)


def clean_run(prefix):
    """Remove everything this invocation wrote, whatever became of the individual trials."""
    name, params = PLANE
    if name == "Redis":
        _redis_delete_pattern(params.get("host", "127.0.0.1"), params.get("port", 6379),
                              f"{prefix}*")
    elif name == "S3":
        subprocess.run(["aws", "s3", "rm", f"s3://{params.get('bucket_name')}/{prefix}",
                        "--recursive", "--only-show-errors"], capture_output=True)


def s3_cost_guard(args, confirmed):
    """Refuse a sweep whose S3 bill would be a surprise. Returns False to stop.

    The other planes are free: a local Redis or a TCP mesh costs the same whether a run is
    twenty trials or twenty thousand. S3 charges per request, and this sweep's default is
    --rounds 40000, which was chosen for a plane where a round is a couple of syscalls. At
    eight peers that default is about five dollars and eleven hours of wall clock, entered by
    leaving one flag off -- so the estimate is printed for every S3 run and a large one has to
    be asked for.

    The estimate is deliberately rough and on the high side: one PUT per rank per round, and
    one GET per rank per round per peer, which counts the poll loops' repeated GETs as if
    every object were found on the first ask (they are not, so the real GET count is higher --
    but GETs are twelve times cheaper than PUTs and the PUT count is exact).
    """
    peers = max(args.peers)
    rounds = args.rounds
    trials = args.trials
    puts = trials * peers * rounds
    gets = trials * peers * rounds * peers
    # us-east-1/eu-central-1 standard pricing, per 1000 requests, 2026.
    dollars = puts / 1000. * 0.005 + gets / 1000. * 0.0004
    print(f"S3 request estimate: {puts} PUT + {gets} GET, about ${dollars:.2f} "
          f"({trials} trials x {peers} peers x {rounds} rounds)")
    if dollars <= args.max_cost or confirmed:
        return True
    print(f"estimated ${dollars:.2f} is over --max-cost ${args.max_cost:.2f}. Re-run with "
          f"--yes to go ahead, or lower --rounds/--trials/--peers. The sweep's default of "
          f"--rounds 40000 is sized for a free data plane.", file=sys.stderr)
    return False


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


def last_round(outdir, rank):
    """Highest round the rank has logged so far, or -1."""
    try:
        text = open(os.path.join(outdir, f"r{rank}.log")).read()
    except OSError:
        return -1
    hits = re.findall(r"rank \d+: round (\d+) ok", text)
    return int(hits[-1]) if hits else -1


def start_job(comm, npeers, rounds, ms, outdir):
    procs = []
    for r in range(npeers):
        log = open(os.path.join(outdir, f"r{r}.log"), "w")
        # Deliberately does NOT pass env=: handing the child a rebuilt environment makes
        # criu's dump fail with "External socket is used", every time. Inherit instead.
        # --shape goes last on purpose: the pgrep patterns below match on the positional
        # arguments, so no option may ever move one of them.
        # stdin from /dev/null, NOT inherited: a rank that inherits the launcher's stdin can
        # end up holding one end of whatever socketpair drives the launching terminal or CI
        # harness, and criu then refuses the dump with "External socket is used" — which looks
        # exactly like a protocol regression and scores every trial as a criu skip. Observed
        # for real: the same sweep dumped fine launched detached and failed 8/8 launched from
        # a harness-plumbed foreground shell.
        p = subprocess.Popen(
            [SUBJECT, str(r), str(npeers), CONFIG, comm, str(rounds), str(ms),
             str(PRINT_EVERY), str(PAYLOAD_INTS), "--shape", SHAPE],
            stdin=subprocess.DEVNULL,
            stdout=log, stderr=subprocess.STDOUT, preexec_fn=os.setsid, cwd=HERE)
        procs.append((p, log))
    return procs


def collect(outdir, npeers):
    """(checksums, failures) -- checksums is rank -> checksum for ranks that finished."""
    sums, failures = {}, []
    for r in range(npeers):
        path = os.path.join(outdir, f"r{r}.log")
        try:
            text = open(path).read()
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


def find_rank_pid(comm, rank):
    out = subprocess.run(["pgrep", "-f", os.path.basename(SUBJECT) + f" {rank} .* {comm} "],
                         capture_output=True, text=True).stdout.split()
    return int(out[0]) if out else None


def checkpoint_restore(pid, imgdir, log, reap=None):
    os.makedirs(imgdir, exist_ok=True)
    d = subprocess.run(["criu", "dump", "--unprivileged", "-t", str(pid), "-D", imgdir,
                        "--tcp-close", "--shell-job", "-v4", "-o", "dump.log"],
                       capture_output=True, text=True)
    if d.returncode != 0:
        log.append(f"dump rc={d.returncode}: {d.stderr.strip()[:300]}")
        return False
    if reap is not None:
        try:
            reap.wait(timeout=30)
        except Exception:
            pass
    r = subprocess.run(["criu", "restore", "--unprivileged", "-D", imgdir, "-d",
                        "--tcp-close", "--shell-job", "-v4", "-o", "restore.log"],
                       capture_output=True, text=True)
    if r.returncode != 0:
        log.append(f"restore rc={r.returncode}: {r.stderr.strip()[:300]}")
        return False
    return True


def kill_all(comm):
    subprocess.run(["pkill", "-9", "-f", os.path.basename(SUBJECT) + f" .* {comm} "],
                   capture_output=True)


def baseline(npeers, rounds, ms, root):
    comm = f"{RUN_PREFIX}base{npeers}x{rounds}"
    outdir = os.path.join(root, comm)
    os.makedirs(outdir, exist_ok=True)
    clean_comm(comm)
    started = time.monotonic()
    procs = start_job(comm, npeers, rounds, ms, outdir)
    for p, log in procs:
        p.wait(timeout=1200)
        log.close()
    elapsed = time.monotonic() - started
    sums, failures = collect(outdir, npeers)
    if failures or len(sums) != npeers:
        print(f"BASELINE FAILED for shape {SHAPE} at {npeers} peers: {failures} {sums}",
              file=sys.stderr)
        sys.exit(1)
    shutil.rmtree(outdir, ignore_errors=True)
    # The checksums are out, so the baseline's objects are dead. On a store plane they would
    # otherwise sit there for the whole sweep -- half a million keys before the first trial
    # wrote one of its own -- and every later cleanup would scan past them.
    clean_comm(comm)
    return sums, elapsed


def run(args, root):
    """Take the baselines, then run the trials. Returns the process exit status."""
    # The clean run's measured duration sizes the per-trial finish window: a checkpointed run
    # legitimately costs the freeze, the restore and a round of link repair on top, but a shape
    # whose clean run takes minutes must not be scored dead at a fixed 180 s - that timed out
    # perfectly healthy jobs and read as a protocol failure.
    expected = {}
    finish_timeout = {}
    for n in args.peers:
        sums, elapsed = baseline(n, args.rounds, args.ms, root)
        expected[n] = sums
        finish_timeout[n] = max(180.0, 3.0 * elapsed + 120.0)
    for n, s in expected.items():
        print(f"baseline shape={SHAPE} {n} peers: {s}")

    passed = failed = skipped = 0
    for trial in range(args.trials):
        npeers = random.choice(args.peers)
        comm = f"{RUN_PREFIX}t{trial}p{npeers}"
        outdir = os.path.join(root, comm)
        os.makedirs(outdir, exist_ok=True)
        clean_comm(comm)
        log = []
        procs = start_job(comm, npeers, args.rounds, args.ms, outdir)

        n_ckpt = random.randint(1, args.max_checkpoints)
        ok = True
        void = False
        checkpoints = []
        for k in range(n_ckpt):
            time.sleep(random.uniform(args.delay_range[0], args.delay_range[1]))
            target = random.randrange(npeers)
            pid = find_rank_pid(comm, target)
            if pid is None:
                # The job outran the checkpoint. That trial proves nothing about the
                # protocol, so it must not be able to report a pass.
                log.append(f"ckpt {k}: rank {target} ALREADY FINISHED (trial is void)")
                void = True
                continue
            before = last_round(outdir, target)
            log.append(f"ckpt {k}: rank {target} pid {pid} at round {before}")
            # criu kills the dumped process; reap it before restoring, or its pid is still
            # taken and the restore fails with "Can't fork for <pid>: File exists".
            reap_dumped = next((p for p, _ in procs if p.pid == pid), None)
            if not checkpoint_restore(pid, os.path.join(outdir, f"img{k}"), log,
                                      reap=reap_dumped):
                ok = False
                break
            checkpoints.append((target, before))

        if not ok:
            kill_all(comm)
            clean_comm(comm)
            # A criu failure is an environment limitation, not a protocol verdict.
            print(f"trial {trial}: SKIP (criu) {' | '.join(log)}")
            skipped += 1
            continue

        finished = wait_for_finish(outdir, npeers, timeout_s=finish_timeout[npeers])
        sums, failures = collect(outdir, npeers)
        for target, before in checkpoints:
            after = last_round(outdir, target)
            if after <= before:
                failures.append(
                    f"rank {target} logged no round past {before} after its restore")
        kill_all(comm)
        # Every verdict below is read out of the rank logs, which --keep preserves; what the
        # trial left in the store is not evidence and would otherwise sit there for a whole
        # object_ttl_s, so a long sweep would carry every earlier trial's message history.
        clean_comm(comm)

        if void or not checkpoints:
            # No checkpoint landed. If the job nevertheless finished clean, the trial proves
            # nothing about checkpointing and is a SKIP. If the job is broken too - a rank
            # dead before any freeze, a wrong checksum - that is a FAILURE in its own right,
            # and skipping it would let a build that crashes outright report a clean sweep.
            clean = finished and not failures and all(
                sums.get(r) == want for r, want in expected[npeers].items())
            if clean:
                kill_all(comm)
                skipped += 1
                print(f"trial {trial}: SKIP peers={npeers} {' | '.join(log)} :: "
                      "no checkpoint landed while the job was running - the run proves "
                      "nothing; give it more --rounds or a lower --delay-range")
                if not args.keep:
                    shutil.rmtree(outdir, ignore_errors=True)
                continue
            failures.append("no checkpoint landed AND the job did not finish clean")
        if not finished:
            failures.append("timed out before every rank reached DONE")
        for r, want in expected[npeers].items():
            if r not in sums:
                failures.append(f"rank {r} never reached DONE")
            elif sums[r] != want:
                failures.append(f"rank {r} checksum {sums[r]} != baseline {want}")

        if failures:
            failed += 1
            print(f"trial {trial}: FAIL peers={npeers} {' | '.join(log)} :: {'; '.join(failures)}")
        else:
            passed += 1
            print(f"trial {trial}: pass peers={npeers} ckpts={n_ckpt} {' | '.join(log)}")
            if not args.keep:
                shutil.rmtree(outdir, ignore_errors=True)

    print(f"\n== {passed} passed, {failed} failed, {skipped} skipped (criu) ==")
    return 1 if failed else 0


def main():
    global PRINT_EVERY, PAYLOAD_INTS, SHAPE, CONFIG, PLANE
    ap = argparse.ArgumentParser()
    ap.add_argument("--trials", type=int, default=20)
    ap.add_argument("--peers", type=int, nargs="+", default=[2])
    ap.add_argument("--rounds", type=int, default=40000)
    ap.add_argument("--ms", type=int, default=0)
    ap.add_argument("--config", default=CONFIG,
                    help="FMI config the ranks run with, and with it the data plane under "
                         "test; must enable exactly one backend. Default: "
                         "$FMI_CHECKPOINT_CONFIG, else fmi.json beside this script")
    ap.add_argument("--shape", default=os.environ.get("FMI_SHAPE", "baseline"),
                    help="app shape the subject runs; see '<subject> --list-shapes'")
    ap.add_argument("--print-every", type=int, default=25)
    ap.add_argument("--payload-ints", type=int, default=1,
                    help="size the vector collective; >1 makes single messages span segments, "
                         "so a freeze can land part way through a payload")
    ap.add_argument("--delay-range", type=float, nargs=2, default=[0.25, 1.2],
                    help="seconds before each checkpoint; a low range catches ranks that are "
                         "still establishing their mesh")
    ap.add_argument("--max-checkpoints", type=int, default=1)
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--keep", action="store_true")
    ap.add_argument("--max-cost", type=float, default=1.0,
                    help="S3 only: estimated dollars this run may spend before it asks for "
                         "--yes. The other data planes are free and ignore it")
    ap.add_argument("--yes", action="store_true",
                    help="go ahead with an S3 run over --max-cost")
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
    print(f"data plane: {PLANE[0]} (from {CONFIG})")
    if PLANE[0] == "S3" and not s3_cost_guard(args, args.yes):
        return 2

    available = known_shapes()
    if available is None:
        print(f"cannot run the subject at {SUBJECT} -- build it first, or set "
              f"FMI_CHECKPOINT_SUBJECT", file=sys.stderr)
        return 2
    if SHAPE not in available:
        print(f"unknown shape {SHAPE!r}; the subject registered: {', '.join(available)}",
              file=sys.stderr)
        return 2

    random.seed(args.seed)
    # Per-invocation subdirectory, wiping only itself: a shared root cleared at startup
    # destroys the previous run's kept failure specimens the moment any later sweep starts -
    # measured three times in one day as diagnostic evidence lost to an unrelated run.
    root = os.path.join(HERE, "sweep", f"run{os.getpid()}")
    shutil.rmtree(root, ignore_errors=True)
    os.makedirs(root)

    try:
        return run(args, root)
    finally:
        # However the run ended -- verdicts, a crash, Ctrl-C -- nothing of it stays in the
        # store. Every name this invocation used starts with RUN_PREFIX for exactly this.
        clean_run(RUN_PREFIX)


if __name__ == "__main__":
    sys.exit(main())
