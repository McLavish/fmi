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
"""
import argparse
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
# Overridable so a differently-named build tree works without editing this file.
SUBJECT = os.environ.get("FMI_CHECKPOINT_SUBJECT", os.path.join(
    REPO, "build", "runbooks", "criu-transparent-checkpoint", "fmi_checkpoint_subject"))
CONFIG = os.environ.get("FMI_CHECKPOINT_CONFIG", os.path.join(HERE, "fmi.json"))

DONE_RE = re.compile(r"rank (\d+): DONE rounds=(\d+) checksum=(-?\d+)")


PRINT_EVERY = 25
PAYLOAD_INTS = 1
SHAPE = "baseline"


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
    comm = f"base{npeers}x{rounds}x{os.getpid()}"
    outdir = os.path.join(root, comm)
    os.makedirs(outdir, exist_ok=True)
    subprocess.run(["redis-cli", "DEL", f"fmi:direct:{comm}"], capture_output=True)
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
    return sums, elapsed


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--trials", type=int, default=20)
    ap.add_argument("--peers", type=int, nargs="+", default=[2])
    ap.add_argument("--rounds", type=int, default=40000)
    ap.add_argument("--ms", type=int, default=0)
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
    args = ap.parse_args()

    global PRINT_EVERY, PAYLOAD_INTS, SHAPE
    PRINT_EVERY = args.print_every
    PAYLOAD_INTS = args.payload_ints
    SHAPE = args.shape

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
        comm = f"sw{trial}p{npeers}x{os.getpid()}"
        outdir = os.path.join(root, comm)
        os.makedirs(outdir, exist_ok=True)
        subprocess.run(["redis-cli", "DEL", f"fmi:direct:{comm}"], capture_output=True)
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


if __name__ == "__main__":
    sys.exit(main())
