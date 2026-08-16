#!/usr/bin/env python3
"""Multi-host checkpoint/restore sweep over an unmodified FMI program.

The multi-host sibling of sweep.py: ranks are spread round-robin over a set of hosts
(--nodes), launched over ssh, and criu dump/restore runs on whichever host holds the
target rank. The restore host is chosen by --restore: "same" re-restores where the dump
happened, "next"/"random" restore on a DIFFERENT host, which exercises true cross-host
process migration. A shared filesystem must hold the repo checkout, the build tree and
the run directory at the SAME absolute path on every node (this file assumes it lives
inside that checkout), because criu reopens the subject binary, its cwd and its log file
by path on the restore host.

Cross-host restore prerequisites, all environmental:
  * identical distro/library versions on every node (criu remaps shared libraries by
    path and content),
  * criu run PRIVILEGED (sudo, as this driver does, or CAP_SYS_ADMIN): a cross-host
    restore must land in a time namespace preserving CLOCK_MONOTONIC, or every absolute
    steady_clock deadline in the image is off by the machines' uptime difference and a
    forward jump expires them all at once,
  * disjoint PID ranges per node (write a distinct base into /proc/sys/kernel/ns_last_pid
    on each node) -- criu restores the dumped PID verbatim, and a restore fails with
    "File exists" if that PID is taken on the destination,
  * a Redis reachable from every node (the DirectTCP registry): the config's
    registry_host must NOT be 127.0.0.1 anywhere, or a restored rank would look for the
    registry on whatever machine it wakes up on,
  * advertise_host left empty in the config, so each rank advertises the address that
    routes to the registry from the machine it is currently on.

Trial semantics, pass/fail/skip scoring, the checksum contract and the void rules are
sweep.py's, unchanged. --max-checkpoints 0 turns a trial into a pure multi-host liveness
check (pass = every rank DONE with the baseline checksum, no checkpoint involved).

The launch plumbing repeats sweep.py's hard-won rules: stdin from /dev/null and stdout
into a regular file (an inherited ssh/terminal socketpair makes every criu dump fail
with "External socket is used"), setsid so no controlling terminal is attached, --shape
kept last so pgrep patterns on the positional args keep working. Cross-host restore adds
--manage-cgroups=ignore on both dump and restore: ranks land in per-session systemd
cgroups whose paths exist only on the host that created them, and criu would otherwise
try to re-enter the dump host's session cgroup on the restore host.

One log-reading caveat unique to multi-host: rank logs are written over NFS by the rank's
host and read by the driver, so a log's server-side view can lag the rank by a few
seconds while the file is open. All pass/fail verdicts read logs only after the ranks
exited (close flushes), so verdicts are exact. The one reading that cannot wait for the
file to close is the pre-dump "round the rank had reached" snapshot, and over NFS it saw
nothing at all -- a reading of -1, which turns "logged a round PAST that one" into a
tautology. It is therefore taken ON the machine that owns the log (`last_round_remote`),
one extra ssh call per checkpoint. The primary criterion -- every rank DONE with the exact
baseline checksum -- was never affected either way.
"""
import argparse
import os
import random
import re
import shlex
import shutil
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", ".."))
SUBJECT = os.environ.get("FMI_CHECKPOINT_SUBJECT", os.path.join(
    REPO, "build", "runbooks", "criu-transparent-checkpoint", "fmi_checkpoint_subject"))

DONE_RE = re.compile(r"rank (\d+): DONE rounds=(\d+) checksum=(-?\d+)")

PRINT_EVERY = 25
PAYLOAD_INTS = 1
SHAPE = "baseline"
TRACE = False

# Disjoint pid bands per node, as seeded into /proc/sys/kernel/ns_last_pid. criu restores the
# dumped pid verbatim, so a pid already taken on the destination is a restore that fails with
# "File exists". Same map and span as runbooks/drain-migration/cluster.py: the bases are 500000
# apart, so 500000 -- not 512*1024 -- is the widest span that cannot admit a neighbour's pid.
DEFAULT_PID_BANDS = {
    "10.164.0.3": 1000000,   # criu-testing
    "10.164.0.4": 1500000,   # criu-node-2
    "10.164.0.5": 2000000,   # criu-node-1
    "10.164.0.6": 2500000,   # criu-node-3
}
PID_BAND_SPAN = 500000
PID_BANDS = dict(DEFAULT_PID_BANDS)
PLACEMENT_MAP = None          # rank -> node index, or None for round-robin
HOME_NODE = {}                # rank -> the node it was LAUNCHED on (pid-band home)


class SetupError(RuntimeError):
    """A cluster/setup violation. Never a protocol verdict -- it aborts the invocation."""


def pid_band_of(node):
    return PID_BANDS.get(node)


def assert_pid_in_band(node, pid, what):
    base = pid_band_of(node)
    if base is None:
        return
    if not (base <= pid < base + PID_BAND_SPAN):
        raise SetupError(f"{what}: pid {pid} on {node} is outside its band "
                         f"[{base}, {base + PID_BAND_SPAN}) -- re-seed ns_last_pid there")


def check_moved(rank, src, dest, pid, log):
    """Criterion: the rank actually changed machine, and its restored pid still belongs to the
    band of the machine it was LAUNCHED on. Returns a failure string or None.

    "Home" is the launch node, not the previous host. criu restores the dumped pid verbatim and
    keeps it for the life of the process, so after one hop the pid no longer matches the band of
    the machine the rank is sitting on -- that is the point, and it is what makes the pid
    evidence that this process was moved rather than started here. The corollary is that the
    "outside the destination's band" half only applies when the destination is not home: a rank
    chained back to the machine it started on (B2 does exactly that on its third hop) has a pid
    inside the destination's band, legitimately."""
    if dest == src:
        return f"pid {pid} was restored on its source host {src} -- not a cross-host move"
    home_node = HOME_NODE.get(rank, src)
    home, away = pid_band_of(home_node), pid_band_of(dest)
    if home is not None and not (home <= pid < home + PID_BAND_SPAN):
        return (f"restored pid {pid} is outside the band of its launch host {home_node} "
                f"[{home}, {home + PID_BAND_SPAN})")
    if dest != home_node and away is not None and away <= pid < away + PID_BAND_SPAN:
        return f"restored pid {pid} falls inside the destination band for {dest}"
    log.append(f"moved {src} -> {dest} (pid {pid} keeps its launch-host {home_node} band)")
    return None


class Cluster:
    """ssh plumbing. Every node is reached over ssh, including the driver's own host --
    one code path, and it keeps rank processes out of the driver's session/cgroup."""

    def __init__(self, nodes, ssh_key, ssh_user):
        self.nodes = nodes
        # Deliberately NO ControlMaster multiplexing: a mux master that wedges (observed
        # here with a detached-launch session) silently hangs every later call to that
        # node. One plain handshake per call costs ~0.2 s and has no shared failure state.
        self.base = ["ssh", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=accept-new",
                     "-o", "ConnectTimeout=10"]
        if ssh_key:
            self.base += ["-i", ssh_key]
        self.user = ssh_user

    def run(self, node, cmd, timeout=60):
        """Run cmd (a shell string) on node; returns CompletedProcess."""
        return subprocess.run(self.base + [f"{self.user}@{node}", cmd],
                              capture_output=True, text=True, timeout=timeout)

    def all_nodes(self, cmd, timeout=60):
        return [(n, self.run(n, cmd, timeout)) for n in self.nodes]


def start_job(cluster, comm, npeers, rounds, ms, outdir, config):
    """Launch each rank on nodes[rank % len(nodes)], detached. Returns rank -> (node, pid)."""
    placement = {}
    for r in range(npeers):
        if PLACEMENT_MAP is not None:
            node = cluster.nodes[PLACEMENT_MAP[r] % len(cluster.nodes)]
        else:
            node = cluster.nodes[r % len(cluster.nodes)]
        log = os.path.join(outdir, f"r{r}.log")
        # NOTE ";" not "&&" before setsid: with "cd X && cmd &" the WHOLE "cd && cmd" list
        # is backgrounded as a subshell, so $! names the subshell (criu would dump the
        # wrong process) and that subshell inherits the ssh session's stdout/stderr and
        # holds the channel open forever (observed: every launch hung for its full
        # timeout). With ";" the background job is the bare setsid command.
        trace = "FMI_LINK_TRACE=1 " if TRACE else ""
        inner = (f"cd {shlex.quote(HERE)}; {trace}setsid {shlex.quote(SUBJECT)} {r} {npeers} "
                 f"{shlex.quote(config)} {shlex.quote(comm)} {rounds} {ms} {PRINT_EVERY} "
                 f"{PAYLOAD_INTS} --shape {shlex.quote(SHAPE)} "
                 f"< /dev/null > {shlex.quote(log)} 2>&1 & echo LAUNCHED:$!")
        out = cluster.run(node, inner)
        m = re.search(r"LAUNCHED:(\d+)", out.stdout)
        if out.returncode != 0 or not m:
            raise RuntimeError(f"failed to launch rank {r} on {node}: "
                               f"{out.stdout} {out.stderr}")
        pid = int(m.group(1))
        assert_pid_in_band(node, pid, f"launch of rank {r}")
        HOME_NODE[r] = node
        placement[r] = (node, pid)
    return placement


def pid_alive(cluster, node, pid):
    return cluster.run(node, f"test -d /proc/{pid}").returncode == 0


def wait_pid_gone(cluster, node, pid, timeout_s=30):
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if not pid_alive(cluster, node, pid):
            return True
        time.sleep(0.3)
    return False


def last_round(outdir, rank):
    try:
        text = open(os.path.join(outdir, f"r{rank}.log")).read()
    except OSError:
        return -1
    hits = re.findall(r"rank \d+: round (\d+) ok", text)
    return int(hits[-1]) if hits else -1


def last_round_remote(cluster, node, outdir, rank):
    """The pre-dump round, read ON the machine that owns the log rather than over NFS.

    Reading it from the driver can see nothing at all while the rank still has the file open
    (close-to-open consistency), and a pre-dump reading of -1 turns "the rank logged a round
    PAST the one it had reached" into a tautology that every finished rank satisfies. One extra
    ssh call per checkpoint buys a criterion that means something. Falls back to the NFS read
    for a subject whose log does not carry this counter."""
    path = os.path.join(outdir, f"r{rank}.log")
    out = cluster.run(node, f"grep -oE 'round [0-9]+ ok' {shlex.quote(path)} | tail -1")
    m = re.search(r"round (\d+) ok", out.stdout)
    return int(m.group(1)) if m else last_round(outdir, rank)


def collect(outdir, npeers):
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
        time.sleep(0.5)
    return False


def resolve_node(cluster, designator):
    """A node designator is an INDEX into --nodes (so a scenario is cluster-agnostic) or a
    hostname, exactly as in runbooks/drain-migration/campaign.json."""
    s = str(designator)
    if s.isdigit() and int(s) < len(cluster.nodes):
        return cluster.nodes[int(s)]
    if s in cluster.nodes:
        return s
    raise SetupError(f"node designator {designator!r} is neither an index into "
                     f"{cluster.nodes} nor one of its members")


def destinations(cluster, sources, restore_policy, rng, explicit=None):
    """One destination per (rank, source-host) pair in `sources`, which is an ordered list of
    (rank, node). `spread` puts each migrating rank on a DIFFERENT survivor -- the shape a
    round-robin destination policy never produces."""
    evacuated = {n for _, n in sources}
    survivors = [n for n in cluster.nodes if n not in evacuated] or list(cluster.nodes)
    out = []
    if explicit is not None:
        host = resolve_node(cluster, explicit)
        return [host] * len(sources)
    if restore_policy == "spread":
        for i, _ in enumerate(sources):
            out.append(survivors[i % len(survivors)])
        return out
    if restore_policy == "random":
        # One destination for the whole event, as before: a cut lands the set together.
        pick = rng.choice(survivors)
        return [pick] * len(sources)
    for _, node in sources:
        if restore_policy == "same":
            out.append(node)
        else:                                        # "next"
            out.append(cluster.nodes[(cluster.nodes.index(node) + 1) % len(cluster.nodes)])
    return out


def checkpoint_restore(cluster, comm, rank, placement, imgdir, restore_policy, rng, log,
                       explicit_to=None, failures=None):
    """Dump rank where it lives, restore per policy. Updates placement on success."""
    node, pid = placement[rank]
    os.makedirs(imgdir, exist_ok=True)
    t0 = time.monotonic()
    # Privileged criu, deliberately (mirrors runbooks/drain-migration/cluster.py): only a
    # privileged restore creates the time namespace that preserves CLOCK_MONOTONIC across
    # hosts. The sequenced protocol's deadlines are steady_clock too, so an --unprivileged
    # cross-host restore onto a machine with a longer uptime expires them all at once; it
    # only ever looked fine on clusters whose nodes booted together.
    flags = "--tcp-close --shell-job --manage-cgroups=ignore -v4"
    d = cluster.run(node, f"sudo criu dump {flags} -t {pid} -D {shlex.quote(imgdir)} -o dump.log",
                    timeout=120)
    t_dump = time.monotonic() - t0
    if d.returncode != 0:
        log.append(f"dump rc={d.returncode} on {node}: {d.stderr.strip()[:300]}")
        return False
    # criu kills the dumped process; it was reparented to init at launch, so init reaps
    # it. The PID must actually be gone before a same-host restore can reclaim it.
    if not wait_pid_gone(cluster, node, pid):
        log.append(f"dumped pid {pid} still present on {node}")
        return False

    dest = destinations(cluster, [(rank, node)], restore_policy, rng, explicit_to)[0]
    t1 = time.monotonic()
    r = cluster.run(dest, f"cd {shlex.quote(HERE)} && sudo criu restore {flags} "
                          f"-D {shlex.quote(imgdir)} -o restore.log -d", timeout=120)
    t_restore = time.monotonic() - t1
    if r.returncode != 0:
        log.append(f"restore rc={r.returncode} on {dest}: {r.stderr.strip()[:300]}")
        return False
    if not pid_alive(cluster, dest, pid):
        log.append(f"restored pid {pid} not running on {dest}")
        return False
    placement[rank] = (dest, pid)
    log.append(f"restored on {dest}" + (" (cross-host)" if dest != node else " (same host)")
               + f" [timing dump={t_dump:.2f}s restore={t_restore:.2f}s "
                 f"window={time.monotonic() - t0:.2f}s]")
    if restore_policy != "same" and failures is not None:
        bad = check_moved(rank, node, dest, pid, log)
        if bad:
            failures.append(bad)
    return True


def evacuate_node(cluster, comm, nodes, placement, imgroot, restore_policy, rng, log, outdir,
                  explicit_to=None, failures=None):
    """The whole-machine analogue of checkpoint_restore: dump EVERY live rank on `nodes`,
    and only then restore them all on the destination — the machine is emptied first, so no
    rank of the moved set ever briefly coexists with a half-moved peer on the old host.

    `nodes` may name SEVERAL machines, in which case all of their ranks are dumped before any
    of them is restored: one cut across two machines, the widest form of the event. Each rank
    keeps its own source host, so `--restore next`/`spread` route per rank rather than per cut.

    Returns rank -> pre-dump round on success, False on a criu failure, None when the named
    machines held no live rank (the caller voids the attempt)."""
    if isinstance(nodes, str):
        nodes = [nodes]
    ranks = [r for r, (n, p) in sorted(placement.items())
             if n in nodes and pid_alive(cluster, n, p)]
    if not ranks:
        return None
    # Privileged criu, deliberately (mirrors runbooks/drain-migration/cluster.py): only a
    # privileged restore creates the time namespace that preserves CLOCK_MONOTONIC across
    # hosts. The sequenced protocol's deadlines are steady_clock too, so an --unprivileged
    # cross-host restore onto a machine with a longer uptime expires them all at once; it
    # only ever looked fine on clusters whose nodes booted together.
    flags = "--tcp-close --shell-job --manage-cgroups=ignore -v4"
    before = {}
    t0 = time.monotonic()
    sources = [(r, placement[r][0]) for r in ranks]
    # Every rank of every named machine is dumped BEFORE any of them is restored -- the
    # sequenced protocol's "single cut": both images of a co-evacuated pair carry unacked
    # traffic to each other, and the replay has to reconcile both directions.
    for r in ranks:
        node, pid = placement[r]
        imgdir = os.path.join(imgroot, f"r{r}")
        os.makedirs(imgdir, exist_ok=True)
        before[r] = last_round_remote(cluster, node, outdir, r)
        d = cluster.run(node, f"sudo criu dump {flags} -t {pid} -D {shlex.quote(imgdir)} "
                              f"-o dump.log", timeout=120)
        if d.returncode != 0:
            log.append(f"evac dump rank {r} rc={d.returncode} on {node}: "
                       f"{d.stderr.strip()[:200]}")
            return False
    for r in ranks:
        node, pid = placement[r]
        if not wait_pid_gone(cluster, node, pid):
            log.append(f"evac pid {pid} still present on {node}")
            return False
    t_dumps = time.monotonic() - t0
    dests = destinations(cluster, sources, restore_policy, rng, explicit_to)
    for (r, src), dest in zip(sources, dests):
        imgdir = os.path.join(imgroot, f"r{r}")
        res = cluster.run(dest, f"cd {shlex.quote(HERE)}; sudo criu restore {flags} "
                                f"-D {shlex.quote(imgdir)} -o restore.log -d", timeout=120)
        _, pid = placement[r]
        if res.returncode != 0 or not pid_alive(cluster, dest, pid):
            log.append(f"evac restore rank {r} rc={res.returncode} on {dest}: "
                       f"{res.stderr.strip()[:200]}")
            return False
        placement[r] = (dest, pid)
        if restore_policy != "same" and failures is not None:
            bad = check_moved(r, src, dest, pid, log)
            if bad:
                failures.append(bad)
    log.append("evacuated " + ",".join(nodes) + ": "
               + " ".join(f"r{r}:{src}->{d}" for (r, src), d in zip(sources, dests))
               + f" [timing k={len(ranks)} dumps={t_dumps:.2f}s "
                 f"cut={time.monotonic() - t0:.2f}s]")
    return before


def kill_all(cluster, comm):
    cluster.all_nodes(f"pkill -9 -f {shlex.quote(os.path.basename(SUBJECT) + ' .* ' + comm + ' ')}"
                      " || true")


def registry_del(comm, redis_host, redis_port=6379):
    # The port matters: a campaign registry is rarely on 6379, and deleting the key from the
    # WRONG Redis silently leaves the stale entry the next trial would read.
    subprocess.run(["redis-cli", "-h", redis_host, "-p", str(redis_port),
                    "DEL", f"fmi:direct:{comm}"], capture_output=True)


def baseline(cluster, npeers, rounds, ms, root, config, redis_host, redis_port=6379):
    comm = f"mhbase{npeers}x{rounds}x{os.getpid()}"
    outdir = os.path.join(root, comm)
    os.makedirs(outdir, exist_ok=True)
    registry_del(comm, redis_host, redis_port)
    started = time.monotonic()
    placement = start_job(cluster, comm, npeers, rounds, ms, outdir, config)
    if not wait_for_finish(outdir, npeers, timeout_s=1200):
        kill_all(cluster, comm)
        print(f"BASELINE TIMED OUT for shape {SHAPE} at {npeers} peers "
              f"(placement {placement})", file=sys.stderr)
        sys.exit(1)
    elapsed = time.monotonic() - started
    sums, failures = collect(outdir, npeers)
    kill_all(cluster, comm)
    if failures or len(sums) != npeers:
        print(f"BASELINE FAILED for shape {SHAPE} at {npeers} peers: {failures} {sums}",
              file=sys.stderr)
        sys.exit(1)
    shutil.rmtree(outdir, ignore_errors=True)
    return sums, elapsed


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--nodes", nargs="+", required=True,
                    help="hostnames/IPs of the cluster nodes; ranks go round-robin")
    ap.add_argument("--ssh-key", default=os.environ.get("FMI_SSH_KEY", ""))
    ap.add_argument("--ssh-user", default=os.environ.get("FMI_SSH_USER", os.environ.get("USER", "ec2-user")))
    ap.add_argument("--config", required=True,
                    help="fmi.json with a cluster-reachable registry_host; see module doc")
    ap.add_argument("--redis-host", default="",
                    help="registry host for pre-trial cleanup; default: parsed from --config")
    ap.add_argument("--trials", type=int, default=20)
    ap.add_argument("--peers", type=int, nargs="+", default=[4])
    ap.add_argument("--rounds", type=int, default=40000)
    ap.add_argument("--ms", type=int, default=0)
    ap.add_argument("--shape", default=os.environ.get("FMI_SHAPE", "baseline"))
    ap.add_argument("--print-every", type=int, default=25)
    ap.add_argument("--payload-ints", type=int, default=1)
    ap.add_argument("--delay-range", type=float, nargs=2, default=[0.25, 1.2])
    ap.add_argument("--max-checkpoints", type=int, default=1,
                    help="0 = no checkpointing: a pure multi-host liveness/correctness trial")
    ap.add_argument("--restore", choices=["same", "next", "random", "spread"], default="same",
                    help="where a dumped rank is restored, relative to its dump host. "
                         "'next'/'random'/'spread' are cross-host: they require advertise_host "
                         "to be EMPTY in the config, so the restored rank's next registry "
                         "publish re-derives the address of the machine it actually woke up on "
                         "(publish_self re-resolves on every publish). 'spread' puts each rank "
                         "of one event on a DIFFERENT survivor.")
    ap.add_argument("--evacuate", action="store_true",
                    help="each checkpoint event empties a whole NODE: every live rank it "
                         "hosts is dumped, then all of them are restored on the --restore "
                         "destination")
    ap.add_argument("--evacuate-node", nargs="+", default=None, metavar="NODE",
                    help="which node each checkpoint event evacuates, positionally (a node "
                         "INDEX into --nodes, or a hostname); implies --evacuate and fixes the "
                         "number of events. An element may name SEVERAL nodes comma-joined "
                         "('2,3'), which empties both machines in ONE cut: every rank of both "
                         "is dumped before any of them is restored.")
    ap.add_argument("--target-rank", nargs="+", type=int, default=None, metavar="RANK",
                    help="which rank each checkpoint event targets, positionally, instead of a "
                         "random one; fixes the number of events. Repeat a rank to cut the "
                         "SAME rank again ('--target-rank 1 1 1').")
    ap.add_argument("--restore-to", nargs="+", default=None, metavar="NODE",
                    help="explicit destination per checkpoint event, positionally (index into "
                         "--nodes, or a hostname); overrides --restore for that event.")
    ap.add_argument("--exact-checkpoints", action="store_true",
                    help="perform exactly --max-checkpoints events instead of a random 1..N")
    ap.add_argument("--place", default=None, metavar="R:NODE,...",
                    help="explicit placement map, e.g. '0:0,1:1,2:2,3:2,...' (rank : node "
                         "index). Round-robin never co-locates ring NEIGHBOURS -- it places r "
                         "and r+len(nodes) together -- so a scenario that needs neighbours on "
                         "one machine has to say so.")
    ap.add_argument("--pid-bands", default="default",
                    help="'default' asserts the campaign's per-node ns_last_pid bands on every "
                         "launched and restored pid; 'none' disables the assertion")
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--keep", action="store_true")
    ap.add_argument("--trace", action="store_true",
                    help="run every rank with FMI_LINK_TRACE=1 (beacon diagnosis)")
    args = ap.parse_args()

    global PRINT_EVERY, PAYLOAD_INTS, SHAPE, TRACE, PID_BANDS, PLACEMENT_MAP
    PRINT_EVERY = args.print_every
    PAYLOAD_INTS = args.payload_ints
    SHAPE = args.shape
    TRACE = args.trace
    if args.pid_bands == "none":
        PID_BANDS = {}
    if args.place:
        PLACEMENT_MAP = {int(k): int(v)
                         for k, v in (p.split(":") for p in args.place.split(","))}
    if args.evacuate_node:
        args.evacuate = True

    if not os.path.exists(SUBJECT):
        print(f"subject not built at {SUBJECT}", file=sys.stderr)
        return 2
    if not os.path.isabs(args.config) or not os.path.exists(args.config):
        print(f"--config must be an absolute path that exists on every node: {args.config}",
              file=sys.stderr)
        return 2

    import json
    with open(args.config) as f:
        backend = json.load(f)["backends"]["DirectTCP"]
    redis_host = args.redis_host or backend["registry_host"]
    redis_port = int(backend.get("registry_port", 6379))
    if redis_host in ("127.0.0.1", "localhost"):
        print("registry_host is loopback -- a restored rank would look for the registry on "
              "whatever machine it wakes up on; use a cluster-reachable address",
              file=sys.stderr)
        return 2

    cluster = Cluster(args.nodes, args.ssh_key, args.ssh_user)
    for n, res in cluster.all_nodes("echo ok"):
        if res.returncode != 0 or "ok" not in res.stdout:
            print(f"cannot ssh to {n}: {res.stderr.strip()}", file=sys.stderr)
            return 2

    rng = random.Random(args.seed)
    root = os.path.join(HERE, "sweep", f"mhrun{os.getpid()}")
    shutil.rmtree(root, ignore_errors=True)
    os.makedirs(root)

    expected = {}
    finish_timeout = {}
    for n in args.peers:
        sums, elapsed = baseline(cluster, n, args.rounds, args.ms, root, args.config,
                                 redis_host, redis_port)
        expected[n] = sums
        finish_timeout[n] = max(180.0, 3.0 * elapsed + 120.0)
        print(f"baseline shape={SHAPE} {n} peers: {sums} ({elapsed:.1f}s)")

    passed = failed = skipped = 0
    for trial in range(args.trials):
        npeers = rng.choice(args.peers)
        comm = f"mh{trial}p{npeers}x{os.getpid()}"
        outdir = os.path.join(root, comm)
        os.makedirs(outdir, exist_ok=True)
        registry_del(comm, redis_host, redis_port)
        log = []
        placement = start_job(cluster, comm, npeers, args.rounds, args.ms, outdir,
                              args.config)
        log.append("placement " + " ".join(f"{r}@{n}" for r, (n, _) in sorted(placement.items())))

        # A scripted event list (--target-rank / --evacuate-node) fixes the count; so does
        # --exact-checkpoints. Otherwise the historical random 1..N.
        if args.max_checkpoints <= 0:
            n_ckpt = 0
        elif args.target_rank:
            n_ckpt = len(args.target_rank)
        elif args.evacuate_node:
            n_ckpt = len(args.evacuate_node)
        elif args.exact_checkpoints:
            n_ckpt = args.max_checkpoints
        else:
            n_ckpt = rng.randint(1, args.max_checkpoints)
        ok = True
        void = False
        checkpoints = []
        move_failures = []
        for k in range(n_ckpt):
            time.sleep(rng.uniform(args.delay_range[0], args.delay_range[1]))
            to_k = args.restore_to[k % len(args.restore_to)] if args.restore_to else None
            if args.evacuate:
                if args.evacuate_node:
                    nodes = [resolve_node(cluster, d)
                             for d in str(args.evacuate_node[k]).split(",")]
                else:
                    nodes = [rng.choice(sorted({n for _, (n, _) in placement.items()}))]
                log.append(f"ckpt {k}: evacuating {','.join(nodes)}")
                result = evacuate_node(cluster, comm, nodes, placement,
                                       os.path.join(outdir, f"img{k}"), args.restore,
                                       rng, log, outdir, to_k, move_failures)
                if result is None:
                    log.append(f"ckpt {k}: {','.join(nodes)} had no live ranks (trial is void)")
                    void = True
                    continue
                if result is False:
                    ok = False
                    break
                checkpoints.extend(sorted(result.items()))
                continue
            target = args.target_rank[k] if args.target_rank else rng.randrange(npeers)
            node, pid = placement[target]
            if not pid_alive(cluster, node, pid):
                log.append(f"ckpt {k}: rank {target} ALREADY FINISHED (trial is void)")
                void = True
                continue
            before = last_round_remote(cluster, node, outdir, target)
            log.append(f"ckpt {k}: rank {target} pid {pid} on {node} at round {before}")
            if not checkpoint_restore(cluster, comm, target, placement,
                                      os.path.join(outdir, f"img{k}"), args.restore,
                                      rng, log, to_k, move_failures):
                ok = False
                break
            checkpoints.append((target, before))

        if not ok:
            kill_all(cluster, comm)
            print(f"trial {trial}: SKIP (criu) {' | '.join(log)}")
            skipped += 1
            continue

        finished = wait_for_finish(outdir, npeers, timeout_s=finish_timeout[npeers])
        sums, failures = collect(outdir, npeers)
        failures.extend(move_failures)
        for target, before in checkpoints:
            after = last_round(outdir, target)
            if after <= before:
                failures.append(
                    f"rank {target} logged no round past {before} after its restore")
        kill_all(cluster, comm)

        if args.max_checkpoints > 0 and (void or not checkpoints):
            clean = finished and not failures and all(
                sums.get(r) == want for r, want in expected[npeers].items())
            if clean:
                skipped += 1
                print(f"trial {trial}: SKIP peers={npeers} {' | '.join(log)} :: "
                      "no checkpoint landed while the job was running")
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
            print(f"trial {trial}: FAIL peers={npeers} {' | '.join(log)} :: "
                  f"{'; '.join(failures)}")
        else:
            passed += 1
            what = f"ckpts={len(checkpoints)}" if checkpoints else "no-ckpt smoke"
            print(f"trial {trial}: pass peers={npeers} {what} {' | '.join(log)}")
            if not args.keep:
                shutil.rmtree(outdir, ignore_errors=True)

    print(f"\n== {passed} passed, {failed} failed, {skipped} skipped (criu) ==")
    return 1 if failed else 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except SetupError as e:
        # A setup violation aborts the invocation and is NEVER reported as a protocol verdict.
        print(f"SETUP ERROR: {e}", file=sys.stderr)
        sys.exit(3)
