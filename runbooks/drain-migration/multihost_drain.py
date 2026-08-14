#!/usr/bin/env python3
"""Cross-host drain migration of unmodified FMI ranks over four machines: the campaign driver.

The multi-host sibling of `drain_driver.py`, and it **imports** that driver rather than
restating it: the scoring, the verdicts, the socket-evidence rules and the control-plane reader
that produced the published single-host evidence are used here unchanged, so a result from this
driver and a result from that one mean the same thing. What is new is only what a cluster adds:
ssh, placement across machines, and the `migrate` event that lets several ranks drain in ONE
cut (`cluster.py`).

Two regimes, from the same scenario file:

  **seq** -- one rank at a time, the sequence `drain_driver.migrate` performs, with every local
  operation replaced by a remote one and **one addition**: after `restored`, wait for the batch
  lease to clear. A rank that leased for itself releases the lease one Redis round trip AFTER it
  emitted `restored`; firing the next migration on the event alone races that release and the
  next migration runs leaseless.

  **cut** -- a whole set of ranks in one batch: the driver takes the batch lease itself, emits
  one `migrate` event per rank carrying that batch id (which is what makes each rank skip
  self-leasing), waits for EVERY rank to seal, cross-checks the sealed counters pairwise --
  `sealed[a].sent.b == sealed[b].received.a`, byte-exactness evidence before anything is dumped
  -- and only then dumps them all, reaps them all, restores them all on their destinations and
  waits for every `restored` before releasing the lease. The machine is emptied before anything
  comes back, so no rank of the moved set ever coexists with a half-moved peer on the old host
  (272caee's ordering, with the batch lease and the counter check added).

Phase C scenarios drive the batch path. The three library defects that used to block concurrent
batches (C-1 stale `draining` on a co-migrating peer, C-2 the event cursor advancing before
dispatch, C-3 a leaked batch lease on a failed drain) **landed at 63382da**, each with its own
mutation-verified test. `--allow-batch` is no longer a refusal to override: it is an
acknowledgement that the scenario drives the batch path, which is younger evidence than the
sequential one and whose verdicts should be read as such.

Prerequisites (phase A of the plan, all of them environmental):
  * the repo, the build tree, this directory and the run directory at the SAME absolute path on
    every node -- criu reopens the binary, the cwd and the log file by path on the restore host;
  * one Redis every node can reach, named in the config, **never loopback**;
  * `advertise_host: ""`, so a restored rank re-derives the address of the machine it woke up
    on (`resolve_advertise_ip` runs again on the restore leg);
  * disjoint pid bands per node, seeded into `/proc/sys/kernel/ns_last_pid`;
  * criu with `cap_sys_ptrace,cap_checkpoint_restore` and `--unprivileged` on both legs.

One log-reading caveat inherited from multihost_sweep.py: rank logs are written over NFS by the
rank's host and read here, so a log's server-side view can lag while the file is open. Every
pass/fail verdict reads logs only after the ranks exited (close flushes); the one softer read is
the pre-migration "round the rank had reached" snapshot, which can undercount and makes the
progressed-after-restore check conservative in the trial's favour.
"""
import argparse
import itertools
import json
import os
import random
import shutil
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", ".."))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

import cluster as cl                                                          # noqa: E402
import drain_driver                                                           # noqa: E402
from drain_driver import (SOCKET_IMAGES, clean_comm, collect, data_plane,     # noqa: E402
                          drain_signal_number, known_shapes, last_round,
                          read_events, save_events, socket_lines, wait_for_event,
                          wait_for_finish, wait_until_warm)

SUBJECT = os.environ.get("FMI_CHECKPOINT_SUBJECT", os.path.join(
    REPO, "build", "runbooks", "criu-transparent-checkpoint", "fmi_checkpoint_subject"))
CONFIG = os.environ.get("FMI_DRAIN_CONFIG", os.path.join(HERE, "fmi_drain_multihost.json"))
CAMPAIGN = os.path.join(HERE, "campaign.json")

# Every knob a scenario may set, with the value used when neither the scenario nor the
# campaign's "defaults" block names it.
FIELD_DEFAULTS = {
    "peers": 4,
    "shape": "baseline",
    "rounds": 40000,
    "ms": 1,
    "trials": 3,
    "place": "rr",
    "payload_ints": 1,
    "print_every": 25,
    "warmup_round": 1,
    "warmup_timeout": 120.0,
    "delay_range": [0.5, 1.5],
    "seal_timeout": 60.0,
    "restore_timeout": 120.0,
    "lease_free_timeout": 30.0,
    "dump_timeout": 300.0,
    "criu_restore_timeout": 300.0,
    "wait_gone_timeout": 60.0,
    "ssh_timeout": 60.0,
    "batch_lease_ms": 120000,
    "to": "next",
    "cuts": "single",
}

HARD_VERDICTS = ("socket", "criu", "drain")
VERDICT_LABEL = {
    "socket": "FAIL (A SOCKET SURVIVED THE DRAIN)",
    "criu": "FAIL (criu itself, no socket evidence — triage the environment)",
    "drain": "FAIL (the drain protocol)",
}


# --------------------------------------------------------------------------- the scenario file

# Knobs the command line pinned (--trials, --rounds). They beat everything in the campaign file:
# a scenario's own value is a considered default, but an operator who typed --trials 1 to take a
# quick look meant 1, not "1 unless the scenario disagrees". Set once in main().
OVERRIDES = {}


def field(scen, defaults, key, shape=None):
    """One knob's value for a scenario, and -- when `shape` is given -- for that shape.

    A command-line OVERRIDE wins outright. Below that the precedence is
    scenario-before-defaults, and within each, **by_shape before flat**:

        scen["by_shape"][shape][key] > scen[key] > defaults["by_shape"][shape][key]
                                                 > defaults[key] > FIELD_DEFAULTS[key]

    `by_shape` exists because a scenario may name a LIST of shapes and the shapes of this subject
    differ by seventy-fold in cost per round: measured cross-host at 4 peers, `p2p_ring` runs
    1.1 ms/round and `uneven_participation` 102 ms/round. One `rounds` for a list of shapes
    therefore cannot mean one job length -- B7's three shapes at a shared 1700 rounds would be
    71 s, 174 s and 2 s respectively -- and job length is not cosmetic here: too short and the
    job outruns the migration (a SKIP that proves nothing), too long and the campaign does not
    fit in its window. Sizing per shape is what makes a scenario's trials comparable to each
    other and to the single-host table.
    """
    if key in OVERRIDES:
        return OVERRIDES[key]
    if shape is not None:
        by_shape = scen.get("by_shape") or {}
        if shape in by_shape and key in by_shape[shape]:
            return by_shape[shape][key]
    if key in scen:
        return scen[key]
    if shape is not None:
        by_shape = defaults.get("by_shape") or {}
        if shape in by_shape and key in by_shape[shape]:
            return by_shape[shape][key]
    if key in defaults:
        return defaults[key]
    return FIELD_DEFAULTS[key]


def load_campaign(path):
    """(defaults, [scenario]) from campaign.json, with the shape of every entry checked here.

    A scenario that is malformed must fail before a single rank is launched: the campaign is
    hours long and a typo discovered in hour three costs the whole block.
    """
    try:
        with open(path) as f:
            doc = json.load(f)
    except (OSError, ValueError) as exc:
        sys.exit("cannot read the campaign at %s: %s" % (path, exc))
    defaults = doc.get("defaults", {})
    scenarios = doc.get("scenarios", [])
    if not isinstance(scenarios, list) or not scenarios:
        sys.exit("%s has no scenarios" % path)
    seen = set()
    for scen in scenarios:
        name = scen.get("name")
        if not name or name in seen:
            sys.exit("every scenario needs a unique name; %r is missing or repeated" % name)
        seen.add(name)
        if scen.get("kind") not in ("clean", "seq", "cut"):
            sys.exit("scenario %s: kind must be clean, seq or cut (got %r)"
                     % (name, scen.get("kind")))
        if scen.get("kind") != "clean" and "ranks" not in scen and "evacuate" not in scen:
            sys.exit("scenario %s: a seq/cut scenario needs `ranks` or `evacuate`" % name)
        if scen.get("phase", "B") not in ("B", "C"):
            sys.exit("scenario %s: phase must be B or C" % name)
    return defaults, scenarios


def shapes_of(scen, defaults):
    """A scenario's shapes: `shape` may be one name or a list, and a list means "run it once per
    shape" -- the trial count applies to each."""
    value = field(scen, defaults, "shape")
    return [value] if isinstance(value, str) else list(value)


# ----------------------------------------------------------------------------- placement

def resolve_node(nodes, designator):
    """A node designator is an INDEX into --nodes or a hostname.

    Indices keep campaign.json cluster-agnostic: the campaign says "the third machine in node
    order", the invocation says which machine that is. A hostname is accepted too, for a
    scenario that is genuinely about one named machine.
    """
    if isinstance(designator, bool):
        raise cl.SetupError("node designator %r is a bool" % designator)
    if isinstance(designator, int):
        if not 0 <= designator < len(nodes):
            raise cl.SetupError("node index %d is outside --nodes (%d given)"
                                % (designator, len(nodes)))
        return nodes[designator]
    text = str(designator)
    if text.isdigit():
        return resolve_node(nodes, int(text))
    if text in nodes:
        return text
    raise cl.SetupError("node %r is not in --nodes (%s)" % (text, ", ".join(nodes)))


def placement_for(place, nodes, npeers):
    """{rank: node} for a placement policy.

      rr       rank r on nodes[r % N] -- the default, and note what it does NOT give you: at 8
               ranks over 4 machines, r and r+4 share a machine and ring NEIGHBOURS never do.
      block    contiguous blocks, so ring neighbours DO share a machine.
      explicit a {rank: designator} map, for a scenario that needs a specific adjacency (the
               mutual-seal cut wants its two migrators to be each other's ring neighbours).
    """
    n = len(nodes)
    if isinstance(place, dict):
        out = {}
        for rank in range(npeers):
            if str(rank) not in place and rank not in place:
                raise cl.SetupError("explicit placement does not name rank %d" % rank)
            out[rank] = resolve_node(nodes, place.get(str(rank), place.get(rank)))
        return out
    if place == "rr":
        return {r: nodes[r % n] for r in range(npeers)}
    if place == "block":
        per_node = (npeers + n - 1) // n
        return {r: nodes[min(r // per_node, n - 1)] for r in range(npeers)}
    raise cl.SetupError("unknown placement %r (rr, block or an explicit map)" % (place,))


def destinations_for(to, nodes, moving, place_map):
    """{rank: destination node} for one migration step.

      "next"      the node after the rank's current one, in --nodes order
      "spread"    round-robin over the machines that are NOT being emptied by this step -- the
                  shape a sequenced evacuation never produced: k ranks landing on k different
                  survivors
      "one"       every migrator onto the same survivor
      index/host  that machine, for every migrator
      a list      one destination per migrator, positionally (a chained scenario's itinerary)
    """
    if isinstance(to, list):
        if len(to) != len(moving):
            raise cl.SetupError("`to` lists %d destinations for %d moving ranks"
                                % (len(to), len(moving)))
        return {rank: resolve_node(nodes, d) for rank, d in zip(moving, to)}
    if to == "next":
        return {rank: nodes[(nodes.index(place_map[rank]) + 1) % len(nodes)] for rank in moving}
    if to in ("spread", "one"):
        emptied = {place_map[rank] for rank in moving}
        survivors = [n for n in nodes if n not in emptied] or \
                    [n for n in nodes if n != place_map[moving[0]]]
        if not survivors:
            raise cl.SetupError("no survivor node is left to move onto")
        if to == "one":
            return {rank: survivors[0] for rank in moving}
        return {rank: survivors[i % len(survivors)] for i, rank in enumerate(moving)}
    node = resolve_node(nodes, to)
    return {rank: node for rank in moving}


def ranks_on(place_map, node):
    return sorted(r for r, n in place_map.items() if n == node)


def plan_steps(scen, defaults, nodes, npeers, place_map):
    """The whole itinerary of one trial, computed before anything runs.

    Pure: it takes the initial placement and SIMULATES each move, so a chained scenario's later
    steps see where the rank actually is. That is what makes `--dry-run` a real review artefact
    rather than a sketch -- the printed plan is the plan that will execute.

    A step is {"kind", "index", "moves": [{"rank", "src", "dest"}], "batch"?}.
    """
    kind = scen["kind"]
    if kind == "clean":
        return []
    place_map = dict(place_map)
    to = field(scen, defaults, "to")

    if "evacuate" in scen:
        targets = scen["evacuate"]
        targets = targets if isinstance(targets, list) else [targets]
        target_nodes = [resolve_node(nodes, t) for t in targets]
        groups = ([[n] for n in target_nodes]
                  if field(scen, defaults, "cuts") == "per-node" else [target_nodes])
        moving_groups = []
        for group in groups:
            moving = sorted(r for n in group for r in ranks_on(place_map, n))
            if not moving:
                raise cl.SetupError("no rank is placed on %s" % ", ".join(group))
            # An evacuation is a set of ranks; the KIND says how they leave. `seq` empties the
            # machine one rank at a time (each its own signal, its own dump, its own restore --
            # B5's "two ranks onto two survivors, sequentially"), `cut` empties it in one batch.
            moving_groups += [[r] for r in moving] if kind == "seq" else [moving]
    else:
        ranks = scen["ranks"]
        moving_groups = [sorted(ranks)] if kind == "cut" else [[r] for r in ranks]

    # For a sequential evacuation "spread" must spread over the WHOLE rank list, not per step:
    # the point of B5 is two ranks of one machine landing on two different survivors.
    flat = [r for group in moving_groups for r in group]
    if kind == "seq" and "evacuate" in scen and not isinstance(to, list):
        dest_map = destinations_for(to, nodes, flat, place_map)
    else:
        dest_map = None

    steps = []
    # A positional cursor, not a lookup by rank: a chained scenario migrates the SAME rank
    # several times (B2 is `ranks: [1, 1, 1]`), so `flat.index(rank)` would hand every step the
    # first destination and quietly turn a three-machine itinerary into one move.
    cursor = 0
    for index, group in enumerate(moving_groups):
        if dest_map is not None:
            dests = {rank: dest_map[rank] for rank in group}
        elif isinstance(to, list):
            dests = destinations_for(to[cursor:cursor + len(group)], nodes, group, place_map)
        else:
            dests = destinations_for(to, nodes, group, place_map)
        cursor += len(group)
        step = {"kind": kind, "index": index,
                "moves": [{"rank": r, "src": place_map[r], "dest": dests[r]} for r in group]}
        steps.append(step)
        for move in step["moves"]:
            place_map[move["rank"]] = move["dest"]
    return steps


# ----------------------------------------------------------------------------- one trial

class Trial:
    """Everything one trial needs, in one object so the step functions stay readable."""

    def __init__(self, args, scen, defaults, shape, npeers, comm, outdir, nodes, cluster,
                 params, bands, span, drain_signal, signal_offset):
        self.args = args
        self.scen = scen
        self.defaults = defaults
        self.shape = shape
        self.npeers = npeers
        self.comm = comm
        self.outdir = outdir
        self.nodes = nodes
        self.cluster = cluster
        self.params = params
        self.bands = bands
        self.span = span
        self.drain_signal = drain_signal
        self.signal_offset = signal_offset
        self.placement = {}
        # {rank: the node it was LAUNCHED on}. Fixed for the whole trial, unlike `placement`,
        # which follows the rank around. criu restores a pid verbatim, so the pid belongs to the
        # launch node's band for the rank's whole life -- see cluster.assert_restored_pid.
        self.home = {}
        self.epochs = {}
        self.migrations = []
        self.cuts = []
        self.log = []

    def f(self, key):
        # Always shape-aware: a trial knows its shape, and `rounds`/`payload_ints` are sized per
        # shape (see `field`).
        return field(self.scen, self.defaults, key, self.shape)


def note_counters(sealed):
    return " ".join("%s=%s" % (k, v) for k, v in sorted(sealed.items())
                    if k.startswith(("sent.", "received.")))


def await_seal(t, ranks, epochs):
    """Every rank's own `sealed` event. Returns (verdict, note, {rank: fields})."""
    sealed = {}
    for rank in ranks:
        fields = wait_for_event(t.params, t.comm, "sealed", rank, epochs[rank],
                                t.f("seal_timeout"))
        if fields is None:
            return "drain", ("no sealed event for rank %d at epoch %d within %ss"
                             % (rank, epochs[rank], t.f("seal_timeout"))), sealed
        sealed[rank] = fields
        t.log.append("sealed r%d %s" % (rank, note_counters(fields) or "(no links)"))
    return "ok", "", sealed


def check_seal_state(t, ranks):
    """State T and a zero-socket fd scan for every rank, one ssh call each.

    Both, and in this order, for `drain_driver.migrate`'s reason: the seal is emitted before the
    process releases its last two sockets and stops, so a dump taken on the event alone can land
    before either happened.
    """
    for rank in ranks:
        node, pid = t.placement[rank]
        rc, payload = cl.seal_wait_remote(t.cluster, node, pid, t.f("seal_timeout"))
        t.log.append("seal-wait r%d@%s fds=%d sockets=%d"
                     % (rank, node, len(payload.get("fds", [])),
                        len(payload.get("sockets", []))))
        # The socket reading first, whatever the exit code: rc 4 IS "stopped with sockets", and
        # a socket at the stop is the protocol verdict, never a timeout.
        if payload.get("sockets"):
            return "socket", ("rank %d still holds %d socket fd(s) after its seal: %s"
                              % (rank, len(payload["sockets"]), payload["sockets"][:4]))
        if rc != 0:
            return "drain", ("rank %d sealed but never reached state T on %s (state %r)"
                             % (rank, node, payload.get("state")))
    return "ok", ""


def cross_check_counters(t, ranks, sealed):
    """sealed[a].sent.b == sealed[b].received.a, for every pair inside one batch.

    Byte-exactness evidence taken BEFORE anything is dumped: at the seal every link has been
    half-closed and read to EOF, so the two ends' cumulative counters are a statement about the
    same bytes. If they disagree, a byte was lost or duplicated in the cut and no amount of
    later checksum agreement would tell you where.

    (The library makes the same comparison at the next connect, from the hello's counters. This
    is the driver's independent reading of it, and it is available even for a link that is never
    re-established.)
    """
    problems = []
    for a, b in itertools.combinations(sorted(ranks), 2):
        for sender, receiver in ((a, b), (b, a)):
            sent = sealed.get(sender, {}).get("sent.%d" % receiver)
            received = sealed.get(receiver, {}).get("received.%d" % sender)
            if sent is None or received is None:
                problems.append("rank %d/%d: sealed events carry no sent.%d/received.%d pair"
                                % (sender, receiver, receiver, sender))
            elif sent != received:
                problems.append("counters disagree: rank %d sent.%d=%s but rank %d "
                                "received.%d=%s" % (sender, receiver, sent, receiver, sender,
                                                    received))
    return problems


def dump_all(t, ranks, imgroot):
    """`criu dump` every rank, then the three socket readings. Returns (verdict, note).

    Precedence is `drain_driver.migrate`'s: the dump log's socket lines are read BEFORE the
    return code, because a dump that failed *because* of a socket is a protocol verdict and a
    dump that failed for its own reasons is an environment one, and the log is what tells them
    apart. The image files are the same claim stated in a way that does not depend on how a criu
    version words its log.
    """
    for rank in ranks:
        node, pid = t.placement[rank]
        imgdir = os.path.join(imgroot, "r%d" % rank)
        os.makedirs(imgdir, exist_ok=True)
        started = time.monotonic()
        out = cl.criu_dump(t.cluster, node, pid, imgdir, timeout=t.f("dump_timeout"))
        # The image directory is on the shared filesystem, so the log is readable right here --
        # no copying, and the dump is strictly before any restore, which is what gives NFS
        # close-to-open consistency for it.
        #
        # But READABLE has to be checked, not assumed. `socket_lines` answers ([], 0) for a log
        # it cannot open, which is byte-for-byte the answer for a clean one, so an unreadable log
        # would satisfy the runbook's acceptance criterion 2 without ever being looked at. criu
        # writes it 0600 as root, and it landed readable on three nodes and root-only on the
        # fourth (see cluster.criu_dump), so the hole was open for one machine of four and
        # invisible in the summary line. A log that cannot be read is a SETUP error -- the
        # evidence is missing, which is neither a pass nor a protocol verdict.
        logpath = os.path.join(imgdir, "dump.log")
        if not os.path.exists(logpath):
            raise cl.SetupError("rank %d: criu wrote no dump.log at %s on %s -- the socket "
                                "evidence cannot be read, so nothing here can be scored"
                                % (rank, logpath, node))
        if not os.access(logpath, os.R_OK):
            raise cl.SetupError(
                "rank %d: %s is not readable by this driver (criu runs under sudo and writes it "
                "0600 as root; it is only squashed to the invoking user where the shared tree is "
                "NFS). An unreadable log is indistinguishable from a clean one, so this would "
                "have passed the socket check without performing it." % (rank, logpath))
        hits, n_hits = socket_lines(logpath)
        t.log.append("dump r%d@%s rc=%d socket-lines=%d %.1fs"
                     % (rank, node, out.returncode, n_hits, time.monotonic() - started))
        if n_hits:
            return "socket", ("rank %d: dump.log mentions a socket %dx: %s"
                              % (rank, n_hits, " / ".join(hits[:3])))
        if out.returncode != 0:
            return "criu", ("rank %d: dump rc=%d on %s: %s"
                            % (rank, out.returncode, node, out.stderr.strip()[:300]))
        in_image = [name for name in SOCKET_IMAGES
                    if os.path.exists(os.path.join(imgdir, name))]
        if in_image:
            return "socket", ("rank %d: the image contains %s — a socket was checkpointed"
                              % (rank, ", ".join(in_image)))
    return "ok", ""


def reap_all(t, ranks):
    """criu kills what it dumped; the pid must be free before anything reclaims it."""
    for rank in ranks:
        node, pid = t.placement[rank]
        if not cl.wait_pid_gone(t.cluster, node, pid, timeout_s=t.f("wait_gone_timeout")):
            return "criu", "dumped pid %d of rank %d is still present on %s" % (pid, rank, node)
    return "ok", ""


def restore_all(t, moves, imgroot):
    """Restore every rank on its destination and SIGCONT it. Updates the placement."""
    for move in moves:
        rank, src, dest = move["rank"], move["src"], move["dest"]
        _, pid = t.placement[rank]
        imgdir = os.path.join(imgroot, "r%d" % rank)
        started = time.monotonic()
        out = cl.criu_restore(t.cluster, dest, imgdir, cwd=HERE,
                              timeout=t.f("criu_restore_timeout"))
        t.log.append("restore r%d %s->%s rc=%d %.1fs"
                     % (rank, src, dest, out.returncode, time.monotonic() - started))
        if out.returncode != 0:
            return "criu", ("rank %d: restore rc=%d on %s: %s"
                            % (rank, out.returncode, dest, out.stderr.strip()[:300]))
        # A setup violation, never a protocol verdict: the bands are the cluster's promise that
        # a verbatim-restored pid cannot collide on the destination.
        cl.assert_restored_pid(t.bands, t.home.get(rank, src), dest, rank, pid, t.span)
        t.placement[rank] = (dest, pid)
        # The image was taken of a process in group-stop -- the migrator sequence ends in
        # raise(SIGSTOP) -- and criu faithfully restores that state. Nothing in the library can
        # undo it from the inside: the thread that would call SIGCONT is the stopped one.
        ok, payload = cl.cont_remote(t.cluster, dest, pid, timeout=t.f("ssh_timeout"))
        if not ok:
            return "criu", "rank %d: restored process %d is not there on %s (%s)" \
                           % (rank, pid, dest, payload)
        t.log.append("SIGCONT r%d@%s %s" % (rank, dest,
                                            "(was stopped, as expected)" if payload.get("sent")
                                            else "(already running: %s)" % payload.get("state")))
    return "ok", ""


def await_restored(t, ranks, epochs):
    """Every rank's `restored` at epoch+1 -- the event that says the restore leg completed."""
    for rank in ranks:
        fields = wait_for_event(t.params, t.comm, "restored", rank, epochs[rank] + 1,
                                t.f("restore_timeout"))
        if fields is None:
            return "drain", ("rank %d never emitted restored at epoch %d within %ss"
                             % (rank, epochs[rank] + 1, t.f("restore_timeout")))
        t.log.append("restored r%d epoch=%s incarnation=%s"
                     % (rank, fields.get("epoch"), fields.get("incarnation")))
    return "ok", ""


def record_migrations(t, moves, epochs, batch_of):
    for move in moves:
        rank = move["rank"]
        t.migrations.append({"rank": rank, "epoch": epochs[rank], "src": move["src"],
                             "dest": move["dest"], "before": move.get("before", -1),
                             "batch": batch_of(rank, epochs[rank])})
        t.epochs[rank] = epochs[rank] + 1


def migrate_one(t, move, imgroot):
    """One rank, by signal: the sequence drain_driver.migrate performs, over ssh.

    sigqueue -> sealed -> seal-wait (state T and zero sockets) -> dump -> socket evidence ->
    pid gone -> restore on the destination -> SIGCONT -> restored -> **batch lease free**.

    The last step is the multi-host addition and it is not decoration: the rank released the
    lease it took for itself one Redis round trip after emitting `restored`, so a driver that
    starts the next migration on the event alone can have its own SET NX overwritten by that
    release.
    """
    rank, src, dest = move["rank"], move["src"], move["dest"]
    node, pid = t.placement[rank]
    if dest == src:
        return "void", ("rank %d would come back on %s, the machine it is already on — this "
                        "trial would prove nothing about a cross-host move" % (rank, src))
    if node != src:
        raise cl.SetupError("rank %d is on %s, but the plan says %s" % (rank, node, src))
    epoch = t.epochs.get(rank, 0)
    move["before"] = last_round(t.outdir, rank)
    t.log.append("migrate r%d %s->%s pid %d at round %d epoch %d"
                 % (rank, src, dest, pid, move["before"], epoch))

    ok, payload = cl.sigqueue_remote(t.cluster, src, pid, t.drain_signal, epoch,
                                     timeout=t.f("ssh_timeout"))
    if not ok:
        return "drain", "sigqueue to rank %d on %s failed: %s" % (rank, src, payload)
    t.log.append("sigqueue SIGRTMIN+%d(%d) epoch=%d -> r%d@%s"
                 % (t.signal_offset, t.drain_signal, epoch, rank, src))

    epochs = {rank: epoch}
    verdict, note, _ = await_seal(t, [rank], epochs)
    if verdict != "ok":
        return verdict, note
    for stage in (lambda: check_seal_state(t, [rank]),
                  lambda: dump_all(t, [rank], imgroot),
                  lambda: reap_all(t, [rank]),
                  lambda: restore_all(t, [move], imgroot),
                  lambda: await_restored(t, [rank], epochs)):
        verdict, note = stage()
        if verdict != "ok":
            return verdict, note
    record_migrations(t, [move], epochs, lambda r, e: "%s|%d@%d" % (t.comm, r, e))
    if not cl.batch_lease_wait_free(t.params, t.comm, t.f("lease_free_timeout")):
        return "drain", ("the batch lease of %s was still held %ss after rank %d restored "
                         "(owner %r)" % (t.comm, t.f("lease_free_timeout"), rank,
                                         cl.batch_lease_owner(t.params, t.comm)))
    return "ok", ""


def evacuate(t, step, imgroot):
    """Several ranks in ONE cut, driven by `migrate` events under a driver-held batch lease.

    The lease is what makes concurrency legal: a rank that reads a migrate event carrying a
    non-empty batch adopts that batch id and skips self-leasing
    (`DrainTCP::quiesce_and_drain`), so the one-batch-per-communicator rule is enforced by the
    driver holding the key for the whole cut -- and any other migration that tries to start
    meanwhile finds it held and refuses.

    Ordering, 272caee's, plus the two additions: ALL sealed -> ALL state-T/fd checks -> the
    pairwise counter cross-check -> ALL dumps -> ALL pids gone -> ALL restores + SIGCONT -> ALL
    restored -> release. The machine is emptied before anything comes back.
    """
    moves = step["moves"]
    ranks = [m["rank"] for m in moves]
    for move in moves:
        if move["dest"] == move["src"]:
            return "void", ("rank %d would come back on %s, the machine it is already on"
                            % (move["rank"], move["src"]))
    batch = "%s|cut%d" % (t.comm, step["index"])
    owner = "driver:%d:%s" % (os.getpid(), batch)
    if not cl.batch_lease_take(t.params, t.comm, owner, t.f("batch_lease_ms")):
        return "drain", ("could not take the batch lease of %s for the cut; it is held by %r"
                         % (t.comm, cl.batch_lease_owner(t.params, t.comm)))
    t.log.append("batch %s lease taken by %s" % (batch, owner))
    try:
        epochs = {}
        for move in moves:
            rank = move["rank"]
            epochs[rank] = t.epochs.get(rank, 0)
            move["before"] = last_round(t.outdir, rank)
            entry = cl.emit_migrate(t.params, t.comm, rank, epochs[rank], batch,
                                    ttl_s=int(t.params.get("registry_ttl_s", 3600)))
            if entry is None:
                return "drain", "could not emit the migrate event for rank %d" % rank
            t.log.append("migrate r%d %s->%s epoch=%d batch=%s id=%s"
                         % (rank, move["src"], move["dest"], epochs[rank], batch, entry))

        if t.scen.get("negative") == "second_batch":
            # The negative case, asserted where it belongs: while a cut is in flight, a SECOND
            # batch must not be able to start. The guard is the lease, and it is checked from
            # the same side a second driver would come from.
            #
            # Deliberately not by emitting a second migrate event at a rank outside the batch:
            # that rank would self-lease, find the key held, and `quiesce_and_drain` would
            # break every one of its links by design (a drain that cannot start is loud and
            # terminal for that rank) -- which kills the job rather than proving it survives.
            intruder = "driver:%d:intruder" % os.getpid()
            if cl.batch_lease_take(t.params, t.comm, intruder, t.f("batch_lease_ms")):
                cl.batch_lease_release(t.params, t.comm, intruder)
                return "drain", ("the batch lease admitted a SECOND holder while a cut was in "
                                 "flight — one batch per communicator is not enforced")
            t.log.append("negative: a second batch was refused the lease, as required")

        verdict, note, sealed = await_seal(t, ranks, epochs)
        if verdict != "ok":
            return verdict, note
        verdict, note = check_seal_state(t, ranks)
        if verdict != "ok":
            return verdict, note
        problems = cross_check_counters(t, ranks, sealed)
        if problems:
            return "drain", "; ".join(problems)
        t.log.append("counters agree pairwise across the batch (%d ranks)" % len(ranks))
        for stage in (lambda: dump_all(t, ranks, imgroot),
                      lambda: reap_all(t, ranks),
                      lambda: restore_all(t, moves, imgroot),
                      lambda: await_restored(t, ranks, epochs)):
            verdict, note = stage()
            if verdict != "ok":
                return verdict, note
        record_migrations(t, moves, epochs, lambda r, e: batch)
        t.cuts.append({"batch": batch, "ranks": list(ranks),
                       "epochs": dict((r, epochs[r]) for r in ranks)})
        return "ok", ""
    finally:
        # Released whatever happened: a lease left behind is 120 s in which no migration of this
        # communicator can start, and the next scenario would read as a protocol failure.
        if cl.batch_lease_release(t.params, t.comm, owner):
            t.log.append("batch %s lease released" % batch)
        else:
            t.log.append("batch %s lease was already gone at release" % batch)


# ------------------------------------------------------------------------- the event trail

def check_event_trail(events, migrations, cuts):
    """`leaving(e) -> sealed(e) -> restored(e+1)` per migration, and for a cut: every member
    sealed before any member restored.

    Read from the stream itself rather than from what the driver believes it saw: the events are
    what a survivor's trigger thread acted on, and the order they are in is the order it acted.
    """
    problems = []
    position = {}
    for index, (_, fields) in enumerate(events):
        key = (fields.get("type"), fields.get("rank"), fields.get("epoch"))
        position.setdefault(key, index)

    for m in migrations:
        rank, epoch = str(m["rank"]), m["epoch"]
        found = {}
        for kind, want_epoch in (("leaving", epoch), ("sealed", epoch), ("restored", epoch + 1)):
            index = position.get((kind, rank, str(want_epoch)))
            if index is None:
                problems.append("rank %s has no %s event at epoch %d" % (rank, kind, want_epoch))
            else:
                found[kind] = index
                batch = events[index][1].get("batch")
                if m["batch"] and batch != m["batch"]:
                    problems.append("rank %s's %s event carries batch %r, expected %r"
                                    % (rank, kind, batch, m["batch"]))
        if len(found) == 3 and not found["leaving"] < found["sealed"] < found["restored"]:
            problems.append("rank %s's events are out of order: leaving@%d sealed@%d restored@%d"
                            % (rank, found["leaving"], found["sealed"], found["restored"]))

    for cut in cuts:
        sealed_at, restored_at = [], []
        for rank in cut["ranks"]:
            epoch = cut["epochs"][rank]
            s = position.get(("sealed", str(rank), str(epoch)))
            r = position.get(("restored", str(rank), str(epoch + 1)))
            if s is not None:
                sealed_at.append((rank, s))
            if r is not None:
                restored_at.append((rank, r))
        if sealed_at and restored_at and max(s for _, s in sealed_at) > min(r for _, r in restored_at):
            problems.append("batch %s: a rank restored before every member of the batch had "
                            "sealed (sealed %s, restored %s)"
                            % (cut["batch"], sealed_at, restored_at))
    return problems


# ----------------------------------------------------------------------------- the campaign

def cross_host_baseline(args, cluster, params, scen, defaults, shape, npeers, place, root,
                        bands, span, cache):
    """A clean cross-host run's checksums and wall clock. Nothing is migrated.

    Per (shape, peers): the oracle every migrated trial of that shape is scored against. It is
    taken ON THE CLUSTER, with the same placement, because "the same checksums as a single-host
    run" and "the same checksums as a four-machine run" are different claims and only the second
    one separates a migration bug from a cluster that is wired wrong.
    """
    # The key is everything a checksum depends on. A shape's result is a function of (shape,
    # rank, num_peers, params) by the shape contract, and `rounds` and `payload_ints` are two of
    # those params -- caching on (shape, peers) alone would score a 4096-int scenario against a
    # 1-int baseline and call the disagreement a migration bug.
    key = (shape, npeers, field(scen, defaults, "rounds", shape),
           field(scen, defaults, "payload_ints", shape))
    if key in cache:
        return cache[key]
    # Every component of the cache key is in the name. The name was (shape, peers) alone while
    # the key was (shape, peers, rounds, payload_ints): two baselines of one shape at different
    # sizes then missed the cache, agreed on a directory, and the second read the FIRST's `DONE`
    # lines out of the stale logs still sitting there -- scoring every later trial against the
    # wrong oracle. Now a distinct key is a distinct directory by construction.
    comm = "mhd-base-%s-%d-%dr-%di-%d" % (shape, npeers, key[2], key[3], os.getpid())
    outdir = os.path.join(root, comm)
    os.makedirs(outdir, exist_ok=True)
    clean_comm(params, comm)
    place_map = placement_for(place, cluster.nodes, npeers)
    started = time.monotonic()
    placement = cl.launch_ranks(cluster, comm, place_map, SUBJECT, args.config, outdir,
                                key[2], field(scen, defaults, "ms", shape),
                                print_every=field(scen, defaults, "print_every", shape),
                                payload_ints=key[3],
                                shape=shape, cwd=HERE, bands=bands, span=span)
    finished = wait_for_finish(outdir, npeers, timeout_s=args.baseline_timeout)
    elapsed = time.monotonic() - started
    sums, failures = collect(outdir, npeers)
    cl.kill_all(cluster, SUBJECT, comm)
    clean_comm(params, comm)
    if not finished or failures or len(sums) != npeers:
        print("BASELINE FAILED for shape %s at %d peers: finished=%s %s %s (placement %s)"
              % (shape, npeers, finished, failures, sums, placement), file=sys.stderr)
        print("    evidence kept in %s" % outdir, file=sys.stderr)
        raise cl.SetupError("the four-machine clean run of shape %s at %d peers is not clean; "
                            "nothing migrated can be scored against it" % (shape, npeers))
    if not args.keep:
        shutil.rmtree(outdir, ignore_errors=True)
    # A migrated run legitimately costs the seal, the dump, the restore and a round of
    # re-establishment on top of a clean one -- per migration.
    cache[key] = (sums, elapsed)
    print("baseline shape=%s %d peers: %s (%.1fs)" % (shape, npeers, sums, elapsed))
    return cache[key]


def run_trial(args, cluster, params, scen, defaults, shape, npeers, trial_index, root, bands,
              span, drain_signal, signal_offset, expected, baseline_elapsed, rng):
    """One trial: launch, warm up, execute the plan, score. Returns (outcome, note).

    outcome is "pass", "skip", "fail" or a hard verdict from {socket, criu, drain}.
    """
    comm = "mhd-%s-%s-t%d-p%d-%d" % (scen["name"], shape, trial_index, npeers, os.getpid())
    outdir = os.path.join(root, comm)
    os.makedirs(outdir, exist_ok=True)
    clean_comm(params, comm)
    t = Trial(args, scen, defaults, shape, npeers, comm, outdir, cluster.nodes, cluster, params,
              bands, span, drain_signal, signal_offset)
    place_map = placement_for(t.f("place"), cluster.nodes, npeers)
    steps = plan_steps(scen, defaults, cluster.nodes, npeers, place_map)

    started = time.monotonic()
    try:
        t.placement = cl.launch_ranks(cluster, comm, place_map, SUBJECT, args.config, outdir,
                                      t.f("rounds"), t.f("ms"), print_every=t.f("print_every"),
                                      payload_ints=t.f("payload_ints"), shape=shape, cwd=HERE,
                                      bands=bands, span=span)
    except cl.SetupError:
        # A launch that was refused half way through still left ranks running.
        cl.kill_all(cluster, SUBJECT, comm)
        clean_comm(params, comm)
        raise
    t.home = {rank: node for rank, (node, _) in t.placement.items()}
    t.log.append("peers=%d shape=%s placement %s"
                 % (npeers, shape, " ".join("r%d@%s" % (r, n)
                                            for r, (n, _) in sorted(t.placement.items()))))

    verdict, note = "ok", ""
    for step in steps:
        if not wait_until_warm(outdir, npeers, t.f("warmup_round"), t.f("warmup_timeout")):
            verdict, note = "void", ("not every rank got past round %d before the migration "
                                     "was due" % t.f("warmup_round"))
            break
        time.sleep(rng.uniform(*t.f("delay_range")))
        imgroot = os.path.join(outdir, "img%d" % step["index"])
        try:
            if step["kind"] == "cut":
                verdict, note = evacuate(t, step, imgroot)
            else:
                verdict, note = migrate_one(t, step["moves"][0], imgroot)
        except cl.SetupError:
            cl.kill_all(cluster, SUBJECT, comm)
            save_events(params, comm, outdir)
            # AFTER save_events, and it has to happen: the four keys of an aborted trial outlive
            # the driver otherwise. A stale event stream is the dangerous one -- a later driver
            # reads a communicator's stream from "-", so a leftover `sealed` under a reused name
            # would satisfy a wait for a rank that has not been asked to migrate yet. The
            # evidence is already on disk in outdir/events.log.
            clean_comm(params, comm)
            raise
        if verdict != "ok":
            break
        if len(steps) > 1:
            # Between two cuts as well: the second cut's SET NX must not race the first cut's
            # release, and a rank that leased for itself releases after its `restored`.
            if not cl.batch_lease_wait_free(params, comm, t.f("lease_free_timeout")):
                verdict, note = "drain", ("the batch lease was still held %ss after step %d"
                                          % (t.f("lease_free_timeout"), step["index"]))
                break

    if verdict in HARD_VERDICTS:
        cl.kill_all(cluster, SUBJECT, comm)
        save_events(params, comm, outdir)
        clean_comm(params, comm)
        print("  trial %d: %s %s :: %s" % (trial_index, VERDICT_LABEL[verdict],
                                           " | ".join(t.log), note))
        print("    evidence kept in %s" % outdir)
        return verdict, note

    finish_timeout = max(300.0, 3.0 * baseline_elapsed + 120.0 * max(1, len(t.migrations)))
    finished = wait_for_finish(outdir, npeers, timeout_s=finish_timeout)
    sums, failures = collect(outdir, npeers)
    for m in t.migrations:
        after = last_round(outdir, m["rank"])
        if after <= m["before"]:
            failures.append("rank %d logged no round past %d after its restore"
                            % (m["rank"], m["before"]))
        if m["dest"] == m["src"]:
            failures.append("rank %d did not change machine" % m["rank"])
    events = read_events(params, comm)
    failures += check_event_trail(events, t.migrations, t.cuts)
    elapsed = time.monotonic() - started
    cl.kill_all(cluster, SUBJECT, comm)

    if verdict == "void" and not t.migrations:
        # No migration landed at all. If the job nevertheless finished clean the trial proves
        # nothing and is a SKIP; if the job is broken too, that is a failure in its own right.
        clean = finished and not failures and all(sums.get(r) == want
                                                  for r, want in expected.items())
        if clean:
            clean_comm(params, comm)
            print("  trial %d: SKIP %s :: %s — the run proves nothing; give it more rounds or a "
                  "lower delay_range" % (trial_index, " | ".join(t.log), note))
            if not args.keep:
                shutil.rmtree(outdir, ignore_errors=True)
            return "skip", note
        failures.append("no migration landed (%s) AND the job did not finish clean" % note)
    elif verdict == "void":
        t.log.append("note: %s" % note)

    if not finished:
        failures.append("timed out before every rank reached DONE")
    for r, want in expected.items():
        if r not in sums:
            failures.append("rank %d never reached DONE" % r)
        elif sums[r] != want:
            failures.append("rank %d checksum %s != four-machine baseline %s" % (r, sums[r], want))

    if failures:
        save_events(params, comm, outdir)
        clean_comm(params, comm)
        print("  trial %d: FAIL %s :: %s" % (trial_index, " | ".join(t.log), "; ".join(failures)))
        print("    evidence kept in %s" % outdir)
        return "fail", "; ".join(failures)
    if args.keep:
        save_events(params, comm, outdir)
    clean_comm(params, comm)
    moved = ", ".join("r%d %s->%s@e%d" % (m["rank"], m["src"], m["dest"], m["epoch"])
                      for m in t.migrations)
    print("  trial %d: pass %s :: %s :: %.1fs wall" % (trial_index, " | ".join(t.log), moved,
                                                       elapsed))
    if not args.keep:
        shutil.rmtree(outdir, ignore_errors=True)
    return "pass", ""


def describe(scen, defaults, nodes, comm="<comm>"):
    """The plan for one scenario, printed without touching ssh or Redis (--dry-run)."""
    lines = []
    name = scen["name"]
    kind = scen["kind"]
    npeers = field(scen, defaults, "peers")
    place = field(scen, defaults, "place")
    lines.append("scenario %s (phase %s, kind %s)%s" % (name, scen.get("phase", "B"), kind,
                                                        "  [optional]" if scen.get("optional")
                                                        else ""))
    if scen.get("why"):
        lines.append("  why: %s" % scen["why"])
    for shape in shapes_of(scen, defaults):
        lines.append("  shape=%s peers=%d rounds=%s ms=%s payload_ints=%s trials=%s place=%s"
                     % (shape, npeers, field(scen, defaults, "rounds", shape),
                        field(scen, defaults, "ms", shape),
                        field(scen, defaults, "payload_ints", shape),
                        field(scen, defaults, "trials", shape),
                        place if not isinstance(place, dict) else "explicit"))
    place_map = placement_for(place, nodes, npeers)
    lines.append("  placement: " + " ".join("r%d@%s" % (r, place_map[r])
                                            for r in sorted(place_map)))
    if kind == "clean":
        lines.append("  no migration: a four-machine clean run per shape, whose checksums are "
                     "the oracle for every migrated trial and are compared by hand against the "
                     "single-host table in README.md")
        return lines
    steps = plan_steps(scen, defaults, nodes, npeers, place_map)
    epochs = {}
    for step in steps:
        batch = ("%s|cut%d" % (comm, step["index"]) if step["kind"] == "cut" else None)
        for move in step["moves"]:
            rank = move["rank"]
            epoch = epochs.get(rank, 0)
            epochs[rank] = epoch + 1
            trail = "leaving(%d) -> sealed(%d) -> restored(%d)" % (epoch, epoch, epoch + 1)
            request = ("migrate event batch=%s" % batch if step["kind"] == "cut"
                       else "sigqueue epoch=%d" % epoch)
            lines.append("  step %d [%s] rank %d: %s -> %s | %s | %s"
                         % (step["index"], step["kind"], rank, move["src"], move["dest"],
                            request, trail))
        if step["kind"] == "cut":
            ranks = [m["rank"] for m in step["moves"]]
            lines.append("      one cut: driver holds lease %r for %d rank(s) %s; all sealed -> "
                         "state-T/fd checks -> pairwise counters (sealed[a].sent.b == "
                         "sealed[b].received.a) -> all dumps -> all pids gone -> all restores + "
                         "SIGCONT -> all restored -> release"
                         % (batch, len(ranks), ranks))
            if scen.get("negative") == "second_batch":
                lines.append("      negative: a second batch lease take during the cut must be "
                             "REFUSED, and the job must finish clean anyway")
        else:
            lines.append("      then: wait for the batch lease to clear before the next step")
    # cl.CRIU, not the bare word "criu": the printed plan is reviewed as if it were the
    # invocation, so it has to be the invocation -- including the `sudo` the time-namespace
    # finding made necessary.
    lines.append("  criu (both legs, every rank): %s dump %s -t <pid> -D <img> -o dump.log"
                 % (cl.CRIU, cl.CRIU_FLAGS))
    lines.append("                               cd %s && %s restore %s -D <img> -o "
                 "restore.log -d" % (HERE, cl.CRIU, cl.CRIU_FLAGS))
    lines.append("  verified per trial: every rank DONE with the four-machine baseline "
                 "checksum; the migrated rank logged a round past the one it was frozen on; "
                 "zero socket fds at state T; no socket line in dump.log; no socket image file; "
                 "both criu legs rc 0 with no TCP flags; the event trail above; the rank "
                 "actually changed machine (else void); the restored pid keeps its home band "
                 "and is outside the destination's")
    return lines


def parse_bands(text):
    if not text:
        return dict(cl.DEFAULT_PID_BANDS)
    if text.strip().lower() == "none":
        # Only for a cluster whose nodes are addressed by name rather than by the IPs the bands
        # are keyed on, or one where the bands are known to be seeded and checked elsewhere.
        # Without them a restore that collides with a local pid fails as a criu error.
        return {}
    bands = {}
    for item in text.split(","):
        item = item.strip()
        if not item:
            continue
        node, _, base = item.partition("=")
        try:
            bands[node.strip()] = int(base)
        except ValueError:
            sys.exit("--pid-bands wants node=base pairs; %r is not one" % item)
    return bands


def main():
    global SUBJECT
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0],
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--campaign", default=CAMPAIGN, help="scenario file (default: campaign.json)")
    ap.add_argument("--only", action="append", default=[],
                    help="run only this scenario; repeat or comma-separate for several")
    ap.add_argument("--phase", choices=["B", "C"], default=None,
                    help="run only this phase's scenarios")
    ap.add_argument("--allow-batch", action="store_true",
                    help="acknowledge and run the phase C (batch) scenarios; the C-1..C-3 "
                         "library fixes they need landed at 63382da")
    ap.add_argument("--include-optional", action="store_true",
                    help="also run scenarios marked optional")
    ap.add_argument("--nodes", nargs="+", required=True,
                    help="node addresses in campaign order; a scenario's node indices index "
                         "THIS list")
    ap.add_argument("--config", default=CONFIG,
                    help="absolute path, existing at the same path on every node")
    ap.add_argument("--subject", default=SUBJECT, help="the FMI application to migrate")
    ap.add_argument("--tree", default=REPO,
                    help="the shared checkout whose HEAD every node must agree on")
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--keep", action="store_true", help="keep the logs and images of passes too")
    ap.add_argument("--dry-run", action="store_true",
                    help="print the plan for the selected scenarios and exit; no ssh, no Redis")
    ap.add_argument("--ssh-key", default=os.environ.get("FMI_SSH_KEY", ""))
    ap.add_argument("--ssh-user",
                    default=os.environ.get("FMI_SSH_USER", os.environ.get("USER", "luca")))
    ap.add_argument("--pid-bands", default="",
                    help="node=base,... (default: the plan's phase-A7 bands); 'none' disables "
                         "the band assertion")
    ap.add_argument("--pid-band-span", type=int, default=cl.PID_BAND_SPAN)
    ap.add_argument("--skip-preflight", action="store_true",
                    help="skip the cluster identity gate (only for a re-run inside one session)")
    ap.add_argument("--continue-on-fail", action="store_true",
                    help="keep going after a failing scenario; the plan's failure policy says "
                         "STOP and write the finding up first, so this is not the default")
    ap.add_argument("--trials", type=int, default=None, help="override every scenario's trials")
    ap.add_argument("--rounds", type=int, default=None, help="override every scenario's rounds")
    ap.add_argument("--baseline-timeout", type=float, default=1800.0)
    ap.add_argument("--signal-offset", type=int, default=None,
                    help="override the config's drain_signal_offset")
    ap.add_argument("--run-root", default=os.path.join(HERE, "runs"))
    args = ap.parse_args()

    SUBJECT = os.path.abspath(args.subject)
    # The imported scoring helpers are the published driver's, and one of them (known_shapes)
    # asks the subject what it registered. Point it at the same binary this driver launches.
    drain_driver.SUBJECT = SUBJECT
    args.config = os.path.abspath(args.config)

    defaults, scenarios = load_campaign(args.campaign)
    # Into OVERRIDES, NOT into `defaults`: `field` reads the scenario before the defaults, and
    # every scenario in campaign.json names its own `trials`, so an override folded into the
    # defaults was silently inert -- `--trials 1` ran the scenario's three anyway. Sizing knobs
    # given on the command line have to outrank the file, or they are decoration.
    if args.trials is not None:
        OVERRIDES["trials"] = args.trials
    if args.rounds is not None:
        OVERRIDES["rounds"] = args.rounds

    wanted = [name.strip() for item in args.only for name in item.split(",") if name.strip()]
    selected = []
    for scen in scenarios:
        if wanted and scen["name"] not in wanted:
            continue
        if args.phase and scen.get("phase", "B") != args.phase:
            continue
        if scen.get("optional") and not (args.include_optional or scen["name"] in wanted):
            continue
        selected.append(scen)
    if wanted:
        missing = [name for name in wanted if name not in {s["name"] for s in selected}]
        if missing:
            sys.exit("no such scenario: %s" % ", ".join(missing))
    if not selected:
        sys.exit("nothing selected")

    batch_scenarios = [s["name"] for s in selected if s.get("phase", "B") == "C"]
    # Checked here, but only ENFORCED below, after --dry-run has had its say: printing a phase C
    # plan is how it gets reviewed before the fixes exist, and refusing to print it would make
    # the refusal itself unreviewable.
    def refuse_batch():
        sys.exit(
            "The phase C scenarios %s drive the single-cut BATCH path, which is opt-in:\n"
            "pass --allow-batch.\n"
            "\n"
            "The three library defects this flag used to guard against LANDED at 63382da,\n"
            "each with its own mutation-verified test:\n"
            "  C-1  resume_after_restore left `draining` set on links to peers that are\n"
            "       themselves mid-migration, so the first member to restore burned plain\n"
            "       max_timeout dialling a frozen co-member  — fixed: LinkState::peer_migrating,\n"
            "       and the restore loop closes-and-reopens the window for such links;\n"
            "  C-2  poll_control_events advanced last_stream_id BEFORE dispatch, so a throw in\n"
            "       a multi-event tick (normal under batches) silently dropped a co-batched\n"
            "       migrate — fixed: the cursor advances only past a dispatched event;\n"
            "  C-3  a drain that could not start leaked holds_batch_lock/batch_id, refusing\n"
            "       every migration for the lease's 120 s — fixed: release_batch_state().\n"
            "\n"
            "So the flag no longer overrides a known-red path; it acknowledges that these\n"
            "scenarios exercise the batch path, whose evidence is younger than the sequential\n"
            "phase's, and that their verdicts should be read as such."
            % ", ".join(batch_scenarios))

    nodes = list(args.nodes)
    if len(set(nodes)) != len(nodes):
        sys.exit("--nodes contains a duplicate: %s" % nodes)
    bands = parse_bands(args.pid_bands)

    plane = data_plane(args.config)
    params = plane[1]
    signal_offset = args.signal_offset if args.signal_offset is not None \
        else int(params.get("drain_signal_offset", 3))
    drain_signal = drain_signal_number(signal_offset)
    print("data plane: %s (from %s); drain signal SIGRTMIN+%d = %d"
          % (plane[0], args.config, signal_offset, drain_signal))
    registry = str(params.get("registry_host", ""))
    if registry in ("127.0.0.1", "localhost", "::1", ""):
        sys.exit("registry_host is %r: a restored rank re-resolves its advertise address "
                 "against the registry, so a loopback registry publishes the loopback of "
                 "whatever machine each rank woke up on. Use a cluster-reachable address."
                 % registry)

    if args.dry_run:
        print("DRY RUN — nothing is launched, no ssh, no Redis\n")
        for scen in selected:
            try:
                for line in describe(scen, defaults, nodes):
                    print(line)
            except cl.SetupError as exc:
                # A scenario that cannot be planned against THIS --nodes list (a node index the
                # cluster does not have, an explicit map that misses a rank) is a setup error
                # here too, and worth all of one line rather than a traceback.
                print("scenario %s cannot be planned for these nodes: %s" % (scen["name"], exc),
                      file=sys.stderr)
                return 2
            if scen.get("phase", "B") == "C" and not args.allow_batch:
                print("  NOTE: this is a phase C scenario; it drives the batch path and needs "
                      "--allow-batch (the C-1..C-3 library fixes landed at 63382da — the flag "
                      "acknowledges batch mode, it no longer overrides a known-red path)")
            print("")
        return 0

    if batch_scenarios and not args.allow_batch:
        refuse_batch()

    if not os.path.exists(SUBJECT):
        sys.exit("subject not built at %s" % SUBJECT)
    available = known_shapes()
    if available is None:
        sys.exit("cannot run the subject at %s — build it first, or pass --subject" % SUBJECT)
    for scen in selected:
        for shape in shapes_of(scen, defaults):
            if shape not in available:
                sys.exit("scenario %s wants shape %r; the subject registered: %s"
                         % (scen["name"], shape, ", ".join(available)))

    cluster = cl.Cluster(nodes, args.ssh_key, args.ssh_user)
    root = os.path.join(args.run_root, "mhd%d" % os.getpid())
    os.makedirs(root, exist_ok=True)
    print("logs and images under %s" % root)

    facts = {}
    if not args.skip_preflight:
        problems, facts = cl.preflight(cluster, params, SUBJECT, args.tree, args.config,
                                       bands=bands, span=args.pid_band_span)
        with open(os.path.join(root, "preflight.json"), "w") as f:
            json.dump({"problems": problems, "facts": facts}, f, indent=2, sort_keys=True)
        if problems:
            print("PREFLIGHT FAILED — this is a setup error, not a protocol verdict:",
                  file=sys.stderr)
            for problem in problems:
                print("  * %s" % problem, file=sys.stderr)
            return 2
        print("preflight ok: HEAD %s, subject %s"
              % (sorted(set(facts.get("head", {}).values())),
                 sorted(set(str(s)[:12] for s in facts.get("subject_sha256", {}).values()))))

    rng = random.Random(args.seed)
    baselines = {}
    totals = {"pass": 0, "fail": 0, "skip": 0}
    hard = 0
    stopped = None
    for scen in selected:
        print("\n=== %s (phase %s, %s) %s"
              % (scen["name"], scen.get("phase", "B"), scen["kind"], scen.get("why", "")))
        npeers = field(scen, defaults, "peers")
        for shape in shapes_of(scen, defaults):
            try:
                expected, elapsed = cross_host_baseline(
                    args, cluster, params, scen, defaults, shape, npeers,
                    field(scen, defaults, "place"), root, bands, args.pid_band_span, baselines)
            except cl.SetupError as exc:
                print("SETUP/BASELINE ERROR: %s" % exc, file=sys.stderr)
                return 2
            if scen["kind"] == "clean":
                totals["pass"] += 1
                continue
            for trial in range(int(field(scen, defaults, "trials", shape))):
                try:
                    outcome, note = run_trial(args, cluster, params, scen, defaults, shape,
                                              npeers, trial, root, bands, args.pid_band_span,
                                              drain_signal, signal_offset, expected, elapsed, rng)
                except cl.SetupError as exc:
                    print("SETUP ERROR (never a protocol verdict): %s" % exc, file=sys.stderr)
                    return 2
                if outcome in HARD_VERDICTS:
                    hard += 1
                    totals["fail"] += 1
                    stopped = "%s/%s trial %d: %s :: %s" % (scen["name"], shape, trial,
                                                            outcome, note)
                elif outcome == "fail":
                    totals["fail"] += 1
                    stopped = "%s/%s trial %d: %s" % (scen["name"], shape, trial, note)
                else:
                    totals[outcome] += 1
                if stopped and not args.continue_on_fail:
                    break
            if stopped and not args.continue_on_fail:
                break
        if stopped and not args.continue_on_fail:
            break

    with open(os.path.join(root, "baselines.json"), "w") as f:
        json.dump({"%s@%dp-%drounds-%dints" % (shape, peers, rounds, ints): sums
                   for (shape, peers, rounds, ints), (sums, _) in baselines.items()}, f,
                  indent=2, sort_keys=True)
    print("\n== %d passed, %d failed (%d protocol verdicts), %d skipped =="
          % (totals["pass"], totals["fail"], hard, totals["skip"]))
    if stopped:
        print("STOPPED after: %s" % stopped)
        print("The failure policy: keep the trial directory, write the finding up (file:line, "
              "seed, repro) BEFORE running anything else. Widening a timeout, retrying, pinning "
              "advertise_host or adding a TCP flag are not responses to it.")
    return 1 if totals["fail"] else 0


if __name__ == "__main__":
    sys.exit(main())
