-------------------------------- MODULE Membership --------------------------------
(***************************************************************************)
(* FMI migration protocol v2 --- NORMATIVE CONTRACT 3 (Axis D, membership) *)
(*                                                                         *)
(* Formalises the membership state machine of                              *)
(*   docs/superpowers/specs/2026-07-27-sequenced-incarnation-links-design.md *)
(* section "Normative contract 3 --- membership state machine (Axis D)",   *)
(* together with the "Restore commit discipline" sub-section and the       *)
(* process-level facts from "Normative contract 4" that the discipline     *)
(* depends on.                                                             *)
(*                                                                         *)
(* The point of the model is the *pair* of layers:                         *)
(*                                                                         *)
(*   1. the Redis directory state machine (one Lua script per edge, each   *)
(*      CAS-guarded on (run_id, rank, expected incarnation, expected       *)
(*      state)), and                                                       *)
(*   2. the OS-process layer underneath it: `criu restore --leave-stopped` *)
(*      creates a STOPPED process, only SIGCONT makes it RUNNING, SIGKILL  *)
(*      while stopped removes it.                                          *)
(*                                                                         *)
(* Twin safety --- "a twin can never pair, ack, or touch the control       *)
(* plane" --- is a property of layer 2 that is *enforced* by layer 1.      *)
(* Modelling only layer 1 would make it vacuous.                           *)
(*                                                                         *)
(***************************************************************************)
(*                                                                         *)
(* ======================= DELIBERATELY ABSTRACTED AWAY ================== *)
(*                                                                         *)
(* Everything below is NOT modelled. Read this before trusting any result. *)
(*                                                                         *)
(*  * `run_id` and `dirgen`. Every mutation in the real design is CAS'd on *)
(*    (run_id, rank, incarnation, state) and bumps `dirgen`. Here the CAS  *)
(*    is modelled by the enabling condition of each action reading the     *)
(*    current values of `rstate`, `inc` (and, for restore, the attempt's   *)
(*    recorded incarnation `rinc`) atomically. `run_id` is constant in a   *)
(*    single model run, so stale-run rejection is out of scope.            *)
(*                                                                         *)
(*  * ATOMICITY GRANULARITY. Each Lua script is one TLA+ action, i.e. it   *)
(*    is atomic. That is exactly what Redis guarantees, so this is a       *)
(*    faithful abstraction of the directory, but it means the model says   *)
(*    nothing about a *partially applied* script.                          *)
(*                                                                         *)
(*  * TIME. Lease TTL is modelled as a nondeterministic `LeaseExpiry*`     *)
(*    action, not as a clock. Consequently the model cannot answer "is the *)
(*    TTL long enough"; it only answers "is the protocol safe for every    *)
(*    possible expiry instant", which is the stronger and more useful      *)
(*    question.                                                            *)
(*                                                                         *)
(*  * MESSAGES, LINKS, EPOCHS, REPLAY. Contracts 1 and 2 (message identity *)
(*    and transport durability) are entirely absent. No frames, no         *)
(*    transport_seq, no retention, no handshake. The single point of       *)
(*    contact with contract 2 is the `SurvivorTimeout` monitor action      *)
(*    (see `ClearPauseAtCommit` below), which is an *assumed* consequence  *)
(*    of the design's own argument, not something derived here.            *)
(*                                                                         *)
(*  * THE IMAGE ITSELF. Staging, digest verification and the artifact      *)
(*    store collapse into one boolean `image[r]` meaning "durably staged   *)
(*    AND digest-verified". Image corruption is not modelled.              *)
(*                                                                         *)
(*  * PROCESS IDENTITY VERIFICATION (pidfd + start time + nonce). The      *)
(*    model assumes the agent always freezes the right process. Recycled   *)
(*    PIDs are out of scope.                                               *)
(*                                                                         *)
(*  * AGENT RECOVERY. `AgentCrash` is permanent: a crashed agent takes no  *)
(*    further action, ever. A *partitioned but alive* agent is modelled    *)
(*    separately and deliberately, via `FencedSigCont` (below), because    *)
(*    that is the case fencing exists for.                                 *)
(*                                                                         *)
(*  * ONE RESTORE SLOT PER AGENT PER RANK. Instance ids are drawn from     *)
(*    `Agents \cup {ORIG}`, so an agent may have at most one outstanding   *)
(*    restored process per rank. With |Agents| = 2 this still permits the  *)
(*    two-concurrent-restores scenario the arbitration exists for, but it  *)
(*    bounds how many stale frozen processes can pile up. This is a state- *)
(*    space device, not a protocol claim.                                  *)
(*                                                                         *)
(*  * GARBAGE COLLECTION of abandoned frozen originals. A frozen ORIG is   *)
(*    never reaped after a successful migration. Harmless for the safety   *)
(*    properties (a STOPPED process is not RUNNING); a real resource leak  *)
(*    the implementation must handle.                                      *)
(*                                                                         *)
(*  * MULTI-RANK COUPLING. The membership machine is per rank. The only    *)
(*    inter-rank coupling in contract 3 is the pause set, which drives the *)
(*    global `migrations_in_flight` suspension flag. Making every rank     *)
(*    migratable therefore multiplies the state space without adding a     *)
(*    single new membership interleaving, so the configs keep one rank     *)
(*    migratable and one pure survivor (the survivor exists so that        *)
(*    `SurvivorTimeout` has a subject).                                    *)
(*                                                                         *)
(***************************************************************************)

EXTENDS Naturals, FiniteSets

CONSTANTS
    Ranks,                 \* set of logical ranks
    Agents,                \* set of per-node restore agents (sidecars)
    Migratable,            \* subset of Ranks the orchestrator may pause

    MaxMigrations,         \* bound: total request_pause events
    MaxSuspectedAborts,    \* bound: abort_activation on a *live* agent
    MaxIncarnation,        \* bound: asserted by TypeOK, not by any guard
    MaxCrashes,            \* bound: total AgentCrash events

    \* ---- protocol variants, one module serves several configurations ----
    AbortEdgesEnabled,     \* TRUE  = spec (abort_pause/_restore/_activation exist)
                           \* FALSE = the pre-review design without failure edges
    ClearPauseAtCommit,    \* TRUE  = clear the pause entry at commit_restore
                           \* FALSE = spec (clear it at ACTIVATING -> ACTIVE)
    FencedSigCont,         \* TRUE  = spec (SIGCONT re-checks the directory)
                           \* FALSE = agent SIGCONTs on its own stale belief
    KeepFrozenOriginal,    \* TRUE  = cross-host: `--leave-stopped`, original frozen
                           \* FALSE = same-host: criu kills+reaps so the pid can be
                           \*         reclaimed (LocalRankAgent.cpp:417 discipline)
    OrchestratorAbortPauseFromPausing,
                           \* Added by the integration/audit pass.
                           \* TRUE  = the module as originally written: the
                           \*   orchestrator's `abort_pause` is accepted from
                           \*   PAUSING as well as from CHECKPOINTED.
                           \* FALSE = contract 3's transition table LITERALLY:
                           \*   the only PAUSING -> ACTIVE edge is `abort_pause`
                           \*   "driven by: agent, on dump failure", and the
                           \*   orchestrator's `abort_pause` appears only on the
                           \*   CHECKPOINTED row.  Use with |Agents| = 1 to ask
                           \*   whether the table alone satisfies its own
                           \*   normative rule ("every state must have at least
                           \*   one outbound edge that an EXTERNAL ACTOR can
                           \*   drive") when the local agent is the thing that
                           \*   failed.
    UnboundedRestoreRetries
                           \* TRUE  = spec as written: nothing bounds how often the
                           \*         restore phase may be retried, so the lease may
                           \*         expire before `criu restore` finishes and
                           \*         `abort_restore` may be re-issued for ever.
                           \* FALSE = assume the restore phase makes progress. Only
                           \*         used to isolate whether the *cooperative*
                           \*         protocol is live; see the TLC finding recorded
                           \*         in MembershipLiveness.cfg.

ASSUME Migratable \subseteq Ranks
ASSUME MaxMigrations \in Nat /\ MaxSuspectedAborts \in Nat
ASSUME MaxIncarnation \in Nat /\ MaxCrashes \in Nat
ASSUME AbortEdgesEnabled \in BOOLEAN /\ ClearPauseAtCommit \in BOOLEAN
ASSUME FencedSigCont \in BOOLEAN /\ KeepFrozenOriginal \in BOOLEAN
ASSUME UnboundedRestoreRetries \in BOOLEAN
ASSUME OrchestratorAbortPauseFromPausing \in BOOLEAN

ORIG    == "ORIG"     \* the process that called `join`; not owned by any agent
NoAgent == "NONE"

InstIds == Agents \cup {ORIG}

DirStates == { "UNJOINED",          \* no directory entry yet (pre-`join`)
               "STARTING",
               "ACTIVE",
               "PAUSING",
               "CHECKPOINTED",
               "RESTORE_RESERVED",
               "ACTIVATING" }

PStatus == { "ABSENT",    \* the process slot was never used / was reaped
             "RUNNING",   \* schedulable: can pair, ack, touch the control plane
             "STOPPED",   \* SIGSTOP'd or `criu restore --leave-stopped`
             "KILLED" }   \* SIGKILL'd; terminal, slot reusable by a later restore

VARIABLES
    rstate,    \* [Ranks -> DirStates]      the directory's state field
    inc,       \* [Ranks -> Nat]            the directory's incarnation field
    paused,    \* [Ranks -> BOOLEAN]        pause-set membership
    mid,       \* [Ranks -> Nat]            migration_id (0 = no migration)
    lease,     \* [Ranks -> Agents \cup {NoAgent}]  restore lease, SET-NX-EX
    procs,     \* [Ranks -> [InstIds -> PStatus]]   the OS-process layer
    cur,       \* [Ranks -> InstIds]        which instance currently *is* rank r
    rinc,      \* [Ranks -> [Agents -> Nat]] incarnation an attempt restored at
                                            \* (the `expected_inc` it will CAS on)
    won,       \* [Ranks -> SUBSET Agents]  agents whose commit_restore ever won
                                            \* (agent-local belief, not directory)
    winner,    \* [Ranks -> Agents \cup {NoAgent}] attempt_id recorded by the last
                                            \* winning commit_restore (directory)
    crashed,   \* SUBSET Agents             permanently dead agents
    image,     \* [Ranks -> BOOLEAN]        image durably staged AND digest-verified
    dumpfail,  \* [Ranks -> BOOLEAN]        criu dump exhausted its retries
    nmig,      \* Nat                       request_pause events so far
    nabort,    \* Nat                       abort_activation-on-a-live-agent so far
    poisoned   \* BOOLEAN                   a survivor threw Utils::Timeout mid-op

vars == << rstate, inc, paused, mid, lease, procs, cur, rinc, won, winner,
           crashed, image, dumpfail, nmig, nabort, poisoned >>

-----------------------------------------------------------------------------
(* Helpers *)

Alive(a) == a \notin crashed

RunningInsts(r) == { i \in InstIds : procs[r][i] = "RUNNING" }

(* The restoring agent may SIGCONT only if its own commit_restore won.        *)
(* Under FencedSigCont it re-reads the directory immediately before the       *)
(* signal and refuses if it is no longer the recorded winner of an ACTIVATING *)
(* rank.  Without the fence it acts on the belief it formed at commit time,   *)
(* which is exactly what a slow-but-alive agent has after the orchestrator    *)
(* aborted its activation.                                                    *)
CanSigCont(a, r) ==
    /\ a \in won[r]
    /\ (FencedSigCont => (rstate[r] = "ACTIVATING" /\ winner[r] = a))

(* An attempt can still win the commit CAS iff the rank is still reserved at  *)
(* the very incarnation the attempt restored from.                            *)
CanStillWin(a, r) ==
    /\ rstate[r] = "RESTORE_RESERVED"
    /\ rinc[r][a] = inc[r]

-----------------------------------------------------------------------------
TypeOK ==
    /\ rstate   \in [Ranks -> DirStates]
    /\ inc      \in [Ranks -> 0..MaxIncarnation]
    /\ paused   \in [Ranks -> BOOLEAN]
    /\ mid      \in [Ranks -> 0..MaxMigrations]
    /\ lease    \in [Ranks -> Agents \cup {NoAgent}]
    /\ procs    \in [Ranks -> [InstIds -> PStatus]]
    /\ cur      \in [Ranks -> InstIds]
    /\ rinc     \in [Ranks -> [Agents -> 0..MaxIncarnation]]
    /\ won      \in [Ranks -> SUBSET Agents]
    /\ winner   \in [Ranks -> Agents \cup {NoAgent}]
    /\ crashed  \in SUBSET Agents
    /\ image    \in [Ranks -> BOOLEAN]
    /\ dumpfail \in [Ranks -> BOOLEAN]
    /\ nmig     \in 0..MaxMigrations
    /\ nabort   \in 0..MaxSuspectedAborts
    /\ poisoned \in BOOLEAN

Init ==
    /\ rstate   = [r \in Ranks |-> "UNJOINED"]
    /\ inc      = [r \in Ranks |-> 0]
    /\ paused   = [r \in Ranks |-> FALSE]
    /\ mid      = [r \in Ranks |-> 0]
    /\ lease    = [r \in Ranks |-> NoAgent]
    /\ procs    = [r \in Ranks |-> [i \in InstIds |-> "ABSENT"]]
    /\ cur      = [r \in Ranks |-> ORIG]
    /\ rinc     = [r \in Ranks |-> [a \in Agents |-> 0]]
    /\ won      = [r \in Ranks |-> {}]
    /\ winner   = [r \in Ranks |-> NoAgent]
    /\ crashed  = {}
    /\ image    = [r \in Ranks |-> FALSE]
    /\ dumpfail = [r \in Ranks |-> FALSE]
    /\ nmig     = 0
    /\ nabort   = 0
    /\ poisoned = FALSE

-----------------------------------------------------------------------------
(* ---------------------------- directory scripts ------------------------- *)

(* `join` --- create-at-incarnation-0 only if absent.  Driven by the rank.    *)
JoinA(r) ==
    /\ rstate[r] = "UNJOINED"
    /\ rstate' = [rstate EXCEPT ![r] = "STARTING"]
    /\ procs'  = [procs  EXCEPT ![r][ORIG] = "RUNNING"]
    /\ cur'    = [cur    EXCEPT ![r] = ORIG]
    /\ UNCHANGED << inc, paused, mid, lease, rinc, won, winner, crashed,
                    image, dumpfail, nmig, nabort, poisoned >>

Join == \E r \in Ranks : JoinA(r)

(* `mark_active` STARTING -> ACTIVE.  Driven by the rank's first poll.        *)
MarkActiveFromStartingA(r) ==
    /\ rstate[r] = "STARTING"
    /\ procs[r][cur[r]] = "RUNNING"
    /\ rstate' = [rstate EXCEPT ![r] = "ACTIVE"]
    /\ UNCHANGED << inc, paused, mid, lease, procs, cur, rinc, won, winner,
                    crashed, image, dumpfail, nmig, nabort, poisoned >>

MarkActiveFromStarting == \E r \in Ranks : MarkActiveFromStartingA(r)

(* `request_pause(ranks..., migration_id)` ACTIVE -> PAUSING. Orchestrator.   *)
RequestPauseA(r) ==
    /\ r \in Migratable
    /\ rstate[r] = "ACTIVE"
    /\ nmig < MaxMigrations
    /\ rstate' = [rstate EXCEPT ![r] = "PAUSING"]
    /\ paused' = [paused EXCEPT ![r] = TRUE]
    /\ mid'    = [mid    EXCEPT ![r] = nmig + 1]
    /\ nmig'   = nmig + 1
    /\ UNCHANGED << inc, lease, procs, cur, rinc, won, winner, crashed,
                    image, dumpfail, nabort, poisoned >>

RequestPause == \E r \in Ranks : RequestPauseA(r)

(* `mark_checkpointed` PAUSING -> CHECKPOINTED.  Only after the image is      *)
(* durably staged *and* digest-verified.  Driven by the agent.                *)
MarkCheckpointedA(a, r) ==
    /\ Alive(a)
    /\ rstate[r] = "PAUSING"
    /\ image[r]
    /\ rstate' = [rstate EXCEPT ![r] = "CHECKPOINTED"]
    /\ UNCHANGED << inc, paused, mid, lease, procs, cur, rinc, won, winner,
                    crashed, image, dumpfail, nmig, nabort, poisoned >>

MarkCheckpointed == \E a \in Agents, r \in Ranks : MarkCheckpointedA(a, r)

(* `acquire_restore_lease(rank, attempt_id)` --- SET-NX-EX.                   *)
(* Per spec the CAS accepts CHECKPOINTED *or* RESTORE_RESERVED-with-expired-  *)
(* lease.  That second clause is why RESTORE_RESERVED is not a dead end even  *)
(* when abort_restore is removed.                                             *)
AcquireRestoreLeaseA(a, r) ==
    /\ Alive(a)
    /\ image[r]
    /\ lease[r] = NoAgent
    /\ rstate[r] \in {"CHECKPOINTED", "RESTORE_RESERVED"}
    /\ lease'  = [lease  EXCEPT ![r] = a]
    /\ rstate' = [rstate EXCEPT ![r] = "RESTORE_RESERVED"]
    /\ UNCHANGED << inc, paused, mid, procs, cur, rinc, won, winner, crashed,
                    image, dumpfail, nmig, nabort, poisoned >>

AcquireRestoreLease == \E a \in Agents, r \in Ranks : AcquireRestoreLeaseA(a, r)

(* `commit_restore(rank, expected_inc, attempt_id)` --- CAS incarnation+1.    *)
(* NOTE: per the spec the CAS is on (run_id, rank, expected incarnation,      *)
(* expected state).  It deliberately does NOT re-check the lease, so a        *)
(* lease-less straggler may still attempt it; the CAS is the arbiter.         *)
CommitRestoreA(a, r) ==
    /\ Alive(a)
    /\ rstate[r] = "RESTORE_RESERVED"
    /\ procs[r][a] = "STOPPED"        \* the attempt actually restored a process
    /\ rinc[r][a] = inc[r]            \* expected_inc still matches
    /\ rstate' = [rstate EXCEPT ![r] = "ACTIVATING"]
    /\ inc'    = [inc    EXCEPT ![r] = inc[r] + 1]
    /\ winner' = [winner EXCEPT ![r] = a]
    /\ won'    = [won    EXCEPT ![r] = won[r] \cup {a}]
    /\ lease'  = [lease  EXCEPT ![r] = NoAgent]
    /\ paused' = IF ClearPauseAtCommit THEN [paused EXCEPT ![r] = FALSE] ELSE paused
    /\ UNCHANGED << mid, procs, cur, rinc, crashed, image, dumpfail,
                    nmig, nabort, poisoned >>

CommitRestore == \E a \in Agents, r \in Ranks : CommitRestoreA(a, r)

(* `mark_active` ACTIVATING -> ACTIVE --- the restored process's first poll.  *)
(* It is the *process* that drives this, so it can only happen once the       *)
(* winner is RUNNING.  Per spec this edge, and only this edge, clears the     *)
(* pause entry; it is also where the image blob is deleted.                   *)
MarkActiveFromActivatingA(r) ==
    /\ rstate[r] = "ACTIVATING"
    /\ winner[r] # NoAgent
    /\ procs[r][winner[r]] = "RUNNING"
    /\ rstate'   = [rstate   EXCEPT ![r] = "ACTIVE"]
    /\ paused'   = [paused   EXCEPT ![r] = FALSE]
    /\ mid'      = [mid      EXCEPT ![r] = 0]
    /\ image'    = [image    EXCEPT ![r] = FALSE]
    /\ dumpfail' = [dumpfail EXCEPT ![r] = FALSE]
    /\ UNCHANGED << inc, lease, procs, cur, rinc, won, winner, crashed,
                    nmig, nabort, poisoned >>

MarkActiveFromActivating == \E r \in Ranks : MarkActiveFromActivatingA(r)

-----------------------------------------------------------------------------
(* ------------------------------ failure edges --------------------------- *)
(* The spec's normative rule: "every state must have at least one outbound    *)
(* edge that an external actor can drive."  All three are gated on            *)
(* AbortEdgesEnabled so that one module can also model the pre-review design. *)

(* `abort_pause` driven by the agent when criu dump exhausted its retries.    *)
AbortPauseOnDumpFailureA(a, r) ==
    /\ AbortEdgesEnabled
    /\ Alive(a)
    /\ rstate[r] = "PAUSING"
    /\ dumpfail[r]
    /\ rstate'   = [rstate   EXCEPT ![r] = "ACTIVE"]
    /\ paused'   = [paused   EXCEPT ![r] = FALSE]
    /\ mid'      = [mid      EXCEPT ![r] = 0]
    /\ lease'    = [lease    EXCEPT ![r] = NoAgent]
    /\ dumpfail' = [dumpfail EXCEPT ![r] = FALSE]
    /\ UNCHANGED << inc, procs, cur, rinc, won, winner, crashed, image,
                    nmig, nabort, poisoned >>

AbortPauseOnDumpFailure ==
    \E a \in Agents, r \in Ranks : AbortPauseOnDumpFailureA(a, r)

(* `abort_pause` driven by the orchestrator: migration cancelled.  Needs no   *)
(* agent -- that is what makes it satisfy the normative rule even when every  *)
(* agent on the node is gone.  Adversarial: no fairness.                      *)
AbortPauseCancelA(r) ==
    /\ AbortEdgesEnabled
    /\ \/ rstate[r] = "CHECKPOINTED"                         \* the table's row
       \/ (rstate[r] = "PAUSING" /\ OrchestratorAbortPauseFromPausing)
    /\ rstate'   = [rstate   EXCEPT ![r] = "ACTIVE"]
    /\ paused'   = [paused   EXCEPT ![r] = FALSE]
    /\ mid'      = [mid      EXCEPT ![r] = 0]
    /\ lease'    = [lease    EXCEPT ![r] = NoAgent]
    /\ dumpfail' = [dumpfail EXCEPT ![r] = FALSE]
    /\ image'    = [image    EXCEPT ![r] = FALSE]
    /\ UNCHANGED << inc, procs, cur, rinc, won, winner, crashed,
                    nmig, nabort, poisoned >>

AbortPauseCancel == \E r \in Ranks : AbortPauseCancelA(r)

(* `abort_restore` RESTORE_RESERVED -> CHECKPOINTED.                          *)
AbortRestoreA(r) ==
    /\ AbortEdgesEnabled
    /\ UnboundedRestoreRetries
    /\ rstate[r] = "RESTORE_RESERVED"
    /\ rstate' = [rstate EXCEPT ![r] = "CHECKPOINTED"]
    /\ lease'  = [lease  EXCEPT ![r] = NoAgent]
    /\ UNCHANGED << inc, paused, mid, procs, cur, rinc, won, winner, crashed,
                    image, dumpfail, nmig, nabort, poisoned >>

AbortRestore == \E r \in Ranks : AbortRestoreA(r)

(* `abort_activation` ACTIVATING -> CHECKPOINTED, "fenced; agent died before  *)
(* SIGCONT".  Modelled in two flavours that differ ONLY in fairness:          *)
(*                                                                           *)
(*   Dead      -- the orchestrator has genuine evidence (heartbeat/pidfile    *)
(*                gone).  Weakly fair: the orchestrator will act.             *)
(*   Suspected -- the agent is merely slow.  No fairness; bounded by          *)
(*                MaxSuspectedAborts purely to keep the state space finite.   *)
(*                                                                           *)
(* Neither kills the stopped restored process: on the cross-host path the     *)
(* orchestrator cannot reach the target node.  That is precisely why SIGCONT  *)
(* must be fenced.                                                            *)
AbortActivationBody(r) ==
    /\ AbortEdgesEnabled
    /\ rstate[r] = "ACTIVATING"
    /\ winner[r] # NoAgent
    /\ procs[r][winner[r]] = "STOPPED"    \* fence: not yet continued
    /\ rstate' = [rstate EXCEPT ![r] = "CHECKPOINTED"]

AbortActivationDeadA(r) ==
    /\ AbortActivationBody(r)
    /\ winner[r] \in crashed
    /\ UNCHANGED << inc, paused, mid, lease, procs, cur, rinc, won, winner,
                    crashed, image, dumpfail, nmig, nabort, poisoned >>

AbortActivationDead == \E r \in Ranks : AbortActivationDeadA(r)

AbortActivationSuspectedA(r) ==
    /\ AbortActivationBody(r)
    /\ winner[r] \notin crashed
    /\ nabort < MaxSuspectedAborts
    /\ nabort' = nabort + 1
    /\ UNCHANGED << inc, paused, mid, lease, procs, cur, rinc, won, winner,
                    crashed, image, dumpfail, nmig, poisoned >>

AbortActivationSuspected == \E r \in Ranks : AbortActivationSuspectedA(r)

(* Lease TTL.  Expiry alone does not move the directory state -- the spec     *)
(* lets `acquire_restore_lease` re-take a RESTORE_RESERVED entry whose lease  *)
(* expired.  Split by fairness for exactly the same reason as above.          *)
LeaseExpiryCrashedA(r) ==
    /\ lease[r] \in crashed
    /\ lease' = [lease EXCEPT ![r] = NoAgent]
    /\ UNCHANGED << rstate, inc, paused, mid, procs, cur, rinc, won, winner,
                    crashed, image, dumpfail, nmig, nabort, poisoned >>

LeaseExpiryCrashed == \E r \in Ranks : LeaseExpiryCrashedA(r)

LeaseExpirySpuriousA(r) ==
    /\ UnboundedRestoreRetries
    /\ lease[r] # NoAgent
    /\ lease[r] \notin crashed
    /\ lease' = [lease EXCEPT ![r] = NoAgent]
    /\ UNCHANGED << rstate, inc, paused, mid, procs, cur, rinc, won, winner,
                    crashed, image, dumpfail, nmig, nabort, poisoned >>

LeaseExpirySpurious == \E r \in Ranks : LeaseExpirySpuriousA(r)

-----------------------------------------------------------------------------
(* --------------------------- agent-side / OS steps ---------------------- *)

(* Freeze the local target (SIGSTOP / criu seize).                            *)
AgentFreezeA(a, r) ==
    /\ Alive(a)
    /\ rstate[r] = "PAUSING"
    /\ procs[r][cur[r]] = "RUNNING"
    /\ procs' = [procs EXCEPT ![r][cur[r]] = "STOPPED"]
    /\ UNCHANGED << rstate, inc, paused, mid, lease, cur, rinc, won, winner,
                    crashed, image, dumpfail, nmig, nabort, poisoned >>

AgentFreeze == \E a \in Agents, r \in Ranks : AgentFreezeA(a, r)

(* criu dump + stage + digest-verify.  KeepFrozenOriginal distinguishes the   *)
(* two shapes the spec records:                                               *)
(*   TRUE  cross-host, `--leave-stopped`: the original stays frozen and is    *)
(*         available for abort recovery.                                      *)
(*   FALSE same-host: "criu ptrace-seizes, dumps, then kills and reaps the    *)
(*         task, freeing the pid so the immediate restore can reclaim it"     *)
(*         (LocalRankAgent.cpp:417).  The original is GONE at this point.     *)
AgentDumpA(a, r) ==
    /\ Alive(a)
    /\ rstate[r] = "PAUSING"
    /\ ~image[r]
    /\ ~dumpfail[r]
    /\ procs[r][cur[r]] = "STOPPED"
    /\ image' = [image EXCEPT ![r] = TRUE]
    /\ procs' = IF KeepFrozenOriginal
                THEN procs
                ELSE [procs EXCEPT ![r][cur[r]] = "KILLED"]
    /\ UNCHANGED << rstate, inc, paused, mid, lease, cur, rinc, won, winner,
                    crashed, dumpfail, nmig, nabort, poisoned >>

AgentDump == \E a \in Agents, r \in Ranks : AgentDumpA(a, r)

(* criu dump failed after its retries and rethrew                            *)
(* (LocalRankAgent.cpp:420-437).  Today's code treats this as routine.        *)
AgentDumpFailA(a, r) ==
    /\ Alive(a)
    /\ rstate[r] = "PAUSING"
    /\ ~image[r]
    /\ ~dumpfail[r]
    /\ procs[r][cur[r]] = "STOPPED"
    /\ dumpfail' = [dumpfail EXCEPT ![r] = TRUE]
    /\ UNCHANGED << rstate, inc, paused, mid, lease, procs, cur, rinc, won,
                    winner, crashed, image, nmig, nabort, poisoned >>

AgentDumpFail == \E a \in Agents, r \in Ranks : AgentDumpFailA(a, r)

(* `criu restore --tcp-close --leave-stopped --restore-detached --pidfile`.   *)
(* Runs UNDER THE LEASE.  Produces a STOPPED process, never a running one.    *)
RestoreProcessA(a, r) ==
    /\ Alive(a)
    /\ lease[r] = a
    /\ rstate[r] = "RESTORE_RESERVED"
    /\ procs[r][a] \in {"ABSENT", "KILLED"}
    /\ procs' = [procs EXCEPT ![r][a] = "STOPPED"]
    /\ rinc'  = [rinc  EXCEPT ![r][a] = inc[r]]
    /\ UNCHANGED << rstate, inc, paused, mid, lease, cur, won, winner,
                    crashed, image, dumpfail, nmig, nabort, poisoned >>

RestoreProcess == \E a \in Agents, r \in Ranks : RestoreProcessA(a, r)

(* SIGCONT --- "Only a winner receives SIGCONT."                              *)
SigContA(a, r) ==
    /\ Alive(a)
    /\ procs[r][a] = "STOPPED"
    /\ CanSigCont(a, r)
    /\ procs' = [procs EXCEPT ![r][a] = "RUNNING"]
    /\ cur'   = [cur   EXCEPT ![r] = a]
    /\ rinc'  = [rinc  EXCEPT ![r][a] = 0]
    /\ UNCHANGED << rstate, inc, paused, mid, lease, won, winner, crashed,
                    image, dumpfail, nmig, nabort, poisoned >>

SigCont == \E a \in Agents, r \in Ranks : SigContA(a, r)

(* SIGKILL --- "A loser or lease-less straggler is SIGKILLed while still      *)
(* stopped, so a twin can never pair, ack, or touch the control plane."       *)
(* Never applied to cur[r]: that is the frozen original abort recovery needs. *)
SigKillLoserA(a, r) ==
    /\ Alive(a)
    /\ procs[r][a] = "STOPPED"
    /\ a # cur[r]
    /\ ~CanSigCont(a, r)
    /\ ~CanStillWin(a, r)
    /\ procs' = [procs EXCEPT ![r][a] = "KILLED"]
    /\ rinc'  = [rinc  EXCEPT ![r][a] = 0]
    /\ UNCHANGED << rstate, inc, paused, mid, lease, cur, won, winner, crashed,
                    image, dumpfail, nmig, nabort, poisoned >>

SigKillLoser == \E a \in Agents, r \in Ranks : SigKillLoserA(a, r)

(* After `abort_pause` the rank is ACTIVE again but its process is still      *)
(* frozen; some agent must SIGCONT it.  On the same-host shape                *)
(* (KeepFrozenOriginal = FALSE) after a completed dump there is nothing left  *)
(* to resume -- which is the whole content of the spec's "Frozen-original     *)
(* abort recovery is cross-host only".                                        *)
ResumeAfterAbortA(a, r) ==
    /\ Alive(a)
    /\ rstate[r] = "ACTIVE"
    /\ RunningInsts(r) = {}
    /\ procs[r][cur[r]] = "STOPPED"
    /\ procs' = [procs EXCEPT ![r][cur[r]] = "RUNNING"]
    /\ UNCHANGED << rstate, inc, paused, mid, lease, cur, rinc, won, winner,
                    crashed, image, dumpfail, nmig, nabort, poisoned >>

ResumeAfterAbort == \E a \in Agents, r \in Ranks : ResumeAfterAbortA(a, r)

AgentCrashA(a) ==
    /\ a \notin crashed
    /\ Cardinality(crashed) < MaxCrashes
    /\ crashed' = crashed \cup {a}
    /\ UNCHANGED << rstate, inc, paused, mid, lease, procs, cur, rinc, won,
                    winner, image, dumpfail, nmig, nabort, poisoned >>

AgentCrash == \E a \in Agents : AgentCrashA(a)

-----------------------------------------------------------------------------
(* -------- monitor for the ClearPauseAtCommit question (contract 3) ------- *)
(*                                                                           *)
(* The spec argues: `migrations_in_flight` is derived from the pause set, so  *)
(* clearing the pause entry at commit_restore releases deadline suspension    *)
(* "strictly *before* any peer is permitted to re-pair, handshake, or         *)
(* replay.  A survivor mid-operation then throws Utils::Timeout, and since    *)
(* an exception unwinding a sliced operation poisons the communicator, a      *)
(* textbook-clean migration would intermittently poison the job."             *)
(*                                                                           *)
(* This action is that argument, ASSUMED not derived: it fires whenever a     *)
(* survivor is ACTIVE while the migrating rank is ACTIVATING (restored but    *)
(* not yet polled, hence not yet re-paired) and suspension has been released. *)
(* Nothing else in this module models links or deadlines.                     *)
SurvivorTimeout ==
    /\ ~poisoned
    /\ \E r \in Ranks :
         /\ rstate[r] = "ACTIVATING"
         /\ ~paused[r]
         /\ \E s \in Ranks : s # r /\ rstate[s] = "ACTIVE"
    /\ poisoned' = TRUE
    /\ UNCHANGED << rstate, inc, paused, mid, lease, procs, cur, rinc, won,
                    winner, crashed, image, dumpfail, nmig, nabort >>

-----------------------------------------------------------------------------
(* Terminal stutter.  Without it the bounded model reports a deadlock in the  *)
(* perfectly healthy end state "all ranks ACTIVE and the migration budget is  *)
(* spent", which would drown the real dead ends.  It is enabled ONLY in that  *)
(* healthy end state, so it cannot mask a genuine wedge.                      *)
Quiescent ==
    /\ nmig = MaxMigrations
    /\ \A r \in Ranks : rstate[r] = "ACTIVE" /\ ~paused[r]
    /\ \A r \in Ranks : RunningInsts(r) # {}

Stutter == Quiescent /\ UNCHANGED vars

Next ==
    \/ Join
    \/ MarkActiveFromStarting
    \/ RequestPause
    \/ AgentFreeze
    \/ AgentDump
    \/ AgentDumpFail
    \/ MarkCheckpointed
    \/ AcquireRestoreLease
    \/ RestoreProcess
    \/ CommitRestore
    \/ SigCont
    \/ SigKillLoser
    \/ MarkActiveFromActivating
    \/ AbortPauseOnDumpFailure
    \/ AbortPauseCancel
    \/ AbortRestore
    \/ AbortActivationDead
    \/ AbortActivationSuspected
    \/ LeaseExpiryCrashed
    \/ LeaseExpirySpurious
    \/ ResumeAfterAbort
    \/ AgentCrash
    \/ SurvivorTimeout
    \/ Stutter

(* Fairness is asserted only where the design says an actor is obliged to     *)
(* act.  Adversarial or discretionary steps (RequestPause, AgentDumpFail,     *)
(* AgentCrash, LeaseExpirySpurious, AbortPauseCancel, AbortRestore,           *)
(* AbortActivationSuspected, SurvivorTimeout) carry NO fairness, so TLC may   *)
(* schedule them but never has to.                                            *)
Fairness ==
    /\ WF_vars(Join)
    /\ WF_vars(MarkActiveFromStarting)
    /\ WF_vars(AgentFreeze)
    /\ WF_vars(AgentDump)
    /\ WF_vars(MarkCheckpointed)
    /\ WF_vars(AcquireRestoreLease)
    /\ WF_vars(RestoreProcess)
    /\ WF_vars(CommitRestore)
    /\ WF_vars(SigCont)
    /\ WF_vars(SigKillLoser)
    /\ WF_vars(MarkActiveFromActivating)
    /\ WF_vars(AbortPauseOnDumpFailure)
    /\ WF_vars(AbortActivationDead)
    /\ WF_vars(LeaseExpiryCrashed)
    /\ WF_vars(ResumeAfterAbort)

Spec == Init /\ [][Next]_vars /\ Fairness

(* SpecSF, added by the integration/audit pass.                               *)
(*                                                                            *)
(* Membership.cfg's liveness counterexample is a lasso in which the lease      *)
(* expires in exactly the window in which `criu restore` would have made       *)
(* progress: RestoreProcess is enabled in the lease-held state and disabled in *)
(* the lease-expired one, so it is never CONTINUOUSLY enabled and WF does not  *)
(* oblige it to fire.  SpecSF replaces WF by SF on precisely the two restore-  *)
(* phase steps.  SF here is not a free assumption -- it is exactly the claim   *)
(* "the lease outlives the restore", which contract 3 never states.  If the    *)
(* lasso disappears under SpecSF and only under SpecSF, then the missing       *)
(* liveness is EXACTLY that missing TTL-versus-restore-duration statement,     *)
(* rather than some deeper defect.                                             *)
SpecSF == Init /\ [][Next]_vars /\ Fairness
               /\ SF_vars(RestoreProcess)
               /\ SF_vars(CommitRestore)

-----------------------------------------------------------------------------
(* ------------------------------- invariants ----------------------------- *)

(* THE twin-safety property.  A twin that is merely STOPPED cannot pair, ack  *)
(* or touch the control plane; a second RUNNING instance can.                 *)
AtMostOneRunning ==
    \A r \in Ranks : Cardinality(RunningInsts(r)) <= 1

(* Redis SET-NX makes the *directory* lease single-valued by construction;    *)
(* this invariant records that the model never violates it.  The interesting  *)
(* corollary, which the module does exercise, is that lease exclusion is NOT  *)
(* sufficient for twin safety: a straggler keeps its restored process after   *)
(* its lease expires, so arbitration has to happen at commit_restore.         *)
LeaseExclusion ==
    \A r \in Ranks : Cardinality({a \in Agents : lease[r] = a}) <= 1

(* A rank that the directory calls ACTIVE must still have a process that can  *)
(* be made to run.  Violated by the same-host shape (see KeepFrozenOriginal). *)
ActiveHasProcess ==
    \A r \in Ranks :
        rstate[r] = "ACTIVE" => \E i \in InstIds : procs[r][i] \in {"RUNNING", "STOPPED"}

PauseSetConsistent ==
    \A r \in Ranks : paused[r] => rstate[r] # "ACTIVE"

NoPoison == ~poisoned

(* The dead-end check, stated as the spec's normative rule.  Redundant with   *)
(* TLC's deadlock check, kept because the rule is normative prose and this is *)
(* its literal transcription.                                                 *)
NoDeadEnd == ENABLED Next

-----------------------------------------------------------------------------
(* ------------------------------- properties ----------------------------- *)

(* Incarnation never decreases, and the only step that changes it is          *)
(* commit_restore, which adds exactly one.                                    *)
MonotonicIncarnation ==
    [][ \A r \in Ranks :
          /\ inc'[r] >= inc[r]
          /\ (inc'[r] # inc[r] => inc'[r] = inc[r] + 1) ]_vars

(* A rank that gets paused eventually ends up ACTIVE and out of the pause     *)
(* set -- either at a higher incarnation (migration completed) or at the      *)
(* unchanged one (migration aborted).                                         *)
EventuallyUnpaused ==
    \A r \in Ranks : paused[r] ~> (rstate[r] = "ACTIVE" /\ ~paused[r])

EventuallyActive ==
    \A r \in Ranks : (rstate[r] # "ACTIVE") ~> (rstate[r] = "ACTIVE")

=============================================================================
