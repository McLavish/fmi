------------------------------ MODULE SequencedLink ------------------------------
(***************************************************************************)
(* Formalisation of NORMATIVE CONTRACT 2 -- transport durability -- of      *)
(* docs/design/2026-07-27-sequenced-incarnation-links-design.md.           *)
(*                                                                          *)
(* THE CLAIM BEING CHECKED (the "headline theorem"):                        *)
(*                                                                          *)
(*   With `criu dump --tcp-close` (modelled as the `Freeze` action) able to *)
(*   fire at ANY instant -- including between a frame leaving the socket    *)
(*   and being committed to the drain queue, and between drain-commit and   *)
(*   the ACK write -- a sequenced link with sender retention, two           *)
(*   watermarks and handshake reconciliation loses nothing, duplicates      *)
(*   nothing, and reorders nothing.                                         *)
(*                                                                          *)
(* The design states this as three ordering invariants (spec lines 180-182):*)
(*                                                                          *)
(*   1. An immutable retained copy exists before any DATA byte reaches the  *)
(*      socket.                                                             *)
(*   2. A complete frame is committed to the drain queue before its ACK     *)
(*      becomes writable.                                                   *)
(*   3. Retention is reclaimed only on a validated cumulative ACK.          *)
(*                                                                          *)
(* All three are encoded STRUCTURALLY (as enabling conditions of the        *)
(* actions) and then re-stated as checkable invariants, so that a           *)
(* deliberately broken variant (CONSTANT `AckOnReceipt`) can violate one    *)
(* of them and be caught.                                                   *)
(*                                                                          *)
(****************************************************************************)
(* WHAT THIS SPEC DELIBERATELY DOES NOT MODEL                               *)
(*                                                                          *)
(* Be suspicious of the parts a formal spec is silent about. These are the  *)
(* silences here, each with the reason:                                     *)
(*                                                                          *)
(*  (a) ONE DATA DIRECTION ONLY.  Sender -> receiver DATA plus the reverse  *)
(*      ACK/CREDIT control path.  There is no receiver -> sender DATA flow. *)
(*      CONSEQUENCE: the *ack-behind-data deadlock* the spec mitigates with *)
(*      `drain_reserve_frames >= window_frames` (spec line 197) is NOT      *)
(*      checked here -- it requires two DATA directions sharing one egress  *)
(*      FIFO.  Everything else (NoLoss/NoDup/FIFO/RetentionSafety) is a     *)
(*      per-directed-link property and is fully checked.  See the           *)
(*      "Reserve < W" experiment in the summary: in a single direction the  *)
(*      constraint is not needed, which is direct evidence that the hazard  *)
(*      is genuinely bidirectional and needs a separate model.              *)
(*                                                                          *)
(*  (b) NO LANES, NO FRAGMENTS, NO MESSAGE IDENTITY.  Contract 1's envelope *)
(*      (lane, op_kind, collective_index, root, message_id, fragment_index) *)
(*      is out of scope; this module is contract 2 only.  One frame == one  *)
(*      message.  Fragment reassembly would sit strictly between `Deliver`  *)
(*      and `Commit` and would only widen the volatile window that `Freeze` *)
(*      destroys -- i.e. it makes the broken variant worse, never better.   *)
(*                                                                          *)
(*  (c) BYTES ARE FRAMES.  `window_frames`, `drain_reserve_frames` and      *)
(*      `retention_limit_bytes` collapse into a single frame count W and a  *)
(*      reserve.  `max_frame_bytes` is irrelevant when all frames are one   *)
(*      unit.  Per the Plan C task 1 decision record, the retention cap is  *)
(*      an *admission* threshold, never an eviction threshold, so with      *)
(*      uniform frames it coincides exactly with the window.                *)
(*                                                                          *)
(*  (d) PAYLOAD == transport_seq.  Message identity is its sequence number. *)
(*      WLOG on a single directed link because admission is strictly in      *)
(*      order, so the posted sequence is always <<1,2,...>>.  FIFO is still *)
(*      a real check: it forbids the *consumed* sequence from departing     *)
(*      from that order.                                                    *)
(*                                                                          *)
(*  (e) THE HANDSHAKE IS ATOMIC.  The real exchange is at least one RTT.    *)
(*      Not load-bearing: `Freeze` may fire in `REPAIRING`, which returns   *)
(*      the link to `DEAD` and retries, and the only durable side effect a  *)
(*      handshake has is adopting the peer's cumulative ACK -- which is     *)
(*      idempotent and, by ordering invariant 2, always safe.               *)
(*                                                                          *)
(*  (f) THE PARSER STAGES AT MOST ONE FRAME.  `rcvParse` holds <= 1 frame.  *)
(*      It models the connection-local, VOLATILE part of receive: a frame   *)
(*      whose bytes have begun arriving but which is not yet a committed    *)
(*      whole frame.  `--tcp-close` discards it because its remaining bytes *)
(*      lived in the kernel socket buffer.  A deeper parse buffer only      *)
(*      enlarges the volatile set, so depth 1 is the *conservative* choice  *)
(*      for the correct configuration and understates the damage in the     *)
(*      broken one.                                                         *)
(*                                                                          *)
(*  (g0) FRAMES ARE ATOMIC ON THE WIRE.  There is no partially written frame. *)
(*      The spec's "egress is serialized per socket: a partially written DATA  *)
(*      frame completes before any ACK or control frame begins" (line 195) is  *)
(*      a FRAMING-CORRUPTION rule, and framing corruption is abstracted away   *)
(*      here.  Justification for the durability argument specifically: a       *)
(*      partial DATA frame is discarded by --tcp-close and is by construction  *)
(*      unacked, so it is indistinguishable from a frame never sent -- which   *)
(*      this model does represent.  The interleaving hazard itself is NOT      *)
(*      checked and remains an implementation obligation.                      *)
(*                                                                          *)
(*  (g) NO TIME, NO TIMEOUTS, NO DIRECTORY.  Deadline suspension, the       *)
(*      membership state machine (contract 3), incarnation numbers, and     *)
(*      directory-driven repair detection (spec line 227) are contract 3/4  *)
(*      and are abstracted into a nondeterministic `Restore` that is simply *)
(*      weakly fair.  This spec therefore says nothing about *how* a        *)
(*      survivor learns its peer moved; it assumes it eventually does.      *)
(*                                                                          *)
(*  (h) NO CRASH-WITHOUT-IMAGE, NO PERMANENT PEER DEATH.  `Freeze` is a     *)
(*      planned checkpoint: the drain queue and both watermarks survive     *)
(*      inside the CRIU image.  Genuine peer loss is out of scope (the spec *)
(*      assigns failure detection to the orchestrator, line 294).           *)
(*                                                                          *)
(*  (i) `none` MODE IS NOT MODELLED.  `AckPolicy::OnConsume` (ack only what *)
(*      the application has consumed) is strictly stronger than the CRIU    *)
(*      `OnReceive`/on-drain-commit policy modelled here -- it prunes less. *)
(*      The hard case is the CRIU policy, so that is what is checked.       *)
(***************************************************************************)

EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS
    MaxMsg,        \* how many application messages the sender will ever post.
                   \* Bounds the model; the app stops sending after this many.
    W,             \* fault_tolerance.link.window_frames.  Max unacked frames;
                   \* AppSend BLOCKS when the window is full (spec: window-full
                   \* blocks the send op, so no delivery obligation exists yet).
    Reserve,       \* fault_tolerance.link.drain_reserve_frames.  Max frames the
                   \* receiver will hold committed-but-unconsumed.
    MaxFreezes,    \* bound on the number of `criu dump --tcp-close` events.
    AckOnReceipt,  \* FALSE => correct: ACK becomes writable only after
                   \* drain-commit (ordering invariant 2).
                   \* TRUE  => DELIBERATELY BROKEN: ACK becomes writable as
                   \* soon as the frame is taken off the wire, before commit.

    (* ---- two further fault injections, added by the integration/audit pass.
           They exist so that the "correct" configuration's all-green result is
           not the only evidence about these two invariants: each breaks a
           DIFFERENT clause of contract 2 from AckOnReceipt, and TLC must catch
           both.  Both FALSE = the spec. ---- *)
    DrainQueueVolatile,
                   \* FALSE => spec: the drain queue is DURABLE.  It lives in
                   \*   the checkpointed process's memory, so `criu dump` puts
                   \*   it inside the image ("drain buffers are inside the
                   \*   image", contract 2, ack_safe bullet).
                   \* TRUE  => BROKEN: committed-but-undrained frames are held
                   \*   in connection-local memory and are destroyed with the
                   \*   connection.  This is the implementation mistake of
                   \*   putting the drain queue in the link object rather than
                   \*   in the channel, and it is what makes `ack_safe =
                   \*   next_received` unsound.
    PruneOnTransmit
                   \* FALSE => spec: ordering invariant 3, "retention is
                   \*   reclaimed only on a validated cumulative ACK".
                   \* TRUE  => BROKEN: retention is released as soon as the
                   \*   frame reaches the socket (fire-and-forget), which is
                   \*   what "TCP is reliable so the kernel owns it now" looks
                   \*   like in code.

ASSUME MaxMsg  \in Nat /\ MaxMsg > 0
ASSUME W       \in Nat /\ W > 0
ASSUME Reserve \in Nat /\ Reserve > 0
ASSUME MaxFreezes \in Nat
ASSUME AckOnReceipt \in BOOLEAN
ASSUME DrainQueueVolatile \in BOOLEAN
ASSUME PruneOnTransmit \in BOOLEAN

--------------------------------------------------------------------------------
(* ------------------------------- VARIABLES ------------------------------ *)

VARIABLES
    (* --- sender side (survives Freeze: it is either the survivor's live
           memory, or it is inside the CRIU image) --- *)
    posted,          \* Seq: messages for which AppSend has RETURNED, in order.
                     \* The delivery obligation attaches to exactly these.
    nextSend,        \* Nat: next transport_seq to assign.  Assigned = 1..nextSend-1.
    retained,        \* SUBSET: transport_seqs still in the retention buffer.
    lowestRetained,  \* Nat: oldest seq still retained.  retained = [lowestRetained, nextSend).
    sndCursor,       \* Nat: next seq to push onto the socket.  CONNECTION-LOCAL
                     \* write cursor -- reset by Freeze, re-derived by Handshake.
    replayMark,      \* Nat: end of the replay suffix set by Handshake.  Purely
                     \* observational: separates `Replay` from `Transmit`.
    sndCreditLimit,  \* Nat: highest seq the sender believes the receiver has
                     \* room for.  ABSOLUTE watermark, hence idempotent under
                     \* replay (a credit *counter* would be corrupted by it).

    (* --- network: TCP is FIFO while the connection lives, and every byte in
           either direction is destroyed by --tcp-close --- *)
    dataChan,        \* Seq: DATA frames in flight sender -> receiver.
    ackChan,         \* Seq: control frames in flight receiver -> sender,
                     \* each [ack |-> cumulative ack_safe, credit |-> limit].

    (* --- receiver side --- *)
    rcvParse,        \* Seq (len <= 1): VOLATILE.  A frame off the wire but not
                     \* yet a committed whole frame.  Destroyed by Freeze.
    ackOut,          \* Seq (len <= 1): VOLATILE.  Control frame made writable
                     \* but not yet on the wire.  Destroyed by Freeze.
    drainQ,          \* Seq: DURABLE.  Committed, undrained frames.  Inside the
                     \* CRIU image, so it survives Freeze.
    consumed,        \* Seq: DURABLE.  Messages the application has consumed.
    nextReceived,    \* Nat: DURABLE.  Highest contiguous seq accepted, +1.
    ackSafe,         \* Nat: DURABLE.  Highest seq the sender may prune, +1.
    ackSent,         \* Nat: CONNECTION-LOCAL write cursor -- what ack value was
                     \* last put on this connection.  Lost on Freeze.
    creditSent,      \* Nat: CONNECTION-LOCAL write cursor for credit.  Lost.

    (* --- link / environment --- *)
    linkState,       \* "UP" | "DEAD" | "REPAIRING" | "ABORTED"
    freezes          \* Nat: number of Freeze events so far (finiteness bound).

sndVars  == << posted, nextSend, retained, lowestRetained, sndCursor,
               replayMark, sndCreditLimit >>
netVars  == << dataChan, ackChan >>
rcvVars  == << rcvParse, ackOut, drainQ, consumed, nextReceived, ackSafe,
               ackSent, creditSent >>
linkVars == << linkState, freezes >>

vars == << sndVars, netVars, rcvVars, linkVars >>

--------------------------------------------------------------------------------
(* ------------------------------- HELPERS -------------------------------- *)

Max(a, b) == IF a > b THEN a ELSE b

Range(s) == { s[i] : i \in DOMAIN s }

IsSeqOf(s, S) == /\ DOMAIN s = 1..Len(s)
                 /\ \A i \in 1..Len(s) : s[i] \in S

Seqs      == 1..MaxMsg                 \* every transport_seq ever assigned
SeqsPlus  == 1..(MaxMsg + 1)           \* watermark range ("next" values)
Credits   == 0..(MaxMsg + Reserve)
CtrlFrame == [ack : SeqsPlus, credit : Credits]

(* What the receiver DURABLY holds: only committed frames.  Everything else at
   the receiver -- rcvParse, ackOut -- and everything on either wire is
   destroyed by `criu dump --tcp-close`. *)
DurableAtReceiver == Range(drainQ) \cup Range(consumed)

(* The credit the receiver may currently advertise: an absolute seq watermark.
   It admits exactly `Reserve` frames beyond what the application has consumed,
   which is the Plan C task 1 resolution of the spec's open item 1
   ("Initial CREDIT = drain_reserve_frames ... returned as the application
   consumes"). *)
CreditLimit == Len(consumed) + Reserve

--------------------------------------------------------------------------------
(* --------------------------------- INIT --------------------------------- *)

Init ==
    /\ posted          = << >>
    /\ nextSend        = 1
    /\ retained        = { }
    /\ lowestRetained  = 1
    /\ sndCursor       = 1
    /\ replayMark      = 0
    /\ sndCreditLimit  = Reserve      \* initial CREDIT, per the decision record
    /\ dataChan        = << >>
    /\ ackChan         = << >>
    /\ rcvParse        = << >>
    /\ ackOut          = << >>
    /\ drainQ          = << >>
    /\ consumed        = << >>
    /\ nextReceived    = 1
    /\ ackSafe         = 1
    /\ ackSent         = 1
    /\ creditSent      = Reserve
    /\ linkState       = "UP"
    /\ freezes         = 0

--------------------------------------------------------------------------------
(* -------------------------------- ACTIONS ------------------------------- *)

(* AppSend -- the application calls send() and send() RETURNS.
   Deliberately NOT gated on linkState: the delivery obligation says a
   completed send may be the last time the application ever touches the link,
   so posting must succeed while the link is DEAD or REPAIRING.
   Blocked when the window is full; while blocked there is no obligation yet.
   ORDERING INVARIANT 1 is structural here: the retained copy is created in the
   same atomic step in which the send completes, and strictly before any
   Transmit of that seq is enabled. *)
AppSend ==
    /\ linkState /= "ABORTED"
    /\ nextSend <= MaxMsg
    /\ nextSend - lowestRetained < W          \* window / retention cap: BLOCKS
    /\ posted'   = Append(posted, nextSend)
    /\ retained' = retained \cup {nextSend}   \* invariant 1: retain, then send
    /\ nextSend' = nextSend + 1
    /\ UNCHANGED << lowestRetained, sndCursor, replayMark, sndCreditLimit >>
    /\ UNCHANGED << netVars, rcvVars, linkVars >>

(* The shared body of Transmit and Replay: push one DATA frame onto the wire.
   The `sndCursor \in retained` conjunct is ORDERING INVARIANT 1 as an enabling
   condition -- a byte cannot reach the socket unless an immutable retained
   copy exists.  `sndCursor <= sndCreditLimit` is CREDIT (admission into the
   receiver's drain buffers), which is separate from ACK by construction. *)
PushFrame ==
    /\ linkState = "UP"
    /\ sndCursor < nextSend
    /\ sndCursor <= sndCreditLimit
    /\ sndCursor \in retained
    /\ dataChan'  = Append(dataChan, sndCursor)
    /\ sndCursor' = sndCursor + 1
    (* Ordering invariant 3.  In the spec (PruneOnTransmit = FALSE) retention is
       untouched here and shrinks only in PruneRetention/Handshake.  Under the
       injected fault it is released the instant the bytes reach the socket. *)
    /\ retained'       = IF PruneOnTransmit THEN retained \ {sndCursor} ELSE retained
    /\ lowestRetained' = IF PruneOnTransmit THEN sndCursor + 1 ELSE lowestRetained
    /\ UNCHANGED << posted, nextSend, replayMark, sndCreditLimit >>
    /\ UNCHANGED << ackChan, rcvVars, linkVars >>

(* Transmit -- first transmission of a freshly posted frame. *)
Transmit == sndCursor >= replayMark /\ PushFrame

(* Replay -- retransmission of the unacked suffix [peer.next_received,
   next_send) after a Handshake.  Exactly the suffix, no more and no less:
   Handshake sets sndCursor := peer next_received and replayMark := next_send. *)
Replay == sndCursor < replayMark /\ PushFrame

(* Deliver -- bytes arrive and are parsed.  This is the VOLATILE half of
   receive: the frame is off the wire but is not yet a committed whole frame.
   Under the BROKEN variant (AckOnReceipt = TRUE) the ACK watermark advances
   HERE, violating ordering invariant 2. *)
Deliver ==
    /\ linkState = "UP"
    /\ dataChan /= << >>
    /\ rcvParse = << >>                      \* parser stages one frame at a time
    /\ rcvParse' = << Head(dataChan) >>
    /\ dataChan' = Tail(dataChan)
    /\ ackSafe'  = IF AckOnReceipt /\ Head(dataChan) = nextReceived
                   THEN nextReceived + 1     \* <<< BROKEN: ack before commit
                   ELSE ackSafe
    /\ UNCHANGED << ackOut, drainQ, consumed, nextReceived, ackSent, creditSent >>
    /\ UNCHANGED << sndVars, ackChan, linkVars >>

(* Commit -- the complete frame is committed to the drain queue.  This is the
   DURABLE half: from here the payload is inside the CRIU image.
   ORDERING INVARIANT 2 is structural: `ackSafe` (the only thing EmitAck may
   read) advances in this action and nowhere else in the correct variant.
   The `Len(drainQ) < Reserve` conjunct is the drain reserve; credit guarantees
   it is never actually blocking (checked by NoUnboundedBacklog + no deadlock).
   The `< nextReceived` branch is the dedup path for a replayed duplicate. *)
Commit ==
    /\ linkState = "UP"
    /\ rcvParse /= << >>
    /\ LET s == Head(rcvParse) IN
       \/ /\ s < nextReceived                       \* Duplicate: correct to drop
          /\ rcvParse' = << >>
          /\ UNCHANGED << drainQ, nextReceived, ackSafe, consumed >>
       \/ /\ s = nextReceived                       \* Delivered
          /\ Len(drainQ) < Reserve
          /\ drainQ'       = Append(drainQ, s)      \* commit ...
          /\ nextReceived' = nextReceived + 1
          /\ ackSafe'      = IF AckOnReceipt THEN ackSafe
                             ELSE nextReceived + 1  \* ... then, and only then,
                                                    \* the ACK becomes writable
          /\ rcvParse'     = << >>
          /\ UNCHANGED consumed
       (* s > nextReceived is FatalGap: no action, caught by NoFatalGap. *)
    /\ UNCHANGED << ackOut, ackSent, creditSent >>
    /\ UNCHANGED << sndVars, dataChan, ackChan, linkVars >>

(* EmitAck -- the ACK/CREDIT control frame becomes WRITABLE.  It can only ever
   carry `ackSafe`, which ordering invariant 2 ties to drain-commit.  ACK and
   CREDIT ride the same reverse control frame (both are credit-exempt control
   traffic on the same socket).  Fires only when something has advanced beyond
   what was already written on THIS connection. *)
EmitAck ==
    /\ linkState = "UP"
    /\ ackOut = << >>
    /\ (ackSafe > ackSent \/ CreditLimit > creditSent)
    /\ ackOut'     = << [ack |-> ackSafe, credit |-> CreditLimit] >>
    /\ ackSent'    = ackSafe
    /\ creditSent' = CreditLimit
    /\ UNCHANGED << rcvParse, drainQ, consumed, nextReceived, ackSafe >>
    /\ UNCHANGED << sndVars, netVars, linkVars >>

(* TransmitAck -- the writable control frame reaches the socket. *)
TransmitAck ==
    /\ linkState = "UP"
    /\ ackOut /= << >>
    /\ Len(ackChan) < 1
    /\ ackChan' = Append(ackChan, Head(ackOut))
    /\ ackOut'  = << >>
    /\ UNCHANGED << rcvParse, drainQ, consumed, nextReceived, ackSafe,
                    ackSent, creditSent >>
    /\ UNCHANGED << sndVars, dataChan, linkVars >>

(* PruneRetention -- the sender validates a cumulative ACK and reclaims.
   ORDERING INVARIANT 3 is structural: `retained` shrinks in this action and in
   Handshake (which adopts the peer's cumulative ack from the handshake state)
   and nowhere else.  A cumulative value above next_send is the spec's
   "impossible state" -> loud abort. *)
PruneRetention ==
    /\ ackChan /= << >>
    /\ LET a == Head(ackChan) IN
       IF a.ack > nextSend
       THEN /\ linkState' = "ABORTED"           \* loud abort, never silent
            /\ UNCHANGED << sndVars, dataChan, rcvVars, freezes >>
            /\ ackChan' = Tail(ackChan)
       ELSE /\ lowestRetained' = Max(lowestRetained, a.ack)
            /\ retained'       = { s \in retained : s >= a.ack }
            /\ sndCreditLimit' = Max(sndCreditLimit, a.credit)
            /\ ackChan'        = Tail(ackChan)
            /\ UNCHANGED << posted, nextSend, sndCursor, replayMark >>
            /\ UNCHANGED << dataChan, rcvVars, linkVars >>

(* AppConsume -- the application drains one message.  Returns CREDIT (the
   absolute CreditLimit watermark advances with Len(consumed)).  Not gated on
   linkState: the application keeps computing while the link is being repaired. *)
AppConsume ==
    /\ drainQ /= << >>
    /\ consumed' = Append(consumed, Head(drainQ))
    /\ drainQ'   = Tail(drainQ)
    /\ UNCHANGED << rcvParse, ackOut, nextReceived, ackSafe, ackSent, creditSent >>
    /\ UNCHANGED << sndVars, netVars, linkVars >>

(* ------------------------- THE ARBITRARY FREEZE ------------------------- *)
(* Freeze -- `criu dump --tcp-close`.  Fireable in ANY state, from UP, DEAD or
   REPAIRING, up to MaxFreezes times.  The whole point of the design is that
   the freeze position is arbitrary, so this action has no convenience guards.
     DESTROYED: every frame in flight in BOTH directions, the receiver's
                connection-local parser buffer and its ack/credit write
                cursors, and the sender's connection-local write cursor.
     SURVIVING: the drain queue, the consumed history, BOTH watermarks
                (they are in the CRIU image), and all sender retention. *)
Freeze ==
    /\ freezes < MaxFreezes
    /\ linkState \in {"UP", "DEAD", "REPAIRING"}
    /\ freezes'   = freezes + 1
    /\ linkState' = "DEAD"
    /\ dataChan'  = << >>          \* kernel socket buffers: gone
    /\ ackChan'   = << >>          \* reverse direction too
    /\ rcvParse'  = << >>          \* volatile parse state: gone
    /\ ackOut'    = << >>          \* volatile egress state: gone
    /\ ackSent'   = 0              \* write cursors lost -> must re-advertise
    /\ creditSent'= 0
    /\ sndCursor' = lowestRetained \* write cursor lost; Handshake re-derives it
    /\ replayMark'= 0
    (* The drain queue is the one receiver-side buffer the design REQUIRES to be
       durable, because ack_safe = next_received is justified by "drain buffers
       are inside the image".  DrainQueueVolatile = TRUE tests that requirement
       by destroying it with the connection. *)
    /\ drainQ'    = IF DrainQueueVolatile THEN << >> ELSE drainQ
    /\ UNCHANGED << posted, nextSend, retained, lowestRetained, sndCreditLimit >>
    /\ UNCHANGED << consumed, nextReceived, ackSafe >>

(* Restore -- the image is restored and the peer is running again.  Abstracts
   the whole of contract 3 (lease, commit_restore, incarnation bump, and the
   directory-driven discovery that the peer moved) into one fair step. *)
Restore ==
    /\ linkState = "DEAD"
    /\ linkState' = "REPAIRING"
    /\ UNCHANGED << sndVars, netVars, rcvVars, freezes >>

(* Handshake -- exchange (next_send, next_expected, ack_safe, lowest_retained)
   per direction, validate, then set up the replay suffix.
   Three impossible states abort LOUDLY rather than proceed:
     - peer claims next_received > our next_send  (received what we never sent)
     - peer needs frames older than our lowest_retained (we pruned too far)
     - peer's ack_safe > its own next_received  (malformed watermarks)
   In the correct configuration none of these can ever fire; NoAbort checks
   that.  In the broken configuration the second one is exactly how the loss
   surfaces to the operator. *)
Handshake ==
    /\ linkState = "REPAIRING"
    /\ IF \/ nextReceived > nextSend
          \/ nextReceived < lowestRetained
          \/ ackSafe > nextReceived
       THEN /\ linkState' = "ABORTED"
            /\ UNCHANGED << sndVars, netVars, rcvVars, freezes >>
       ELSE /\ linkState'      = "UP"
            /\ lowestRetained' = Max(lowestRetained, ackSafe)
            /\ retained'       = { s \in retained : s >= ackSafe }
            /\ sndCreditLimit' = Max(sndCreditLimit, CreditLimit)
            /\ sndCursor'      = nextReceived   \* replay from exactly here ...
            /\ replayMark'     = nextSend       \* ... up to exactly here
            /\ ackSent'        = ackSafe        \* the handshake conveys them
            /\ creditSent'     = CreditLimit
            /\ UNCHANGED << posted, nextSend >>
            /\ UNCHANGED << netVars, freezes >>
            /\ UNCHANGED << rcvParse, ackOut, drainQ, consumed, nextReceived,
                            ackSafe >>

(* Stuttering steps so that TLC's deadlock check reports only GENUINE
   deadlocks (a live link with work outstanding and nothing enabled). *)
Done       == Len(consumed) = MaxMsg /\ UNCHANGED vars
AbortedEnd == linkState = "ABORTED"  /\ UNCHANGED vars

Next ==
    \/ AppSend
    \/ Transmit
    \/ Replay
    \/ Deliver
    \/ Commit
    \/ EmitAck
    \/ TransmitAck
    \/ PruneRetention
    \/ AppConsume
    \/ Freeze
    \/ Restore
    \/ Handshake
    \/ Done
    \/ AbortedEnd

(* Fairness: every protocol-machinery action is weakly fair -- the progress
   engine is autonomous and services the link "while the application computes,
   with no dependence on the application's next FMI call" (spec line 219).
   AppSend is NOT fair (the application may simply stop sending) and Freeze is
   NOT fair (nothing forces a checkpoint). *)
Fairness ==
    /\ WF_vars(Transmit)
    /\ WF_vars(Replay)
    /\ WF_vars(Deliver)
    /\ WF_vars(Commit)
    /\ WF_vars(EmitAck)
    /\ WF_vars(TransmitAck)
    /\ WF_vars(PruneRetention)
    /\ WF_vars(AppConsume)
    /\ WF_vars(Restore)
    /\ WF_vars(Handshake)

Spec == Init /\ [][Next]_vars /\ Fairness

--------------------------------------------------------------------------------
(* ------------------------------ INVARIANTS ------------------------------ *)

TypeOK ==
    /\ IsSeqOf(posted, Seqs)          /\ Len(posted) <= MaxMsg
    /\ nextSend       \in SeqsPlus
    /\ retained       \subseteq Seqs
    /\ lowestRetained \in SeqsPlus
    /\ sndCursor      \in SeqsPlus
    /\ replayMark     \in 0..(MaxMsg + 1)
    /\ sndCreditLimit \in Credits
    /\ IsSeqOf(dataChan, Seqs)        /\ Len(dataChan) <= W
    /\ IsSeqOf(ackChan, CtrlFrame)    /\ Len(ackChan)  <= 1
    /\ IsSeqOf(rcvParse, Seqs)        /\ Len(rcvParse) <= 1
    /\ IsSeqOf(ackOut, CtrlFrame)     /\ Len(ackOut)   <= 1
    /\ IsSeqOf(drainQ, Seqs)          /\ Len(drainQ)   <= Reserve
    /\ IsSeqOf(consumed, Seqs)        /\ Len(consumed) <= MaxMsg
    /\ nextReceived \in SeqsPlus
    /\ ackSafe      \in SeqsPlus
    /\ ackSent      \in 0..(MaxMsg + 1)
    /\ creditSent   \in Credits
    /\ linkState    \in {"UP", "DEAD", "REPAIRING", "ABORTED"}
    /\ freezes      \in 0..MaxFreezes

(* ---- ordering invariant 1: retained copy before any byte on the wire ---- *)
OrderingInv1 ==
    /\ \A i \in 1..Len(dataChan) : dataChan[i] \in retained
    /\ \A i \in 1..Len(posted)   : posted[i] < nextSend

(* ---- ordering invariant 2: commit before the ACK becomes writable ------- *)
(* An ACK may only be issued for a durably-held frame.  Equivalently: every
   seq the sender has been TOLD it may prune is already in the CRIU image. *)
OrderingInv2 ==
    /\ \A s \in 1..(ackSafe - 1) : s \in DurableAtReceiver
    /\ ackSafe <= nextReceived

(* ---- ordering invariant 3: reclaim only on a validated cumulative ACK --- *)
OrderingInv3 ==
    /\ lowestRetained <= ackSafe                  \* never prune past ack_safe
    /\ retained = { s \in 1..(nextSend - 1) : s >= lowestRetained }

(* ======================= THE HEART OF THE DESIGN ======================== *)
(* The sender NEVER prunes a message the receiver does not durably hold.
   Together with the fact that Freeze destroys exactly the non-durable state,
   this is what makes an arbitrary-instant freeze sound.
   NOTE: OrderingInv2 /\ OrderingInv3 imply this; it is checked separately so
   that a violation is reported in the vocabulary that matters. *)
RetentionSafety ==
    \A s \in 1..(nextSend - 1) : s \in retained \/ s \in DurableAtReceiver

(* The safety half of the delivery obligation: a message whose send() returned
   is always somewhere -- retention, either wire, the parser, or the receiver.
   Strictly weaker than RetentionSafety (it counts volatile locations). *)
NoLossSafety ==
    \A s \in Range(posted) :
        \/ s \in retained
        \/ s \in Range(dataChan)
        \/ s \in Range(rcvParse)
        \/ s \in DurableAtReceiver

NoDuplication ==
    \A i, j \in 1..Len(consumed) : i /= j => consumed[i] /= consumed[j]

(* The consumed sequence is a prefix of the posted sequence: no reordering, no
   substitution, no gaps. *)
FIFO ==
    /\ Len(consumed) <= Len(posted)
    /\ \A i \in 1..Len(consumed) : consumed[i] = posted[i]

NoUnboundedBacklog == Len(drainQ) <= Reserve

(* A frame beyond the receiver's watermark would be the spec's FatalGap:
   "the sender pruned what we never got". *)
NoFatalGap == (rcvParse /= << >>) => Head(rcvParse) <= nextReceived

(* Sharper than NoFatalGap, and a claim about the design rather than a safety
   requirement: because Handshake replays EXACTLY [next_received, next_send),
   a duplicate never actually reaches the receiver on a single directed link.
   If this holds, the implementation's dedup branch is defensive dead code. *)
DedupUnreachable == (rcvParse /= << >>) => Head(rcvParse) = nextReceived

(* The sender never transmits beyond what the receiver has room for. *)
CreditSound == sndCreditLimit <= CreditLimit

(* No impossible state was ever observed. *)
NoAbort == linkState /= "ABORTED"

Safety ==
    /\ TypeOK /\ OrderingInv1 /\ OrderingInv2 /\ OrderingInv3
    /\ RetentionSafety /\ NoLossSafety /\ NoDuplication /\ FIFO
    /\ NoUnboundedBacklog /\ NoFatalGap /\ CreditSound

--------------------------------------------------------------------------------
(* ------------------------------- LIVENESS -------------------------------- *)

(* DeliveryObligation: every message for which AppSend RETURNED is eventually
   consumed by the receiving application -- across an arbitrary number of
   freezes, restores, handshakes and replays. *)
DeliveryObligation ==
    \A s \in Seqs : (s \in Range(posted)) ~> (s \in Range(consumed))

(* Retention is eventually reclaimed: the sender does not leak buffers. *)
RetentionDrains ==
    \A s \in Seqs : (s \in retained) ~> (s \notin retained)

--------------------------------------------------------------------------------
(* --------------------------- NON-TRIVIALITY ------------------------------ *)
(* "No error found" is worthless if the interesting region is unreachable.
   These are checked as INVARIANTS THAT MUST BE VIOLATED.  TLC refuting them is
   the evidence that the model actually gets to the hard states. *)

(* Refutation shows: the model reaches a state where the maximum number of
   freezes has occurred, every message has been consumed by the application,
   retention is fully reclaimed, and the link is back UP.  That state is only
   reachable via Freeze -> Restore -> Handshake -> Replay -> Commit. *)
Probe_CompletesAcrossEveryFreeze ==
    ~ /\ freezes = MaxFreezes
      /\ Len(consumed) = MaxMsg
      /\ retained = { }
      /\ linkState = "UP"

(* Refutation shows: the window really does carry more than one frame in
   flight, so Freeze can and does destroy multiple frames at once. *)
Probe_WindowIsExercised == Len(dataChan) < W

(* Refutation shows: a Freeze happens with data already committed at the
   receiver AND unacked data outstanding at the sender -- i.e. the freeze lands
   in the middle of the stream, not at a convenient boundary. *)
Probe_FreezeMidStream ==
    ~ /\ freezes > 0
      /\ nextReceived > 1
      /\ retained /= { }

(* The spec (line 187) asserts: "All four freeze positions relative to
   drain-commit and ACK-write were enumerated; none produces loss or
   duplication."  `Freeze` is enabled in EVERY non-ABORTED state below the
   bound, so showing each of these four states is reachable shows that a freeze
   at that position is in the checked state space.  All four must be REFUTED. *)
Probe_FreezePos1_FrameOnTheWire   == ~ (Len(dataChan) > 0)
Probe_FreezePos2_ParsedNotCommitted == ~ (Len(rcvParse) > 0)
Probe_FreezePos3_CommittedNotAcked ==
    ~ (Len(drainQ) > 0 /\ ackSafe > ackSent /\ ackChan = << >>)
Probe_FreezePos4_AckOnTheWire     == ~ (Len(ackChan) > 0)

================================================================================
