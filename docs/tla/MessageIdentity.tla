-------------------------------- MODULE MessageIdentity --------------------------------
(***************************************************************************)
(* FMI migration protocol v2 -- NORMATIVE CONTRACT 1 (message identity).   *)
(*                                                                         *)
(* Source of truth:                                                        *)
(*   docs/superpowers/specs/2026-07-27-sequenced-incarnation-links-design.md*)
(*   section "Normative contract 1 -- message identity".                   *)
(*                                                                         *)
(* WHAT THIS MODULE IS FOR                                                 *)
(*                                                                         *)
(* It answers exactly one question: *which envelope fields are necessary*  *)
(* to make silent substitution impossible on the Direct (unframed TCP)     *)
(* transport when the two ranks run DIVERGENT programs.                    *)
(*                                                                         *)
(* Every receive is classified as exactly one of                           *)
(*                                                                         *)
(*   "Correct"             the frame the receiver's program intended,      *)
(*   "SilentSubstitution"  a different frame accepted as if correct,       *)
(*   "LoudAbort"           mismatch detected; the job fails                *)
(*                         (FMI: Utils::IdentityMismatch poisons the       *)
(*                          communicator).                                 *)
(*                                                                         *)
(* and the envelope is parameterised by CONSTANT EnvelopeMode, so the same *)
(* module is model-checked with progressively richer envelopes.            *)
(*                                                                         *)
(* WHAT IS MODELLED, FAITHFULLY, FROM THE C++                              *)
(*                                                                         *)
(* N = 2 ranks over the two directed pairs 0->1 and 1->0.  The per-rank    *)
(* frame patterns are read off src/comm/PeerToPeer.cpp at num_peers = 2:   *)
(*                                                                         *)
(*   send/recv    PeerToPeer.cpp:6-12    one P2P frame, sender -> dest.    *)
(*   bcast(0)     PeerToPeer.cpp:14-27   rounds = ceil(log2 2) = 1, so     *)
(*                                       exactly one 1-byte frame 0->1.    *)
(*   barrier      PeerToPeer.cpp:29-33   a 1-byte allreduce with a         *)
(*                                       {commutative,associative} nop, so *)
(*                                       left_to_right is FALSE and it     *)
(*                                       takes allreduce_no_order          *)
(*                                       (PeerToPeer.cpp:86-94).  At N=2,  *)
(*                                       PeerToPeer.cpp:108-118 gives      *)
(*                                       rank0 peer = 0 XOR 1 = 1 > 0 ->   *)
(*                                       RECV then SEND, and rank1         *)
(*                                       peer = 0 < 1 -> SEND then RECV.   *)
(*                                       So a barrier is two frames:       *)
(*                                       1->0 first, then 0->1.            *)
(*   reduce(0)    PeerToPeer.cpp:35-42   left_to_right = !(comm && assoc). *)
(*                                       At N=2 BOTH branches emit exactly *)
(*                                       one frame 1->0 of the same length *)
(*                                       (reduce_ltr -> gather, rank1      *)
(*                                       sends buf_len = 1*len; and        *)
(*                                       reduce_no_order, rank1 sends      *)
(*                                       sendbuf.len).  The two branches   *)
(*                                       are therefore distinguishable     *)
(*                                       ONLY by the commutative/          *)
(*                                       associative flags in the          *)
(*                                       envelope -- which is why the spec *)
(*                                       puts them there.  Alphabet symbol *)
(*                                       "reduce" is {comm,assoc} = TRUE,  *)
(*                                       "reduce_nc" is both FALSE.        *)
(*                                                                         *)
(* GROUND TRUTH -- how "Correct" is defined, independently of the envelope *)
(*                                                                         *)
(* This definition must NOT be "whatever the envelope checks", or the      *)
(* FullTuple result would be vacuous.  It is derived from FMI's operation  *)
(* contract instead:                                                       *)
(*                                                                         *)
(*   P2P lane:        the j-th p2p recv on a directed pair is intended to  *)
(*                    consume the j-th p2p send on that pair (FIFO).       *)
(*   COLLECTIVE lane: all ranks must issue the same sequence of            *)
(*                    collectives, so collective #c on rank r and          *)
(*                    collective #c on rank s denote the SAME logical      *)
(*                    operation instance.  A frame is correctly delivered  *)
(*                    iff sender and receiver agree on which operation     *)
(*                    instance they are in (collective_index), which       *)
(*                    operation it is (op_kind, root), which algorithm     *)
(*                    branch it selected (commutative/associative), and    *)
(*                    the payload length.                                  *)
(*                                                                         *)
(* Honest consequence: because the ground-truth identity is exactly the    *)
(* FullTuple field set, "FullTuple => no silent substitution" is true by   *)
(* construction.  The load-bearing results of this module are therefore    *)
(* NOT that one, but:                                                      *)
(*   (a) the three NECESSITY results -- OrdinalOnly, LaneOnly and          *)
(*       LaneAndIndex each admit a silent substitution, with a concrete    *)
(*       machine-found trace;                                              *)
(*   (b) NoFalseAbort -- FullTuple validation never rejects a well-formed  *)
(*       program pair (this one is genuinely not free); and                *)
(*   (c) the LoudAbort witness -- the check actually FIRES rather than     *)
(*       silently hanging.                                                 *)
(*                                                                         *)
(* DELIBERATELY ABSTRACTED AWAY -- read this before trusting the result    *)
(*                                                                         *)
(*  1. MIGRATION.  There is no epoch, no incarnation, no checkpoint, no    *)
(*     counter reset.  One job lifetime only.  Consequence: message_id     *)
(*     never discriminates anything here, because with a lossless FIFO     *)
(*     link both endpoints count the same lane identically.  That is the   *)
(*     spec's own point ("the per-lane message_id is a FIFO position, so   *)
(*     it cannot detect reordering").  message_id's *job-lifetime, never   *)
(*     reset* property matters against the ClientServer counter reset      *)
(*     (spec, "Why the current protocol is unsound", bullet 1), which is a *)
(*     KEYED store, not a FIFO stream, and is therefore a different        *)
(*     transport model -- out of scope here.                               *)
(*  2. CLIENTSERVER.  Only the Direct case is modelled.  The spec records  *)
(*     that on ClientServer the bcast/barrier collision cannot fire        *)
(*     because op-kind-qualified key names already separate those          *)
(*     namespaces; the collision is specific to the unframed Direct        *)
(*     transport, where matching is socket + arrival order.                *)
(*  3. THE TRANSPORT.  Links are reliable, ordered, unbounded FIFO.  No    *)
(*     loss, no reordering, no duplication, no connection break, no        *)
(*     window/credit/retention.  That is all Axis C (contract 2) and is    *)
(*     the subject of a different module.                                  *)
(*  4. BYTES.  Payload values are not modelled, only their length.  A      *)
(*     silent substitution here means "the receiver committed a frame      *)
(*     belonging to a different logical operation", not a specific wrong   *)
(*     numeric answer.                                                     *)
(*  5. FRAGMENTATION.  fragment_index and partial writes are omitted; at   *)
(*     N=2 every operation in the alphabet emits at most one frame per     *)
(*     directed pair.                                                      *)
(*  6. wire_version and transport_seq are job-wide / Axis C respectively   *)
(*     and are not modelled.                                               *)
(*  7. root NECESSITY IS NOT EXHIBITED AT N=2.  For root to discriminate,  *)
(*     two collectives would have to agree on lane, op_kind,               *)
(*     collective_index and length while disagreeing on root AND produce   *)
(*     a frame on the same directed pair.  At N=2 the only bcast in which  *)
(*     rank1 receives is bcast(root=0), so no such pair exists.  root is   *)
(*     carried and validated per the spec, but this module does not prove  *)
(*     it necessary.  That needs N >= 3.                                   *)
(*  8. N=2 UNDERSTATES THE reduce FLAG DIVERGENCE.  At N=2 reduce_ltr and  *)
(*     reduce_no_order emit the same pattern and compute the same result,  *)
(*     so a flag divergence is *latent*: the model correctly reports the   *)
(*     two ranks as executing different logical operations, but the wrong  *)
(*     answer only materialises at N >= 3 where gather-based ltr and the   *)
(*     binomial tree genuinely differ.                                     *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS
    EnvelopeMode,   \* "OrdinalOnly" | "LaneOnly" | "LaneAndIndex" | "FullTuple"
    Alphabet,       \* set of program operations each rank may issue
    MaxProgLen      \* bound on the length of each rank's program

Ranks == {0, 1}
Other(r) == 1 - r

\* bcast/reduce root is pinned to 0; see abstraction note 7.
Root0 == 0

Lanes    == {"P2P", "COLLECTIVE"}
OpKinds  == {"send", "bcast", "barrier", "reduce"}
Verdicts == {"Correct", "SilentSubstitution", "LoudAbort"}

IsCollective(o) == o \in {"bcast", "barrier", "reduce", "reduce_nc"}

MaxActions == 2 * MaxProgLen        \* barrier contributes two actions

Min(S) == CHOOSE x \in S : \A y \in S : x <= y
Remove(s, i) == SubSeq(s, 1, i-1) \o SubSeq(s, i+1, Len(s))

(***************************************************************************)
(* The envelope.  `mid` is the per-lane per-directed-pair message_id.      *)
(* `len` is total_length.  `comm`/`assoc` are the reduction function's     *)
(* commutative/associative flags, which the spec requires in the envelope  *)
(* and validated per collective (not in a job-wide fingerprint).           *)
(***************************************************************************)
IdentSet == [ lane  : Lanes,
              opk   : OpKinds,
              ci    : 0..MaxProgLen,
              root  : Ranks,
              len   : {1},
              comm  : BOOLEAN,
              assoc : BOOLEAN,
              mid   : 0..MaxProgLen ]

AllFields == {"lane", "opk", "ci", "root", "len", "comm", "assoc", "mid"}

(***************************************************************************)
(* Which fields the receiver can actually compare, per envelope mode.      *)
(*                                                                         *)
(* OrdinalOnly models TODAY'S Direct: src/comm/Direct.cpp:28-72 sends a    *)
(* raw partial-write loop and reads exactly buf.len bytes known a priori.  *)
(* There is no tag, sequence or discriminator in the bytes at all, so the  *)
(* "ordinal" is nothing but arrival position -- it agrees by construction  *)
(* and carries zero information.  The checked set is therefore EMPTY: the  *)
(* receiver accepts whatever byte the socket hands it.                     *)
(***************************************************************************)
CheckedFields ==
    CASE EnvelopeMode = "OrdinalOnly"  -> {}
      [] EnvelopeMode = "LaneOnly"     -> {"lane", "mid"}
      [] EnvelopeMode = "LaneAndIndex" -> {"lane", "mid", "ci"}
      [] EnvelopeMode = "FullTuple"    -> AllFields

\* Lane demultiplexing ("Separate FIFO and drain queue per lane", contract 1)
\* is only possible once a lane byte exists on the wire.
HasLaneDemux == EnvelopeMode # "OrdinalOnly"

Agree(f, e, flds) == \A x \in flds : f[x] = e[x]
Accepted(f, e)    == Agree(f, e, CheckedFields)
IsIntended(f, e)  == Agree(f, e, AllFields)

Verdict(f, e) ==
    IF ~Accepted(f, e)    THEN "LoudAbort"
    ELSE IF IsIntended(f, e) THEN "Correct"
    ELSE "SilentSubstitution"

(***************************************************************************)
(* Programs, and the action lists they compile to.                         *)
(*                                                                         *)
(* An "action" is one frame-level send or recv, tagged with the identity   *)
(* the RAII operation scope would publish for it (contract 1, "Where the   *)
(* identity is produced": the scope lives on Communicator, above the       *)
(* Channel interface, so collectives and application p2p share one socket  *)
(* but not one identity).                                                  *)
(***************************************************************************)
Act(k, ln, o, c, rt, cm, as) ==
    [kind |-> k, lane |-> ln, opk |-> o, ci |-> c, root |-> rt,
     len |-> 1, comm |-> cm, assoc |-> as]

ActionsOfOp(r, o, c) ==
    CASE o = "send"    -> << Act("S", "P2P", "send", 0, Other(r), TRUE, TRUE) >>
      \* the sender writes root = dest, so the receiver expects root = itself
      [] o = "recv"    -> << Act("R", "P2P", "send", 0, r, TRUE, TRUE) >>
      [] o = "bcast"   -> IF r = Root0
                          THEN << Act("S", "COLLECTIVE", "bcast", c, Root0, TRUE, TRUE) >>
                          ELSE << Act("R", "COLLECTIVE", "bcast", c, Root0, TRUE, TRUE) >>
      \* barrier: rank0 recv-then-send, rank1 send-then-recv (PeerToPeer.cpp:108-118)
      [] o = "barrier" -> IF r = 0
                          THEN << Act("R", "COLLECTIVE", "barrier", c, Root0, TRUE, TRUE),
                                  Act("S", "COLLECTIVE", "barrier", c, Root0, TRUE, TRUE) >>
                          ELSE << Act("S", "COLLECTIVE", "barrier", c, Root0, TRUE, TRUE),
                                  Act("R", "COLLECTIVE", "barrier", c, Root0, TRUE, TRUE) >>
      \* reduce(root=0): one frame 1->0 in both algorithm branches
      [] o = "reduce"  -> IF r = Root0
                          THEN << Act("R", "COLLECTIVE", "reduce", c, Root0, TRUE, TRUE) >>
                          ELSE << Act("S", "COLLECTIVE", "reduce", c, Root0, TRUE, TRUE) >>
      [] o = "reduce_nc" -> IF r = Root0
                          THEN << Act("R", "COLLECTIVE", "reduce", c, Root0, FALSE, FALSE) >>
                          ELSE << Act("S", "COLLECTIVE", "reduce", c, Root0, FALSE, FALSE) >>

\* collective_index is per-Communicator and counts collectives only
\* (contract 1: "Zero and unused on the P2P lane").
CiBefore(p, k) == Cardinality({ j \in 1..(k-1) : IsCollective(p[j]) })

RECURSIVE Compile(_, _, _)
Compile(r, p, k) ==
    IF k > Len(p) THEN << >>
    ELSE ActionsOfOp(r, p[k], CiBefore(p, k)) \o Compile(r, p, k+1)

AllProgs == UNION { [1..n -> Alphabet] : n \in 1..MaxProgLen }

(***************************************************************************)
(* A sufficient condition for a well-formed program pair at N=2: same      *)
(* length, matched p2p, and identical collectives at identical positions.  *)
(* Used only by the NoFalseAbort invariant and by the "aligned" config.    *)
(***************************************************************************)
Compatible(p, q) ==
    /\ Len(p) = Len(q)
    /\ \A i \in 1..Len(p) :
         \/ (p[i] = q[i] /\ IsCollective(p[i]))
         \/ (p[i] = "send" /\ q[i] = "recv")
         \/ (p[i] = "recv" /\ q[i] = "send")

-----------------------------------------------------------------------------

VARIABLES
    prog,      \* prog[r]: rank r's program, a sequence over Alphabet. Fixed at Init.
    pc,        \* pc[r]: index of rank r's next frame-level action
    chan,      \* chan[r]: FIFO of frames in flight TOWARDS rank r (one TCP stream)
    lastRecv,  \* the most recent receive: {rank, expected, got, verdict}
    aborted    \* TRUE once a LoudAbort has fired; the job is dead

vars == << prog, pc, chan, lastRecv, aborted >>

NoRecv == [rank |-> 9, verdict |-> "none", got |-> "none", expected |-> "none"]

Acts(r) == Compile(r, prog[r], 1)
Cur(r)  == Acts(r)[pc[r]]
Live(r) == pc[r] <= Len(Acts(r))

\* message_id: per-lane, per-directed-pair ordinal, derived from how many
\* same-lane sends (resp. receives) this rank has already performed.
CountBefore(r, k, ln) ==
    Cardinality({ i \in 1..(pc[r]-1) : Acts(r)[i].kind = k /\ Acts(r)[i].lane = ln })

ToIdent(a, m) ==
    [lane |-> a.lane, opk |-> a.opk, ci |-> a.ci, root |-> a.root,
     len |-> a.len, comm |-> a.comm, assoc |-> a.assoc, mid |-> m]

EmitFrame(r)   == ToIdent(Cur(r), CountBefore(r, "S", Cur(r).lane))
ExpectFrame(r) == ToIdent(Cur(r), CountBefore(r, "R", Cur(r).lane))

\* Which queued frame this receive may consume.  With a lane byte the
\* receiver demultiplexes into per-lane drain queues and skips frames of the
\* other lane; without one (raw Direct) it must take the head of the socket.
Avail(r) ==
    IF ~HasLaneDemux
    THEN IF Len(chan[r]) > 0 THEN {1} ELSE {}
    ELSE LET L == ExpectFrame(r).lane
             S == { i \in 1..Len(chan[r]) : chan[r][i].lane = L }
         IN IF S = {} THEN {} ELSE {Min(S)}

-----------------------------------------------------------------------------

Init ==
    /\ prog \in [Ranks -> AllProgs]
    /\ pc = [r \in Ranks |-> 1]
    /\ chan = [r \in Ranks |-> << >>]
    /\ lastRecv = NoRecv
    /\ aborted = FALSE

Emit(r) ==
    /\ ~aborted
    /\ Live(r)
    /\ Cur(r).kind = "S"
    /\ chan' = [chan EXCEPT ![Other(r)] = Append(@, EmitFrame(r))]
    /\ pc' = [pc EXCEPT ![r] = @ + 1]
    /\ UNCHANGED << prog, lastRecv, aborted >>

\* A receive whose lane queue is empty simply does not fire: the rank blocks,
\* exactly as Direct::recv_object blocks.  Reachable deadlock is intended and
\* is why the configs set CHECK_DEADLOCK FALSE.
Deliver(r) ==
    /\ ~aborted
    /\ Live(r)
    /\ Cur(r).kind = "R"
    /\ Avail(r) # {}
    /\ LET i == Min(Avail(r))
           f == chan[r][i]
           e == ExpectFrame(r)
           v == Verdict(f, e)
       IN /\ chan' = [chan EXCEPT ![r] = Remove(@, i)]
          /\ pc' = [pc EXCEPT ![r] = @ + 1]
          /\ lastRecv' = [rank |-> r, verdict |-> v, got |-> f, expected |-> e]
          \* an IdentityMismatch unwinds through the communicator and kills
          \* the job; no rank makes further progress
          /\ aborted' = (v = "LoudAbort")
    /\ UNCHANGED prog

Next == \E r \in Ranks : Emit(r) \/ Deliver(r)

Fairness == WF_vars(Next)

Spec == Init /\ [][Next]_vars /\ Fairness

-----------------------------------------------------------------------------

TypeOK ==
    /\ prog \in [Ranks -> AllProgs]
    /\ pc \in [Ranks -> 1..(MaxActions + 1)]
    /\ \A r \in Ranks :
         /\ Len(chan[r]) <= MaxActions
         /\ \A i \in 1..Len(chan[r]) : chan[r][i] \in IdentSet
    /\ aborted \in BOOLEAN
    /\ \/ lastRecv = NoRecv
       \/ /\ lastRecv.rank \in Ranks
          /\ lastRecv.verdict \in Verdicts
          /\ lastRecv.got \in IdentSet
          /\ lastRecv.expected \in IdentSet

(***************************************************************************)
(* THE headline invariant.  Every receive resolves to Correct or           *)
(* LoudAbort; never to a different message accepted as if correct.         *)
(***************************************************************************)
NoSilentSubstitution == lastRecv.verdict # "SilentSubstitution"

(***************************************************************************)
(* Sharper variants, used to force TLC to exhibit one *specific* failure   *)
(* shape rather than whichever it reaches first.                           *)
(***************************************************************************)
\* p2p frame consumed as a collective fragment or vice versa
NoCrossLaneSubstitution ==
    ~ /\ lastRecv.verdict = "SilentSubstitution"
      /\ lastRecv.got.lane # lastRecv.expected.lane

\* two different collectives colliding: same lane, same length, same ordinal
NoCollectiveCollision ==
    ~ /\ lastRecv.verdict = "SilentSubstitution"
      /\ lastRecv.got.lane = "COLLECTIVE"
      /\ lastRecv.expected.lane = "COLLECTIVE"

\* the spec's "a bare per-communicator collective counter is also
\* insufficient": collective_index agrees but the operations differ
NoEqualIndexCollision ==
    ~ /\ lastRecv.verdict = "SilentSubstitution"
      /\ lastRecv.got.ci = lastRecv.expected.ci
      /\ lastRecv.got.lane = "COLLECTIVE"
      /\ lastRecv.expected.lane = "COLLECTIVE"

(***************************************************************************)
(* No false positives: a well-formed program pair must never be rejected.  *)
(* This one is NOT true by construction and is the reason the envelope     *)
(* cannot simply be "hash everything".                                     *)
(***************************************************************************)
NoFalseAbort ==
    Compatible(prog[0], prog[1]) => lastRecv.verdict \in {"none", "Correct"}

\* Deliberately-failing invariant used as a WITNESS that validation fires
\* at all under FullTuple, instead of hanging or accepting.
NoLoudAbort == lastRecv.verdict # "LoudAbort"

(***************************************************************************)
(* Liveness.  Every action strictly increases pc[0] + pc[1], so the state  *)
(* graph is finite and acyclic and every fair behaviour reaches a terminal *)
(* condition: both ranks finished, blocked in a receive, or aborted.  This *)
(* is easy but not vacuous -- it would catch a modelling error in which    *)
(* some action could fire forever.                                         *)
(***************************************************************************)
Stuck(r) == IF ~Live(r) THEN TRUE
            ELSE IF Cur(r).kind = "R" THEN Avail(r) = {} ELSE FALSE

Quiescent == aborted \/ (\A r \in Ranks : Stuck(r))

Terminates == <>Quiescent

(***************************************************************************)
(* Constraints used by the focused configs.                                *)
(***************************************************************************)
\* The spec's own motivating counterexample, verbatim:
\*     rank0: bcast(root=0); barrier()
\*     rank1: barrier();     bcast(root=0)
SpecCounterexample ==
    /\ prog[0] = << "bcast", "barrier" >>
    /\ prog[1] = << "barrier", "bcast" >>

AlignedOnly == Compatible(prog[0], prog[1])

=============================================================================
