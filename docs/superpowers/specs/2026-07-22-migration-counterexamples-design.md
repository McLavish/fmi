# Migration Protocol Counterexamples Design

## Purpose

Build a diagnostic suite that searches for and reproduces violations of FMI semantics across a
transparent-migration epoch cut. The checkpointing layer must preserve the expressivity of FMI:
arbitrary valid point-to-point schedules remain supported, including schedules whose matching
`send` and `recv` calls occur at different local operation indices and schedules with completed
eager sends still queued at the cut.

The existing `tests/migration_p2p_cut_counterexample.cpp` remains the narrative minimal example.
The new suite broadens it into a reusable corpus and a bounded explorer so protocol changes can be
evaluated against safety and liveness properties instead of only aligned collective workloads.

## Operating Assumptions

- Exactly one orchestrator serializes migration requests and epoch promotion attempts.
- The orchestrator, ranks, Redis, TCPunch, hosts, and network do not crash or partition.
- Migration may be requested at any FMI operation boundary or while another rank is inside a
  valid FMI operation.
- Rank programs may use any valid mix and ordering of point-to-point and collective operations.
- A completed `send` creates a delivery obligation even when its matching `recv` has not started.
- Per-source FIFO, exactly-once delivery, collective synchronization, and eventual progress are
  required to survive migration just as they do without migration.
- Tests may use `state_transfer="criu"` to park and resume a target in place. This is a test
  technique only; it does not invoke `criu` and does not weaken the semantic requirements.
- No production protocol fix is part of this work. The deliverable is counterexamples,
  classification, and reproducible evidence.

## Approaches Considered

### Independent handwritten executables

One source file per schedule gives excellent narrative explanations and naturally isolates hangs.
It also repeats hundreds of lines of synchronization, cleanup, promotion retry, timeout, and
result-classification code. This remains useful for the original minimal example but does not
scale to a broad corpus.

### Table-driven real-FMI runner

A shared operation DSL describes per-rank programs, scheduler phases, the migration request, and
the expected reference outcome. The runner executes each schedule without migration and with
migration, using the real Communicator, ControlPlane, and transport. It is faithful and makes many
curated schedules economical. This is the primary approach.

### Bounded protocol explorer

A small in-repository model enumerates short two- and three-rank point-to-point programs, request
positions, target ranks, and legal scheduler choices. It finds and minimizes classes of traces much
faster than live integration tests, but an abstract result alone is not accepted as evidence because
the model can drift from the Redis scripts or transport behavior.

The selected design combines the table-driven runner with the bounded explorer. Minimized model
traces that represent distinct failures are added to the real-FMI corpus and retain the same stable
scenario identifier.

## Architecture

### Shared scenario representation

A test-only scenario layer represents:

- world size and migration target set;
- backend (`Redis` or `Direct`);
- tagged payloads with explicit byte widths;
- each rank's ordered `send`, `recv`, `barrier`, and scheduler-marker actions;
- the point at which the sole orchestrator requests migration;
- optional orchestrator actions such as expanding the pending set;
- the semantic oracle and maximum duration.

Every send has a stable payload identity. A receive records both the expected identity and the
observed bytes, allowing the suite to distinguish loss, substitution, reordering, duplication, and
partial overwrite. Scheduler markers are test synchronization only and never count as FMI
operations.

The scenario representation is test code, not a new public FMI API. It is deliberately small and
only includes operations needed by a counterexample in the initial corpus.

### Real-FMI execution engine

For each scenario the parent runner executes two isolated child cases:

1. **Baseline:** run the schedule without migration and require the declared FMI result.
2. **Migrated:** run the identical rank program and scheduler, insert the migration action, and
   compare the result with the baseline obligations.

A baseline failure invalidates the scenario; it is never reported as a protocol counterexample.
The migrated execution uses one `FMI::FT::ControlPlane`, requests one cut at a time unless the
scenario is explicitly testing pending-set expansion, and retries `promote_epoch` while the gate is
temporarily closed.

Each scenario runs in a subprocess. The parent applies a deadline, kills only that child on expiry,
records the last phase reached, and continues through the remaining corpus. Rank workers catch
`std::exception`, `std::string`, and unknown exceptions. Redis metadata and CRIU registry state use
unique communicator names and are cleared before and after every case.

Redis scenarios use rank threads because the backend is process-safe in the existing harness.
Direct scenarios use separate rank processes because TCPunch has process-global pairing state.
Direct cases report an infrastructure skip when `tcpunchd` is unavailable; Redis is the required,
deterministic default for the complete corpus.

### Bounded explorer

The explorer models:

- eager tagged sends and per-link FIFO channel queues;
- blocking receives;
- per-rank completed-operation counters;
- the current `observe_operation` first-proposer cut rule;
- target and survivor parking;
- the current promotion gates;
- Redis reconfiguration, which abandons old epoch queues and resets link counters;
- Direct reconfiguration, which drops moved-rank links and keeps survivor links.

It enumerates programs of bounded length for worlds of two and three ranks, all single-target
migration placements, and legal operation interleavings. The reference transition preserves
channel state across the checkpoint; the implementation transition applies the backend's current
epoch-cut behavior. States are deduplicated, and failing traces are minimized by removing
unnecessary actions while retaining the violation.

Explorer results are grouped by semantic classification and normalized trace shape. It prints a
short schedule, target/backend, expected result, actual result, and the stable identifier of an
equivalent real-FMI corpus case when one exists.

## Initial Counterexample Corpus

The initial real-FMI corpus contains at least these distinct schedules:

1. `queued_message_lost`: a completed send is unmatched at the cut and no later send aliases it;
   the post-cut receive times out.
2. `queued_message_substituted`: the existing A/C example; a later message silently occupies the
   abandoned message's queue position.
3. `partially_drained_fifo_suffix`: consume the first of two old messages, cut with the second
   outstanding, then demonstrate that only the undrained suffix is lost or substituted.
4. `multi_message_backlog_shift`: leave multiple old messages queued and show the entire post-cut
   receive stream is shifted.
5. `variable_size_partial_overwrite`: a differently sized later payload partially fills the receive
   buffer while the backend reports success.
6. `bidirectional_backlog`: both endpoints have valid outstanding delivery obligations at the cut.
7. `three_rank_ring_backlog`: every rank has an outstanding ring message, demonstrating that equal
   coarse operation signatures do not imply empty channels.
8. `unrelated_rank_migration`: migrate rank 2 while an outstanding message exists only on the
   survivor 0-to-1 link.
9. `future_matching_send_after_cut`: a receive below the proposed cut depends on a send beyond the
   sender's cut, so healthy ranks can never open the promotion gate.
10. `direct_moved_link_backlog`: moving one endpoint drops unread bytes on an established Direct
    stream.
11. `direct_survivor_link_crosses_epoch`: migrating an unrelated rank keeps old bytes on a survivor
    stream, exposing the conflict between FIFO preservation and the literal epoch-fencing claim.
12. `stale_barrier_object`: an old ClientServer barrier object is counted by suffix in a new epoch,
    allowing a fully aligned collective to return before all current members arrive.
13. `pending_set_expands_after_park`: adding a parked survivor to the target set leaves it unable to
    observe its new target status or mark itself quiesced.
14. `failed_operation_advances_boundary`: a caught operation exception increments the completed
    boundary and permits a cut over unequal successful histories.
15. `promotion_before_full_membership`: promotion checks registered members but does not establish
    that the epoch contains the configured world, allowing a cut to pass before a healthy late
    member joins.
16. `single_rank_self_send`: when self-send is supported by the public API, reduce the queued-message
    violation to one rank and eliminate inter-rank timing as a possible explanation.

Cases that exercise two manifestations of one root cause remain separate when they establish a
different externally visible violation or backend contract. Merely changing the target rank or
payload value does not justify another curated case; those permutations belong in the explorer.

## Result Classification and Exit Contract

The runner assigns one primary outcome to every migrated execution:

- `preserved`: all baseline semantic obligations still hold;
- `wrong_payload`: a receive consumes a later or unrelated send;
- `lost_message`: a completed send can no longer be received;
- `reordered_or_duplicated`: the receive history is not the baseline FIFO history;
- `partial_payload`: a receive reports success with bytes from the wrong-sized message;
- `premature_collective`: a collective returns without all current participants;
- `promotion_stuck`: the cut cannot become promotable although all components remain healthy;
- `operation_stuck`: a rank cannot complete the valid operation needed to reach the cut;
- `setup_error`: the baseline, configuration, or required infrastructure is invalid.

The parent always runs every selected case and prints an aggregate table. Exit status `0` means no
selected semantic counterexample reproduced, `2` means at least one semantic counterexample
reproduced, `1` means a setup or harness error prevented a valid conclusion, and `124` is reserved
for an uncontained top-level watchdog expiry. Known-bad cases remain opt-in and are not registered
as normal passing Boost tests while the protocol is unresolved.

## User Interface and Build Integration

The suite adds one opt-in executable alongside the existing narrative executable. Its command-line
surface is:

- no arguments: run the complete Redis corpus plus available Direct cases;
- `--list`: list scenario identifiers, backend, and intended property;
- `--case <identifier>`: run one curated scenario;
- `--backend redis|direct`: filter the real-FMI corpus;
- `--explore`: run the bounded model explorer and print minimized traces.

`tests/CMakeLists.txt` builds the executable only when the dependencies already required by the
current counterexample are enabled. It is not added to CTest or `Boost_Tests_run` until the protocol
is fixed and the oracle can be inverted into ordinary green regression tests.

## Planned File Responsibilities

- `tests/migration_p2p_cut_counterexample.cpp`: retain the original narrative proof unchanged
  except for small reuse adjustments if needed.
- `tests/migration_counterexample_scenarios.h`: test-only operation, scenario, result, and
  classification types.
- `tests/migration_counterexample_scenarios.cpp`: curated scenario definitions and stable IDs.
- `tests/migration_counterexample_runner.cpp`: CLI, subprocess isolation, real FMI execution,
  orchestration, watchdogs, aggregation, and exit contract.
- `tests/migration_cut_model.cpp`: bounded abstract state machine, exploration, minimization, and
  trace rendering.
- `tests/CMakeLists.txt`: opt-in target wiring.

No production header or source file is modified by this counterexample task.

## Verification

Verification requires fresh evidence for both the harness and the counterexamples:

1. Build the new target with Redis and CRIU support enabled.
2. Run `--list` and confirm every stable corpus identifier appears once.
3. Run each Redis case independently; require its baseline to pass and record the migrated
   classification.
4. Run the complete Redis corpus; require aggregation to continue after timeout-classified cases
   and exit `2` while known violations reproduce.
5. Run the bounded explorer twice and require deterministic normalized output.
6. Replay at least one minimized trace from every explorer classification through the real-FMI
   corpus, or explicitly report that no faithful live replay exists yet.
7. Start `tcpunchd`, run the Direct cases, and distinguish semantic outcomes from rendezvous setup
   failures.
8. Run the existing `fmi_migration_p2p_cut_counterexample` and the focused fault-tolerance Redis
   tests to ensure the new harness has not altered the original evidence or ordinary test code.
9. Run the full build and test binary in the configured environment, reporting any infrastructure
   skips separately from failures.

The implementation is complete only when every curated schedule has a valid no-migration baseline,
all child processes are bounded and reaped, explorer output is deterministic, and the aggregate
report makes each reproduced protocol violation independently understandable.
