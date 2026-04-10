# Implementation Plan: DST Phase 1.5+ — SimModel Oracle & Liveness

**Status**: proposed | **Author**: dst-phase1 session 4 | **Date**: 2026-04-10

## Overview

Phase 0-1 shipped a seeded fuzzer over the state machine (`simulation.rs`, 1000 commands) and Phase 1.5 added an in-memory `SimStorage` backing. Both are **panic detectors, not correctness checkers** — the whole-run test would not fail on #4566 without a dedicated dedup-layer assertion (see `research/dst_vs_tigerbeetle_vopr_review_041026.md:50-59`).

This plan adds the missing ingredient: an **observable-outcome oracle** that shadows what clients should see, plus a **liveness watchdog** that asserts the workload's intended outcomes are eventually realized. Together they turn the existing fuzzer into a proper VOPR-style simulator for Restate's observable contract.

Scope is staged in tiers so each commit is independently valuable and the next tier is justified by the bugs the previous tier finds.

## Why this design

Two prior research docs establish the architectural case and should be treated as canonical:

- `research/sim_model_complexity_analysis_041026.md` — why observable-outcome (FoundationDB pattern) is tractable while internal-state-shadow (naive VOPR-for-Restate) is not. Contains the three-tier breakdown this plan implements.
- `research/dst_vs_tigerbeetle_vopr_review_041026.md` — the VOPR gap analysis that originally flagged "SimModel oracle" as #1 roadmap item.

The short version: Restate's command surface is large (62+ commands, 16 lifecycle handlers, 13 storage tables) but its *observable contract* reduces to ~9 semantic invariants. The oracle shadows the contract, not the implementation. This scales.

## Desired End State

At the end of this plan, the simulation test suite:

1. Runs a workload-intent oracle that tracks what the generator submitted
2. Asserts per-step safety invariants (signals accounted for, no silent drops)
3. Asserts liveness via quiescent drain (every intended-to-complete invocation reaches terminal state)
4. Reproduces #4566 as a pure observable-outcome failure **without** naming dedup
5. Catches per-invocation staleness via a tick-based watchdog
6. Optionally (Tier 2) shadows key-value state, promises, virtual-object locks, and idempotency

Together this is ~280–1100 lines of new code depending on tier depth. Tier 1 is 1–2 days. Tier 2 is 1–2 weeks.

## Out of Scope

- **Internal-state shadow of every storage table** — explicitly rejected; would churn with every state machine change and is not needed (see research doc section "Model type A").
- **Full `run_inner` integration** — still Phase 2, requires entropy leak fixes first (see `research/dst_vs_tigerbeetle_vopr_review_041026.md:72-87`).
- **Invoker-DSL / simulated user code** — Tier 3, deferred until Tier 2 demonstrates value.
- **Multi-partition routing** — Tier 3 companion work; this plan is single-partition.
- **Virtual queue modeling** — explicitly excluded until the vqueue subsystem stabilizes; workload generator will not emit `VQWaitingToRunning`/`VQYieldRunning` at Tier 1/2.
- **Protocol V1 coverage** — model targets Journal V2 only; V1 constructs are skipped by the generator.
- **Wait-freedom, fairness, latency bounds, starvation-under-adversary** — not in the liveness contract being validated.
- **CI seed amplification** — still deferred to whenever the harness is stable enough.

## Technical Approach

### Tier 1 — Workload-intent oracle + quiescent-drain liveness (~280 lines total)

The minimum viable model. Tracks what the generator submitted, asserts it's accounted for.

#### Safety — per-step invariant

```rust
struct WorkloadModel {
    submitted: BTreeMap<InvocationId, InvocationIntent>,
}

struct InvocationIntent {
    signals_sent: Vec<(SignalId, PartitionProcessorRpcRequestId)>,
    responses_sent: Vec<(CompletionId, PartitionProcessorRpcRequestId)>,
    terminated: Option<TerminationFlavor>,
    fully_driven: bool,            // generator will send no more inputs
    fully_driven_at_step: Option<u64>,
    lost_leadership_replies: HashSet<PartitionProcessorRpcRequestId>,
}

fn assert_safety(model: &WorkloadModel, storage: &SimStorage, awaiting: &AwaitingSet) {
    for (iid, intent) in &model.submitted {
        for (sig, req_id) in &intent.signals_sent {
            let in_journal = storage.journal_has_signal(iid, sig);
            let pending = awaiting.contains(req_id);
            let retried = intent.lost_leadership_replies.contains(req_id);
            assert!(
                in_journal || pending || retried,
                "signal {sig:?} for {iid}: not in journal, not pending, not lost-leadership"
            );
        }
        // Same shape for responses, terminations, etc.
    }
}
```

Runs after every `sim_apply_envelope`. Replaces the current `check_invariants` (end-of-run only) at `simulation.rs:124-151`.

#### Liveness — quiescent drain (~80 lines on top of the safety model)

After the main workload, enter a "drain mode" that advances the clock and processes pending work but stops generating new commands:

```rust
async fn run_simulation<R: Rng>(rng: &mut R, seed: u64) {
    let mut model = WorkloadModel::default();
    let mut storage = SimStorage::default();
    let mut tick = 0u64;

    // Phase A: drive workload
    for _ in 0..WORKLOAD_STEPS {
        let cmd = gen_command(rng, &mut model, &storage);
        apply_and_update_model(&cmd, &mut storage, &mut model, &mut tick);
        assert_safety(&model, &storage, &awaiting);
    }

    // Phase B: quiescent drain
    model.mark_all_fully_driven(tick);
    for _ in 0..DRAIN_STEPS {
        tick += 1;
        advance_time_at_most(MAX_TIME_PER_TICK);
        apply_any_pending_actions(&mut storage, &mut model);
        assert_safety(&model, &storage, &awaiting);
    }

    // Liveness assertions
    assert_liveness_at_drain_end(&model, &storage, &awaiting);
}

fn assert_liveness_at_drain_end(
    model: &WorkloadModel,
    storage: &SimStorage,
    awaiting: &AwaitingSet,
) {
    for (iid, intent) in &model.submitted {
        if !intent.fully_driven { continue; }
        let status = storage.invocations.get(iid);
        assert!(
            matches!(status, Some(InvocationStatus::Completed(_) | InvocationStatus::Free) | None),
            "invocation {iid} was fully driven but is {status:?} at drain end"
        );
    }
    assert!(awaiting.is_empty(), "awaiting_rpc_actions not drained: {awaiting:?}");
    assert!(storage.outbox.is_empty(), "outbox not drained: {} entries", storage.outbox.len());
}
```

**What this catches**: #4566-as-safety, #4566-as-liveness, virtual object inbox head stuck, dropped response sinks, stuck promise waiters, outbox non-drain, reciprocal leaks — all from a 280-line model, no knowledge of the state machine's internals.

#### Time advancement invariant (required before liveness is meaningful)

Update `simulation.rs:173` (currently `+10ms` fixed) to respect:

1. **Bounded per-tick advance**: `max_time_per_tick = 10ms` regardless of randomization
2. **Tick monotonic**: the simulator tick counter advances by exactly 1 per `apply_with` call, independent of time
3. **Liveness measured in ticks, not time**: the quiescent drain counts ticks, not milliseconds

Randomized time advance within the `max_time_per_tick` bound is fine and should stay on the roadmap.

#### Validation experiment for Tier 1

Rewrite `issue_4566_dedup_drops_old_epoch_self_proposal` at `simulation.rs:554-673` to use the Tier 1 oracle:

1. Generate 100 `NotifySignal` RPCs interleaved with an `AnnounceLeader` mid-stream
2. Run phase A for 100 ticks
3. Enter quiescent drain for 500 ticks
4. Assert safety AND liveness

**Exit criterion for Tier 1**: the test catches the #4566 bug without any explicit mention of dedup, `EpochSequenceNumber`, or `is_outdated_or_duplicate`. If the test catches the bug, Tier 1 is validated empirically and Tier 2 is justified. If it fails to catch the bug, diagnose why before investing in Tier 2.

### Tier 2 — Observable-outcome oracle + staleness watchdog (~1100 lines total)

Tier 2 is only started **after Tier 1 catches #4566 via quiescent drain**. That is the go/no-go gate.

#### Safety — shadow the observable state

Add KV/promise/lock shadows the workload knows about:

```rust
struct ObservableModel {
    workload: WorkloadModel,
    state: BTreeMap<(ServiceId, String), Option<Bytes>>,
    promises: BTreeMap<(ServiceId, String), PromiseState>,
    object_locks: BTreeMap<ServiceId, Option<InvocationId>>,
    idempotency: BTreeMap<IdempotencyId, InvocationId>,
}
```

The workload generator emits **synthetic invoker effects**: for each invocation it creates, it records the chain of operations it will "run" ("SetState(k=1); GetLazyState(k); Output(k)") and feeds the matching `InvokerEffect::JournalEntry` commands. The oracle computes the expected completion values; the invariant checker asserts the real journal matches.

Catches observable-contract invariants 1–8 (see research doc § "What Restate's observable contract actually looks like").

#### Liveness — per-invocation staleness watchdog

```rust
struct LivenessWatchdog {
    last_progress_tick: BTreeMap<InvocationId, u64>,
    expected_terminal_by_tick: BTreeMap<InvocationId, u64>,
}
```

Rules:

- On any observable change to invocation state → reset `last_progress_tick`
- When workload emits last input for an invocation → set `expected_terminal_by_tick = current_tick + deadline`
- Per-step check: if `current_tick > expected_terminal_by_tick` and not terminal → violation
- Per-step check: if `current_tick - last_progress_tick > stall_threshold` AND (not `Suspended` waiting on workload-future notifications) → violation

#### Targeted deadlock patterns

Cheap, domain-specific checks (10–30 lines each), run at end of drain or end of step:

| Pattern | Check |
|---|---|
| VObj inbox head stuck | `Locked(head)` with no progress on head in K ticks |
| Promise orphan | `CompletePromise` applied, no `GetPromise` waiter notified in K ticks |
| Response sink orphan | `Completed` invocation with non-empty `response_sinks` at drain end |
| Outbox non-drain | Head seq unchanged, entries older than K ticks |
| Timer queue stuck | Scheduled time in past, not fired in K ticks |
| Reciprocal leak | `awaiting_rpc_actions` entry older than longest in-flight invocation |

### Tier 3 — Deferred (documented for roadmap continuity only)

- Multi-partition simulator with outbox → shuffle routing
- Cross-partition end-to-end liveness deadlines
- Progress-rate windowed liveness (degradation detection)
- Invoker-DSL interpreter for synthetic user code
- Integration with `run_inner` loop (requires entropy leaks closed)
- Fault injection beyond leadership churn (crash/restart, storage corruption, message reorder)

Tier 3 is **not in this plan**. It is listed so the plan reads as a staged path to full VOPR equivalence, not a dead end.

## Critical Files for Implementation

| File | Role |
|---|---|
| `crates/worker/src/partition/state_machine/tests/simulation.rs` | Main simulation loop, gen_command, current end-of-run invariants. Main edit target. |
| `crates/worker/src/partition/state_machine/tests/sim_storage.rs` | In-memory storage. Add `HashMap → BTreeMap` fix at line 83 as prerequisite. |
| `crates/worker/src/partition/state_machine/tests/mod.rs` | `TestEnv::apply_with`. May need new drain helpers. |
| **NEW** `crates/worker/src/partition/state_machine/tests/sim_model.rs` | Tier 1 `WorkloadModel` + `InvocationIntent` + `assert_safety` + `assert_liveness_at_drain_end`. |
| **NEW** `crates/worker/src/partition/state_machine/tests/sim_liveness.rs` | Tier 2 `LivenessWatchdog` + deadlock patterns. Added only after Tier 1 gate passes. |
| `docs/tasks/dst-phase1/research/sim_model_complexity_analysis_041026.md` | Canonical design doc. **Do not duplicate here — reference instead.** |
| `docs/tasks/dst-phase1/research/dst_vs_tigerbeetle_vopr_review_041026.md` | Original gap analysis. Context for ranking. |
| `docs/tasks/dst-phase1/research/awakeable_signal_loss_issue_4566_040926.md` | The bug Tier 1 must catch to pass the go/no-go gate. |

## Prerequisites (must land before Tier 1)

Small, independent fixes that unblock the model work. Each is trivial on its own:

1. **`sim_storage.rs:83` HashMap → BTreeMap** — latent non-determinism bug flagged in the VOPR review (`dst_vs_tigerbeetle_vopr_review_041026.md:130`). Trivial, should land on this branch or next.
2. **Bounded per-tick time advance** — replace the hardcoded `+10ms` at `simulation.rs:173` with `rng.random_range(0..=MAX_TIME_PER_TICK)` and add a tick counter separate from the time counter.
3. **Same-seed-twice determinism self-test** — flagged as VOPR gap #9. Cheap regression guard for non-determinism leaks. Should land before Tier 1 to prevent the model from being built on a non-deterministic substrate.

## Tasks

### Phase 1.5.a — Prerequisites (~200 lines, 0.5 days)

#### [P.1] Fix latent non-determinism in SimStorage
- [ ] **P.1.1** Convert `sim_storage.rs:83` `HashMap<IdempotencyId, _>` to `BTreeMap`
  - ✅ Build and tests pass
  - ✅ Grep for other `HashMap` in sim_storage.rs — none remain or all have justification comments

#### [P.2] Bounded tick-based time advancement
- [ ] **P.2.1** Introduce a simulator tick counter independent of wall-clock time
  - ✅ `simulation.rs:173` advances time by `rng.random_range(0..=MAX_TIME_PER_TICK)` with `MAX_TIME_PER_TICK = 10ms`
  - ✅ A separate `tick: u64` counter advances by exactly 1 per `apply_with` call
  - ✅ Existing `state_machine_simulation` and `state_machine_simulation_with_dedup` pass

#### [P.3] Determinism self-test
- [ ] **P.3.1** Add a test that runs the same seed twice and asserts BTreeMap-level equality
  - ✅ Hashes or deep-compares `dedup`, `invocations`, `journal_v2`, `state`, `promises`, `object_locks`, `timers` BTreeMaps
  - ✅ Test runs with a small number of iterations (say 50) to keep CI time bounded
  - ✅ Assertion compares the two runs field-by-field, not via a single hash, so failures point at the drifting table

### Phase 1.5.b — Tier 1: Workload-intent oracle + quiescent drain (~280 lines, 1–2 days)

#### [T1.1] Scaffold `sim_model.rs` with `WorkloadModel` and `InvocationIntent`
- [ ] **T1.1.1** Create the types, the mutation API (`record_signal_sent`, `record_response_sent`, `mark_lost_leadership`, `mark_fully_driven`), and the `assert_safety` per-step check
  - ✅ `WorkloadModel::default()` constructs an empty model
  - ✅ API surface covers the four generators in the current `gen_command`
  - ✅ `assert_safety` asserts every generator-submitted signal is in journal ∪ awaiting ∪ lost-leadership replies
  - ✅ Unit test: feeding a synthetic "bug" (remove a journal entry) triggers the assertion

#### [T1.2] Wire `WorkloadModel` into `state_machine_simulation_with_dedup`
- [ ] **T1.2.1** Replace `check_invariants` at `simulation.rs:124-151` with per-step `assert_safety`
  - ✅ Per-step safety check runs after every `sim_apply_envelope`
  - ✅ `gen_command` updates the model alongside emitting the command
  - ✅ Existing simulation passes with per-step checking

#### [T1.3] Implement quiescent drain phase
- [ ] **T1.3.1** Add Phase B drain loop after the main workload
  - ✅ `mark_all_fully_driven` sets `fully_driven = true` on every tracked invocation
  - ✅ Drain loop runs `DRAIN_STEPS = 500` additional ticks, advancing time and processing pending actions but not generating new commands
  - ✅ `assert_liveness_at_drain_end` checks: every fully-driven invocation is terminal, `awaiting_rpc_actions` empty, outbox drained
  - ✅ Test passes with at least one random seed

#### [T1.4] Tier 1 validation — rewrite the #4566 regression test
- [ ] **T1.4.1** Convert `issue_4566_dedup_drops_old_epoch_self_proposal` to use the Tier 1 oracle
  - ✅ Test generates 100 `NotifySignal` RPCs interleaved with an `AnnounceLeader`
  - ✅ Test asserts via `assert_safety` + `assert_liveness_at_drain_end` only — no direct reference to `is_outdated_or_duplicate`, `EpochSequenceNumber`, or the dedup high-water mark
  - ✅ Test fails on the pre-fix commit (`git stash` the #4566 fix, run, confirm failure)
  - ✅ Test passes on the post-fix commit
  - ✅ **Go/no-go gate**: if the test cannot be made to catch #4566 via the Tier 1 oracle alone, halt the plan and re-evaluate before investing in Tier 2

### Phase 1.5.c — Tier 2: Observable-outcome oracle + liveness watchdog (~800 lines, 1–2 weeks)

**Preconditioned on Tier 1 exit criterion passing.** If T1.4.1 does not catch #4566 via the oracle, do not proceed.

#### [T2.1] Extend the model with KV / promise / lock / idempotency shadows
- [ ] **T2.1.1** Add `ObservableModel` as a wrapper around `WorkloadModel`
  - ✅ Tracks state KV, promises, object locks, idempotency
  - ✅ Update rules keyed on journal command types the workload generator now emits
  - ✅ Unit tests for each shadow independently

#### [T2.2] Synthetic invoker effect generation
- [ ] **T2.2.1** Extend `gen_command` to generate chained journal entries
  - ✅ Per invocation, the generator picks a short DSL program (`SetState k=v; GetLazyState k; Output`) and emits the `InvokerEffect::JournalEntry` commands
  - ✅ The model computes expected completion values and asserts against the real journal
  - ✅ At least 4 DSL primitives supported: SetState, GetLazyState, SendSignal, Output

#### [T2.3] Liveness watchdog
- [ ] **T2.3.1** Add `LivenessWatchdog` with `last_progress_tick` + `expected_terminal_by_tick`
  - ✅ Per-step stall check runs after every step
  - ✅ Staleness threshold is a constant and configurable per test
  - ✅ Legitimate `Suspended` is distinguished from stalled `Suspended` via workload-future set

#### [T2.4] Targeted deadlock patterns
- [ ] **T2.4.1** Add the six targeted checks from the plan table above
  - ✅ Each check is independent, 10–30 lines, runs at end of step or end of drain
  - ✅ Unit test per check that injects the failure shape and verifies detection

### Deferred — Tier 3 items (tracked, not scheduled)

- Multi-partition driver with outbox routing
- Progress-rate windowed liveness
- Invoker-DSL interpreter beyond the 4 primitives
- `run_inner` integration (blocked on entropy leak fixes)
- Fault injection beyond leadership churn

## Coverage Summary

| Tier | Safety lines | Liveness lines | Total | Time | Gate |
|---|---|---|---|---|---|
| Prereqs | ~200 | — | ~200 | 0.5 d | None |
| Tier 1 | ~200 | ~80 | ~280 | 1–2 d | T1.4.1 must catch #4566 via oracle alone |
| Tier 2 | ~500–800 | ~300 | ~1100 | 1–2 w | Only if Tier 1 gate passes |
| Tier 3 | deferred | deferred | — | — | Not in this plan |

## Risks

1. **Tier 1 gate fails**: if the oracle can't catch #4566, we learn the observable-outcome approach needs more than workload-intent tracking. Cost: 1–2 days and a negative result that still informs the strategy.
2. **Synthetic invoker effects are harder than expected**: Tier 2 may need more infrastructure than 800 lines to make the journal entry chains realistic enough. Mitigation: start with the 4-primitive DSL; expand only if the simpler version finds bugs.
3. **Time-coupling subtlety**: randomized time advance + tick-based liveness must be implemented together or liveness checks will be unreliable. Mitigation: Prereqs P.2 must land before any Tier 1 liveness work.
4. **Tier 2 scope creep toward internal-state shadowing**: as the model grows, contributors may be tempted to shadow `journal_v2`, `outbox`, `timers` directly. Mitigation: this plan explicitly lists these as non-goals and the research doc explains why.

## References

- `docs/tasks/dst-phase1/research/sim_model_complexity_analysis_041026.md` — design rationale for observable-outcome vs internal-state-shadow and liveness validation framework
- `docs/tasks/dst-phase1/research/dst_vs_tigerbeetle_vopr_review_041026.md` — original VOPR gap analysis
- `docs/tasks/dst-phase1/research/deterministic_simulation_testing_040926.md` — overall DST roadmap
- `docs/tasks/dst-phase1/research/awakeable_signal_loss_issue_4566_040926.md` — the bug Tier 1 must catch
- `docs/tasks/dst-phase1/specs/plan.md` — the completed Phase 0-1 plan this one builds on
