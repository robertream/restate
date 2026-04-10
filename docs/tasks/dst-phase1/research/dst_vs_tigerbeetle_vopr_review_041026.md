---
date: "2026-04-10T00:00:00-07:00"
git_commit: 774d17c8c
branch: dst-phase1
repository: restate
topic: "Review of DST Phase 1/1.5 implementation against TigerBeetle's VOPR simulator"
tags: [research, testing, dst, simulation, vopr, tigerbeetle, review, gap-analysis]
status: complete
last_updated: "2026-04-10"
last_updated_by: Researcher
---

# Research: DST Phase 1/1.5 vs TigerBeetle VOPR — Gap Analysis & Roadmap

| Field | Value |
|-------|-------|
| Date | 2026-04-10 |
| Git Commit | `774d17c8c` |
| Branch | `dst-phase1` |
| Repository | `restate` |

## Research Question

How does the current DST Phase 1/1.5 implementation in `crates/worker/src/partition/state_machine/tests/{simulation.rs, sim_storage.rs}` compare to TigerBeetle's VOPR (Viewstamped Operation Replicator) simulator? What would it take to bring the implementation to VOPR-equivalence?

## Summary

The current work is a **solid Phase 1 seed fuzzer with a real in-memory storage backend** — more than most projects ever build. But measured against VOPR it is missing the three things that make VOPR *find bugs*:

1. A **model/oracle** to check system behavior against ground truth.
2. **Fault injection** (crashes, message loss, reorder, corruption, clock skew).
3. **End-to-end coverage** (`run_inner` loop + multi-partition + crash/recovery).

The biggest ROI single change is **SimModel oracle + per-step invariants** — without it, every workload-expansion ticket only improves panic/serde coverage, not correctness coverage. The biggest structural investment is closing entropy leaks so Phase 2's `run_inner` integration becomes possible at all.

## What's in place today (good foundation)

Based on `crates/worker/src/partition/state_machine/tests/simulation.rs:154-183` and `simulation.rs:477-530`:

- **One seeded PRNG** drives command generation via `SmallRng::seed_from_u64(seed)` — seed is printed on start and can be overridden via `SIM_SEED` (`simulation.rs:155-159`).
- **Two simulation entry points**: RocksDB-backed `state_machine_simulation` (via `TestEnv::apply_with`) and fully in-memory `state_machine_simulation_with_dedup` (via `SimStorage` at `sim_storage.rs:70-88`).
- **Dedup + epoch fencing** modeled at the apply_record boundary (`simulation.rs:382-455`) — this is what made the #4566 fix demonstrable.
- **`SimStorage`** covers ~22 trait impls in `sim_storage.rs` — the single biggest infrastructure investment and the thing that unblocks Phase 2.
- **Targeted regression**: `issue_4566_dedup_drops_old_epoch_self_proposal` (`simulation.rs:554-673`) — a proper deterministic reproduction of the dedup-layer contract the fix relies on.

This is a legitimate Phase-1 footprint. But the distance from "a seeded fuzzer for `StateMachine::apply`" to "a VOPR-equivalent" is large, and worth being explicit about.

## Gaps vs VOPR — ranked by impact

### 1. No model / oracle → almost no correctness signal

VOPR's highest-value component is its **workload checker**: clients submit requests, the simulator records an expected ground truth (the "state machine model"), and after every step it cross-checks the system against the model. TigerBeetle's simulator verifies ledger balances against an in-memory HashMap that mirrors what *should* be true.

Current invariants are purely structural:
- `check_invariants` at `simulation.rs:124-151` just scans the invocation status table and asserts "deserialization didn't panic" and "scan didn't error."
- `check_sim_storage_invariants` at `simulation.rs:470-474` only reports a count.
- `check_sim_dedup_invariants` at `simulation.rs:457-468` only logs the current dedup entry.

None of these would catch a silent correctness bug — for example, a signal that is dropped (the #4566 bug itself) or a virtual object lock that fails to release. **The whole-run `state_machine_simulation_with_dedup` test generates #4566-shaped commands but would not have failed on the bug without the separate assertion-based test**.

**What to add**:
- A `SimModel` struct that shadows the state machine: for each `InvocationId`, track `{status_enum, signals_submitted, signals_delivered, lock_holder}` etc. from the same generator that emits commands.
- After every `apply_with`, invoke `model.step(command, actions)` and `model.assert_consistent(&storage)`.
- The model is the place where bugs like #4566 become assertable: `assert_eq!(model.signals_delivered, actual_journal_entries)`.

### 2. Per-step invariants, not end-of-run invariants

Related to #1: VOPR checks invariants **after every operation**, so the step index where an invariant first breaks is the minimal reproduction. Currently, `check_invariants` runs once at step 1000 (`simulation.rs:181`). If corruption happens at step 47, later legitimate writes may hide it.

**Fix**: Move invariant checking inside the loop. The cost is bounded by how cheap the checks are — another reason to keep them in-memory and model-driven.

### 3. Partial entropy flow — some inputs come from global sources

A core VOPR rule: **every random input, without exception, threads through the single seeded PRNG**. Any function that calls `rand::thread_rng()`, `Ulid::new()`, `SystemTime::now()`, or `Instant::now()` is a determinism leak.

Current known leaks (from `research/deterministic_simulation_testing_040926.md:70-80`):
- `simulation.rs:71-74` constructs `PartitionProcessorRpcRequestId::from_parts(0, rng.random())` — OK — but the state machine may invoke other `Ulid::new()` / `rand::random()` sites (`crates/types/src/identifiers.rs:280,999`, `balanced_spread_selector.rs:110,121`).
- Tokio time is paused in tests but `std::time::SystemTime` / `std::time::Instant` calls in the hot path are not intercepted (e.g., `crates/timer-queue/src/lib.rs:74`).
- The research doc flags `tokio::select!` branch ordering in `partition/mod.rs:530` as non-deterministic without `rng_seed`.

At Phase 1.5 these leaks don't bite because we call `StateMachine::apply` directly. But **they block Phase 2** the moment the simulator touches `run_inner`.

**Fix now, cheaply**:
- Set `RngSeed` on `TaskCenterBuilder::default_for_tests()` (research:127 flags this as a zero-cost win).
- Add a workspace-level lint or grep-check that fails on new `Ulid::new()` / `rand::random()` / `SystemTime::now()` in simulated crates.
- Introduce an `IdGenerator` trait and inject it into the state machine path so `InvocationId` generation in prod code uses it.

### 4. No fault injection (the heart of VOPR)

VOPR finds bugs because it actively **breaks things**: message drop/duplicate/reorder, disk sector corruption, replica crash-and-restart, clock skew, partial writes. The current simulator does none of this. It is currently a **happy-path fuzzer**, not a deterministic chaos engine.

Incremental fault injection for Phase 1.5, in roughly increasing cost:

| Fault | Implementation |
|---|---|
| **Dedup layer drop** | Already modeled — natural dedup path. |
| **Spurious leadership transition** | Inject `AnnounceLeader` with probability *p* per step (partially done — `gen_command` at `simulation.rs:99-109`). |
| **Reorder within epoch** | Buffer generated envelopes, shuffle within a window, then apply. |
| **Message loss** | Skip `apply_with` for some generated commands; let the model know. |
| **Crash/restart** | Drop `StateMachine`, recreate from `SimStorage`, verify state recovers. Phase 1.5 can do this today. |
| **Storage corruption** | Mutate random bytes in `SimStorage`'s `BTreeMap` values — requires byte-level storage (current sim uses typed values, so this is Phase 2+). |
| **Clock skew** | Advance `time` by `rng.random_range(0..50)` instead of `+10` at `simulation.rs:173`. |

### 5. Crash/recovery loop is missing

VOPR's bread and butter: stop a replica mid-flight, restart it, replay the log, verify state reconstructs bit-identically. `SimStorage` plus `StateMachine::new` already makes this trivial — drop and rebuild the `StateMachine` at random intervals and assert pre/post invariants on the shared `SimStorage`. Today the simulation runs a single long-lived `StateMachine` for 1000 ops (`simulation.rs:484-525`). Without recovery, bugs in startup/snapshot/replay code are invisible.

### 6. Workload surface is narrow

`gen_command` at `simulation.rs:79-122` emits **4 command variants**; `gen_envelope` at `simulation.rs:238-377` emits **6**. The `Command` enum in `wal-protocol` has many more (Timer, PurgeInvocation, InvocationResponse, ScheduleInvocation, PatchState, NotifyGetInvocationResponse, etc., per `plan.md:127-134`). VOPR covers the entire public API of the state machine it tests.

This is a growth-path problem, not a design problem — the spec already calls it out. Worth emphasizing: **without a model (#1), expanding workload coverage increases panic/serde catching but not correctness catching.**

### 7. Single partition, no cross-partition feedback

The simulator runs one partition. Restate's real risk is cross-partition: `Action::NewOutboxMessage` produced on partition A should appear as a command on partition B. Today outbox messages are written into `SimStorage.outbox` at `sim_storage.rs:287-306` and never read back.

Phase 2 should add a minimal **multi-partition driver** that runs N `StateMachine`s + N `SimStorage`s and routes outbox messages between them deterministically. This is still cheap at the apply layer — cheaper than integrating `run_inner`.

### 8. No liveness watchdog

VOPR detects stalls: if no client request completes for K ticks, the simulator panics with "liveness violation." Current simulation can't detect livelock because nothing defines "progress." Once there's a model (#1), "progress" becomes "model state changed" or "at least one invocation completed in the last K ticks."

### 9. No determinism self-check

VOPR runs the same seed twice and **asserts bitwise-identical state hashes** as part of its own test suite. This catches "we accidentally introduced a non-determinism leak" before shipping. Currently nothing verifies that `SIM_SEED=42 cargo nextest run simulation` produces the same state two runs in a row.

**Fix**: Add a test that runs the same seed twice through `SimStorage`, hashes the `dedup` + `invocations` + `journal_v2` BTreeMaps, and asserts equality. Cheap, high-signal, and it will fire the first time someone introduces a `HashMap` iteration dependence.

(Note: `sim_storage.rs:83` uses `HashMap<IdempotencyId, _>` — latent non-determinism source if the simulator ever iterates it. Convert to `BTreeMap`.)

### 10. No `run_inner` loop → missing the race-condition surface

The original #4566 bug lived at `self_propose_and_respond_asynchronously` in the RPC layer, not in `StateMachine::apply`. The current dedup simulation proves the dedup layer is *correct*, and the separate MockActuator test at the RPC layer proves the fix. But the **race** — Bifrost commit interleaving with AnnounceLeader interleaving with RPC response path — only exists when you simulate `run_inner`. Phase 1.5 does not, by design (`plan.md:50-58`).

VOPR equivalence means eventually running `run_inner` deterministically. Blockers:
- `tokio::select!` `rng_seed` (research:127).
- `Bifrost::init_in_memory` integration (already exists — mentioned as "Phase 2 will use this").
- Deterministic `TaskCenter` interleaving, or replacement with a tick loop.
- All the entropy leaks in #3.

## Concrete ranked roadmap

1. **Add a `SimModel` oracle** and run it per-step. This unlocks every other correctness signal. Start small: track `{InvocationId → {status, signals_pending, has_response}}`. Even a minimal model catches bugs the current fuzzer can't.
2. **Per-step invariants + determinism self-test** (same-seed-twice hash). Low effort, high leverage.
3. **Crash/restart loop** — drop and recreate `StateMachine` at random step intervals, assert storage invariants hold.
4. **Randomize time advance** (`simulation.rs:173`) — trivial, surfaces time-of-check bugs.
5. **Fix entropy leaks globally** — set `rng_seed` on test task center, convert the `HashMap` in `SimStorage` to `BTreeMap`, add the workspace grep guard.
6. **Multi-partition driver** with outbox routing — still at the apply layer, still fast.
7. **Workload expansion** — add `Timer`, `InvocationResponse`, `PurgeInvocation`, `ScheduleInvocation`, `NotifyGet*`.
8. **Phase 2 proper**: integrate `Bifrost::init_in_memory` + `run_inner` after the entropy leaks are closed.
9. **Fault injection proper**: start with message reorder/drop at the envelope layer; disk corruption requires byte-level `SimStorage` and can wait.
10. **Liveness watchdog** after (1) is in place — trivial once "progress" is defined.

## Code References

| Concern | File | Lines |
|---------|------|-------|
| Current simulation loop (RocksDB) | `crates/worker/src/partition/state_machine/tests/simulation.rs` | 154-183 |
| Current simulation loop (SimStorage) | `crates/worker/src/partition/state_machine/tests/simulation.rs` | 477-530 |
| `sim_apply_envelope` (dedup + apply) | `crates/worker/src/partition/state_machine/tests/simulation.rs` | 382-455 |
| `gen_command` (4 variants) | `crates/worker/src/partition/state_machine/tests/simulation.rs` | 79-122 |
| `gen_envelope` (6 variants) | `crates/worker/src/partition/state_machine/tests/simulation.rs` | 238-377 |
| `check_invariants` (end-of-run only) | `crates/worker/src/partition/state_machine/tests/simulation.rs` | 124-151 |
| `check_sim_storage_invariants` (count only) | `crates/worker/src/partition/state_machine/tests/simulation.rs` | 470-474 |
| `check_sim_dedup_invariants` (log only) | `crates/worker/src/partition/state_machine/tests/simulation.rs` | 457-468 |
| `issue_4566_dedup_drops_old_epoch_self_proposal` | `crates/worker/src/partition/state_machine/tests/simulation.rs` | 554-673 |
| `SimStorage` state | `crates/worker/src/partition/state_machine/tests/sim_storage.rs` | 70-88 |
| `SimStorage` HashMap leak | `crates/worker/src/partition/state_machine/tests/sim_storage.rs` | 83 |
| Fixed time advance (+10ms) | `crates/worker/src/partition/state_machine/tests/simulation.rs` | 173 |

## Architecture Insights

1. **A fuzzer without a model is a panic detector, not a correctness checker.** The current simulator will catch crashes, serde regressions, and invariant-violating state corruption. It cannot catch dropped signals, lost messages, incorrect lock ownership, or any other "the data is internally consistent but semantically wrong" bug. VOPR's entire design centers on the model being the source of truth.

2. **Entropy leaks are a phase transition, not a gradient.** Phase 1.5 can tolerate them because it never runs `run_inner`. Phase 2 cannot tolerate any leaks at all. The work to close leaks should happen *between* Phase 1.5 and Phase 2, not as part of Phase 2, because it's cross-cutting and otherwise blocks forward progress.

3. **`SimStorage` is the rarest asset here.** In-memory Storage was flagged as the single biggest gap in the prior research doc (`research/deterministic_simulation_testing_040926.md:56`). Now that it exists, almost all the improvements above (crash/restart, multi-partition, determinism self-check, model oracle) are cheap additions *on top of* it. The Phase 2 work unlocked by `SimStorage` is disproportionate to the ~920 lines it cost to build.

4. **The #4566 regression test is at the correct layer.** The whole-run sim proves dedup works; the MockActuator test proves the RPC path. Neither alone would have caught the bug. This is the right pattern — simulation catches interaction bugs, unit tests pin specific contracts — but it means sim needs a model to catch *more* interaction bugs than just the ones specifically designed-in.

5. **VOPR's bug-finding rate comes from the cross-product of faults × workload × time, not from workload alone.** Adding more command variants without adding fault injection gives linear improvement. Adding fault injection multiplies against existing workload. The ranked roadmap reflects this: model (#1) and crash-loop (#3) beat workload expansion (#7) for bug-finding ROI.

## Related Research

- [Deterministic Simulation Testing for Restate](./deterministic_simulation_testing_040926.md) — prior research establishing the Phase 0/1/2/3 roadmap
- [Issue #4566 Research: Awakeable Signal Loss](./awakeable_signal_loss_issue_4566_040926.md) — the concrete bug this work fixes
- [TigerBeetle Liveness Testing](https://tigerbeetle.com/blog/2023-07-06-simulation-testing-for-liveness/)
- [TigerBeetle VOPR source](https://github.com/tigerbeetle/tigerbeetle/tree/main/src/testing) — reference implementation
- [S2 DST Blog Post](https://s2.dev/blog/dst) — best practical Rust DST writeup
- [FoundationDB Simulation Docs](https://apple.github.io/foundationdb/testing.html)

## Open Questions

1. **Model granularity**: How much of the invocation lifecycle does `SimModel` need to shadow to be useful? A minimal model (status + signal count) is cheap and catches #4566-class bugs. A full model (journal contents, lock acquisition order, response routing) is expensive but catches subtler bugs. Where's the sweet spot?

2. **Crash/restart fidelity**: Dropping and recreating `StateMachine` tests the in-memory startup path but not the real snapshot/replay machinery. At what point do we need to integrate real snapshotting into `SimStorage` to get meaningful recovery coverage?

3. **Multi-partition scale**: Does a 2-partition sim catch enough cross-partition bugs to be worth the infrastructure, or do we need 4+ to see interesting interleavings?

4. **CI budget**: VOPR's value comes from running millions of seeds. What's the Restate CI budget for simulation, and should we gate on (a) N seeds per PR, (b) a nightly sweep, or (c) both?

5. **Ordering with `run_inner`**: Once Phase 2 integrates `run_inner`, is setting `tokio::select!` `rng_seed` sufficient, or do we need to replace the select loop with an explicit tick-driven multiplexer the way TigerBeetle does?
