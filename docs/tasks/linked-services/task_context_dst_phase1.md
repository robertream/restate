# Task Context: DST Phase 0-1 — Deterministic State Machine Simulation Test

## Goal

Implement Deterministic Simulation Testing (DST) through Phase 1: write a seed-based property test that generates random `Command` sequences, applies them to the partition processor `StateMachine`, and checks invariants. Also apply Phase 0 quick wins (set `rng_seed`, fix time abstractions).

## Motivation

Issue #4566 (awakeable signals lost during leadership transitions) is a concrete example of a bug that DST would catch. The partition processor state machine is already a nearly-pure function — we can leverage this for seed-based simulation testing with minimal infrastructure investment.

## Architecture Patterns

### State Machine Purity
- `StateMachine::apply` (`crates/worker/src/partition/state_machine/mod.rs:283-292`) takes explicit inputs: `(command, record_created_at, record_lsn, transaction, action_collector, vqueues_cache, is_leader)`
- No internal randomness, no system time, no I/O
- All side effects captured in `ActionCollector` (alias `Vec<Action>`, `actions.rs:27`) + storage transaction
- The only `Instant::now()` at `mod.rs:295` is for metrics histogram, not state/actions

### Existing Test Harness: `TestEnv`
- `TestEnv` (`crates/worker/src/partition/state_machine/tests/mod.rs:74-318`) provides:
  - `TestEnv::create()` — real RocksDB `PartitionStore` on temp dir + `StateMachine`
  - `TestEnv::apply(command)` — starts transaction, calls `state_machine.apply(...)`, commits, returns `Vec<Action>`
  - `TestEnv::apply_multiple(commands)` — iterates commands, accumulates actions
  - Test fixtures in `tests/fixtures.rs` for common states
  - googletest matchers in `tests/matchers.rs` for actions and storage

### Non-Determinism Sources to Fix (Phase 0)
- `tokio::select!` in `run_inner` (`mod.rs:530`) — fix with `rng_seed` on test runtimes
- `NanosSinceEpoch::now()` in `InputRecord` (`record.rs:255`) — inject clock
- `std::time::Instant` in cleaner/status timer — replace with `tokio::time::Instant`

### Command Generation
- `Command` enum defined in `crates/wal-protocol/src/lib.rs:138-215`
- Key variants: `Invoke`, `InvokerEffect`, `Timer`, `NotifySignal`, `InvocationResponse`, `AnnounceLeader`, `TerminateInvocation`, `PurgeInvocation`, etc.
- `ServiceInvocation` (`crates/types/src/invocation/mod.rs`) has many fields — needs an `Arbitrary` impl or builder
- `InvokerEffect` variants (`InvokerEffectKind`): `PinnedDeployment`, `JournalEntry`, `SuspendedV2`, `End`, `Failed`, `Canceled`

### Invariants to Check
- No invocation in impossible state (e.g., `Completed` + `Locked` simultaneously)
- Outbox messages target valid partitions
- Dedup table advances monotonically per producer
- Journal entries are contiguous per invocation
- Timer registrations have future timestamps relative to `record_created_at`
- Action types match leader status (`is_leader` controls action emission)

## Dependencies

- `proptest` or `arbitrary` crate for seed-based command generation
- Existing `TestEnv` harness (RocksDB-backed, sufficient for Phase 1)
- `googletest` matchers already used in state machine tests

## Implementation Approaches

### Approach A: Property-Based Testing with proptest
- Use `proptest` to generate random `Command` sequences from a seed
- Apply through `TestEnv::apply_multiple`
- Check invariants after each apply step
- Shrinking finds minimal failing sequence

### Approach B: Custom Seed-Based Fuzzer
- TigerBeetle-style: custom PRNG from a u64 seed, hand-rolled command generator
- More control over command generation distribution (e.g., bias toward leadership transitions)
- No shrinking, but exact seed reproduction

### Approach C: Hybrid (Recommended)
- Use `proptest` for the seed/shrinking infrastructure
- Custom `Strategy` implementations for `Command` that produce realistic sequences
- State-aware generation: track what invocations exist, what state they're in, generate valid follow-up commands

## Impact Summary

- **Files to create**: 1-2 new test files in `crates/worker/src/partition/state_machine/tests/`
- **Files to modify**: `builder.rs` (rng_seed), possibly `record.rs` (clock injection), `Cargo.toml` (proptest dep)
- **Risk**: Low — all changes are in test infrastructure, no production code changes for Phase 1
- **Existing tests**: Not affected — new test module alongside existing tests

## External Research

- **proptest**: Rust property-based testing framework with shrinking. [docs.rs/proptest](https://docs.rs/proptest)
- **TigerBeetle VOPR**: Custom seed-based simulator, generates random operations + fault injection
- **S2 DST**: Used turmoil + mad-turmoil, found 17 bugs. Meta-test validates determinism by comparing TRACE logs across runs.
- **sled simulation guide**: Minimal state machine DST pattern — `receive(msg) -> Vec<(msg, dest)>` + invariant checking
- **Polar Signals**: Synchronous state machine trait, single-threaded event loop, found 2 critical bugs (data loss + duplication)

## Key Files

| File | Role |
|------|------|
| `crates/worker/src/partition/state_machine/mod.rs:283` | `StateMachine::apply` — the pure function to test |
| `crates/worker/src/partition/state_machine/tests/mod.rs:74` | `TestEnv` harness |
| `crates/worker/src/partition/state_machine/tests/fixtures.rs` | Test fixtures |
| `crates/worker/src/partition/state_machine/tests/matchers.rs` | Action/storage matchers |
| `crates/worker/src/partition/state_machine/actions.rs:27` | `Action` enum + `ActionCollector` |
| `crates/wal-protocol/src/lib.rs:138` | `Command` enum |
| `crates/types/src/invocation/mod.rs` | `ServiceInvocation`, `NotifySignalRequest`, etc. |
| `crates/storage-api/src/deduplication_table/mod.rs:89` | `EpochSequenceNumber` |
| `crates/core/src/task_center/builder.rs:88` | Where to add `rng_seed` |
| `crates/bifrost/src/record.rs:255` | `NanosSinceEpoch::now()` to abstract |
