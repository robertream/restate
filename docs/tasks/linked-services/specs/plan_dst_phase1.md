# Implementation Plan: DST Phase 0-1 — Deterministic State Machine Simulation

## Overview

Implement seed-based simulation testing for the partition processor state machine. A test takes a `u64` seed, generates a sequence of `Command` values using a seeded PRNG, applies them to `StateMachine` through `TestEnv`, and checks invariants. Any failure is reproducible by re-running with the same seed.

Phase 0 extends `TestEnv` with parameterized time/LSN/leader. Phase 1 is a single `simulation.rs` file (~150 lines) with command generation and storage-level invariant checking.

## Desired End State

A single test function, driven by an env var seed (or random if unset):

```rust
#[test(restate_core::test)]
async fn state_machine_simulation() {
    let seed: u64 = std::env::var("SIM_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| rand::random());
    eprintln!("Simulation seed: {seed}");

    let mut env = TestEnv::create().await;
    let mut rng = SmallRng::seed_from_u64(seed);
    let mut known_ids: HashSet<InvocationId> = HashSet::new();
    let mut time = MillisSinceEpoch::new(1_000_000);
    let mut lsn = Lsn::OLDEST;
    let mut is_leader = true;

    for _ in 0..1000 {
        let command = gen_command(&mut rng, &known_ids, &mut is_leader);
        if let Command::Invoke(ref inv) = command {
            known_ids.insert(inv.invocation_id);
        }
        time = time + 10;
        lsn = lsn.next();
        let _actions = env.apply_with(command, time, lsn, is_leader).await;
    }

    check_invariants(&env.storage).await;
    env.shutdown().await;
}
```

On CI, run with N random seeds per PR. When a seed fails, it becomes a stable regression test.

## Out of Scope

- **In-memory Storage implementation** — RocksDB via `TestEnv` is sufficient
- **Full `run_inner` loop simulation** — Phase 2; we test `StateMachine::apply` directly
- **Multi-partition simulation** — Phase 2+; cross-partition outbox messages not fed back
- **Invoker simulation** — Phase 2; `InvokerEffect` commands are generated directly
- **Network simulation (turmoil/madsim)** — Phase 3
- **proptest** — Shrinking doesn't work well for stateful sequences with RocksDB
- **SimState oracle** — No shadow model of invocation lifecycle. Track created IDs only, check invariants against real storage.
- **Weighted command distributions** — Start uniform across a small set. Add weights when data shows value.

## Technical Approach

### Phase 0: Extend `TestEnv`

`TestEnv::apply` currently hardcodes `MillisSinceEpoch::now()`, `Lsn::OLDEST`, and `is_leader: true` (`tests/mod.rs:160-166`). Add a new method:

```rust
pub async fn apply_with(
    &mut self,
    command: Command,
    created_at: MillisSinceEpoch,
    lsn: Lsn,
    is_leader: bool,
) -> Vec<Action> { ... }
```

The existing `apply()` delegates to `apply_with` with `now()`, `OLDEST`, and `true`. Existing tests are unaffected.

### Phase 1: Simulation Test

One file: `crates/worker/src/partition/state_machine/tests/simulation.rs`

#### Command generation

A `gen_command(rng, known_ids, is_leader) -> Command` function that picks uniformly from 4 command types:

1. **`Command::Invoke`** — Create a new invocation with a deterministic `InvocationId` built from the seeded RNG (not `Ulid::new()` or `mock_random()` which use unseeded global RNG).

2. **`Command::InvokerEffect`** — Pick a random known invocation ID, generate one of: `PinnedDeployment`, `JournalEntry`, `SuspendedV2`, `End`. If no known invocations exist, fall back to `Invoke`.

3. **`Command::AnnounceLeader`** — Flip `is_leader` and generate an `AnnounceLeader` with a new epoch. Essential for catching #4566-class bugs.

4. **`Command::TerminateInvocation`** — Pick a random known invocation ID, kill or cancel it. If no known invocations exist, fall back to `Invoke`.

Build `InvocationId` and `ServiceInvocation` directly from the seeded RNG:

```rust
fn gen_invocation_id(rng: &mut impl Rng) -> InvocationId {
    InvocationId::from_parts(
        rng.random::<PartitionKey>(),
        InvocationUuid::from_u128(rng.random::<u128>()),
    )
}
```

#### Invariant checking

After the command loop, scan storage and check (no oracle needed):

1. **No panics** — `apply_with` uses `apply_fallible` internally. If it returns `Err`, log the error but don't panic. If it actually panics, the test fails with the seed.

2. **Valid invocation status** — Every entry in the invocation status table is one of the known `InvocationStatus` variants (not corrupted bytes).

3. **Journal contiguity** — For every `Invoked`/`Suspended` invocation, journal entries 0..N all exist with no gaps.

4. **Completed invocations have results** — Every `Completed` status carries a `ResponseResult`.

Start with invariants 1 and 2 (cheapest, highest signal). Add 3 and 4 incrementally.

#### Growth path

After the MVP works with 4 command types:
- Add `NotifySignal`, `InvocationResponse` (catches #4566 directly)
- Add `Timer`, `ScheduleTimer`
- Add `PurgeInvocation`, `PatchState`
- Add weighted distributions if data shows certain mixes find more bugs
- Split into subdirectory if file exceeds ~500 lines

### File Organization

```
crates/worker/src/partition/state_machine/tests/
├── mod.rs              (existing — add `mod simulation;`, extend TestEnv)
├── fixtures.rs         (existing — unchanged)
├── matchers.rs         (existing — unchanged)
└── simulation.rs       (NEW — ~150 lines: test fn, gen_command, check_invariants)
```

### Build Configuration

Add `rand` as a dev-dependency to `crates/worker/Cargo.toml` (for `SmallRng`). `rand` is already a dependency of many workspace crates. No new external dependencies.

## Critical Files for Implementation

| File | Role |
|------|------|
| `crates/worker/src/partition/state_machine/tests/mod.rs` | Extend `TestEnv` with `apply_with`, add `mod simulation` |
| `crates/worker/src/partition/state_machine/tests/simulation.rs` | NEW — the simulation test |
| `crates/wal-protocol/src/lib.rs:138` | `Command` enum — reference for variants to generate |
| `crates/types/src/invocation/mod.rs` | `ServiceInvocation`, `InvocationId` — build from seeded RNG |
| `crates/worker/src/partition/state_machine/mod.rs:283` | `StateMachine::apply` — the function under test |
| `crates/storage-api/src/invocation_status_table/mod.rs` | `InvocationStatus` enum — for invariant checking |
