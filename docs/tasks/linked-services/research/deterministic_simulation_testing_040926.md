---
date: "2026-04-09T00:00:00-07:00"
git_commit: f0d248e82
branch: linked-services
repository: restate
topic: "How to apply Deterministic Simulation Testing (DST) to Restate"
tags: [research, testing, dst, simulation, bifrost, partition-processor, determinism]
status: complete
last_updated: "2026-04-09"
last_updated_by: Researcher
---

# Research: Deterministic Simulation Testing for Restate

| Field | Value |
|-------|-------|
| Date | 2026-04-09 |
| Git Commit | `f0d248e82` |
| Branch | `linked-services` |
| Repository | `restate` |

## Research Question

How could Deterministic Simulation Testing (DST) — as pioneered by FoundationDB and TigerBeetle — be applied to Restate's architecture? What existing infrastructure supports it, what are the gaps, and what's the most practical adoption path?

## Summary

Restate's architecture is **surprisingly well-positioned** for DST adoption. The partition processor state machine (`StateMachine::apply`) is already a nearly-pure function of its log-sourced inputs. Key abstractions already exist: `TaskCenter` centralizes task spawning, `TransportConnect` trait abstracts networking with `MockConnector`, `Bifrost::init_in_memory` provides an in-memory log, `Clock` trait exists with `MockClock`, timer service has `ManualClock`, and Tokio's `start_paused` is already wired into the test macro. The main gaps are: no in-memory `Storage` implementation (RocksDB is used everywhere), unabstracted randomness (`Ulid::new()`, `rand::random()`), and `tokio::select!` non-determinism in `run_inner`. A phased approach — starting with the state machine layer and expanding outward — is recommended.

## Detailed Findings

### 1. What is DST?

Deterministic Simulation Testing eliminates all sources of non-determinism (time, scheduling, randomness, I/O) so that any execution is perfectly reproducible from a seed. The simulation runs an entire distributed cluster on a single thread, compressing time ~700x (TigerBeetle achieves ~2 millennia of simulated runtime per day on 1000 cores). A failing seed reproduces the exact same bug every time.

**Key principles:**
- All I/O (network, disk) goes through a simulated layer
- Time is controlled by the simulator, not the OS
- Randomness flows through a single seeded PRNG
- Scheduling is deterministic (single-threaded, cooperative)
- Fault injection (crashes, partitions, corruption) is seeded and reproducible

**What it finds:** Race conditions, liveness bugs, recovery failures, and subtle ordering issues that are nearly impossible to reproduce with traditional testing. TigerBeetle found a "resonance bug" between repair and load-balancing algorithms that caused indefinite livelock — undetectable without simulation. S2 found 17 notable bugs. FoundationDB ran ~1 trillion CPU-hours of simulation.

### 2. Restate's DST Readiness Assessment

| Area | Status | Details |
|------|--------|---------|
| **State machine purity** | **Ready** | `StateMachine::apply` is a pure function of `(command, timestamp, lsn, storage_state, is_leader)`. No internal randomness, no system time, no I/O. All side effects captured in `ActionCollector` + storage transaction. |
| **Task spawning** | **Ready** | `TaskCenter` centralizes all spawning. No direct `tokio::spawn`. Partition processors run on isolated single-threaded runtimes. |
| **Tokio time** | **Ready** | `start_paused` already wired into `#[restate_core::test]` macro and `TaskCenterBuilder::default_for_tests()`. `tokio::time::sleep/interval` calls are pauseable. |
| **Wall clock (`restate_clock`)** | **Partial** | `Clock` trait exists with `MockClock` impl (`test-util` feature). But `WallClock` is used as zero-sized struct, not injected via `dyn Clock`. |
| **Timer service** | **Ready** | Fully abstracted `Clock` trait with `ManualClock` for testing (`crates/timer/src/service/clock.rs:53-140`). |
| **Networking** | **Ready** | `TransportConnect` trait with `MockConnector` (in-process loopback), `FailingConnector`, `PassthroughConnector` (`crates/core/src/network/transport_connector.rs:40-246`). |
| **Bifrost (log)** | **Ready** | `MemoryLoglet` fully implements `Loglet` trait. `Bifrost::init_in_memory()` used in 30+ existing tests. |
| **Partition storage** | **Gap** | No in-memory `Storage`/`Transaction` implementation. Every test uses real RocksDB. This is the **largest structural gap**. |
| **Randomness** | **Gap** | `Ulid::new()` and `rand::random()` called directly throughout. No injectable RNG. Affects invocation IDs, cluster fingerprint, nodeset selection. |
| **`tokio::select!` ordering** | **Gap** | `run_inner` uses `tokio::select!` which has non-deterministic branch selection. Tokio's `rng_seed` can fix this but is not currently set. |
| **Invoker** | **Partial** | `MockInvokerHandle` exists but is a no-op sink — doesn't capture or replay invocations. |

### 3. Non-Determinism Sources in Detail

#### Already Controlled
- **Tokio time** — `start_paused` in test runtimes (`task_center.rs:736-737`)
- **Timer service** — `ManualClock` with `advance_time_to()` (`clock.rs:53-140`)
- **Network** — `MockConnector` with in-process loopback (`transport_connector.rs:92-209`)
- **Bifrost** — `MemoryLoglet` with `BTreeMap` storage (`memory_loglet.rs:132`)

#### Needs Abstraction
| Source | Location | Impact | Fix Difficulty |
|--------|----------|--------|---------------|
| `NanosSinceEpoch::now()` in Bifrost records | `crates/bifrost/src/record.rs:255` | Timestamps in log records | Low — inject clock |
| `Ulid::new()` for invocation IDs | `crates/types/src/identifiers.rs:280,999` | Every invocation ID | Medium — injectable ID generator |
| `rand::random()` in nodeset selection | `crates/types/src/replication/balanced_spread_selector.rs:110,121` | Replication topology | Medium — injectable RNG |
| `tokio::select!` branch ordering | `crates/worker/src/partition/mod.rs:530` | Event processing order | Low — set `rng_seed` |
| `SystemTime::now()` in `TimerQueue` | `crates/timer-queue/src/lib.rs:74` | Timer queue sleep durations | Low — use tokio time |
| `SystemTime::now()` in invoker timeouts | `crates/invoker-impl/src/invocation_task/service_protocol_runner.rs:475,501` | Invoker abort/inactivity | Low — use tokio time |
| Status timer jitter | `crates/worker/src/partition/mod.rs:501` | Status reporting only | Negligible |
| `rand::random()` for `TaskCenterInner::id` | `crates/core/src/task_center.rs:372` | One-time ID | Negligible |
| `rand::random()` for `ClusterFingerprint` | `crates/types/src/nodes_config.rs:61` | Cluster identity | Negligible |

### 4. Rust DST Frameworks and Approaches

#### turmoil (Tokio Official)
**[github.com/tokio-rs/turmoil](https://github.com/tokio-rs/turmoil)**

Network simulation framework. Each simulated host runs on its own Tokio runtime, managed by a single-threaded simulation loop. Simulates: network (drop, hold, delay, partition), time (deterministic clock), filesystem (`fs::shim`). Uses conditional re-export pattern (`turmoil::net` vs `tokio::net` via feature flag).

**Limitation:** Does not intercept `getrandom` or `clock_gettime` from dependencies.

#### madsim (Magical Deterministic Simulator)
**[github.com/madsim-rs/madsim](https://github.com/madsim-rs/madsim)**

Full Tokio runtime replacement. Overrides libc functions (`gettimeofday`, `clock_gettime`, `getrandom`). Provides simulator versions for tonic, etcd, S3, Kafka. Used by RisingWave in production. Swap via Cargo package aliasing: `tokio = { version = "0.2", package = "madsim-tokio" }`.

**Limitation:** Any crate linking tokio directly (not through re-exported version) creates runtime mismatch.

#### mad-turmoil (S2's Hybrid)
**[github.com/s2-streamstore/mad-turmoil](https://github.com/s2-streamstore/mad-turmoil)**

Turmoil + libc interception. Overrides `getrandom`, `getentropy`, `clock_gettime`. Found 17 notable bugs. Best practical writeup: [s2.dev/blog/dst](https://s2.dev/blog/dst).

#### State Machine Architecture (Polar Signals)
No async in core traits. Pure `receive(msg) -> Vec<(msg, dest)>` + `tick(time)`. Maximum determinism, maximum refactoring. [polarsignals.com/blog/posts/2025/07/08/dst-rust](https://www.polarsignals.com/blog/posts/2025/07/08/dst-rust)

#### Antithesis (External Hypervisor)
Runs entire docker-compose stack in a deterministic hypervisor (bhyve fork). No code changes for basic coverage. Commercial product. [antithesis.com](https://antithesis.com/product/how_antithesis_works/)

### 5. Recommended Adoption Strategy for Restate

#### Why Restate is Uniquely Suited

Restate's architecture already mirrors the DST-friendly patterns:

1. **Log-driven state machine** — The partition processor reads from an ordered log and applies commands to a state machine. This is exactly the pattern FoundationDB and TigerBeetle use. The log IS the deterministic input stream.

2. **Single-threaded partition processors** — Each partition processor runs on its own `current_thread` Tokio runtime (`task_center.rs:705-787`). No work-stealing non-determinism within a partition.

3. **Explicit side effect collection** — `ActionCollector` captures all outputs from `StateMachine::apply`. No hidden I/O escapes during apply.

4. **Trait-based I/O** — Network (`TransportConnect`), Bifrost (`Loglet`), Timer (`Clock`) already have trait abstractions with test implementations.

#### Phase 0: Quick Wins (Days, Not Weeks)

Zero-cost improvements using existing infrastructure:

1. **Set `rng_seed` on test runtimes** — Add `builder.rng_seed(RngSeed::from_bytes(&seed))` to `TaskCenterBuilder::default_for_tests()`. This makes `tokio::select!` deterministic. Location: `crates/core/src/task_center/builder.rs:88-97`.

2. **Replace `std::time::Instant` with `tokio::time::Instant`** in code that runs under paused time — the partition processor cleaner (`cleaner.rs:95`), status timer (`mod.rs:501`), etc. Paused Tokio time already works but `std::time::Instant` bypasses it.

3. **Inject clock into `InputRecord` creation** — Replace `NanosSinceEpoch::now()` at `record.rs:255` with a configurable source. This makes Bifrost record timestamps deterministic.

#### Phase 1: State Machine Simulation (Weeks)

Test the partition processor state machine with controlled inputs:

1. **Seed-based command generation** — Write a property-based test that generates random `Command` sequences from a seed, applies them to `StateMachine` via `TestEnv`, and checks invariants. This is already very close to possible with the existing `TestEnv` harness (`crates/worker/src/partition/state_machine/tests/mod.rs:74-318`).

2. **Multi-partition simulation** — Generate commands targeting multiple partitions, verify cross-partition outbox messages are correctly produced (via `Action::NewOutboxMessage`), and feed them back as inputs to destination partitions.

3. **Leadership transition simulation** — Generate `AnnounceLeader` commands interspersed with regular commands, verify dedup behavior and state consistency across epoch transitions. This would directly catch issue #4566.

4. **Invariant assertions** — After each apply step, verify:
   - No invocation is in an impossible state (e.g., `Completed` + `Locked`)
   - Every outbox message targets a valid partition
   - Every timer registration has a future timestamp
   - Dedup table is monotonically advancing

#### Phase 2: Partition Processor Loop Simulation (Months)

Simulate the full `run_inner` loop with controlled I/O:

1. **In-memory `Storage` implementation** — The largest gap. Create a `BTreeMap`-backed implementation of the `Storage` + `Transaction` traits. This eliminates RocksDB from the simulation.

2. **Deterministic event loop** — Replace `tokio::select!` in `run_inner` with a simulation-friendly multiplexer that processes events in seed-determined order. Turmoil or madsim can help here.

3. **Simulated invoker** — Extend `MockInvokerHandle` to replay scripted invoker effects (deployment pinning, journal entries, suspend, end).

4. **Fault injection** — Leadership transitions at random points, Bifrost append failures, network partitions between partition processors.

#### Phase 3: Full Cluster Simulation (Quarter+)

Simulate multiple nodes with Bifrost, metadata, and networking:

1. **turmoil or madsim** — Use one of these frameworks to simulate a multi-node Restate cluster on a single thread.

2. **BUGGIFY-style injection** — Scatter `if cfg!(simulation) && sim_random() < 0.25 { inject_fault() }` throughout production code. Guard with a compile-time feature flag.

3. **Continuous simulation** — Run on CI with random seeds. When a seed fails, it's a reproducible regression test.

### 6. Framework Recommendation for Restate

| Approach | Fit for Restate | Why |
|----------|----------------|-----|
| **State machine fuzzing (Phase 1)** | **Best starting point** | `StateMachine::apply` is already pure. `TestEnv` exists. Minimal infrastructure needed. Catches bugs like #4566. |
| **turmoil + mad-turmoil** | **Best for Phase 2-3** | Restate already has `TransportConnect` trait, `MockConnector`. Turmoil adds network simulation on top. mad-turmoil patches libc leaks. |
| **madsim** | **High potential, high cost** | Full runtime replacement would give maximum coverage but requires Cargo aliasing for all Tokio-dependent crates. Heavy migration. |
| **Antithesis** | **Complementary** | Zero code changes for basic coverage. Best as a complement to in-process DST, not a replacement. Commercial. |
| **Pure state machine (Polar Signals)** | **Not recommended** | Would require rewriting `run_inner` without async. Too invasive for an existing codebase of this size. |

### 7. What Bugs Would DST Catch?

Based on issues found by other systems adopting DST:

| Bug Category | Restate Example | DST Phase |
|-------------|-----------------|-----------|
| Signal loss during leadership transition | Issue #4566 | Phase 1 (state machine) |
| Dedup table corruption across epochs | Hypothetical | Phase 1 |
| Timer firing order violations | After delayed leadership transfer | Phase 2 |
| Invocation stuck in impossible state | Completed + new invocations accepted | Phase 1 |
| Cross-partition message loss | Shuffle + leadership change | Phase 2 |
| Livelock under concurrent operations | Repair + rebalance interference | Phase 3 |
| Recovery after crash mid-transaction | RocksDB commit interrupted | Phase 2 |
| Network partition split-brain | Two leaders processing simultaneously | Phase 3 |

## Code References

| Concern | File | Lines |
|---------|------|-------|
| `StateMachine::apply` (pure function) | `crates/worker/src/partition/state_machine/mod.rs` | 283-292 |
| `ActionCollector` (side effect collection) | `crates/worker/src/partition/state_machine/actions.rs` | 27-107 |
| `run_inner` main loop (`tokio::select!`) | `crates/worker/src/partition/mod.rs` | 530-688 |
| `TaskCenter` (centralized spawning) | `crates/core/src/task_center.rs` | 705-787 |
| `start_paused` for partition runtimes | `crates/core/src/task_center.rs` | 736-737 |
| `TaskCenterBuilder::default_for_tests()` | `crates/core/src/task_center/builder.rs` | 61-65 |
| `#[restate_core::test]` macro expansion | `crates/core/derive/src/tc_test.rs` | 427-444 |
| `Clock` trait + `MockClock` | `crates/clock/src/lib.rs` | 40-61 |
| Timer `ManualClock` | `crates/timer/src/service/clock.rs` | 53-140 |
| `TransportConnect` trait | `crates/core/src/network/transport_connector.rs` | 40-49 |
| `MockConnector` (in-process loopback) | `crates/core/src/network/transport_connector.rs` | 92-209 |
| `MemoryLoglet` | `crates/bifrost/src/providers/memory_loglet.rs` | 129-429 |
| `Bifrost::init_in_memory()` | `crates/bifrost/src/bifrost.rs` | 76-80 |
| `NanosSinceEpoch::now()` in records | `crates/bifrost/src/record.rs` | 255 |
| `Storage` + `Transaction` traits | `crates/storage-api/src/lib.rs` | 94-140 |
| `PartitionStore` (only impl) | `crates/partition-store/src/partition_store.rs` | 57 |
| `TestEnv` (state machine test harness) | `crates/worker/src/partition/state_machine/tests/mod.rs` | 74-318 |
| `Ulid::new()` randomness | `crates/types/src/identifiers.rs` | 280, 999 |
| `rand::random()` in nodeset selection | `crates/types/src/replication/balanced_spread_selector.rs` | 110, 121 |
| `MockInvokerHandle` | `crates/invoker-api/src/lib.rs` | 144-220 |
| `TimerQueue` unabstracted time | `crates/timer-queue/src/lib.rs` | 74 |

## Architecture Insights

1. **Restate's log-driven architecture IS the DST-friendly pattern.** FoundationDB and TigerBeetle structure their systems as state machines driven by an ordered log — Restate already is this. The partition processor reads from Bifrost (ordered log) and applies commands to a deterministic state machine. This is the hardest part of DST and Restate has it for free.

2. **Single-threaded partition processors eliminate the hardest non-determinism.** Each partition processor runs on a `current_thread` Tokio runtime. There is no work-stealing scheduler to contend with within a partition. This eliminates the most pernicious source of non-determinism in concurrent systems.

3. **The `ActionCollector` pattern is the ideal simulation boundary.** All state machine side effects are collected into a `Vec<Action>` before being dispatched. A simulator can inspect, filter, reorder, or drop actions to simulate failures — without modifying the state machine itself.

4. **RocksDB is the elephant in the room.** Every test uses real RocksDB via `PartitionStore`. Creating an in-memory `Storage` implementation is the largest investment for DST but would also dramatically speed up state machine tests (currently bottlenecked by RocksDB init/cleanup).

5. **The Tokio `rng_seed` + `start_paused` combination is underutilized.** Setting `rng_seed` on the test runtime builder would make `tokio::select!` branch ordering deterministic. Combined with `start_paused` (already used), this gives significant determinism for free.

## Related Research

- [Issue #4566 Research: Awakeable Signal Loss](./awakeable_signal_loss_issue_4566_040926.md) — A concrete bug that DST Phase 1 would catch
- [S2 DST Blog Post](https://s2.dev/blog/dst) — Best practical Rust DST writeup
- [TigerBeetle Liveness Testing](https://tigerbeetle.com/blog/2023-07-06-simulation-testing-for-liveness/)
- [FoundationDB Simulation Docs](https://apple.github.io/foundationdb/testing.html)
- [Polar Signals: DST in Rust](https://www.polarsignals.com/blog/posts/2025/07/08/dst-rust)
- [RisingWave DST with madsim](https://www.risingwave.com/blog/deterministic-simulation-a-new-era-of-distributed-system-testing/)
- [turmoil GitHub](https://github.com/tokio-rs/turmoil)
- [madsim GitHub](https://github.com/madsim-rs/madsim)
- [mad-turmoil GitHub](https://github.com/s2-streamstore/mad-turmoil)
- [Awesome DST — Curated List](https://github.com/ivanyu/awesome-deterministic-simulation-testing)

## Open Questions

1. **In-memory `Storage` investment**: How much effort to create a `BTreeMap`-backed `Storage`/`Transaction` implementation? The `Storage` trait requires 20+ table trait impls. Could be code-generated from the trait definitions.

2. **RocksDB in simulation**: Alternatively, could RocksDB itself be used deterministically? Its write ordering is deterministic given the same inputs. The non-determinism comes from background compaction and OS-level caching — acceptable if we only need state machine determinism.

3. **Turmoil vs madsim**: Which framework fits Restate's dependency tree better? Turmoil requires less refactoring but misses libc-level non-determinism. madsim is more complete but requires Cargo aliasing for all Tokio-dependent crates.

4. **BUGGIFY in production**: Would the team accept compile-time-gated fault injection points in production code? This is the most effective way to explore edge cases in simulation but adds code noise.

5. **CI integration**: How would continuous simulation runs be integrated? TigerBeetle runs 1000 cores 24/7. A lighter version could run N seeds per PR, with longer runs nightly.

6. **Incremental adoption**: Can we start with just `StateMachine::apply` fuzzing (using existing `TestEnv` + property-based testing) and prove value before investing in the full simulation stack?
