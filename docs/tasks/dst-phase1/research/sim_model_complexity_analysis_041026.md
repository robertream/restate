---
date: "2026-04-10T00:00:00-07:00"
git_commit: 774d17c8c
branch: dst-phase1
repository: restate
topic: "Is building a SimModel oracle for Restate's state machine tractable? Domain complexity vs TigerBeetle/FoundationDB, plus liveness validation strategy"
tags: [research, dst, simulation, model-based-testing, vopr, tigerbeetle, foundationdb, liveness]
status: complete
last_updated: "2026-04-10"
last_updated_note: "Added liveness validation section"
---

# Research: SimModel Complexity for Restate vs TigerBeetle/FDB

## Research Question

> "Both TigerBeetle and FoundationDB have very simple state machine semantics, so producing a model to validate against is trivial. Restate has a much more complicated model, just given the domain. Is constructing an exhaustive valid model for Restate actually tractable?"

## Short answer

The critique has **partial merit** and **partial misunderstanding**:

- **Yes**: Restate's state machine has materially more command variants, journal types, and lifecycle transitions than TigerBeetle. An "exhaustive internal-state shadow" of the full state machine is a real undertaking (rough estimate: 2–4k lines of model code plus machinery).
- **No**: FoundationDB is *not* a counterexample. FDB's state machine semantics are simple in exactly the sense Restate's are simple — once you separate "observable client contract" from "internal implementation." FDB's simulator validates observable contracts (serializable reads at a read version); it does *not* shadow the internal coordinator/proxy/log/storage-server state.
- **Key insight**: the choice is not between "no model" and "exhaustive model." There are three meaningfully distinct model strategies, and the most valuable one (observable-outcome oracle) is tractable at ~500–800 lines for Phase 1.5, catches #4566-class bugs, and does not require shadowing the internal state machine.

## Quantifying Restate's domain complexity

Grounded in the current code (commit `774d17c8c`):

### 1. Top-level command surface — 22 variants

From `crates/wal-protocol/src/lib.rs:137-204`:

| Category | Variants |
|---|---|
| Control plane | `AnnounceLeader`, `UpdatePartitionDurability`, `VersionBarrier`, `UpsertSchema` (4) |
| Invocation lifecycle | `Invoke`, `ProxyThrough`, `TerminateInvocation`, `PurgeInvocation`, `PurgeJournal`, `ResumeInvocation`, `RestartAsNewInvocation`, `AttachInvocation` (8) |
| Invoker effects | `InvokerEffect` (1 — but see §2, this carries ~20 sub-variants) |
| Cross-partition messaging | `InvocationResponse`, `NotifySignal`, `NotifyGetInvocationOutputResponse` (3) |
| Timers | `Timer`, `ScheduleTimer` (2) |
| External | `PatchState` (1) |
| Outbox | `TruncateOutbox` (1) |
| Virtual queues | `VQWaitingToRunning`, `VQYieldRunning` (2) |

### 2. InvokerEffect is the fan-out point — journal v2 has 20 command entry types

From `crates/types/src/journal_v2/command.rs:47-68`:

`Input`, `Output`, `GetLazyState`, `SetState`, `ClearState`, `ClearAllState`, `GetLazyStateKeys`, `GetEagerState`, `GetEagerStateKeys`, `GetPromise`, `PeekPromise`, `CompletePromise`, `Sleep`, `Call`, `OneWayCall`, `SendSignal`, `Run`, `AttachInvocation`, `GetInvocationOutput`, `CompleteAwakeable`.

Each one has a dedicated state-machine handler in `crates/worker/src/partition/state_machine/entries/` (one file per command, typically 100–500 lines each). And these are the **commands**; the matching **completions** in `crates/types/src/journal_v2/notification.rs:122-134` add 11 more variants (`GetLazyState`, `GetLazyStateKeys`, `GetPromise`, `PeekPromise`, `CompletePromise`, `Sleep`, `CallInvocationId`, `Call`, `Run`, `AttachInvocation`, `GetInvocationOutput`) plus the `Signal` notification type.

### 3. Invocation lifecycle — 7 states + metadata

From `crates/storage-api/src/invocation_status_table/mod.rs:141-154`:

```rust
pub enum InvocationStatus {
    Scheduled(ScheduledInvocation),
    Inboxed(InboxedInvocation),
    Invoked(InFlightInvocationMetadata),
    Suspended { metadata, waiting_for_notifications: HashSet<NotificationId> },
    Paused(InFlightInvocationMetadata),
    Completed(CompletedInvocation),
    Free,
}
```

Plus `Killed` as a discriminant (`mod.rs:388-396`). Each status carries structured metadata:
- `InFlightInvocationMetadata` includes `journal_metadata`, `response_sinks: HashSet<ServiceInvocationResponseSink>`, `idempotency_key`, `completion_retention_duration`, `source`, `invocation_target`, `pinned_deployment`, etc.
- `Suspended` additionally carries the set of notification IDs blocking resumption.
- `Completed` carries the `ResponseResult` and retention state.

### 4. Lifecycle handlers — 16 modules

From `crates/worker/src/partition/state_machine/lifecycle/`:

`cancel`, `event`, `manual_resume`, `migrate_journal_table`, `notify_get_invocation_output_response`, `notify_invocation_response`, `notify_signal`, `notify_sleep_completion`, `paused`, `pinned_deployment`, `purge`, `purge_journal`, `restart_as_new`, `resume`, `suspend`, `version_barrier`.

### 5. Action output — 18+ variants

From `crates/worker/src/partition/state_machine/actions.rs`:

`VQEvent`, `VQInvoke`, `Invoke`, `NewOutboxMessage`, `RegisterTimer`, `DeleteTimer`, `AckStoredCommand`, `ForwardCompletion`, `ForwardNotification`, `AbortInvocation`, `IngressResponse`, `IngressSubmitNotification`, `ForwardKillResponse`, `ForwardCancelResponse`, `ForwardPurgeInvocationResponse`, `ForwardPurgeJournalResponse`, `ForwardResumeInvocationResponse`, `ForwardRestartAsNewInvocationResponse`, `ForwardAppendedResponse`.

### 6. Storage tables — 13 logical tables

From `crates/worker/src/partition/state_machine/tests/sim_storage.rs:70-88` (which currently shadows all of them):

`dedup`, `invocations`, `virtual_objects`, `inbox`, `outbox`, `journal_v2`, `journal_v2_completions`, `timers`, `state`, `journal_v1`, `promises`, `idempotency`, `fsm` (applied_lsn, inbox_seq, outbox_seq). Plus the vqueue and journal_events trait impls (`sim_storage.rs:807, 788`).

### Aggregate

- **~62 commands** if you count all subtypes (22 top-level + 20 journal command types + 11 completion types + a few inbound-only variants)
- **~16 lifecycle transition handlers** that mutate `InvocationStatus`
- **~18 action variants** as side effects
- **~13 storage tables** that the state machine reads and writes
- **Protocol versioning** layered on top: Journal V1 / V2, Service protocol V3 / V4

This is undeniably a larger surface than TigerBeetle's `{create_account, create_transfer, lookup_account, lookup_transfer}`.

## The critical conceptual move: *which* model?

The premise that "FDB has a simple state machine and therefore a simple model" is half-right. FDB's **observable semantics** are simple (serializable KV); FDB's **implementation** is one of the most complex distributed systems on earth (coordinators, proxies, log systems, storage servers, resolvers, rate keepers, cluster controllers, master). The simulator validates the simple contract against the complex implementation — it does not shadow the implementation.

This distinction collapses three very different things people call "a model":

### Model type A — Internal-state shadow ("reimplement the state machine in a test")

- For every real storage table, have a shadow.
- For every real command, re-derive what each table should look like.
- Invariant: the shadow equals the real storage at every step.
- **Complexity grows with implementation surface.**
- This is what the user's critique correctly identifies as untractable for Restate. It would be ~2–4k lines of model code and would have to move every time the real state machine moves.
- TigerBeetle comes closest to this because its observable-state *is* its internal-state: ledger balances are both the client contract and the implementation.

### Model type B — Observable-outcome oracle ("a shadow of what the client should see")

- Track only the things a client can externally observe or intend.
- Invariant: the actual client-observable projection of the real storage matches the oracle.
- **Complexity grows with contract surface, not implementation surface.**
- This is what FoundationDB's simulator actually does. The oracle is "a HashMap<Bytes, Bytes> tracking committed writes, plus a read-version timeline." That's it. The KV contract is small even though the implementation is vast.
- This is almost certainly the right choice for Restate.

### Model type C — Workload shadow ("what did the generator do?")

- The PRNG-driven workload generator records its own intent: "at step 47 I submitted signal X to invocation Y."
- Invariant: a replay or projection of the workload matches visible storage.
- **Trivially cheap — proportional to the number of workload primitives, not state machine complexity.**
- This is the minimum model and it already catches #4566-class bugs: "generator submitted 10 signals, storage shows 7 in the journal, the other 3 are not in awaiting_rpc_actions either → corruption."
- Phase 1 VOPR-likes often start here before scaling up to type B.

## What Restate's observable contract actually looks like

From reading the lifecycle handlers and response sink flow, the client-observable behavior of the state machine reduces to roughly 9 semantic invariants:

1. **Exactly-once execution**: an accepted `Invoke` eventually reaches `Completed` (or `Free` via kill/cancel/purge); it is never executed twice unless explicitly `RestartAsNewInvocation`-ed.
2. **Idempotency key fusion**: two `Invoke`s with the same `IdempotencyId` collapse into one invocation; the second gets the first's response (`idempotency` table, `crates/storage-api/src/idempotency_table/`).
3. **Virtual object exclusion**: for each `ServiceId`, at most one invocation is `Invoked` at a time; others queue in `inbox` (`virtual_objects` table).
4. **Workflow singleton**: for each workflow key, at most one invocation is active; subsequent `Invoke`s either attach or are rejected (`crates/worker/src/partition/state_machine/tests/workflow.rs` tests this).
5. **Signal delivery**: a `NotifySignal` to a live invocation is either (a) delivered to the journal, (b) returned as `LostLeadership` (retryable), or (c) dropped because the invocation is `Completed`/`Free`. Never silently lost. — **This is the #4566 invariant.**
6. **Response sink delivery**: when an invocation transitions to `Completed`, every sink in `metadata.response_sinks` receives exactly one response.
7. **Promise consistency**: at most one `CompletePromise` per `(ServiceId, promise_key)`; every `GetPromise`/`PeekPromise` resolves to that value once set.
8. **State consistency**: for each `(ServiceId, key)`, `GetLazyState` observes the last `SetState` or `ClearState` in journal order.
9. **Timer monotonicity**: a `Timer` fires at a real time ≥ its scheduled time.

Some of these span multiple partitions (5, 6, 7 can route via outbox/shuffle) but the rest are single-partition. All are expressible in <500 lines of shadow state.

## A tractable three-tier model strategy

Rather than "build an oracle for the whole state machine" (untractable) or "don't build an oracle" (current state, no correctness signal), stage the model by value-per-line-of-code:

### Tier 1 — Workload-intent model (~200 lines, 1–2 days)

Extend `simulation.rs:79-122` to record generator intent:

```rust
struct WorkloadModel {
    // What the generator submitted, keyed by invocation.
    submitted: BTreeMap<InvocationId, InvocationIntent>,
}

struct InvocationIntent {
    signals_sent: Vec<SignalId>,
    responses_sent: Vec<CompletionId>,
    terminated: Option<TerminationFlavor>,
    expected_leadership_epoch: LeaderEpoch,
    // ... minimal bookkeeping
}
```

Per-step invariant (runs after every `sim_apply_envelope`):

```rust
fn assert_consistent(model: &WorkloadModel, storage: &SimStorage, awaiting: &AwaitingSet) {
    for (iid, intent) in &model.submitted {
        match storage.invocations.get(iid) {
            Some(InvocationStatus::Invoked(_) | Suspended{..}) => {
                // Every intent must be accounted for: either in the journal,
                // in awaiting_rpc_actions, or in a LostLeadership reply.
                for sig in &intent.signals_sent {
                    assert!(
                        storage.journal_has_signal(iid, sig)
                            || awaiting.contains_for(sig.request_id)
                            || model.returned_lost_leadership(sig),
                        "signal {sig:?} for {iid} silently lost"
                    );
                }
            }
            Some(InvocationStatus::Completed(_)) => { /* signals may be dropped */ }
            None | Some(InvocationStatus::Free) => { /* not yet submitted */ }
        }
    }
}
```

**What this catches**: #4566 exactly — silently dropped signals. Dropped or mis-routed responses. Lost leadership transitions. Basic counting bugs. Does not require understanding the state machine's internals.

**What it does not catch**: wrong values (e.g., SET x=1 then GET x returns 2). That's Tier 2.

### Tier 2 — Observable-outcome oracle (~500–800 lines, 1–2 weeks)

Add a KV shadow per service and promise shadow per workflow:

```rust
struct ObservableModel {
    workload: WorkloadModel,
    // Per-service KV — shadow of journal-applied SetState/ClearState
    state: BTreeMap<(ServiceId, String), Option<Bytes>>,
    // Per-(service,promise_key) promise shadow
    promises: BTreeMap<(ServiceId, String), PromiseState>,
    // Virtual object lock holder
    object_locks: BTreeMap<ServiceId, Option<InvocationId>>,
    // Idempotency → representative invocation
    idempotency: BTreeMap<IdempotencyId, InvocationId>,
}
```

The workload generator now issues **simulated invoker effects** — e.g., "this invocation will SetState(k, v) then GetLazyState(k) and expect v back." The model computes the expected completion value; the invariant checker reads the real journal and asserts the completion matches.

**What this catches**: invariants 1–8 in the contract list above. Subtler bugs like "virtual object lock released early" (a non-dead lock at the end of inbox draining), "promise completed twice", "state read sees a stale write".

**What it does not catch**: liveness (workflow stuck), ordering bugs across multi-partition routes (needs Tier 3), time-related bugs (needs randomized clocks).

### Tier 3 — Cross-partition + invoker-action model (~1k+ lines, 1–2 months)

Extends Tier 2 to multiple partitions with outbox→shuffle routing, and models the invoker as a stochastic "user code runner" that issues chains of commands. This is where the model genuinely starts to approach the complexity of the real state machine, and is Phase 2-or-later territory.

### Deliberate non-goal: shadowing every table

The current `SimStorage` shadows all 13 tables so the real state machine can run against it. That is *correct as an implementation artifact* — the real state machine needs somewhere to write. But the model should not re-derive the contents of `journal_v1`, `journal_v2`, `outbox`, `timers`, `fsm`, `dedup`, or `journal_events` by command replay. That path **is** proportional to implementation complexity and is the untractable critique the user raised.

## Restate-specific hazards the model must handle

These are real complications that TigerBeetle/FDB do not face. They don't make modeling untractable, but they do require explicit choices:

### 1. InvokerEffect is a nondeterministic input

The state machine receives `Command::InvokerEffect` (`wal-protocol/lib.rs:174`) carrying the output of a *remote user code runner*. The real invoker's behavior depends on user code. For the model, this means:

- **Option A**: the workload generator also generates synthetic invoker effects (scripted user code) and feeds them as `InvokerEffect` commands. This is what Tier 2 requires.
- **Option B**: treat `InvokerEffect` as oracle input — whatever the generator says the invoker returns is what the model expects. Cheaper but weaker coverage.
- **Option C**: stub the invoker with a tiny interpreter that runs a DSL ("SetState(k,v); Call(service); Output(x)") and have the model know the DSL. Good middle ground.

The current simulation does *none* of these — `gen_command` at `simulation.rs:79-122` never generates `InvokerEffect`, so the whole journal-entry path is untested by the per-step loop. That's a significant coverage gap independent of the model question.

### 2. Protocol version skew

The state machine currently supports Journal V1 and V2 in parallel (`mod.rs:33-34`, `sim_storage.rs:492, 353`). A model built against V2 will produce false positives on V1-only flows unless the workload generator is version-aware. Recommendation: pin the model to V2 only and have the generator skip V1 constructs. V1 is on the way out.

### 3. Virtual queues are fast-moving

The `vqueue_table` and `Stage`/`EntryCard` machinery (`sim_storage.rs:807-898`) is new and still evolving (see the bilrost dev-dep in `crates/worker/Cargo.toml`). Any model built today will need to track vqueue transitions carefully, or explicitly exclude them. **Recommendation**: exclude vqueues from Tier 1 and Tier 2 models; revisit once the subsystem stabilizes. The workload generator should simply not emit `VQWaitingToRunning`/`VQYieldRunning` until the model is extended.

### 4. Cross-partition routing (outbox → shuffle)

Single-partition models will report false positives for commands that were correctly routed to another partition. For Tier 1/Tier 2, either (a) assert "the command left as an `Action::NewOutboxMessage`" and let the model remove it from its expected set, or (b) run a 2-partition sim that routes outbox messages between two `SimStorage` instances (the roadmap already calls this out as #7 in `dst_vs_tigerbeetle_vopr_review_041026.md`).

### 5. Time-dependent commands

`Timer`, `ScheduleTimer`, `Sleep`, and `execution_time` on `Scheduled` invocations all depend on the simulator's clock. TigerBeetle uses a logical clock driven by ticks; Restate's simulation currently advances time by a fixed `+10ms` per step at `simulation.rs:173`. The model has to own the clock, not read `SystemTime::now()`. Note: this interacts with the entropy leaks in the prior review (`dst_vs_tigerbeetle_vopr_review_041026.md:72-87`).

## Revised verdict on the critique

| Claim | Verdict |
|---|---|
| TigerBeetle has simple state machine semantics, so a model is trivial. | **True.** Ledger balances are both the state and the contract. ~500-line model catches most bugs. |
| FoundationDB has simple state machine semantics, so a model is trivial. | **False as stated.** FDB's *contract* is simple (serializable KV); its *implementation* is enormous. The simulator validates the contract, not the implementation. This is the template Restate should copy. |
| Restate has a much more complicated model, given the domain. | **Partially true.** The implementation surface (62+ command types, 16 lifecycle handlers, 13 tables, protocol version skew) *is* materially larger. But the *observable contract* is ~9 invariants. Choosing to model the contract rather than the implementation keeps the model tractable. |
| Exhaustive valid model of Restate is untractable. | **True only for Model Type A** (internal state shadow). For Model Type B (observable-outcome oracle), Tier 2 is a 500–800 line, 1–2 week investment. Tier 1 is a 200-line, 1–2 day investment that catches #4566-class bugs. |

## Rebuttal framing for the team

If the team is worried that "Restate is too complex for a model," the right response is:

1. **Agree** that a literal shadow of the state machine is untractable, would not be worth maintaining, and would have false-positive churn every time the state machine evolves.
2. **Disagree** that the alternative is "no model." The observable-outcome oracle model type is tractable, matches the approach FoundationDB successfully uses at much greater implementation complexity than Restate, and catches the class of bugs we actually care about (silent data loss, ordering violations, duplicate execution, stuck invocations).
3. **Commit** to Tier 1 as a 1–2 day experiment. If Tier 1 catches real bugs, Tier 2 is justified. If Tier 1 catches nothing, Tier 2 is not worth it.
4. **Avoid** the trap of trying to cover every command variant before the model is useful. Every tier should be partial by design — cover the high-value invariants first, backfill as time allows.

## Concrete next step

The cheapest high-ROI move that also **validates the whole model-based testing thesis** is:

**Build Tier 1, targeted exactly at reproducing #4566 through the observable-outcome channel, not through a dedup-layer reproduction.**

Today the #4566 regression test at `simulation.rs:554-673` works by asserting the dedup layer's contract directly. That's a valid unit test of the dedup mechanism but it doesn't validate the model strategy — if we rewrote the test as "generate 100 signals, inject a leadership transition partway through, assert all 100 are accounted for in journal + awaiting_rpc_actions + lost_leadership_replies", and the assertion catches the bug without knowing anything about dedup, that's **empirical proof that a ~200 line observable-outcome oracle is all we need for Phase 1.5**.

If that experiment works, scaling Tier 1 → Tier 2 becomes a straightforward extension. If it fails for unexpected reasons, we learn which invariants are harder to observe than expected, and we make an informed call about Tier 2 vs alternative strategies.

## Liveness validation in this framework

Safety invariants ("bad things don't happen") and liveness invariants ("good things eventually happen") are dual but not symmetric. The observable-outcome oracle described above is a pure **safety** checker — it asserts that whatever the system has done so far is consistent with the contract. Liveness requires an additional construct: a notion of **progress** and a notion of **staleness**.

Liveness is also the hardest part of DST intellectually, because in a finite simulation you cannot prove "eventually" — you can only detect **stalls** and **deadlock-like patterns**. Everything below is about stall detection, not proof.

### What "progress" means for Restate

The observable-outcome oracle already knows what the workload *intends* the system to do. Liveness reuses that intent as the definition of progress:

1. **Invocation progress** — an invocation the workload has fully driven (all inputs supplied, all signals sent, `Output` command reached) should eventually transition to `Completed`. "Eventually" means: bounded number of simulator ticks of quiescent input.
2. **Signal delivery progress** — a `NotifySignal` the workload submitted should eventually appear in its target invocation's journal, be retried via `LostLeadership`, or be observed as a terminal-state drop.
3. **Timer progress** — a `ScheduleTimer` with expiry time T should fire once the simulator clock advances past T.
4. **Virtual object inbox progress** — an inbox queue should drain once its current head invocation terminates.
5. **Outbox drain progress** — outbox entries should eventually be consumed by the routing target (self or another simulated partition) and removed via `TruncateOutbox`.
6. **Response sink progress** — every sink in a completed invocation's `response_sinks` should see its `Action::IngressResponse` or `ForwardCompletion` emitted.
7. **Promise progress** — once a promise is `Completed`, all pending `GetPromise` waiters should resolve.
8. **Awaiting-reciprocal drain** — `awaiting_rpc_actions` should shrink toward zero under quiescent workload; a persistently non-empty set with no progress is a liveness failure.

Each of these reads directly off the same observable-outcome oracle that drives safety. The same shadow state that says "invocation X should have these N signals in its journal" can say "if X is not `Completed` after K ticks of quiescence, something is stuck."

### Why Restate's liveness is harder than TigerBeetle's

TigerBeetle's liveness check is beautifully simple: "if no client request has received a reply in K ticks, panic." Every client request produces a reply, so the absence of replies is an unambiguous stall signal.

Restate does not have this 1:1 property:

- **Legitimate long waits**: a workflow waiting on an external awakeable is correctly `Suspended` for arbitrarily long. You cannot panic on "invocation has been Suspended for K ticks" without massive false positives.
- **Cascading async effects**: a single `Invoke` may fan out into many journal entries, each potentially waiting for a completion. Progress is non-monotonic — an invocation can be `Invoked`, then `Suspended`, then `Invoked` again.
- **User code is an oracle black box**: the real invoker runs user code whose "progress" only Restate can't define unilaterally. The model has to define progress *for simulated user code the workload generator itself wrote*.
- **Cross-partition latency**: an `Action::NewOutboxMessage` may take several simulator ticks to reach its target partition via the routing loop. This is expected, not a stall.

The escape hatch is that the workload generator **owns the user-code side of every invocation it creates**, so the model knows exactly when an invocation *should* have enough inputs to make progress. Liveness checks are only meaningful relative to what the workload has supplied.

### Three liveness primitives

These compose with the three tiers of safety model from the earlier section:

#### 1. Quiescent drain (cheapest, maps to Tier 1)

The simplest pattern, and close to what TigerBeetle does:

1. Run the workload generator for `N` ticks, producing a mix of commands.
2. Enter **quiescent mode**: stop generating new commands. Keep advancing the simulator clock and processing pending in-flight state machine work, timer firings, outbox drains, and cross-partition routing.
3. Run quiescent mode for `K` additional ticks (where `K` is bounded — say 10×N or a fixed 1000).
4. **Assert at end of drain**: every invocation the model says "was fully driven" is now `Completed` or `Free`. Every signal the model submitted is accounted for (in journal, in a retried successful delivery, or in a terminal-state drop with a known reason). `awaiting_rpc_actions` is empty. The outbox is drained.
5. **If any check fails**: dump the full workload history plus the final state and panic.

This is 50–100 lines on top of Tier 1 and it catches:
- #4566 as a liveness symptom (invocation never completes because its driving signal vanished)
- virtual object inbox head stuck after its leader dropped the signal
- outbox never drained because of a shuffle routing bug
- response sink silently dropped
- stuck promise waiters

**What it misses**: anything that would eventually succeed given enough time. A slow-progress bug looks identical to a fast-progress success in quiescent drain. Works well because DST is *supposed* to collapse time — slow correctness bugs are Phase 3 territory, not Phase 1.5.

#### 2. Per-invocation staleness watchdog (maps to Tier 2)

Track, per invocation, the simulator tick at which the oracle last observed a state change:

```rust
struct LivenessWatchdog {
    last_progress_tick: BTreeMap<InvocationId, u64>,
    expected_terminal_by_tick: BTreeMap<InvocationId, u64>,
}
```

Update rules:

- Every time the oracle observes a change to an invocation's status, journal length, or awaiting-reciprocal entry, reset `last_progress_tick` to the current tick.
- When the workload generator emits the last input for an invocation (e.g., sends the final signal, or the invocation's synthetic user code reaches `Output`), record `expected_terminal_by_tick = current_tick + deadline`, where `deadline` is a configurable budget (say, 100 ticks plus 2× the current awaiting-reciprocal depth).

Per-step invariant:

- If `current_tick > expected_terminal_by_tick` and the invocation is not terminal → liveness violation.
- If `current_tick - last_progress_tick > stall_threshold` and the invocation is neither `Completed` nor `Suspended { waiting_for_notifications }` where all blocking notifications are on the workload's own "to-be-sent-later" list → liveness violation.

The second check is the subtle one: `Suspended` is a legitimate non-progress state *if and only if* the thing it's waiting for is genuinely in the workload's future. If the workload has already sent every signal it's going to send, and an invocation is `Suspended` waiting for a signal that will never arrive, that's a stall.

This adds ~150–300 lines to Tier 2 and catches staleness much earlier than quiescent drain — you see the fault at the step it happens, not 1000 steps later when quiescent drain finally panics.

#### 3. Progress-rate liveness (Tier 3+)

Track per-time-window completion rates:

- Count terminal transitions per N-tick window.
- Assert that under a stable workload generator, the completion rate stays above some floor.
- Panic on any window with zero completions despite non-zero accepted invocations.

This is useful for catching *degradation* bugs — the system is still live but has slowed to a crawl. It's also more noise-prone than the first two primitives because completion rate is inherently variable. Probably only worth it once the simpler checks are stable.

### The time-advancement coupling hazard

DST's power comes from collapsing real time into logical ticks. Liveness checks interact with this in an important way: if the simulator advances time freely (e.g., jumps the clock whenever no work is pending), "stuck" is almost impossible to observe — the clock just advances past any timer that would unstick things.

Two remediations:

1. **Bound per-tick time advancement** as a simulator invariant: time advances by at most `max_time_per_tick` (say, 10ms logical) regardless of work pending. A bug that needs more than this budget to make progress now shows up as a stall because the liveness watchdog's tick counter keeps ticking even when the clock is "waiting."
2. **Measure liveness in ticks, not time** — the watchdog uses the simulator's monotonic tick counter, not `time_advanced_ms`. This decouples liveness from clock games.

Both are cheap and should be set before writing any liveness check. They also interact with the randomized time advance recommendation from the prior roadmap (`dst_vs_tigerbeetle_vopr_review_041026.md:102`) — randomized time advance is fine as long as ticks advance monotonically per-step.

### Deadlock patterns worth targeting specifically

Some liveness failures have shapes specific enough to detect with targeted checks, independent of the generic watchdog:

| Pattern | Detection |
|---|---|
| **Virtual object inbox head stuck** | For each `ServiceId` with a non-empty inbox and a `VirtualObjectStatus::Locked(head_id)`, if `head_id`'s invocation has not made progress in K ticks, flag. Catches #4566-as-liveness. |
| **Workflow promise orphan** | A `CompletePromise` was applied but no `GetPromise` waiter was notified within K ticks. Catches the dual bug to #4566 on the promise path. |
| **Response sink orphan** | An invocation transitioned to `Completed` but its `response_sinks` still has entries at end of quiescent drain. Catches dropped ingress responses. |
| **Outbox non-drain** | Outbox has entries older than K ticks and the head sequence number hasn't advanced. Catches shuffle routing bugs. |
| **Timer queue stuck** | A timer's scheduled time is in the past (relative to the simulator clock) and it hasn't fired in K ticks. Catches timer queue bugs. |
| **Reciprocal leak** | `awaiting_rpc_actions` has an entry older than the longest in-flight invocation's lifetime. Catches reciprocal-held-beyond-apply bugs. |

Each of these is 10–30 lines and runs at the end of every step (or end of drain, depending on cost). They exist because they express domain-specific "stuck" shapes that the generic staleness watchdog might miss if the threshold is tuned loosely to avoid false positives.

### How liveness interacts with the three safety tiers

| Tier | Safety model | Liveness add-on | Lines |
|---|---|---|---|
| **Tier 1** — workload-intent shadow | ~200 lines | Quiescent drain + reciprocal-empty + outbox-drain + inbox-drain | +80 |
| **Tier 2** — observable-outcome oracle | ~500–800 lines | Per-invocation staleness watchdog + targeted deadlock patterns | +300 |
| **Tier 3** — cross-partition + invoker DSL | ~1k+ lines | Progress-rate windows + cross-partition end-to-end deadlines | +500 |

**Total for Tier 1 + its liveness layer**: ~280 lines. **Total for Tier 2 + its liveness layer**: ~1100 lines. These are in line with TigerBeetle's VOPR testing harness scale.

### The specific liveness experiment worth running first

Reusing the concrete next step from the earlier section, but with a liveness framing:

**Rewrite the `issue_4566_dedup_drops_old_epoch_self_proposal` regression test to use quiescent drain.** Structure:

1. Generate 100 signal RPCs interleaved with an `AnnounceLeader` mid-stream.
2. Run the simulator until all generated commands are applied.
3. Enter quiescent mode: advance time, process pending work, but submit no new commands.
4. Run quiescent mode for 500 more ticks.
5. Assert the **safety** invariant: every signal is in the journal or in `awaiting_rpc_actions` or was reported as `LostLeadership` to the workload.
6. Assert the **liveness** invariant: `awaiting_rpc_actions` is empty AND (for every invocation with the full signal set delivered) the invocation is `Completed` or still waiting for a signal the workload intentionally withheld.

If that combined test catches the bug without naming dedup, you've validated both the safety model and the liveness primitive at once. It's the same 1–2 day investment as the earlier recommendation but with double the payoff.

### What liveness does NOT try to prove

Important not-goals, to keep scope honest:

1. **Wait-freedom** — no claim about whether *any* invocation progresses, only that "intended-to-complete" ones do.
2. **Fairness** — no claim about whether two competing invocations for the same virtual object are scheduled fairly. Fairness is undefined in Restate's current contract.
3. **Latency bounds under load** — progress-rate liveness is an early-warning for degradation, not a perf test. Real latency SLOs are benchmarked elsewhere.
4. **Starvation in the presence of an adversarial workload** — if the workload never stops generating new high-priority work, older work may stall forever; that's by design.

Keeping these out of the liveness spec avoids the temptation to over-engineer the checker into a full performance model.

## Code References

| Concern | File | Lines |
|---|---|---|
| Top-level `Command` enum (22 variants) | `crates/wal-protocol/src/lib.rs` | 137-204 |
| Journal V2 `Command` (20 variants) | `crates/types/src/journal_v2/command.rs` | 47-68 |
| Journal V2 `Completion` (11 variants) | `crates/types/src/journal_v2/notification.rs` | 122-134 |
| `InvocationStatus` (7 states) | `crates/storage-api/src/invocation_status_table/mod.rs` | 141-154 |
| `InvocationStatusDiscriminants` | `crates/storage-api/src/invocation_status_table/mod.rs` | 388-396 |
| Lifecycle handler modules (16) | `crates/worker/src/partition/state_machine/lifecycle/mod.rs` | 11-26 |
| Entry handler modules | `crates/worker/src/partition/state_machine/entries/` | all |
| `Action` enum (18+ variants) | `crates/worker/src/partition/state_machine/actions.rs` | 30-110 |
| `SimStorage` tables (13) | `crates/worker/src/partition/state_machine/tests/sim_storage.rs` | 70-88 |
| Trait impls for sim storage | `crates/worker/src/partition/state_machine/tests/sim_storage.rs` | 128-898 |
| Main `Command` dispatch | `crates/worker/src/partition/state_machine/mod.rs` | 461-680 |
| Current `gen_command` (4 variants only) | `crates/worker/src/partition/state_machine/tests/simulation.rs` | 79-122 |
| `issue_4566_dedup_drops_old_epoch_self_proposal` | `crates/worker/src/partition/state_machine/tests/simulation.rs` | 554-673 |

## Related Research

- [DST vs TigerBeetle VOPR — Gap Analysis](./dst_vs_tigerbeetle_vopr_review_041026.md) — prior review flagging SimModel as #1 roadmap item and asking Open Question 1 ("Model granularity")
- [Deterministic Simulation Testing for Restate](./deterministic_simulation_testing_040926.md) — original Phase 0/1/2/3 roadmap
- [Issue #4566 Research](./awakeable_signal_loss_issue_4566_040926.md) — the concrete bug Tier 1 would catch
- [FoundationDB Simulation Docs](https://apple.github.io/foundationdb/testing.html)
- [TigerBeetle Liveness Testing Blog](https://tigerbeetle.com/blog/2023-07-06-simulation-testing-for-liveness/)

## Open Questions

1. **Is "observable-outcome" the right model paradigm for the whole team, or would some members prefer an internal-state shadow for specific subsystems (e.g., virtual objects)?** The two are not mutually exclusive — observable-outcome for most of the contract, internal-shadow for one or two places where the contract is hard to express.
2. **Protocol V1 exclusion**: is it safe to pin the model to V2 and drop V1 coverage entirely, or are there fleets still running V1 that need simulation coverage?
3. **Invoker modeling**: which of the three InvokerEffect strategies (generator-supplied, oracle-input, DSL-interpreted) is the team comfortable with? This choice determines Tier 2's shape.
4. **Tier 1 budget**: is 1–2 days of experimentation time available before committing to the larger Tier 2 investment? The answer determines whether to start now or defer past the DST Phase 2 integration work.
5. **Virtual queue modeling**: when does the vqueue subsystem stabilize enough to be worth including in a long-lived model? Is there a design document that would let the model target a stable contract independent of current implementation state?
