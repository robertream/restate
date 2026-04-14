---
date: "2026-03-31T15:22:00-07:00"
git_commit: b1766c03b914aec16875d1a1c1cbe240f84255da
branch: service-macro
repository: restatedev/restate
topic: "Future Design Ideas — Parked Concepts from State Graph Design Session"
tags: [design, future, supervision, backpressure, channels, reactive, aggregation]
status: parked
last_updated: "2026-03-31"
last_updated_by: Claude Opus 4.6
---

# Future Design Ideas

Concepts explored during the state graph and links design session (2026-03-31) that were parked for future consideration. Each was discussed, critiqued, and set aside — either because it's not yet needed, depends on unproven foundations, or maps poorly from the OTP analogy to Restate's durable execution model.

These are starting points for future design sessions, not commitments.

---

## 1. Supervision Policies

**The idea**: Attach supervision strategies (one_for_one, one_for_all, rest_for_one) to links between objects/workflows. When a linked entity fails, the runtime applies the strategy — restarting siblings, escalating failures, enforcing restart limits.

**Why it was parked**: OTP supervision assumes ephemeral processes that lose state on crash. Restate processes replay from journal — their state is intact after failure. The failure modes are fundamentally different. `one_for_all` (restart all siblings) makes sense when a crashed process lost its heap and siblings' assumptions are invalid. In Restate, the failed invocation replays — its state is preserved. The sibling restart semantics don't map cleanly.

**When to revisit**: When real users hit failure patterns that journal replay doesn't solve. The most likely case: external side effects that can't be replayed (e.g., a payment was charged but the workflow crashed before recording the charge). This is the saga/compensation pattern, which may be better addressed by completion handlers on links (already in the State Graph RFC) than by supervision policies.

**Key design artifacts**: `SupervisionPolicy` struct, `SupervisionStrategy` enum, supervision as edge annotation — see original RFC `rfc_process_composition_primitives_033126.md` lines 161-202.

---

## 2. Demand-Driven Backpressure

**The idea**: Queue pressure signals from overwhelmed virtual objects back to callers. Ingress returns 429. Inter-service calls receive backpressure feedback. Configurable per-service thresholds.

**Why it was parked**: Requires runtime changes to the invocation queue path. The need is real but not urgent — most systems won't hit the scale where unbounded queues are a problem. More pressing with broadcast channels (which amplify fan-out), but channels are themselves deferred.

**When to revisit**: When broadcast channels are designed, or when a user reports queue-related degradation at scale.

**Key design artifacts**: `QueuePressure` struct, per-service/per-object thresholds — see original RFC lines 422-461.

---

## 3. Broadcast Channels (Bifrost-Backed)

**The idea**: Partition-aware broadcast infrastructure distinct from links. Publisher appends to a Bifrost log. Each partition maintains a local subscriber list. Delivery is one message per partition, not one per subscriber. Adaptive batching based on throughput needs.

**Why it was separated**: Broadcast is a communication/routing problem, not a graph/structural problem. Links model relationships. Channels deliver messages. They compose but don't collapse into one primitive. The implementation is significant (new subscription registry per partition, Bifrost integration, repartition-aware subscriber lists).

**Status**: Separated into its own RFC track. See placeholder at `rfc_channels_033126.md`.

**Key design insights**:
- A link between two objects is data-plane (relationship metadata). A channel is control-plane (delivery mechanism).
- Publisher's journal has O(1) entries regardless of subscriber count.
- Fan-out is partition-batched: one cross-partition message per partition, not per subscriber.
- Adaptive batching: low-frequency channels deliver immediately, high-frequency channels coalesce.

---

## 4. Reactive Edges

**The idea**: State change on one object automatically triggers a handler invocation on edge-connected objects. The state change IS the message. No explicit broadcast. Revision-coalesced eventual consistency.

**Why it was parked**: Powerful but dangerous. Unresolved problems: cycle detection (A->B->C->A creates unbounded recursive invocations), ordering guarantees, failure semantics of chain reactions, performance model. The auto-resolve awakeable pattern (explicit decision points with timeout) achieves similar outcomes with clear boundaries and no unbounded propagation.

**When to revisit**: If the pattern of "react to state changes on another object" appears frequently in user code and the explicit signal/awakeable approach proves too cumbersome.

**Key insight**: Explicit await points with auto-resolve > implicit reactive edges. The await point gives reactivity where you want it, with clear boundaries.

---

## 5. Reactive Aggregation

**The idea**: Edge properties define algebraic aggregation functions (sum, avg, min, max, countBy). The runtime maintains aggregates incrementally at O(1) cost as children's state changes. Avoids signal storms for monitoring large groups.

**Why it was parked**: Deep dive into semigroups, commutative groups, CRDTs. Intellectually interesting but produced no use case that couldn't be solved by a handler reading children's state. The agent swarm scenario (coordinator monitoring aggregate confidence) works with periodic polling or simple signals.

**When to revisit**: If a concrete user has >1000 children and the polling/signal approach creates measurable performance problems.

**Key design insight**: The algebraic structure matters — commutative groups (with inverses) enable O(1) incremental updates. Commutative monoids (without inverses, like min/max) require O(N) on removal. Built-in sum/avg/count covers 95% of cases.

---

## 6. Capability Handles for LiveState

**The idea**: An object creates a LiveState node and passes a mutable handle to a workflow. The workflow writes through the handle. The object observes. External clients subscribe.

**Why it was parked**: Made unnecessary by the realization that workflow state is inherently observable. The workflow writes to its own state. The SSE infrastructure streams it. The ownership link scopes observation. No separate LiveState node or capability token needed for the common case.

**When to revisit**: If a concrete use case requires shared mutable state that doesn't belong to any single workflow — e.g., a coordination surface written by multiple workflows simultaneously. This is rare and likely better served by a virtual object with handlers.

---

## 7. Four-Node Taxonomy

**The idea**: Four graph node types distinguished by compaction policy and data flow direction:

| | Coalescable (latest value) | Ordered (every message) |
|---|---|---|
| **One -> Many** | LiveState | Broadcast |
| **Many -> One** | LiveAggregation | Stream |

**Why it was parked**: Clean mental model but premature as implementation target. LiveState is what the SSE POC already does. Broadcast needs its own RFC (channels). LiveAggregation and Stream may collapse into simpler patterns once implementation starts.

**Status**: Retained as a conceptual framework for thinking about communication patterns. Not an implementation target.

---

## 8. `ctx.emit()` / Output Streams

**The idea**: An append-only output channel for invocations — `ctx.emit(name, data)` as a stdout equivalent. Journaled but skipped on replay. Graph-scoped SSE multiplexes output from all nodes in the ownership tree.

**Why it evolved**: The session realized that workflow state already serves this purpose. Workflows write progress to their own state. The SSE infrastructure streams it. No separate emit primitive needed for the common case.

**When to revisit**: If workflows need to emit structured events that are distinct from state updates — e.g., log-like events, metrics, or events that should be append-only rather than last-write-wins. The current model (state = progress) works for progress reporting but may not cover all observability needs.

---

## 9. OTP Analogies — What Mapped Well and What Didn't

### Good mappings
- Process -> Invocation (every handler call is a process)
- gen_server -> Virtual Object (long-lived keyed actor)
- gen_statem -> Workflow (runs through states to completion)
- spawn_link -> ctx.spawn() with automatic link
- Process mailbox -> Invocation queue
- receive -> Awakeable / Signal

### Poor mappings
- Supervision trees -> Restate's journal replay changes failure semantics fundamentally
- Process groups (pg) -> Groups are just high in-degree objects, not a separate primitive
- Selective receive -> Not needed; Restate's signal/awakeable system is more structured
- Hot code reloading (code_change/3) -> onUpgrade is a valid adaptation but the mechanics are very different
- GenStage/Flow backpressure -> Restate's partition-based architecture needs different backpressure models

### The lesson
OTP analogies were useful for initial vocabulary and gap identification. They became misleading when used to generate feature requirements. Restate's durable execution model is fundamentally different from OTP's ephemeral process model — features should be designed from Restate's constraints, not imported from Erlang.

---

## 10. Promise Objects and Exclusive Workflow Handlers

**The idea**: Objects that participate in the workflow completion tree — completing via `ctx.freeze()`, blocking their parent's completion like workflows do.

**Promise Objects**: An object created by a workflow that acts as an interactive promise. External callers mutate its state via handlers (freeform, any order). When done, `ctx.freeze()` makes the state immutable and signals completion to the parent workflow. Read-only handlers continue to work after freeze. The parent's link sees the frozen object as completed.

**Key realization during design**: Workflows with exclusive shared handlers already cover the Promise Object use case. Shared handlers become exclusive (queued), running only when `run` is suspended on an await. Same single-writer model as virtual objects — `run` and handlers take turns via the exclusive lock. A workflow where `run` coordinates lifecycle and exclusive handlers provide the interactive surface — `return` is effectively `freeze`. No new tier needed, just an enhancement to workflow handler capabilities. **This is the decided path** — not a question, not an option to evaluate.

**Why it was parked**: The linked workflow model (State Graph RFC) should be proven first. Promise Objects are deferred until we have experience with linked workflows. Exclusive shared handlers on workflows are the mechanism for interactive workflows — the design is decided, the implementation timing is deferred.

**When to revisit**: After linked workflow completion semantics (`Completing` → `Completed`) are implemented and users have experience with the model. If the "workflow with exclusive handlers" pattern proves insufficient for freeform interactive use cases, Promise Objects become the answer.

---

## 11. Shared Ownership / Reference-Counted GC for Links

**The idea**: Multiple objects can link to the same workflow or object. Links are not exclusive. The entity persists as long as any link holder retains a link — reference-counted GC. The last link removal triggers cleanup.

```
Account/acct-123   --[link]--> BillingAccount/acct-123
Organization/org-1 --[link]--> BillingAccount/acct-123
```

Both have the billing account in their ego graph. Both observe its state via SSE. The spawner is not special — other objects can add links afterward.

**Why it was parked**: Reference-counted GC across partitions is a distributed reference counting problem — one of the hardest problems in distributed systems. Even intra-partition, you need to handle: concurrent unlink races, orphaned entities when the last linker crashes mid-unlink, and the semantic question of what "GC" means for an object that has persistent state. The v1 State Graph RFC uses single ownership (the spawner owns the link). Other entities can observe via `attach` (existing Restate primitive) without creating ownership.

**When to revisit**: When concrete use cases require true shared ownership beyond what `attach` provides. The most likely case: organizational hierarchies where an entity genuinely belongs to multiple parents. If shared ownership is needed, the link storage model must support multiple linkers per target with atomic reference counting.

---

## 12. Bidirectional Links

**The idea**: When a parent spawns a child, create two directed links atomically — parent → child and child → parent. The child can reference its parent through the back-link. `ctx.spawn()` creates a bidirectional link by default.

**Why it was parked**: The child → parent back-link has unclear value. Workflow state is only mutable by the workflow itself, so the parent link in the child can't be written to — it's a read-only back-pointer. The RFC never identified a use case where a child workflow reads its parent link. Bidirectional links also:
- Double the number of link writes on spawn
- Create cycles in the graph (complicating traversal and GC)
- Have unclear semantics with shared ownership (which parent?)

The v1 State Graph RFC uses unidirectional links (parent → child only). If a child needs to communicate with a parent, it can do so through signals or by the parent attaching to the child's result.

**When to revisit**: If a concrete use case emerges where a child workflow needs to discover or reference its parent. The most likely case: a child workflow that needs to read the parent's state for context. This could also be solved by passing context as input rather than creating a structural back-link.

---

## References

- Original combined RFC: `rfc_process_composition_primitives_033126.md`
- State Graph RFC (active): `rfc_state_graph_and_links_033126.md`
- Channels RFC (placeholder): `rfc_channels_033126.md`
- Erlang/OTP research compiled during session (see original RFC references section)
