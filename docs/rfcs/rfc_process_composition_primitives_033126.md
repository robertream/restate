---
date: "2026-03-31T12:00:00-07:00"
git_commit: b1766c03b914aec16875d1a1c1cbe240f84255da
branch: service-macro
repository: restatedev/restate
topic: "RFC: Process Composition Primitives — Completing the Durable OTP Model"
tags: [rfc, architecture, otp, edges, supervision, process-groups, state-replication]
status: draft
last_updated: "2026-03-31"
last_updated_by: Claude Opus 4.6
last_updated_note: "Unified Object Links, Supervision, and Groups into a single edge primitive"
---

# RFC: Process Composition Primitives — Completing the Durable OTP Model

| Field | Value |
|-------|-------|
| Date | 2026-03-31 |
| Git Commit | `b1766c03b` |
| Branch | `service-macro` |
| Repository | `restatedev/restate` |
| Status | Draft |
| Authors | Robert Ream |

---

## Motivation

Restate has established a durable process model that maps cleanly to Erlang/OTP:

| OTP | Restate |
|-----|---------|
| Process | Invocation (every handler call is a process) |
| `gen_server` | Virtual Object (long-lived, keyed, single-writer) |
| `gen_statem` | Workflow (runs through states to completion) |
| `spawn(fun)` | Service handler invocation (stateless, run-and-return) |
| Process heap | Journal (durable execution context) |
| PID | InvocationId |
| Mailbox | Invocation queue (FIFO, single-writer for exclusive handlers) |
| `receive` | Awakeable / Signal (suspend until event) |
| Process crash + supervisor restart | Crash + journal replay (stronger: progress is never lost) |

The individual process primitive is strong — durable, language-agnostic, and observable (via the live-state-poc SSE stream). But OTP's power comes not from individual processes but from **composition primitives between processes**: supervision trees, links/monitors, process groups, selective receive, hot code reloading, universal introspection, and demand-driven backpressure.

Restate currently lacks these composition primitives. Relationships between invocations are implicit — encoded in application logic rather than expressed to the runtime. This RFC proposes four extensions that would complete the model.

---

## Context: What Exists Today

### Invocation Lifecycle

Every invocation progresses through a state machine (`crates/storage-api/src/invocation_status_table/mod.rs:141`):

```
Scheduled → Inboxed → Invoked → Suspended/Paused → Completed → Free
```

Parent-child relationships are tracked via `Source::Service(parent_id, parent_target)` (`crates/types/src/invocation/mod.rs:663`) and `ServiceInvocationResponseSink::PartitionProcessor(JournalCompletionTarget)` (same file, line 636). When a child completes, the response routes back to the parent's journal slot.

### Signals and Cancellation

Journal v2 unifies completions and signals under `NotificationId` (`crates/types/src/journal_v2/notification.rs:25`):

- `CompletionId` — result of a child call or sleep
- `SignalIndex` — numeric signal (index 1 = cancel)
- `SignalName` — named signal (awakeables use this in protocol v4+)

Cancellation is a built-in signal (`BuiltInSignal::Cancel = 1`) that propagates through the invoker to the SDK. Inter-invocation signals exist via `SendSignalCommand` (`crates/types/src/journal_v2/command.rs:315`).

### Attach (Closest Fan-in)

`AttachInvocationCommand` (`command.rs:356`) allows one invocation to subscribe to another's result. This is the only existing "observation" primitive — but it only observes completion, not lifecycle transitions.

### Live State (POC on this branch)

The `StateRouter` (`crates/ingress-http/src/state_router.rs`) provides real-time SSE streaming of virtual object state changes. Revision-based, incremental, repartition-resilient. This is the foundation for external observability and client-side state replication.

---

## Core Insight: One Primitive, Three Interpretations

The original RFC proposed three separate features — object links, supervision policies, and object groups — as independent systems with independent storage, query paths, and SSE event types. On closer analysis, they collapse into **a single primitive: the edge**.

In OTP:
- A **supervision tree** is a link topology with attached restart policies
- A **process group** is a set of processes sharing a label
- A **monitor** is a unidirectional observation relationship

All three are relationships between processes. They differ only in semantics — what the runtime *does* when it traverses the relationship. Restate can model all three with one edge type that carries a type annotation:

| Edge Type | OTP Equivalent | Runtime Behavior |
|-----------|----------------|------------------|
| **Link** | `link/1` + supervisor | Bidirectional lifecycle coupling. Failure/clear propagates as signal. Optionally carries supervision policy. |
| **Ref** | `monitor/2` | Unidirectional observation. No lifecycle coupling. Enables graph traversal and scoped replication. |
| **Group** | `pg:join/3` | Many-to-label membership. Enables discovery, multicast, and group-scoped subscription. |

The edge is the primitive. Supervision, groups, and observation are *interpretations* of edges by the runtime.

---

## Proposal 1: Object Edges — The Unified Composition Primitive

### The OTP Analogy

Erlang's process composition is built on three relationship types: **links** (bidirectional failure coupling), **monitors** (unidirectional observation), and **process groups** (named membership sets). These are the primitives that enable supervision trees, failure propagation, pub/sub, and service discovery. Without them, every inter-process coordination pattern must be hand-coded.

### What Restate Needs

Virtual objects today are isolated. An Account object has no structural relationship to its Billing, Sessions, or Organization objects. There is no way to address a set of objects, no way to declare lifecycle dependencies, and no way to subscribe to a subgraph of related state. Developers maintain these relationships in application state keys and hand-code traversal, fan-out, and failure handling in every handler.

### Design

Introduce **object edges** — typed, labeled relationships stored as first-class metadata alongside object state.

```rust
struct Edge {
    source: ServiceId,              // e.g., Account/acct-123
    target: ServiceId,              // e.g., Billing/acct-123
    edge_type: EdgeType,
    label: String,                  // e.g., "billing", "price-watchers"
    metadata: Option<EdgeMetadata>, // supervision policy, etc.
    created_at: MillisSinceEpoch,
}

enum EdgeType {
    /// Bidirectional lifecycle coupling. If target's state is cleared or a linked
    /// handler fails, source receives a signal. Optionally carries a supervision
    /// policy that the runtime applies to the set of Link edges sharing a label.
    Link,

    /// Unidirectional observation. Source → Target only. No lifecycle coupling.
    /// Enables graph traversal and scoped state replication.
    Ref,

    /// Many-to-label membership. Enables discovery (query members by label)
    /// and multicast (send to all members of a label).
    Group,
}
```

**Three edge types, one storage model, one event pipeline.**

### Edge Type: Link — Lifecycle Coupling and Supervision

Links model **existential dependencies** between objects. "I cannot be consistent if you are gone."

```typescript
// In a handler:
ctx.link("Billing", key, "billing");
ctx.link("Sessions", key, "sessions");
ctx.unlink("Billing", key, "billing");
```

**Runtime behavior:**

- When a linked target's state is cleared (lifecycle end), the runtime sends a signal to the source object
- When a linked target's handler fails, the runtime can apply a supervision policy (see below)
- Links are bidirectional — both objects can traverse and both receive lifecycle signals

**Supervision as edge annotation:**

A supervision tree *is* a set of Link edges from a parent object that share a label, with a policy attached. The policy tells the runtime what to do when any member of the set fails:

```typescript
// Link with supervision policy
ctx.link("PaymentService", key, "order-fulfillment", {
    supervision: {
        strategy: "one_for_all",   // restart all siblings if one fails
        maxRestarts: 3,
        window: "60s",
    }
});
ctx.link("ShippingService", key, "order-fulfillment", {
    supervision: { /* same policy — shared via label */ }
});
```

```rust
struct SupervisionPolicy {
    strategy: SupervisionStrategy,
    max_restarts: u32,
    max_restart_window: Duration,
    on_exhaustion: ExhaustionAction, // Fail | Escalate
}

enum SupervisionStrategy {
    /// Only the failed linked object's handler is retried.
    OneForOne,
    /// All objects sharing this link label have their handlers cancelled and retried.
    OneForAll,
    /// The failed object and all objects linked after it (by creation order) are retried.
    RestForOne,
}
```

The runtime reads the supervision policy from the edge metadata. When a handler on a linked target fails:
1. Look up all Link edges from the source with the same label
2. Apply the strategy (cancel siblings if `OneForAll`, etc.)
3. Track restart count per label; when exhausted, escalate to the source as a normal error signal

This means supervision is not a new journal entry type or a new runtime subsystem — it is **behavioral metadata on Link edges**, interpreted by the existing signal propagation path.

### Edge Type: Ref — Unidirectional Observation

Refs model **observation without dependency**. "I want to know about you, but I can live without you."

```typescript
ctx.ref("AuditLog", key, "audit");
ctx.ref("Dashboard", dashboardKey, "watched-accounts");
```

**Runtime behavior:**

- No lifecycle coupling — source is not signaled when target's state is cleared
- Enables graph traversal: the StateRouter can walk Ref edges to build a subgraph subscription
- Ref edges are unidirectional — only source → target traversal

### Edge Type: Group — Named Membership Sets

Groups model **shared identity**. "I belong to this set, and anyone can find and talk to the set."

```typescript
// Join a group
ctx.joinGroup("price-watchers");
ctx.leaveGroup("price-watchers");

// Multicast to all group members
ctx.sendToGroup("price-watchers", "onPriceUpdate", { ticker: "AAPL", price: 150.0 });

// Query membership
const members = ctx.groupMembers("price-watchers");
```

**Implementation as edges:** A group join creates a `Group` edge from the object to a virtual group target:

```
Counter/a --[Group: "price-watchers"]--> (group:price-watchers)
Counter/b --[Group: "price-watchers"]--> (group:price-watchers)
Counter/c --[Group: "price-watchers"]--> (group:price-watchers)
```

`sendToGroup("price-watchers", ...)` becomes: "follow all Group edges with label `price-watchers`, fan out one-way calls to each source."

**Runtime behavior:**

- Group membership stored as edges, indexed by label
- Membership automatically cleaned up when an object's state is cleared (edge removed)
- `sendToGroup` fans out as one-way calls to all current members
- Consistency model: strong eventual consistency (mirroring Erlang's `pg`). Cross-partition group queries may see briefly stale membership during repartition.

### Unified Storage Model

All three edge types share one storage layer:

- **One column family** in the partition store, indexed by `(source, label)` and `(target, label)` for bidirectional traversal
- **One event type** in the SSE pipeline: `EdgeMutation { added | removed, edge }` — replaces the need for separate `EdgeAdded`, `EdgeRemoved`, `GroupMembershipChanged` event types
- **One query path**: `GET /restate/objects/{service}/{key}/edges` returns all edges; filter by type or label

Edge mutations increment the object's revision counter, so they flow through the existing `StateChangeEvent` → `StateRouter` → SSE pipeline with no new infrastructure.

### Impact on Live State Replication

This is where edges become transformative. The current SSE endpoint subscribes to a single object. With edges, a client can **subscribe to a graph rooted at an object**:

```
GET /restate/objects/Account/acct-123/graph
```

The StateRouter walks edges from the root (Link and Ref edges define the subgraph boundary), subscribes to each reachable object, and multiplexes their change events onto a single SSE stream. The client gets a live-updating local copy of the entire entity subgraph — Account state, Billing state, Session states — from one connection.

Group edges extend this further: subscribe to a group label to get state changes for all group members.

### What This Enables

- **Scoped replication**: subscribe to an entity root, get the full subgraph replicated to a browser database
- **Scoped access control**: access to root implies access to subgraph — the graph boundary is the permission boundary
- **Scoped queries**: "everything related to this account" without the client knowing the schema
- **Failure propagation**: Link edges carry lifecycle semantics — the runtime propagates failure signals through the graph
- **Declarative supervision**: supervision policies on Link edges encode operational knowledge about failure domains into the graph itself
- **Pub/sub without coordinator objects**: Group edges enable reactive inter-object communication
- **Service discovery**: objects register in Group edges, callers query by label
- **One primitive to learn**: developers understand edges → they understand supervision, observation, groups, and scoped replication

---

## Proposal 2: State Migration Hooks — `on_upgrade` for Virtual Objects

### The OTP Analogy

OTP's `code_change/3` callback lets a running process transform its state when a new module version is loaded. The process is suspended, the callback transforms old state to new state, and the process resumes — all without losing connections or in-flight work.

### What Restate Needs

When deploying a new service version that changes the state schema, developers handle migration defensively in application code: check for old format, convert on read, hope nothing breaks. There is no structured migration path. This is especially painful for virtual objects with long-lived state — the object may hold state written months ago by a version that no longer exists.

### Design

Introduce an `onUpgrade` handler type for virtual objects, called lazily on first access after a deployment version change.

```typescript
// In service definition:
const counter = restate.object({
    name: "Counter",
    handlers: {
        increment: async (ctx) => { /* ... */ },
        onUpgrade: async (ctx, { fromVersion, toVersion }) => {
            // Migrate state from v1 to v2
            const old = await ctx.get("count");
            if (typeof old === "number") {
                await ctx.set("count", { value: old, lastModified: Date.now() });
            }
        },
    },
});
```

**Runtime behavior:**

- Partition store tracks the last deployment version that wrote to each object's state
- On first handler invocation after a new deployment, if the version differs, `onUpgrade` runs first (under the exclusive lock, before the actual handler)
- `onUpgrade` is itself a journaled invocation — if it crashes, it replays
- State version is updated atomically after `onUpgrade` completes
- Optional: bulk migration command via admin API to eagerly migrate all objects of a service

### Why Lazy

Eager migration of all objects on deployment is impractical at scale — a service with millions of keyed objects would block deployment for hours. Lazy migration amortizes the cost: each object migrates on first access, with no downtime and no bulk operation.

### Interaction with Edges

When an object has Link edges to other objects that also need migration, the question of ordering arises. Two options:

1. **No ordering guarantee** — each object migrates independently on first access. Handlers must tolerate mixed-version neighbors during the migration window. This is simpler and matches OTP's per-process `code_change/3` model.
2. **Graph-ordered migration** — `onUpgrade` on a root triggers `onUpgrade` on linked children first (depth-first). This ensures the subgraph is consistent when the root's handler runs. More complex, but eliminates mixed-version windows for strongly coupled entities.

Recommendation: start with (1). If mixed-version tolerance proves too burdensome, add (2) as an opt-in annotation on Link edges.

---

## Proposal 3: Invocation Introspection Protocol — `sys` for Durable Processes

### The OTP Analogy

Every OTP process responds to system messages: get state, get status, trace calls, suspend, resume. The `sys` module provides a universal debug protocol built into every process. This makes any running system fully inspectable without code changes.

### What Restate Needs

The admin API provides service-level and object-level state queries. But there is no way to inspect a *running invocation*: its current journal position, pending child calls, what notifications it's waiting on, how long it has been running, or its execution trace. This is the equivalent of an Erlang system where you can't `sys:get_state` or `observer:start`.

### Design

Introduce a **universal invocation introspection endpoint**:

```
GET /restate/invocations/{invocation_id}
```

Returns:

```json
{
    "invocation_id": "inv_abc123",
    "status": "suspended",
    "service": "OrderProcessor",
    "key": "order-456",
    "handler": "process",
    "source": { "type": "ingress", "request_id": "req_789" },
    "journal_length": 12,
    "waiting_for": [
        { "type": "completion", "id": 8, "target": "PaymentService/charge" },
        { "type": "signal_name", "name": "approval" }
    ],
    "children": [
        { "invocation_id": "inv_def456", "target": "PaymentService/charge", "status": "invoked" },
        { "invocation_id": "inv_ghi789", "target": "ShippingService/reserve", "status": "completed" }
    ],
    "edges": [
        { "type": "link", "label": "order-fulfillment", "target": "PaymentService/order-456" },
        { "type": "link", "label": "order-fulfillment", "target": "ShippingService/order-456" }
    ],
    "timestamps": {
        "created": "2026-03-31T10:00:00Z",
        "running_since": "2026-03-31T10:00:01Z",
        "suspended_since": "2026-03-31T10:00:05Z"
    },
    "deployment_id": "dp_v3"
}
```

**Extended operations** (mirroring `sys`):

| Operation | Endpoint | OTP Equivalent |
|---|---|---|
| Get status | `GET /invocations/{id}` | `sys:get_status` |
| Get journal | `GET /invocations/{id}/journal` | `sys:get_state` (the journal *is* the state) |
| Cancel | `DELETE /invocations/{id}` | `sys:terminate` |
| Send signal | `POST /invocations/{id}/signal` | `Pid ! Signal` |
| Trace | `GET /invocations/{id}/journal` (SSE) | `sys:trace` |

**The SSE variant** of journal inspection would stream journal entries as they are appended — a live execution trace. Combined with the live state stream, this gives complete real-time observability of both the process (invocation) and its state (virtual object).

### Interaction with Edges

Introspection becomes significantly more powerful with edges. The introspection response includes the object's edges, which means:

- **Graph visualization**: an "observer" UI can walk edges from any root and render the full entity graph with live status
- **Supervised group visibility**: Link edges with supervision annotations show the failure domain structure alongside invocation status
- **Group membership**: Group edges in the introspection response show which groups the object belongs to

The introspection endpoint and the edge graph together provide the same visibility that OTP's Observer tool gives Erlang developers — but over HTTP/SSE, for any language.

### What This Enables

- **Live debugging**: watch a suspended workflow's pending calls and signals in real time
- **Operational dashboards**: visualize invocation trees and edge graphs, identify stuck workflows, measure latency per step
- **Programmatic coordination**: external systems can inspect invocation status and send signals based on observed state
- **The "observer" equivalent**: a web UI that shows the full process graph of a running system, with live-updating status — Restate's version of Erlang's Observer tool

---

## Proposal 4: Demand-Driven Backpressure — Flow Control Between Invocations

### The OTP Analogy

GenStage/Flow provides demand-driven backpressure: consumers specify how many events they can handle, producers emit at most that many. The pipeline is self-throttling by construction — no consumer can be overwhelmed.

### What Restate Needs

Virtual object invocation queues are unbounded. A service under load that fans out to many objects can overwhelm them. There is no mechanism for an object to signal "I'm overloaded, slow down" — the queue grows until the partition store absorbs it or the system degrades. With Group edges enabling easy multicast (`sendToGroup` with 10,000 members), backpressure becomes critical.

### Design

Introduce **queue pressure signals** — runtime-level feedback from invocation queues to callers.

```rust
struct QueuePressure {
    service_id: ServiceId,
    queue_depth: u32,
    max_queue_depth: u32,       // configurable per-service
    pressure: PressureLevel,    // Normal | Elevated | Critical
}
```

**Behavior:**

- When a virtual object's inboxed invocation count exceeds a configurable threshold, the runtime emits a `QueuePressure` signal
- Callers (ingress, other invocations) receive backpressure feedback:
  - **Ingress**: returns `429 Too Many Requests` with `Retry-After`
  - **Inter-service calls**: the calling invocation's journal records the backpressure, and the SDK can choose to delay, batch, or circuit-break
- The partition processor tracks queue depth per keyed object and per service
- Configurable per-service via deployment metadata: `maxQueueDepth`, `pressureThresholds`

### Interaction with Edges

Group edges make backpressure more important — and also provide a natural scope for it. Instead of per-object backpressure only, the runtime can aggregate pressure across a group:

- `sendToGroup("price-watchers", ...)` checks aggregate queue depth of group members before fan-out
- If aggregate pressure is `Critical`, the runtime can reject the multicast or apply a throttle
- Group-level pressure metrics flow through the SSE pipeline for real-time observability

---

## Implementation Phases

The unified edge model simplifies phasing — each phase adds behavior to the same underlying primitive rather than introducing new concepts.

```
Phase 1: Edge Storage + Inert Graph
┌─────────────────────────────────────────────────────┐
│  Edge column family in partition store               │
│  Edge mutations in SSE pipeline (EdgeMutation event) │
│  Graph subscription endpoint (walk edges, multiplex) │
│  Edge query API (by source, target, label, type)     │
│  Introspection endpoint (status + journal + edges)   │
└──────────────────────────┬──────────────────────────┘
                           │ Edges exist and are observable,
                           │ but the runtime does not interpret them.
                           │
Phase 2: Runtime Interprets Edge Types
┌──────────────────────────▼──────────────────────────┐
│  Link: lifecycle signals on clear/failure            │
│  Group: sendToGroup fan-out, membership queries      │
│  Ref: no new behavior (traversal-only, already works)│
│  State migration (onUpgrade) hooks                   │
└──────────────────────────┬──────────────────────────┘
                           │ Edges carry behavioral semantics.
                           │ The graph is alive.
                           │
Phase 3: Supervision + Backpressure
┌──────────────────────────▼──────────────────────────┐
│  Supervision policies as Link edge annotations       │
│  Runtime applies restart strategies on failure       │
│  Queue pressure signals (per-object + per-group)     │
│  Backpressure-aware sendToGroup                      │
└─────────────────────────────────────────────────────┘
```

Each phase delivers standalone value:

- **Phase 1** gives developers graph structure, scoped SSE replication, and invocation introspection. Applications immediately benefit from visual observability and client-side subgraph sync.
- **Phase 2** makes the graph reactive. Link edges propagate lifecycle events. Group edges enable pub/sub. State migration solves a practical pain point.
- **Phase 3** adds operational intelligence. Supervision policies encode failure domain knowledge. Backpressure prevents cascading overload in fan-out patterns.

---

## Relationship to Live State POC

The live-state-poc on this branch (`crates/ingress-http/src/state_router.rs`) provides the delivery mechanism for the edge system:

| Capability | SSE Integration |
|---|---|
| Edge mutations | New `EdgeMutation` event type in the state change stream |
| Graph subscription | StateRouter walks edges from root, subscribes to each node, multiplexes events |
| Group membership | Group edge mutations as `EdgeMutation` events |
| Supervision status | Link edge annotations visible in introspection + SSE |
| State migration | Version change events in the state change stream |
| Invocation tracing | Journal entries streamed as SSE for live execution traces |
| Backpressure | Queue pressure metrics in the state/introspection stream |

The SSE infrastructure is not just an observability feature — it is the **replication channel** for all structural metadata about the process graph. The edge system adds new event types to the same stream, building toward the vision of a client subscribing to an entity root and receiving a live, consistent, offline-capable local copy of the full subgraph.

---

## The Completed Model

With all four proposals, the mapping to OTP is complete — and in several areas, stronger:

| OTP Primitive | Restate Equivalent | Advantage Over OTP |
|---|---|---|
| Links | Link edges | Durable, persistent across restarts, queryable |
| Monitors | Ref edges | Same advantages; enables scoped replication |
| Process groups (`pg`) | Group edges | Durable membership; SSE-observable; same primitive as links |
| Supervision trees | Supervision policies on Link edges | Durable; declared on the graph, not in a separate tree |
| `code_change/3` | `onUpgrade` handler | Lazy per-object; journaled (crash-safe) |
| `sys` module | Invocation introspection API + SSE | HTTP-accessible; language-agnostic; streamable |
| GenStage / Flow | Queue pressure signals | Runtime-level; automatic; group-aware |
| None (OTP lacks this) | Live state replication via SSE | Real-time client-side subgraph sync |

The key simplification: **three OTP primitives (links, monitors, process groups) collapse into one Restate primitive (edges) with three type annotations**. One storage model, one event pipeline, one query path, one concept for developers to learn.

The thesis: **Restate becomes a durable, language-agnostic Erlang/OTP — with built-in real-time state replication to clients.** Developers write handlers in any language. They get isolated single-writer actors with durable state, structural relationships expressed as edges, declarative failure handling via supervision annotations, demand-driven flow control, and live-syncing of entity subgraphs to frontends. No BEAM required. No infrastructure assembly required. The runtime manages state, orchestrates execution, propagates changes, and maintains the process graph.

---

## Open Questions

1. **Edge storage model**: Should edges share the object state column family (simpler, same revision stream) or use a separate column family (independent indexing, no revision coupling)? Sharing is simpler for SSE but may complicate edge-only queries.

2. **Cross-partition graph traversal**: Edges can span partitions. Should graph subscriptions be handled by the ingress layer (fan-out to multiple partitions) or by a new graph-aware routing layer? The ingress approach is simpler but may not scale for deep graphs.

3. **Supervision scope**: Should supervision policies apply only to Link edges between virtual objects, or also to invocation-level relationships (e.g., concurrent child calls within a single handler)? The edge model naturally fits inter-object supervision; intra-invocation supervision may need a complementary journal-level primitive.

4. **Group consistency**: Is strong eventual consistency sufficient for Group edge membership, or do some use cases require linearizable group operations? OTP's `pg` chose eventual consistency deliberately — does Restate's durable storage change the tradeoff?

5. **Backpressure granularity**: Per-object queue depth is the natural unit, but per-group aggregate pressure may be more useful for multicast throttling. Should both be supported from the start?

6. **State migration ordering**: Should `onUpgrade` respect edge topology (depth-first from root) or run independently per-object? See the discussion in Proposal 2.

7. **Edge cardinality limits**: Should there be a maximum number of edges per object? Group edges in particular could grow unbounded if thousands of objects join the same label. What is the storage and query cost model?

8. **SDK impact**: The edge API is the primary new SDK surface. What is the minimum API to validate the model — just `link`/`ref`/`joinGroup` + `edges()` query? Or does `sendToGroup` need to ship in the same release?

---

## References

### Codebase

- Invocation lifecycle: `crates/types/src/invocation/mod.rs`
- Invocation status state machine: `crates/storage-api/src/invocation_status_table/mod.rs:141`
- Signal/notification system: `crates/types/src/journal_v2/notification.rs`
- Cancellation propagation: `crates/worker/src/partition/state_machine/lifecycle/cancel.rs`
- Attach (fan-in): `crates/types/src/journal_v2/command.rs:356`
- StateRouter (live state): `crates/ingress-http/src/state_router.rs`
- Partition store change detection: `crates/partition-store/src/partition_store.rs:67`
- Spawn processor task (SSE integration): `crates/worker/src/partition_processor_manager/spawn_processor_task.rs`

### Erlang/OTP

- [OTP Design Principles](https://www.erlang.org/doc/system/design_principles.html)
- [Supervisor Behaviour](https://learnyousomeerlang.com/supervisors)
- [pg Module (kernel)](https://www.erlang.org/doc/apps/kernel/pg.html)
- [sys Module (stdlib)](https://www.erlang.org/doc/apps/stdlib/sys.html)
- [Release Handling](https://www.erlang.org/doc/system/release_handling.html)
- [GenStage](https://hexdocs.pm/gen_stage/GenStage.html)
- [Joe Armstrong — Making Reliable Distributed Systems in the Presence of Software Errors (2003)](https://erlang.org/download/armstrong_thesis_2003.pdf)
