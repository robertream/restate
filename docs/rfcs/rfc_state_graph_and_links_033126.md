---
date: "2026-03-31T15:22:00-07:00"
git_commit: b1766c03b914aec16875d1a1c1cbe240f84255da
branch: service-macro
repository: restatedev/restate
topic: "RFC: State Graph and Links"
tags: [rfc, architecture, links, workflows, state-graph, observability]
status: draft
last_updated: "2026-04-02"
last_updated_by: Claude Opus 4.6
last_updated_note: "Plan review pass 2: simplified completion to Result<T,E>, removed inline graph from completion, added Completing invocation status, specified journal entries (OneWayCallCommand + CreateLink + AttachInvocationCommand), new types (Link/HandlerInvocation sink variants, LinkCompletionNotification), cross-partition from day one, removed content-addressable graphs references, onCompleted is object-only, no framework timeout defaults, removed speculative REST API."
---

# RFC: State Graph and Links

| Field | Value |
|-------|-------|
| Date | 2026-03-31 |
| Git Commit | `b1766c03b` |
| Branch | `service-macro` |
| Repository | `restatedev/restate` |
| Status | Draft |
| Authors | Robert Ream |

---

## Scope

This RFC covers **linked workflows** — workflows spawned by objects or other workflows, connected by unidirectional parent → child links. Links make workflow relationships explicit, workflow progress observable, and computation graphs inspectable.

**Out of scope** (separate RFCs):
- Object-to-object links (see `rfc_state_introspection_033126.md` — object links for ego graph inclusion)
- State migration / `onUpgrade` (see `rfc_on_upgrade_state_migration_033126.md`)
- State introspection API design (deferred)
- Shared ownership / ref-counted GC (see `future_design_ideas_033126.md` section 11)
- Bidirectional links (see `future_design_ideas_033126.md` section 12)

---

## Motivation

Restate's durable execution model provides three tiers of computation — service handlers, workflows, and virtual objects — each with distinct durability and lifecycle semantics. Today these tiers operate in isolation: objects don't know about the workflows they spawn, workflows can't observe each other's progress, and external clients have no way to watch a computation unfold.

This RFC proposes **links** as the composition primitive that connects workflows into observable, manageable state graphs. Links make relationships between entities explicit, workflow progress observable, and computation graphs inspectable — by other services, by external clients, and by human or AI supervisors.

The foundation is the live-state-poc SSE infrastructure on this branch, which already streams virtual object state changes in real time.

---

## The Execution Model

### Three Tiers

Restate provides three computation tiers — **service handlers** (stateless durable functions), **workflows** (stateful durable processes that run to completion), and **virtual objects** (persistent keyed state with transactional handlers). This RFC focuses on how workflows and objects compose via links. For full tier semantics, see the Restate documentation.

| | Service Handler | Workflow | Virtual Object |
|---|---|---|---|
| State | None | Transient, durable while running, frozen on completion | Persistent until cleared |
| Lifetime | Single invocation | Until completion + retention | Until explicit clear |
| Mutability | N/A | `run` handler only (external write access deferred) | Object handlers + link-delegated writes |
| Result | Return value | `Result<T, E>` (delivered on completion) | N/A (state is mutable) |
| Completion | When handler returns | When handler returns **and** all linked children complete | N/A (persistent) |

### The Persistence Boundary

Workflow state is transient. Object state is persistent. The **link** is how computed facts cross from transient execution into persistent record.

If you want a workflow's result to outlive the workflow, it must be stored in or associated with a virtual object. This is not a limitation — it forces an explicit decision about what persists and what doesn't. Transient execution state (progress, intermediate results, retries) stays with the workflow. Persistent facts (the final answer, the business record) go to the object.

### Mutability Contracts

| Entity | Who can write | Mechanism |
|---|---|---|
| Workflow state | The `run` handler | Direct state ops during execution |
| Object state | Object handlers + runtime (link-delegated) | Handler invocation or link status update |
| Completed results | Nobody | Immutable after completion |

**Link-delegated writes**: When an object creates a link to a workflow, the runtime updates the link's status and result in the link table on workflow completion. The object authorized this by creating the link — the write is scoped to link metadata only.

Workflow state is written exclusively by the `run` handler. It is readable via shared handlers and externally via SSE. External write access (exclusive handlers on workflows) is deferred — see [Future: Workflow Handlers](#future-workflow-handlers) below.

### Workflow State Lifecycle

Workflow state evolves during execution and **freezes on completion**:

- **Running** (`Invoked`): state updates as the workflow progresses. Observable via SSE.
- **Completing** (`Completing`): the workflow's `run` handler has returned, but linked children are still executing. The workflow's own code is done — it cannot modify its own state further. But its linked children are still executing, and their state is still evolving. The result is **not observable** to link holders during this phase.
- **Completed** (`Completed`): all linked child workflows have also completed (recursively). The entire graph — the workflow's own state, its children's state, and their children's state — is now **frozen and immutable**. The `Result<T, E>` is delivered to link holders.
- **Failed**: the workflow failed during execution. State freezes at whatever point execution stopped. The error is the result. Linked children are cancelled (see Workflow Tree Cancellation below).

`Completing` is a new `InvocationStatus` variant. When the `run` handler returns and there are no active linked children, the invocation skips directly to `Completed` (existing behavior unchanged). `Completing` is only entered when linked children are still active.

The key invariant: **a completed workflow's graph is fully immutable — no node in it is still running.** This means the frozen graph is a consistent, point-in-time snapshot of the entire computation.

The transition from `Completing` → `Completed` happens automatically when the last linked child completes. No application code runs during this transition.

#### Completing State Visibility

The `Completing` state is **not observable through links**. Link holders see the workflow as `running` until it fully completes (all children done), at which point the link status transitions to `success` or `failed` with the result. Two terminal link states, no intermediate "almost done" state to handle.

#### Graph Curation is Load-Bearing

Because linked children block completion, **curation determines what you wait for**. A workflow that forgets to remove a link to a child that hangs forever will itself never complete. This is by design — it forces the developer to be explicit about which children are part of the computation and which are disposable helpers. The owning object or workflow can enforce timeouts via cancellation.

```typescript
async run(ctx, { query }) {
    ctx.startLinkedWorkflow("Parser", ...);      // essential — keep linked
    ctx.startLinkedWorkflow("TempCache", ...);    // helper — unlink before returning

    // ... work happens ...

    ctx.removeLink("TempCache");                  // don't wait for this
    return { synthesis: output };                  // now only waiting for Parser
}
```

Unlinked children continue running independently — they just don't block the parent's completion and aren't part of the frozen graph.

---

## Links — The Composition Primitive

### What Links Are

A link is a **directed, unidirectional relationship** from a parent entity to a child workflow, stored as first-class metadata in a dedicated link table. Links are parent → child only. Links make relationships explicit to the runtime, enabling:

- Observable state graphs (ego graph SSE subscription)
- Lifecycle management (workflow completion blocking on children)
- Workflow result access (link status shows completion state)
- Graph retention for analytics, learning, and reporting

### Link Ownership

Links have **single ownership**. The entity that creates the link owns it. There is no shared ownership or reference counting in v1. If a workflow is spawned by an object, the object owns the link. If a workflow spawns a child workflow, the parent workflow owns the link.

### Client API

Three operations:

1. **Start a linked workflow** — creates the child invocation and the link
2. **Remove a linked workflow** — detaches the child from the parent's graph
3. **Read/traverse links** — query link status and graph structure (details deferred)

### Creating Links

Objects and workflows create links to child workflows:

```typescript
// Object starts a linked workflow
ctx.startLinkedWorkflow("FulfillOrder", orderKey, {
    label: "fulfillment",
    onCompleted: { handler: "onFulfillmentDone" },  // object-only
});

// Workflow starts linked child workflows
ctx.startLinkedWorkflow("Parser", `${ctx.key}-parse`, { label: "parser" });
ctx.startLinkedWorkflow("Scorer", `${ctx.key}-score`, { label: "scorer" });
```

### Link Status

The link provides a **window into the linked workflow's current status**:

```typescript
// Running:
// { status: "running", workflow: "Workflow/fulfill-123" }

// Success:
// { status: "success", result: { tracking: "1Z999" }, workflow: "Workflow/fulfill-123" }

// Failed:
// { status: "failed", error: { ... }, workflow: "Workflow/fulfill-123" }
```

### Completion Result

Completion delivers `Result<T, E>` — the workflow's return value on success, or the error on failure. No separate status field; the Result discriminant encodes success vs. failure.

Graph traversal (inspecting the frozen state graph of children) is a separate concern with its own API — deferred.

### Completion Handlers (Virtual Objects Only)

For flow control — chaining workflows, handling failure, compensating — a virtual object can register a completion handler when creating a link:

```typescript
ctx.startLinkedWorkflow("FulfillOrder", orderKey, {
    label: "fulfillment",
    onCompleted: { handler: "onFulfillmentDone" },
});

async onFulfillmentDone(ctx, result: Result<T, E>) {
    if (result.ok) {
        ctx.set("tracking", result.value.tracking);
        ctx.startLinkedWorkflow("SendConfirmation", ctx.key, { label: "confirmation" });
    } else {
        ctx.startLinkedWorkflow("RefundPayment", ctx.key, { label: "refund" });
    }
}
```

Two things happen when a linked workflow completes, independently:

1. **The link status is always updated** by the runtime — `running` → `success`/`failed` with result. No handler required.
2. **The completion handler, if registered, is queued as a normal handler invocation.** It runs under the object's exclusive lock.

`onCompleted` only makes sense for virtual objects. Workflows can explicitly await linked children via `AttachInvocationCommand` if they need to react to completion.

Completion handlers are regular object handlers — workflows are reusable (they don't know who's watching), the object defines the flow, and handlers are optional. If a handler fails, standard Restate retry semantics apply; the link status is unaffected.

### Workflow Tree Cancellation

Cancelling a workflow cancels its entire linked subtree. Cancellation is **not a recursive tree walk from the parent** — each node cancels its own direct children:

1. Parent receives cancel signal
2. Parent reads its link table, sends cancel signal to each direct child
3. Each child receives cancel signal, reads its own link table, sends cancel signal to its children
4. Recursion emerges from each node handling its own links

This composes naturally with cross-partition links — each partition processor only reads its own link table entries.

```
Cancel(Workflow/research-123)
  → Cancel(Workflow/parse-123)        // parse-123 cancels its own children
  → Cancel(Workflow/score-123)        // score-123 cancels its own children
```

As cancelled children complete (with failure), the parent's linked children count decrements, eventually allowing the parent to transition from `Completing` → `Completed`.

### Timeouts

No framework-level timeout defaults. The owning object or workflow controls timeout policy via cancellation — it can set a timer and cancel the linked subtree if the timer fires. An optional timeout parameter on `startLinkedWorkflow` may be added as an ergonomic convenience after we have more experience with the model.

### Failure Propagation

When a linked child fails, the child's link status updates to `failed` with the error, but the parent continues. The parent will still transition to completed once all linked children (including failed ones) have completed. Child failure does not automatically kill the parent.

If a parent workflow wants to react to child failure, it can await the child (via `AttachInvocationCommand`) and handle the error — using application logic, not runtime policy. Advanced failure propagation strategies (automatic propagation, supervision) are deferred to a future RFC.

### Link Removal and Orphaning

`removeLink` detaches the child from the parent's graph. The child is orphaned (continues running independently). The link is removed from the link table. The child no longer blocks the parent's completion and is not part of the frozen graph.

---

## Implementation: Journal Entries and New Types

### Journal Entries for Starting a Linked Workflow

Starting a linked workflow produces up to three journal entries:

1. **`OneWayCallCommand`** (existing, unchanged) — starts the child invocation. No response sink (the parent doesn't explicitly await the child through this command).

2. **`CreateLink`** (new) — records the link in the link table. Sends an outbox message to the child's partition to register a `ServiceInvocationResponseSink::Link` on the child's `response_sinks` set. This sink ensures the parent is notified when the child completes.

3. **`AttachInvocationCommand`** (existing, unchanged) — only present in two cases:
   - **Workflow explicitly awaits the child**: registers a `ServiceInvocationResponseSink::PartitionProcessor` sink (existing variant), which delivers the result to the parent's journal completion slot when the child completes.
   - **Object registers `onCompleted`**: registers a `ServiceInvocationResponseSink::HandlerInvocation` sink (new variant), which enqueues a handler invocation on the parent object when the child completes.

### Journal Entry for Removing a Link

**`RemoveLink`** (new) — removes the link from the link table. The child is orphaned.

### New `ServiceInvocationResponseSink` Variants

**`ServiceInvocationResponseSink::Link`** — registered on the child's `response_sinks` set by `CreateLink`. When the child completes, generates a `LinkCompletionNotification` routed to the parent's partition. Fields: `owner_service_id: ServiceId`, `child_invocation_id: InvocationId`.

**`ServiceInvocationResponseSink::HandlerInvocation`** — registered on the child's `response_sinks` set by `AttachInvocationCommand` when an object registers `onCompleted`. When the child completes, enqueues a handler invocation on the parent object. Fields: `service_id: ServiceId`, `handler_name: String`.

### New Message Type

**`LinkCompletionNotification`** — sent from the child's partition to the parent's partition when a linked child completes. This is distinct from `InvocationResponse`, which delivers results to journal completion slots for explicit awaits. Fields: `owner_service_id: ServiceId`, `child_invocation_id: InvocationId`, `result: ResponseResult`.

On receipt, the parent's partition processor:
1. Updates the link record in the link table with the child's status and result
2. Checks if the parent is in `Completing` state and all linked children are now done
3. If so, transitions the parent to `Completed`

### New `InvocationStatus` Variant

**`InvocationStatus::Completing`** — entered when the `run` handler returns and there are active linked children. The handler code is finished but the invocation cannot finalize until all linked children complete.

If there are no linked children when the `run` handler returns, the invocation skips directly to `Completed` (existing behavior).

The invocation lifecycle with links:

```
Scheduled → Inboxed → Invoked ⇌ Suspended ⇌ Paused → Completing → Completed → Free
```

### Link Table

A new storage table for link records. Stores the parent→child relationship, link label, child invocation ID, status, and result. Indexed by parent entity for efficient link queries.

### Cross-Partition

Links are cross-partition from day one. Most links will be cross-partition since parent and child typically have different service types and keys.

- `CreateLink` registers sinks cross-partition via outbox messages
- `LinkCompletionNotification` is routed cross-partition via outbox
- Cancellation propagates cross-partition — each node cancels its own direct children via outbox

---

## Workflow State Graphs

### Workflows Build State Graphs

A workflow's state graph grows as it spawns children. Each spawn adds a node. Each link makes that node's state visible:

```
Workflow/research-123
    state: { status: "researching", plan: {...}, pct: 45 }
    children:
        +-- Workflow/parse-123   state: { status: "complete", pct: 100, output: [...] }
        +-- Workflow/score-123   state: { status: "scoring", pct: 60 }
        +-- Workflow/score-124   state: { status: "pending" }
```

An SSE observer watching the root sees the tree grow in real time — new children appear, progress updates flow in, results materialize.

### Graph Curation

Before returning, a workflow curates its graph — keeping the children that matter and unlinking the rest. Because linked children block completion, **curation determines both what you wait for and what is preserved in the frozen graph**.

```typescript
async run(ctx, { query }) {
    ctx.startLinkedWorkflow("Parser", ...);
    ctx.startLinkedWorkflow("Scorer", ...);
    ctx.startLinkedWorkflow("TempCache", ...);       // helper
    ctx.startLinkedWorkflow("FailedAttempt", ...);    // dead end

    // ... work happens ...

    // Before returning, curate the graph
    ctx.removeLink("TempCache");                     // orphan this helper
    ctx.removeLink("FailedAttempt");                 // dead end, not part of the story

    // Parser and Scorer remain linked — we wait for them
    return { synthesis: output };
}
```

### Workflow Progress is Observable by Default

Every workflow has state. Workflows update state as they execute. The SSE infrastructure streams state changes. Links define the graph boundary. Therefore: **workflow progress is observable with no extra primitives**.

```typescript
async run(ctx, { query }) {
    ctx.set("status", "planning");
    ctx.set("pct", 0);

    const plan = await ctx.call("Planner", "plan", { query });
    ctx.set("plan", plan);
    ctx.set("status", "researching");
    ctx.set("pct", 25);

    // ... spawn children, do work ...

    ctx.set("status", "synthesizing");
    ctx.set("pct", 75);

    const result = await synthesize(findings);
    ctx.set("status", "complete");
    ctx.set("pct", 100);

    return result;
}
```

An observer subscribing to the owning object's ego graph sees every status change, every progress update, from every workflow in the tree. No special progress API. No output stream primitive. The state IS the progress.

---

## Resolved Design Decisions

1. **Completion delivers `Result<T, E>` only**: No separate status field. No inline frozen graph. Graph traversal is a separate API (deferred).

2. **Children block parent completion**: A workflow does not complete until all linked children complete (recursively). This guarantees the frozen graph is fully immutable. This is essential to the semantics — when you wait for completion, all associated work completes.

3. **`Completing` invocation status**: New `InvocationStatus` variant for "handler code is done, waiting for linked children before finalizing completion." Entered when `run` returns with active linked children. Skipped when there are no linked children.

4. **Mutability contract and link-delegated writes**: When an object creates a link to a workflow, the runtime updates the link's status and result in the link table on workflow completion. Authorized by the object creating the link.

5. **Link status delivery**: The runtime always updates the link status (`running` → `success`/`failed` with result) in the link table when the linked workflow completes. Independent of completion handlers.

6. **`onCompleted` is object-only**: Virtual objects register completion handlers because they have no persistent execution context to await in. Workflows explicitly await children via `AttachInvocationCommand`.

7. **`onCompleted` implemented via response sinks**: A new `ServiceInvocationResponseSink::HandlerInvocation` variant, registered via `AttachInvocationCommand`. Consistent with the existing pattern for "what happens when an invocation completes."

8. **Graph curation is load-bearing**: Removing a link before returning determines what you wait for and what appears in the frozen graph. Forgetting to remove a link to a hanging child = parent never completes. Owner controls timeout policy via cancellation.

9. **Unidirectional links**: Parent → child only. No back-links. Children communicate to parents via signals or input context. See `future_design_ideas_033126.md` section 12.

10. **Single ownership**: The entity that creates the link owns it. No shared ownership or reference counting. Other entities observe via `attach`. See `future_design_ideas_033126.md` section 11.

11. **`removeLink` orphans the child**: The child continues running independently. The link is removed from the link table. The child no longer blocks the parent's completion.

12. **Completing state not observable through links**: During `Completing`, link status remains `running`. Result delivered on full completion only. Two terminal link states: `success`, `failed`.

13. **Workflow state write access is `run`-only**: Readable via shared handlers and SSE. External write access (exclusive handlers) is deferred.

14. **Cross-partition from day one**: Most links are cross-partition. All link operations (creation, completion notification, cancellation) use outbox messages for cross-partition communication.

15. **No framework-level timeout defaults**: The owning entity controls timeout policy via cancellation. Optional timeout parameter on `startLinkedWorkflow` may be added later.

16. **Cancellation is local per node**: Each node cancels its own direct children. Recursion emerges from each child handling its own links. Composes naturally with cross-partition.

17. **Link storage in dedicated table**: Links stored in a new link table, not as state keys. Separate from the state table.

18. **Journal entries for linked workflow**: `OneWayCallCommand` (start child) + `CreateLink` (record link, register sink) + optional `AttachInvocationCommand` (explicit await or `onCompleted`).

## Open Questions

1. **Link table schema**: What fields, indexes, and query patterns does the link table need to support?

2. **Workflow state and result separation in storage**: Workflow state in the partition store state table; result in `CompletedInvocation`? Or unified?

3. **Graph traversal API**: How does a parent inspect the frozen state graph of completed children? Deferred but needed.

4. **Orphan GC policy**: Default timeout for orphaned workflows? Configurable per-service?

5. **`Completing` status internals**: What data does the `Completing` variant carry? `InFlightInvocationMetadata`? The return value? Active children count?

---

## Future: Workflow Handlers

_Deferred — external write access to workflow state is not part of the initial implementation._

Workflows today support `run` (exclusive, writes state) and shared handlers (read-only, e.g. `getStatus`). A future extension adds **exclusive handlers** that can mutate workflow state while `run` is suspended:

- **Exclusive handlers**: Execute when `run` is suspended (awaiting a call, sleep, etc.). Take the exclusive lock. Can read and write state. Enables interactive workflows where supervisors push guidance to a running workflow.
- **Lifecycle rules**: Exclusive and shared handlers are rejected after `run` returns (`Completing`/`Completed` phases — state is settling or frozen).

This unlocks agent/supervisor interaction patterns but is orthogonal to links and observable state.

---

## References

### Codebase

- Invocation lifecycle: `crates/types/src/invocation/mod.rs`
- Invocation status state machine: `crates/storage-api/src/invocation_status_table/mod.rs:141`
- Response sinks / fan-out: `crates/storage-api/src/invocation_status_table/mod.rs` (`response_sinks: HashSet<ServiceInvocationResponseSink>`)
- Call command processing: `crates/worker/src/partition/state_machine/entries/call_commands.rs`
- Attach invocation command: `crates/worker/src/partition/state_machine/entries/attach_invocation_command.rs`
- Signal/notification system: `crates/types/src/journal_v2/notification.rs`
- Cancellation propagation: `crates/worker/src/partition/state_machine/lifecycle/cancel.rs`
- Attach (fan-in): `crates/types/src/journal_v2/command.rs:356`
- StateRouter (live state): `crates/ingress-http/src/state_router.rs`
- Partition store change detection: `crates/partition-store/src/partition_store.rs:67`
- Spawn processor task (SSE integration): `crates/worker/src/partition_processor_manager/spawn_processor_task.rs`

### Related RFCs

- [onUpgrade State Migration](rfc_on_upgrade_state_migration_033126.md) — lazy state migration for virtual objects
- [State Introspection](rfc_state_introspection_033126.md) — object links for ego graph inclusion, SSE streaming
- [Future Design Ideas](../design/future_design_ideas_033126.md) — parked concepts (supervision, backpressure, shared ownership, bidirectional links, etc.)
- Channels RFC (TBD) — broadcast channels backed by Bifrost, partition-aware fan-out
