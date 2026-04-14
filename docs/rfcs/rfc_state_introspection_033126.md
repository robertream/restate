---
date: "2026-03-31T18:00:00-07:00"
git_commit: b1766c03b914aec16875d1a1c1cbe240f84255da
branch: service-macro
repository: restatedev/restate
topic: "RFC: State Introspection — Object Links and Ego Graph Concepts"
tags: [rfc, architecture, introspection, ego-graph, sse, observability]
status: draft
last_updated: "2026-04-02"
last_updated_by: Claude Opus 4.6
last_updated_note: "Plan review pass 2: removed speculative REST API endpoints and SSE graph stream design. API design is deferred. Retained object links concept and existing SSE POC documentation."
---

# RFC: State Introspection — Object Links and Ego Graph Concepts

| Field | Value |
|-------|-------|
| Date | 2026-03-31 |
| Git Commit | `b1766c03b` |
| Branch | `service-macro` |
| Repository | `restatedev/restate` |
| Status | Draft |
| Authors | Robert Ream |
| Depends On | [State Graph and Links RFC](rfc_state_graph_and_links_033126.md) |

---

## Motivation

The live-state-poc SSE infrastructure on this branch streams virtual object state changes in real time — for a single object. With links (State Graph RFC), entities form graphs. External clients will need a way to observe these graphs, but the API design for that is deferred.

This RFC defines **object links** — lightweight links from objects to other objects, purely for ego graph membership — and documents the existing SSE POC implementation.

---

## Object Links

### What Object Links Are

An object link is a **directed relationship from one virtual object to another**, stored as first-class metadata. Object links exist purely for **ego graph inclusion** — they make the linked object appear in the parent's state graph.

Object links have **no lifecycle semantics**:
- They do not block anything. Objects are persistent — there is no "completion" to wait for.
- They do not delegate write permission. No link-status updates, no completion handlers.
- They do not trigger GC. Objects have independent lifecycles.

Object links are the simplest form of link: "this object is related to that object, and I want to see both in one graph."

### Creating Object Links

```typescript
// Link to an existing object — appears in ego graph
ctx.linkObject("BillingAccount", billingKey, { label: "billing" });
ctx.linkObject("ShippingAddress", addressKey, { label: "shipping" });
```

### Removing Object Links

```typescript
ctx.unlinkObject("BillingAccount", "billing");
```

Removing an object link removes the object from the parent's ego graph. The linked object is unaffected — it continues to exist independently.

### Object Links in the Ego Graph

Object links appear alongside workflow links in the ego graph:

```
Account/acct-123
+-- FulfillOrder (workflow links):
|   +-- "order-1": { status: "success", result: { ... } }
|   +-- "order-2": { status: "running" }
+-- BillingAccount (object links):
|   +-- "billing": { target: "BillingAccount/acct-123" }
+-- ShippingAddress (object links):
    +-- "shipping": { target: "ShippingAddress/addr-456" }
```

### Relationship to Workflow Links

| | Workflow Links (State Graph RFC) | Object Links (this RFC) |
|---|---|---|
| Purpose | Composition, lifecycle management, observable execution | Ego graph inclusion, observability |
| Lifecycle semantics | Children block parent completion | None — objects are persistent |
| Link status | running → success/failed with result | Always "active" |
| Completion handlers | Supported (object-only) | Not applicable |
| Delegated writes | Runtime updates link status on completion | None |
| GC on removal | Child is orphaned | No effect on linked object |

---

## Existing POC: Single-Entity SSE Streaming

The live-state-poc on this branch already implements single-entity state streaming. Understanding what exists is essential — future ego graph features will extend this infrastructure, not replace it.

### Current Implementation

**Endpoint**: `GET /{service}/{key}/state` (accepts `text/event-stream`)

**Key files**:
- `crates/ingress-http/src/state_router.rs` — `StateRouter` struct: subscription management, state caching, event fan-out
- `crates/ingress-http/src/handler/objects.rs` — SSE endpoint handler, `router_event_to_sse` serialization
- `crates/ingress-http/src/handler/path_parsing.rs` — route parsing (`ObjectStateRequestType`)
- `crates/partition-store/src/partition_store.rs:67` — partition-side change detection, `SubscriptionRequest` handling
- `crates/worker/src/partition_processor_manager/spawn_processor_task.rs` — wires partition store subscriptions to StateRouter

### How It Works

1. **Client subscribes** via SSE to `/{service}/{key}/state` with optional `Last-Event-ID` header
2. **StateRouter** maintains a per-`ServiceId` subscription entry with cached state and a broadcast channel
3. **Cold path**: first subscriber for a key sends `SubscriptionRequest::Subscribe` to the partition store via `partition_senders`
4. **Partition store** responds with a `Replace` event (full state snapshot), then sends `Patch` events on subsequent state changes
5. **StateRouter.handle_state_change** updates cached state and fans out to all broadcast receivers
6. **SSE serialization**: `RPL {json}` for full replace, `ASN {json}` / `DEL [keys]` for patches, `CLR` for clear-all. Each event carries a monotonic revision as the SSE `id` field

### Repartition Resilience

The POC already handles partition leadership changes:
- `on_partition_closed` removes the partition sender but **preserves Active subscription entries** with their cached state
- `add_partition` sends `Resubscribe` (with cached revision) for Active entries, `Subscribe` for Pending entries
- Clients experience no disconnection — they continue receiving events once the new partition leader starts sending updates
- Revision monotonicity guards in `handle_state_change` deduplicate stale events

---

## Application: AI Agent Observability

An AI agent is a workflow. Its state is its working memory. Its children are sub-agents. The ego graph gives observers a view of the entire agent tree:

```typescript
// Agent writes decisions into state
ctx.set("lastDecision", { action: "search", query: "ocean temperature data" });
ctx.set("findings", [...findings, newFinding]);
ctx.set("confidence", 0.73);
```

An observer (human dashboard, AI supervisor, or another workflow) can observe the root agent's ego graph and see state from every agent in the tree. Await points with action schemas make the graph interactive:

```typescript
const approval = ctx.awakeable("delegation-check", {
    description: "Agent wants to delegate: coral reef impact",
    autoResolve: { after: "5s", with: { approved: true } },
    actions: {
        approve: {},
        reject: { reason: "string" },
        redirect: { newTopic: "string" },
    }
});
ctx.set("status", "awaiting-delegation-approval");
```

The pending action appears in the state. The UI renders it. A human or AI supervisor acts on it or lets it auto-resolve. Same ego graph, same link primitives — no agent-specific infrastructure.

---

## Open Questions

1. **Object link storage**: Object links have no lifecycle semantics — they're pure metadata. Should they share the link table with workflow links (distinguished by kind) or use a lighter-weight mechanism?

2. **Circular object links**: Objects can link to each other (A→B, B→A). Ego graph traversal must handle cycles — depth limits naturally bound this, but should circular links be explicitly prevented or just handled?

3. **Introspection API design**: How do external clients observe the ego graph? REST snapshot? SSE stream? Both? Deferred — requires its own design discussion.

---

## References

### Codebase

- StateRouter (live state): `crates/ingress-http/src/state_router.rs`
- Partition store change detection: `crates/partition-store/src/partition_store.rs:67`
- Spawn processor task (SSE integration): `crates/worker/src/partition_processor_manager/spawn_processor_task.rs`

### Related RFCs

- State Graph and Links RFC: `rfc_state_graph_and_links_033126.md` — defines workflow links, ego graphs, observable state graphs
