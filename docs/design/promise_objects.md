# Spec: Promise Objects — Resolvable Virtual Objects

## Context

Workflows handle saga sequencing and flow control. Virtual objects handle interactive user scenarios — handlers called in any order by multiple sources (web APIs, email, support agents, webhooks). A Promise Object is a virtual object that can **resolve**, returning an explicit result to a parent workflow like a rich, interactive promise.

**Problem:** Workflows are the only entities that participate in the completion tree. Complex user interactions (order forms, multi-step approvals, agent convergence) are better modeled as VOs with freeform handler calls, but VOs have no way to signal "I'm done" to a parent workflow.

**Solution:** The `ChildOf` link record determines how a VO behaves. A VO with a `ChildOf` link is a promise object — it gains the ability to resolve, and its lifecycle is bound to its parent. When resolved, state becomes immutable and a result is delivered to the parent via `LinkCompletionNotification`. When the parent removes the link, the resolved VO and its subtree are GCed. No new tables needed — the ChildOf relation is the single source of truth for promise object behavior.

## Use Cases

- **Order workflow:** Parent workflow spawns an Order VO. External APIs call `addItem`, `setShipping`, `applyDiscount` in any order. When the order is ready, a `finalize` handler calls `ctx.resolve(orderSummary)`. Parent receives the summary and proceeds with payment.
- **Multi-source join:** A VO acts as a convergence point. It receives data from email, text, webhooks — each via different handlers. When all required fields are present, it resolves.
- **Agent swarm:** Coordinator workflow spawns agent VOs. Each accumulates results via handler calls. When an agent is done, it resolves with its findings. Coordinator collects all results.

## Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| What resolves | Virtual objects with a parent link (ChildOf record) | Resolve signals completion to a parent — meaningless without one |
| Resolve trigger | `ctx.resolve(result)` — SDK call from any exclusive handler | Application logic decides when the VO is done |
| Handler behavior | Returns normally after resolve | Resolve is a side-effect, not a terminator |
| Post-resolve exclusive handlers | Continue to execute, but state mutations rejected | Allows onCompleted handlers to read state; keeps model simple |
| Post-resolve shared handlers | Continue to execute, read-only | External callers can still query resolved state |
| State immutability | Server enforces (reject SetState/ClearState/ClearAllState) + SDK prevents | Defense in depth. `ctx.isResolved()` deferred — SDK tracks locally after `ctx.resolve()` succeeds; other handlers catch `OBJECT_RESOLVED` error. |
| Resolve result delivery | Immediate `LinkCompletionNotification` to parent | Simple — no result buffering needed |
| Linked children | Resolve does NOT wait for children (MVP) | Simplifies implementation. VO should call resolve after children complete (via onCompleted handlers). |
| Double resolve | Always reject second resolve with error | Resolve is a one-shot operation. ChildOf stores only a bool, not the result. |
| Retention / cleanup | Parent controls via RemoveLink | When parent removes link, ChildOf is deleted, resolved VO's state is GCed (ClearAllState), and GC propagates — VO's own children are recursively cleaned up. |
| Parent cancellation | Existing link cancellation semantics | Cancel propagates to child VO as normal |
| Rejection error | `OBJECT_RESOLVED` error code | Journal completion error delivered to handler (not HTTP 409) |
| Resolved flag storage | `ChildOf { resolved: bool }` on the link table | No new table. ChildOf already on VO's partition. Point read O(1). |
| Creation API | Reuse `LinkCommand` targeting a VO handler | No new command needed. `ctx.promiseObject()` is SDK sugar over `ctx.linkWorkflow()` targeting a VO. |
| Declaration | Soft opt-in at deploy | VO declares itself as a "promise object" for SDK API differentiation (like workflow `run`). Runtime doesn't enforce — any linked VO can technically resolve. |

## Type Changes

### Link Table — `ChildOf` Gets a Resolved Flag

Extends the type refactoring from the onCompleted spec:

```rust
// crates/storage-api/src/link_table/mod.rs

pub enum LinkRelation {
    ParentOf(ChildStatus),
    ChildOf { resolved: bool },
}
```

`key_byte()` is unchanged — `ChildOf { resolved: true }` and `ChildOf(false)` both use `0x01`. The bool is in the value, not the key.

### Proto — ChildOf Gains a Resolved Field

```protobuf
// crates/storage-api/proto/dev/restate/storage/v1/domain.proto

message Link {
  // ... existing from onCompleted spec ...

  message ChildOf {
    bool is_resolved = 1;   // NEW
  }

  oneof relation {
    ParentOfRunning parent_of_running = 2;
    ParentOfCompleted parent_of_completed = 3;
    ChildOf child_of = 5;
  }
}
```

### New Journal Command — ResolveCommand

```rust
// crates/types/src/journal_v2/command.rs

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolveCommand {
    pub result: ResponseResult,  // Reuse existing type — structurally identical
    pub completion_id: CompletionId,
}
```

### Service Protocol — ResolveCommandMessage

```protobuf
// service-protocol/dev/restate/service/protocol.proto

message ResolveCommandMessage {
  uint32 result_completion_id = 1;
  oneof result {
    Value value = 14;
    Failure failure = 15;
  };
}
```

## Server-Side Changes

### 1. ResolveCommand Handler

**`crates/worker/src/partition/state_machine/entries/resolve_command.rs`** (new file)

When the state machine processes a ResolveCommand:

```rust
// 1. Verify the VO has a ChildOf link (resolve requires a parent).
let Some(parent_link) = ctx.storage
    .get_first_parent(&vo_service_id)
    .await
    .map_err(Error::Storage)?
else {
    // No parent link — resolve is invalid.
    return deliver_error_completion("Cannot resolve: no parent link");
};

// 2. Already resolved? Reject — resolve is a one-shot operation.
if let LinkRelation::ChildOf { resolved: true } = parent_link.relation {
    return deliver_error_completion("Object already resolved");
}

// 3. Update ChildOf to resolved.
ctx.storage.put_link(&Link {
    local_service_id: vo_service_id.clone(),
    remote_service_id: parent_link.remote_service_id.clone(),
    relation: LinkRelation::ChildOf { resolved: true },
})?;

// 4. Send LinkCompletionNotification to parent with the resolve result.
ctx.handle_outgoing_message(OutboxMessage::LinkCompletionNotification(
    LinkCompletionNotification {
        owner_service_id: parent_link.remote_service_id.clone(),
        child_service_id: vo_service_id.clone(),
        result: command.result.clone(),
    },
))?;

// 5. Deliver success completion to the handler.
deliver_completion(Ok(()));
```

### 2. State Mutation Rejection When Resolved

**`crates/worker/src/partition/state_machine/entries/` — SetState, ClearState, ClearAllState handlers**

Before processing any state mutation command, check whether the VO is resolved via a point read:

```rust
// At the top of set_state_command, clear_state_command, clear_all_state_command:
if let Some(parent_link) = ctx.storage.get_first_parent(&service_id).await? {
    if let LinkRelation::ChildOf { resolved: true } = parent_link.relation {
        return deliver_error_completion(OBJECT_RESOLVED_ERROR);
    }
}
```

**Performance:** For unlinked VOs (the vast majority), `get_first_parent` prefix-scans ChildOf records and returns `None` immediately — one key comparison, short-circuit. For linked VOs, it's a single point read + bool check. Caching can be added later if profiling shows this matters.

`OBJECT_RESOLVED_ERROR` is an `InvocationError` with a Restate-specific error code (not HTTP 409 — these are journal completion errors delivered to the handler, not HTTP responses).

**Applies to:** SetState, ClearState, ClearAllState (KV state mutations only).

**Does NOT apply to:**
- **CompletePromise** — allowed after resolve. onCompleted handlers may need to resolve promises that shared handlers are waiting on. Promises are already immutable once completed (second completion returns error).
- **Internal cleanup operations** — GC propagation ClearAllState triggered by RemoveParentLink bypasses the resolved check. Internal operations use a different code path that doesn't go through handler entry processing.

### 4. ResolveCommand Validation

The ResolveCommand handler rejects resolve from shared handlers:

```rust
// In resolve_command.rs:
if invocation_target.handler_ty().is_shared() {
    return deliver_error_completion("Cannot resolve from a shared handler");
}
```

Only exclusive handlers can resolve. Shared handlers run concurrently and would race.

### 5. Exclusive Handler Rejection (Optional — Deferred)

For MVP, exclusive handlers still execute after resolve (they just can't mutate state). This is the simplest model. If we later want to reject new exclusive invocations entirely, we'd add a check in the inbox processing path.

### 6. Codec Changes

**`crates/service-protocol-v4/src/entry_codec.rs`**

Add encode/decode for `ResolveCommand` ↔ `ResolveCommandMessage`:
- Encode: write `result` (Value or Failure) + `result_completion_id`
- Decode: read them back into `ResolveCommand`

### 7. Command Dispatch

**`crates/worker/src/partition/state_machine/entries/mod.rs`**

Add `ResolveCommand` to the command dispatch match:
```rust
CommandType::Resolve => {
    let entry = raw_entry.decode::<Codec, ResolveCommand>()?;
    self.on_resolve_command(entry).await
}
```

## Changes to `on_remove_parent_link`

Currently `on_remove_parent_link` only deletes the ChildOf record. For resolved VOs, it must also GC the object and propagate:

```rust
async fn on_remove_parent_link(&mut self, notification: RemoveParentLink) -> Result<(), Error> {
    // Read ChildOf before deleting to check resolved flag.
    let was_resolved = match self.storage.get_first_parent(&notification.child_service_id).await? {
        Some(link) => matches!(link.relation, LinkRelation::ChildOf { resolved: true }),
        None => false, // Stale — ignore
    };

    // Existing: delete ChildOf record.
    self.storage.delete_link(...)?;

    // NEW: if the VO was resolved, GC its state and propagate to children.
    if was_resolved {
        self.storage.delete_all_user_state(&notification.child_service_id)?;
        self.storage.delete_all_promises(&notification.child_service_id)?;

        // Propagate: remove all ParentOf links, sending RemoveParentLink to each child.
        for link in self.storage.get_children_of(&notification.child_service_id) {
            self.storage.delete_link(&notification.child_service_id, ParentOf, &link.remote_service_id)?;
            self.handle_outgoing_message(OutboxMessage::RemoveParentLink(RemoveParentLink {
                child_service_id: link.remote_service_id,
                parent_service_id: notification.child_service_id.clone(),
            }))?;
        }
    }
    Ok(())
}
```

## What Does NOT Change

- `LinkCommand` — reused as-is for creating promise object links
- `ServiceInvocationResponseSink::Link` — unchanged
- `send_response_to_sinks` — unchanged
- Exclusive handler acceptance — no inbox changes for MVP
- `on_link_completion_notification` on parent — unchanged (parent receives result normally)

## SDK API

### `ctx.promiseObject()` (Sugar)

```typescript
// SDK sugar — no server changes needed beyond ResolveCommand
const childId = await ctx.promiseObject(
  MyOrderVO,
  "order-123",
  { handler: "init", args: orderInput, onCompleted: "onOrderDone" }
);

// Equivalent to:
const childId = await ctx.linkWorkflow(
  MyOrderVO.init(orderInput),
  { key: "order-123", onCompleted: "onOrderDone" }
);
```

### `ctx.resolve()` (New SDK method)

```typescript
// Inside a VO handler:
ctx.resolve(restate.output(orderSummary));

// Or with failure:
ctx.resolve(restate.failure("Order invalid", 400));
```

### `ctx.isResolved()` (Deferred)

Deferred from MVP. No server-side `IsResolvedCommand` needed. The SDK tracks a local flag after `ctx.resolve()` succeeds within the same handler. Other handlers catch `OBJECT_RESOLVED` errors on mutation attempts. A server-backed `ctx.isResolved()` can be added later if users need cross-handler preemptive checks.

### Soft Opt-In Declaration

```typescript
// VO declares itself as a promise object for better SDK API surface:
const orderVO = restate.promiseObject({
  name: "Order",
  init: async (ctx: ObjectContext, input: OrderInput) => { ... },
  handlers: {
    addItem: async (ctx: ObjectContext, item: Item) => { ... },
    resolve: async (ctx: ObjectContext) => {
      const summary = ctx.get("summary");
      ctx.resolve(restate.output(summary));
    },
  },
  shared: {
    getStatus: async (ctx: ObjectSharedContext) => { ... },
  },
});
```

## Edge Cases

1. **Resolve without parent link:** ResolveCommand returns an error completion. Cannot resolve a standalone VO.

2. **Double resolve:** Always rejected with error. Resolve is a one-shot operation — the handler receives an error completion.

3. **State mutation after resolve:** Server rejects with `OBJECT_RESOLVED` error completion. The SDK also prevents this via `ctx.isResolved()` check.

5. **Resolve with running linked children:** For MVP, resolve sends the result immediately. It does NOT wait for children. The VO should use onCompleted handlers to react to child completions and call resolve only when ready.

6. **RemoveLink after resolve:** Parent removes link → RemoveParentLink deletes ChildOf record, GCs the resolved VO's state (ClearAllState), AND propagates — the resolved VO's own ParentOf links are removed, triggering RemoveParentLink to its children recursively. The entire resolved subtree is cleaned up.

7. **Parent canceled/killed:** Existing link cancellation propagates. ChildOf record deleted during cleanup.

8. **Shared handler after resolve:** Continues to work. Can read state. Cannot mutate (no SetState access in shared handlers anyway).

9. **onCompleted handler after resolve:** Fires normally when a child of the resolved VO completes. Can read state, make calls, but cannot mutate state (mutation rejected by resolved check).

## Key Files to Modify

| File | Change |
|------|--------|
| `crates/storage-api/src/link_table/mod.rs` | `ChildOf { resolved: bool }` + replace `has_parents` with `get_first_parent` |
| `crates/storage-api/proto/dev/restate/storage/v1/domain.proto` | `is_resolved` field on `ChildOf` message |
| `crates/storage-api/src/protobuf_types.rs` | Proto ↔ Rust conversion for `ChildOf { resolved: bool }` |
| `crates/partition-store/src/link_table/mod.rs` | Serialize/deserialize `ChildOf { resolved: bool }` |
| `crates/worker/src/partition/state_machine/mod.rs` | `on_remove_parent_link` — GC resolved VOs + propagate to children |
| `crates/types/src/journal_v2/command.rs` | Add `ResolveCommand` + `ResolveResult` |
| `service-protocol/dev/restate/service/protocol.proto` | Add `ResolveCommandMessage` |
| `crates/service-protocol-v4/src/entry_codec.rs` | Encode/decode `ResolveCommand` |
| `crates/worker/src/partition/state_machine/entries/resolve_command.rs` | **New file** — ResolveCommand handler |
| `crates/worker/src/partition/state_machine/entries/mod.rs` | Command dispatch for Resolve |
| `crates/worker/src/partition/state_machine/entries/set_state_command.rs` | Resolved check before mutation |
| `crates/worker/src/partition/state_machine/entries/clear_state_command.rs` | Resolved check before mutation |
| `crates/worker/src/partition/state_machine/entries/clear_all_state_command.rs` | Resolved check before mutation |
| `crates/worker/src/partition/state_machine/tests/linked_workflows.rs` | New resolve tests |

## Dependencies

This spec depends on:
- **Linked Workflows MVP** (implemented, on `linked-workflows` branch)
- **onCompleted Handler Registration** (spec at `docs/design/vo_child_link_on_completed.md`) — for the type refactoring (`LinkRelation`, `ChildStatus`, `ChildOf`) and the onCompleted callback mechanism

**Coordination note:** The onCompleted spec's `LinkedNotification` handler writes the ChildOf record on the child's partition. It must initialize `ChildOf { resolved: false }`. The onCompleted spec has been updated to use `ChildOf { resolved: bool }` — the initial write should set `resolved: false`.

## Verification

1. **Tests in `linked_workflows.rs`:**
   - Happy path: VO with ChildOf link → handler calls ResolveCommand → ChildOf updated to resolved → LinkCompletionNotification sent to parent with result → parent receives result
   - Unhappy path: VO handler calls SetState after resolve → rejected with OBJECT_RESOLVED error

2. **Build & lint:**
   - `cargo check`
   - `cargo clippy --all-features --all-targets --workspace -- -D warnings`
   - `cargo fmt --all -- --check`

3. **Full test suite:**
   - `cargo nextest run --all-features`
