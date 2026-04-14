# Implementation Plan: Unified Response Sinks

## Goal

Unify the "callback on completion" mechanism across `InvocationStatus` (WI completion) and `VirtualObjectStatus` (VO completion) by extending `ServiceInvocationResponseSink` with a new `ServiceCompletion` variant for handler-style callbacks. Both entity types hold a `response_sinks` collection; both dispatch via a shared `send_response_to_sinks` function.

Eliminates `LinkCompletionSink` as a separate type and strips value data from `EdgeState::LinkedFrom` records.

## Motivation

The current Phase B design has dual dispatch pipes for completion callbacks:

1. **`InvocationStatus.response_sinks`** (Phase A) — fires at `end_invocation` via `send_response_to_sinks`
2. **`EdgeState::LinkedFrom(LinkCompletionSink)`** (Phase B) — fires at `end_invocation` (WI children) or `CompleteServiceCommand` (VO children) via a separate LinkedFrom scan

These two pipes solve the same problem with different mechanisms. The `LinkedFrom` value (`LinkCompletionSink`) duplicates callback data that could live on the target entity's status. VOs currently have no sink collection at all — their completion callbacks are entirely LinkedFrom-based.

Unification buys:
- One dispatch path for all completion callbacks
- VO and WI symmetry — both hold `response_sinks` collections
- `LinkedFrom` edge values strip to presence-only (smaller storage)
- `LinkCompletionSink` type deleted entirely
- Simpler mental model: "to await or callback on X's completion, add a sink to X's response_sinks"

## Core Types

### `ServiceInvocationResponseSink` — extended

```rust
pub enum ServiceInvocationResponseSink {
    /// Existing — deliver the result to a journal completion entry.
    /// Used by: CallCommand (result_completion_id), AttachInvocationCommand,
    /// AttachServiceCommand. Fires via OutboxMessage::ServiceResponse.
    PartitionProcessor(JournalCompletionTarget),

    /// Existing — HTTP reply.
    Ingress {
        request_id: PartitionProcessorRpcRequestId,
    },

    /// NEW — fire a handler invocation on a target service with the result
    /// bytes as argument. Used by LinkServiceCommand / StartLinkedCommand
    /// with result_completion_handler: Some(name). Fires via
    /// OutboxMessage::ServiceInvocation targeting the named handler.
    ServiceCompletion(ServiceCompletionTarget),

    // Link { sink, completion_id } — DELETED
}
```

### `ServiceCompletionTarget` — new

```rust
pub struct ServiceCompletionTarget {
    pub service_id: ServiceId,
    pub handler_name: ByteString,
}
```

Parallel to the existing `JournalCompletionTarget { caller_id, caller_completion_id }`.

### `VirtualObjectStatus` — inline `response_sinks` on non-terminal variants

Rather than wrapping the enum in a `VirtualObjectState` struct, inline the sinks directly on the variants where they're meaningful. This is type-enforced: a `Completed` VO provably has no sinks (they've been fired at completion time). Mirrors Phase A's `InvocationStatus` pattern (sinks live on `Invoked`/`Suspended`/`Paused` but NOT on `Completed`/`Free`).

```rust
pub enum VirtualObjectStatus {
    #[default]
    Unlocked {
        response_sinks: HashSet<ServiceInvocationResponseSink>,
    },
    Locked {
        invocation_id: InvocationId,
        response_sinks: HashSet<ServiceInvocationResponseSink>,
    },
    Completed(ResponseResult),   // NO sinks — terminal state
}

impl VirtualObjectStatus {
    pub fn response_sinks(&self) -> Option<&HashSet<ServiceInvocationResponseSink>> {
        match self {
            Self::Unlocked { response_sinks } | Self::Locked { response_sinks, .. } => Some(response_sinks),
            Self::Completed(_) => None,
        }
    }

    pub fn response_sinks_mut(&mut self) -> Option<&mut HashSet<ServiceInvocationResponseSink>> {
        match self {
            Self::Unlocked { response_sinks } | Self::Locked { response_sinks, .. } => Some(response_sinks),
            Self::Completed(_) => None,
        }
    }
}
```

**Trait methods unchanged**: existing `get_virtual_object_status` / `put_virtual_object_status` still serve — they now return the enriched enum. No new trait methods needed, no wrapper struct.

**Transition from `Locked` / `Unlocked` to `Completed`**: the sinks are drained (moved out and fired via `send_response_to_sinks`) as part of the transition. The new `Completed(result)` variant has no field for sinks, so the transition implicitly discards them.

**Breaking change note**: both `Locked` and `Unlocked` variants change shape (from tuple / unit to struct variants with fields). All pattern match sites across the codebase need updating. This is the biggest "cascade" of the refactor — every place that reads `VirtualObjectStatus::Locked(iid)` becomes `VirtualObjectStatus::Locked { invocation_id: iid, .. }`.

### `EdgeState::LinkedFrom` — strip value

```rust
pub enum EdgeState {
    LinkedTo(LinkStatus),
    LinkedFrom,  // presence-only marker; no value data
}
```

### Deleted types

- `LinkCompletionSink` — replaced by `ServiceInvocationResponseSink` variants
- `ServiceInvocationResponseSink::Link` variant — no longer needed
- `LinkCompletionNotification.sink` and `.result` fields — see below

### `LinkCompletionNotification` — simplified

```rust
pub struct LinkCompletionNotification {
    pub local: EntityId,   // parent (routes here)
    pub remote: EntityId,  // child that completed
}
```

Becomes a pure graph-state-transition message. No result payload, no sink. Dispatch of completion callbacks happens independently via the unified `response_sinks` pipeline. This notification is only for transitioning `LinkedTo(Active) → Completed` on the parent side and checking `Completing` resume.

### `LinkRequest` (replaces `LinkServiceRequest`)

```rust
pub struct LinkRequest {
    pub local: EntityId,                                  // child
    pub caller_invocation_id: InvocationId,
    pub caller_completion_id: CompletionId,
    pub handler_sink: Option<ServiceCompletionTarget>,    // Some only for VO parents with result_completion_handler
}
```

Sent by `LinkServiceCommand` handlers to the child's partition for linking to an EXISTING VO child. WI parents never carry a `handler_sink` — they use `AttachInvocationCommand` later if they want result delivery.

`StartLinkedCommand` uses a different mechanism (piggyback on `ServiceInvocation` — see below) because the child is being created, not linked-to.

### `ServiceInvocation.link_from` — NEW field for piggyback link establishment

`StartLinkedCommand` creates a new WI child and establishes a link to it atomically. The child doesn't exist yet, so we can't send a separate `LinkRequest` to "the child's partition" — we need the link establishment to happen as part of invocation creation.

Add a new field to `ServiceInvocation`:

```rust
pub struct ServiceInvocation {
    // ... existing fields (invocation_id, invocation_target, argument, source, etc.)
    pub response_sink: Option<ServiceInvocationResponseSink>,
    pub link_from: Option<EntityId>,   // NEW — parent identity for LinkedFrom edge
}
```

Semantics: if `link_from.is_some()`, the child-side `on_service_invocation` handler writes a `LinkedFrom` presence marker to the child's edge table keyed by that parent EntityId AS PART OF invocation creation. Independent of `response_sink`.

The two fields are orthogonal:
- `link_from: Some(parent)` + `response_sink: None` → graph-only link (no callback registered)
- `link_from: Some(parent)` + `response_sink: Some(ServiceCompletion(target))` → link with onCompleted handler (VO parent case)
- `link_from: Some(parent)` + `response_sink: Some(PartitionProcessor(...))` → hypothetical, not used
- `link_from: None` + `response_sink: Some(...)` → normal invocation with a response sink (existing Phase A behavior — Call, AttachInvocation)
- `link_from: None` + `response_sink: None` → normal fire-and-forget invocation (OneWayCall)

`StartLinkedCommand` handler constructs `ServiceInvocation` with BOTH `link_from: Some(caller_entity_id)` AND `response_sink: Option<ServiceCompletion(handler_target)>` depending on whether the parent registered a handler.

## Dispatch

`send_response_to_sinks` extended with a new arm:

```rust
fn send_response_to_sinks(
    &mut self,
    sinks: impl IntoIterator<Item = ServiceInvocationResponseSink>,
    result: ResponseResult,
    // ... existing params
) {
    for sink in sinks {
        match sink {
            PartitionProcessor(target) => enqueue OutboxMessage::ServiceResponse(InvocationResponse { target, result }),
            Ingress { request_id } => send_ingress_response(request_id, result, ...),
            ServiceCompletion(target) => enqueue OutboxMessage::ServiceInvocation with:
                - invocation_target: VirtualObject(target.service_id, target.handler_name, Exclusive)
                - argument: failure-encoded bytes (JSON) on ResponseResult::Failure, raw bytes on Success
                - source: Source::Internal
        }
    }
}
```

**Failure encoding**: The `ServiceCompletion` arm preserves the Phase A failure-encoding convention — `ResponseResult::Failure(err)` becomes JSON `{"error_code": N, "message": "..."}` as the handler invocation's argument bytes. Phase A's onCompleted handler behavior is preserved verbatim.

## Who writes which sink where

| Command | Sink variant written | Target entity's `response_sinks` |
|---------|---------------------|----------------------------------|
| `CallCommand` | `PartitionProcessor(JournalCompletionTarget { caller_id, result_completion_id })` | New WI's `response_sinks` (set by `ServiceInvocation.response_sink` path) |
| `LinkServiceCommand` with `result_completion_handler: Some(h)` (VO parent, VO child) | `ServiceCompletion(ServiceCompletionTarget { parent_sid, h })` | Child VO's `response_sinks` |
| `LinkServiceCommand` with `result_completion_handler: None` | No sink | — (graph-only link) |
| `LinkServiceCommand` (WI parent) | No sink (WI parents never register handler sinks via link) | — |
| `StartLinkedCommand` with `result_completion_handler: Some(h)` (VO parent) | `ServiceCompletion(...)` | Child WI's `response_sinks` |
| `StartLinkedCommand` (WI parent) | No sink | — |
| `AttachInvocationCommand` (WI target) | `PartitionProcessor(JournalCompletionTarget)` | Target WI's `response_sinks` (Phase A, unchanged) |
| `AttachServiceCommand` (VO target) | `PartitionProcessor(JournalCompletionTarget)` | Target VO's `response_sinks` (NEW — VO state gains sinks collection) |

## Completion dispatch flow

### WI completion (`end_invocation`)

1. Compute `response_result` (existing)
2. If `response_sinks` is non-empty or `completion_retention_duration > 0`, process response sinks via `send_response_to_sinks`
3. Scan `InvocationEdges(iid, LinkedFrom, *)` (presence-only records) and emit `LinkCompletionNotification` for each parent — **graph-only message, no payload**
4. Delete all `InvocationEdges` for this invocation
5. Continue existing completion teardown (store completed, free, drop journal, unlock VO)

### VO completion (`CompleteServiceCommand`)

1. Current Phase A guard: reject if any `LinkedTo(Active)` children exist on parent side (unchanged)
2. Read `VirtualObjectStatus` for the VO
3. Extract `response_sinks` from the `Locked` or `Unlocked` variant (pattern match; `Completed` would already be handled by the double-completion guard)
4. Process the extracted sinks via `send_response_to_sinks` with the completion result
5. Transition to `VirtualObjectStatus::Completed(result)` — the new variant has no sink field, sinks are implicitly discarded
6. Persist updated `VirtualObjectStatus`
7. Scan `ServiceEdges(sid, LinkedFrom, *)` (presence-only records) and emit `LinkCompletionNotification` for each parent — graph-only
8. Continue existing completion teardown

## Parent-side notification handling

`on_link_completion_notification` simplified — no sink dispatch, graph-only:

```rust
async fn on_link_completion_notification(&mut self, notification: LinkCompletionNotification) -> Result<(), Error> {
    let parent = &notification.local;
    let child = &notification.remote;

    match parent {
        EntityId::Object(parent_sid) => {
            // Transition ServiceEdges(parent_sid, LinkedTo, child) → Completed
            // Check VO's CompleteServiceCommand guard implications (no resume needed — VOs complete explicitly)
        }
        EntityId::WorkflowInvocation(parent_iid) => {
            // Transition InvocationEdges(parent_iid, LinkedTo, child) → Completed
            // Check if parent is in Completing status AND no LinkedTo(Active) remain
            // If so, call resume_completing_invocation
        }
    }
}
```

No `send_response_to_sinks` call, no onCompleted handler invocation, no journal delivery. Those already happened on the child's side via the unified response_sinks pipeline.

## Unlink sink cleanup

When `UnlinkServiceCommand` / `UnlinkInvocationCommand` fires, the child's `response_sinks` must be filtered to remove any `ServiceCompletion` sink that was registered by the unlinking parent.

`on_unlink_request` (existing handler, extended):

```rust
async fn on_unlink_request(&mut self, request: UnlinkRequest) -> Result<(), Error>
where S: ... {
    let parent_entity_id = /* derived from request.parent_sink */;
    let parent_service_id_opt = match &parent_entity_id {
        EntityId::Object(sid) => Some(sid.clone()),
        EntityId::WorkflowInvocation(_) => None, // WI parents don't register ServiceCompletion sinks
    };

    match &request.local {
        EntityId::Object(child_sid) => {
            // 1. Delete LinkedFrom edge (existing)
            self.storage.delete_service_edge(child_sid, EdgeLabel::LinkedFrom, &parent_entity_id)?;

            // 2. NEW: Remove matching ServiceCompletion sink from child VO's response_sinks
            if let Some(parent_sid) = &parent_service_id_opt {
                let mut status = self.storage.get_virtual_object_status(child_sid).await?;
                if let Some(response_sinks) = status.response_sinks_mut() {
                    response_sinks.retain(|sink| match sink {
                        ServiceInvocationResponseSink::ServiceCompletion(target) => &target.service_id != parent_sid,
                        _ => true, // keep non-link sinks (attach, ingress, partition_processor)
                    });
                    self.storage.put_virtual_object_status(child_sid, &status)?;
                }
                // If status is Completed (response_sinks_mut returns None), there's nothing to clean — sinks already drained
            }

            // 3. GC cascade check (existing)
        }
        EntityId::WorkflowInvocation(child_iid) => {
            // 1. Delete LinkedFrom edge (existing)
            self.storage.delete_invocation_edge(child_iid, EdgeLabel::LinkedFrom, &parent_entity_id)?;

            // 2. NEW: Remove matching ServiceCompletion sink from child WI's response_sinks
            if let Some(parent_sid) = &parent_service_id_opt {
                let mut status = self.storage.get_invocation_status(child_iid).await?;
                if let Some(response_sinks) = status.get_response_sinks_mut() {
                    response_sinks.retain(|sink| match sink {
                        ServiceInvocationResponseSink::ServiceCompletion(target) => &target.service_id != parent_sid,
                        _ => true,
                    });
                    self.storage.put_invocation_status(child_iid, &status)?;
                }
            }
        }
    }

    // Send UnlinkResponse back to parent
    // ...
}
```

Preserves non-link sinks:
- `PartitionProcessor` (attach, call response)
- `Ingress` (HTTP reply)
- `ServiceCompletion` from OTHER parents

Edge cases:
- WI parents unlinking: skip sink removal (they never added `ServiceCompletion`)
- Parent unlinked before child existed: edge delete is no-op, filter is no-op
- Child already completed: `response_sinks` drained at completion, filter no-op
- Child GC'd: state read returns default/empty, filter no-op

## Link establishment — two paths

### Path A: `LinkServiceCommand` (existing child)

`LinkServiceCommand` targets an existing keyed service. The handler enqueues a `LinkRequest` to the child's partition. The child-side `on_link_request` handler does a read-modify-write on the target's state:

1. Read target's `VirtualObjectState` (for VO targets) or `InvocationStatus` (for WI targets)
2. **Already-completed target — short-circuit**: if the target is already in a terminal completed state (`VirtualObjectStatus::Completed(result)` for VO, or `InvocationStatus::Completed(completed)` for WI):
   - Do NOT write a `LinkedFrom` edge (no graph relationship — the target is done)
   - Do NOT add any sink to `response_sinks` (target has no sink collection anymore — it's terminal)
   - **Immediately dispatch** the `handler_sink` if present, using the target's stored completion result:
     - Build the handler invocation (`OutboxMessage::ServiceInvocation` targeting the `ServiceCompletionTarget`) with the target's completion bytes as argument, using Phase A's failure encoding for `ResponseResult::Failure`
     - Send the invocation out via `handle_outgoing_message`
   - Send `LinkResponse(Ok)` back to the parent — the link "succeeded" in the sense that the callback fired immediately. The parent's journal receives a success completion.
   - On the parent side, the handler delivers `LinkServiceCompletion::Void` to the SDK AND the onCompleted handler (if specified) fires immediately as a separate handler invocation. The parent sees both outcomes.

   **Rationale**: mirrors `AttachInvocationCommand` semantics — attaching to an already-completed target delivers the result immediately rather than failing. Preserves the "fire handler on child completion" guarantee even when the child completed before the link was established.

3. **Normal path** (target not yet completed): write `LinkedFrom` presence marker to the child's edge table (ServiceEdges for VO child, InvocationEdges for WI child)
4. If `request.handler_sink.is_some()`, add `ServiceInvocationResponseSink::ServiceCompletion(handler_sink.unwrap())` to the child's `response_sinks` collection
5. Persist the updated state + edge atomically
6. Send `LinkResponse(Ok)` back to parent

### Path B: `StartLinkedCommand` (new WI child created + linked atomically)

`StartLinkedCommand` creates a new WI child and establishes a link atomically. The child doesn't exist yet, so the link establishment piggybacks on the invoke path via the new `ServiceInvocation.link_from` field.

The parent-side `StartLinkedCommand` handler:

1. Validate caller is a keyed service, target is a workflow run handler, no self-link
2. Compute the parent `EntityId` from caller type (Object or WorkflowInvocation)
3. Optionally build a `ServiceCompletion` sink if `result_completion_handler.is_some()` AND caller is VO parent
4. Write `LinkedTo(Active)` edge to appropriate parent edge table
5. Build `ServiceInvocation` with:
   - `link_from: Some(parent_entity_id)` — signals child-side to write LinkedFrom
   - `response_sink: Some(ServiceCompletion(target))` if handler was provided, else `None`
6. Enqueue `OutboxMessage::ServiceInvocation`

The child-side `on_service_invocation` handler (or `on_pre_flight_invocation`):

1. Detect `service_invocation.link_from.is_some()`
2. **Reject if the target is already completed** — for workflow run targets, check `VirtualObjectStatus::Locked` / `Completed` on the workflow VO. If Completed, send `LinkResponse(Err)` via a dedicated error path and do NOT create the invocation. (This mirrors the Phase B behavior from Task 4.2 that we want to preserve.)
3. Write `LinkedFrom` presence marker to the child's edge table, keyed by `link_from.unwrap()` parent entity
4. Continue normal invocation creation — `from_service_invocation` collects `response_sink` (if Some) into `PreFlightInvocationMetadata.response_sinks` naturally
5. Send `LinkResponse(Ok)` back (not via LinkRequest's return path — via a direct outbox message triggered by the `link_from` detection)

### Why two paths?

- **LinkServiceCommand**: child exists; we need cross-partition message to tell child's partition "you have a new parent, update your state." → `LinkRequest` RPC-style round-trip.
- **StartLinkedCommand**: child is being created; the invoke message IS the cross-partition event, and we can piggyback the link info on it. → `ServiceInvocation.link_from` piggyback.

Both paths reach the same end state: child has a `LinkedFrom` presence marker + optionally a `ServiceCompletion` sink in `response_sinks`. The difference is only in how the cross-partition message is structured.

**`ServiceInvocationResponseSink::Link` variant is deleted** — both paths now use either `LinkRequest` (dedicated message) or `ServiceInvocation.link_from` (new field), neither of which needs the legacy Link-sink piggyback.

## Attach commands

### `AttachInvocationCommand` (Phase A, unchanged)

Already uses `PartitionProcessor(JournalCompletionTarget)`. Target's `response_sinks` (on `InvocationStatus`) is updated via existing `do_append_response_sink`. No behavior change.

### `AttachServiceCommand` (Phase B, behavior change)

Currently writes a LinkedFrom edge with `LinkCompletionSink::Invocation(caller_iid, Some(completion_id))`. After refactor:

1. `on_attach_service` reads target VO's `VirtualObjectState`
2. If `state.status == Completed(result)`: immediately deliver via `OutboxMessage::ServiceResponse(InvocationResponse { target, result })` back to the caller
3. Else: add `PartitionProcessor(JournalCompletionTarget { caller_id, completion_id })` to `state.response_sinks` and persist
4. No LinkedFrom edge write (AttachServiceCommand is purely for result delivery, not graph management)

Wait — does AttachServiceCommand write a LinkedFrom edge currently? Let me think... yes, currently Task 6.2's `on_attach_service` writes `EdgeState::LinkedFrom(LinkCompletionSink::Invocation(caller_iid, Some(completion_id)))`. After refactor, the LinkedFrom edge is presence-only, so we just write `EdgeState::LinkedFrom` (no sink), and separately add the `PartitionProcessor` sink to the VO's `response_sinks`.

Do we even need to write LinkedFrom for AttachServiceCommand? Attach is for result delivery, not lifecycle graph management. Arguably we should NOT add a LinkedFrom — the attach is not a parent-child relationship. The attach is tracked via `response_sinks` only.

Decision: **AttachServiceCommand does NOT write a LinkedFrom edge**. It only adds to `response_sinks`. Attach is orthogonal to the link graph.

Same for `AttachInvocationCommand` — it doesn't touch edge tables.

## Test migration

All 17 existing linked-services tests will need updates:
- Assertions on `LinkedFrom(LinkCompletionSink::...)` become `LinkedFrom` (presence-only)
- Assertions on `LinkCompletionNotification { sink, result }` become `LinkCompletionNotification { local, remote }` (no payload fields)
- Fixture setup for linked children needs to write sinks to the child's state, not to the LinkedFrom value
- `attach_service_then_complete` / `attach_service_after_completed` tests verify `response_sinks` on the VO state instead of LinkedFrom records

New tests to add (fast TDD — minimize functions, merge assertions where possible):

1. **`vo_unlink_cleans_up_service_completion_sink`** — VO parent unlinking removes its own `ServiceCompletion` sink but preserves sinks from other parents and other variants:
   - Setup: child with TWO `ServiceCompletion` sinks (VO parents A and B, both with onCompleted handlers) AND a `PartitionProcessor` sink (from a WI parent attaching via `AttachInvocationCommand`)
   - Action: VO parent A unlinks (VO unlinker → filter runs)
   - Assertions (multiple in one test):
     - A's `ServiceCompletion` sink is removed from child's `response_sinks`
     - B's `ServiceCompletion` sink is preserved
     - The `PartitionProcessor` attach sink is preserved
     - Child's `LinkedFrom(A)` edge is deleted
     - Child's `LinkedFrom(B)` edge is preserved
   - Covers the VO-unlinker code path (filter runs, removes matching service, preserves others).

2. **`wi_unlink_preserves_attach_sink`** — CRITICAL: WI parent unlinking MUST preserve `PartitionProcessor` sinks (otherwise a blocked WI awaits forever):
   - Setup:
     - WI parent has started a linked WI child via `StartLinkedCommand`
     - WI parent has issued `AttachInvocationCommand` targeting the child → child has `PartitionProcessor(WI_parent_iid, completion_id)` in its `response_sinks`
     - WI parent's `run` has returned, WI is in `InvocationStatus::Completing` (blocking on the attach completion)
     - WI parent still has `LinkedTo(Active)` edge to the child in InvocationEdges
   - Action: WI parent issues `UnlinkInvocationCommand` for the child (WI unlinker → filter SKIPPED)
   - Assertions:
     - Child's `PartitionProcessor` sink is PRESERVED in `response_sinks` (unchanged count, exact variant present)
     - Child's `LinkedFrom` edge is deleted
     - WI parent's `LinkedTo` edge in InvocationEdges is deleted
     - WI parent is STILL in `InvocationStatus::Completing` (unlink didn't break the Completing check because the filter never touched response_sinks)
     - Child later completes → `PartitionProcessor` sink fires → `InvocationResponse` delivered to WI parent's journal at `completion_id`
     - WI parent's `Completing` check resolves (attach completion arrived) → `resume_completing_invocation` runs → WI parent transitions to `Completed`
   - **Why this test is critical**: If we ever accidentally made WI unlink cleanup remove `PartitionProcessor` sinks too (e.g., by extending the filter to cover all variants), this test catches it immediately. A blocked WI never completing is a silent bug — it doesn't fail loudly, just hangs. This test is the canary.

2. **`link_service_command_on_completed_target_fires_handler_immediately`** — one test for the short-circuit-deliver semantic:
   - Setup: target VO in `VirtualObjectStatus::Completed(success_bytes)` with an empty `response_sinks` collection
   - Action: parent (VO) applies `LinkServiceCommand` with `result_completion_handler: Some("onDone")` targeting the completed VO
   - Assertions:
     - Child-side `on_link_request` detects the completed status and takes the short-circuit path
     - Child's `LinkedFrom` edge is NOT written (no graph relationship — target is terminal)
     - Child's `response_sinks` remains unchanged (no sink stored on a terminal entity)
     - An `OutboxMessage::ServiceInvocation` is emitted targeting the parent VO's `onDone` handler with the completion bytes as argument
     - `LinkResponse(Ok)` is sent back to parent
     - On parent side, `LinkServiceCompletion::Void` is delivered to the parent's journal — the link "succeeded" (callback fired)
     - Parent's `LinkedTo(Active)` edge was written optimistically but is transitioned to `LinkedTo(Completed)` by a subsequent `LinkCompletionNotification` — OR is deleted — this detail depends on whether the short-circuit path emits a graph-only notification too. Plan should decide.

   **Plan decision needed**: does the short-circuit path on the child emit a `LinkCompletionNotification` to the parent so the parent's `LinkedTo(Active)` edge gets transitioned to `LinkedTo(Completed)`? Or does the parent skip writing `LinkedTo` when the child is already known to be completed?

   Probably the cleanest: the parent writes `LinkedTo(Active)` optimistically (it doesn't know the child's state when issuing the `LinkRequest`), and the child-side handler, in addition to firing the handler sink, ALSO emits a `LinkCompletionNotification { local: parent_entity, remote: child_entity }` to transition the parent's edge. This keeps the edge lifecycle consistent — all links go through `Active → Completed`, even short-circuited ones.

   Alternative: the `LinkResponse(Ok)` message carries an additional "child was already completed" flag, and the parent-side `on_link_response` handler treats this as an immediate edge transition. Avoids the extra message but couples link-response semantics with completion semantics.

   **Recommendation**: emit `LinkCompletionNotification` from the short-circuit path. Keeps the edge transition logic uniform.

## Deletion checklist

- `LinkCompletionSink` enum — deleted from `types/invocation/mod.rs`
- `ServiceInvocationResponseSink::Link { sink, completion_id }` variant — deleted
- `LinkCompletionNotification.sink: LinkCompletionSink` field — deleted
- `LinkCompletionNotification.result: ResponseResult` field — deleted
- `EdgeState::LinkedFrom(LinkCompletionSink)` — strip to `EdgeState::LinkedFrom`
- `LinkServiceRequest` — renamed to `LinkRequest`, fields updated
- Proto `LinkCompletionSink` message — deleted
- Proto `EdgeState.LinkedFrom` — strip value
- Proto `LinkCompletionNotification` — strip sink + result fields
- `service_edges_table` trait methods that return/take `LinkCompletionSink` — updated
- `on_link_sink_invocation` helper (from Task 4.2) — replaced by `on_service_invocation`'s new `link_from` detection logic

## New code checklist

- `ServiceInvocationResponseSink::ServiceCompletion(ServiceCompletionTarget)` variant + proto
- `ServiceCompletionTarget` struct + proto
- `ServiceInvocation.link_from: Option<EntityId>` field + proto (for StartLinkedCommand piggyback)
- `VirtualObjectStatus::Unlocked { response_sinks }` + `Locked { invocation_id, response_sinks }` — inline sinks on non-terminal variants
- `VirtualObjectStatus::response_sinks()` / `response_sinks_mut()` helper methods
- Proto `VirtualObjectStatusV2` (or equivalent) schema updated to match new enum shape
- `send_response_to_sinks` new arm for `ServiceCompletion`
- `CompleteServiceCommand` handler rewrite (walk VO response_sinks + set Completed + clear sinks)
- `on_link_request` handler (renamed from `on_link_service_request`) — validates not-completed, writes LinkedFrom presence, adds sink to child's response_sinks
- `on_service_invocation` detects `link_from: Some(_)` — validates not-completed, writes LinkedFrom presence, continues normal creation (response_sink flows into sinks collection naturally)
- `on_unlink_request` sink cleanup logic (filter ServiceCompletion by parent service_id)
- `on_link_completion_notification` simplified to graph-only (no sink dispatch)
- `end_invocation` LinkedFrom scan emits presence-only notifications
- `LinkServiceCommand` handler: builds `handler_sink: Option<ServiceCompletionTarget>` and sends `LinkRequest`
- `StartLinkedCommand` handler: builds `ServiceInvocation` with `link_from: Some(parent_entity)` and optional `response_sink: ServiceCompletion(...)`
- `on_attach_service` updated to add `PartitionProcessor` sink to VO's response_sinks instead of writing to LinkedFrom
- `OnNotifyInvocationResponse` dispatch for the command types (already exists from previous refactors; verify `ServiceCompletion` sink doesn't confuse it)

## Trait bound cascade

`LinkedServicesStorage` super-trait (from commit `82d650dbb`) already bundles the edge table bounds. This refactor adds `ReadVirtualObjectStatusTable + WriteVirtualObjectStatusTable` to more handlers (specifically `on_link_request` and `on_unlink_request`, which now do read-modify-write on VO state). Extend `LinkedServicesStorage` to include these if convenient.

## Out of scope

- Changes to Phase A's `ServiceInvocationResponseSink` variants (`PartitionProcessor`, `Ingress`) — unchanged
- Changes to `CallCommand` — it already uses `PartitionProcessor` sinks
- Workflow state retention policy / journal drop — unchanged
- `StartLinkedCommand` target validation (must be workflow run handler) — unchanged
- Full test coverage for all edge cases — add the two new unlink-sink-cleanup tests; defer other coverage
- Performance optimizations for the `response_sinks` collection read-modify-write path — deferred

## Estimated scope

40-50 files touched. Similar magnitude to Task 2.1's atomic refactor (commit `4f6a81d84`). Single atomic commit.

## Risks

1. **`VirtualObjectStatus` wire format change** — `VirtualObjectState` is a new serialization shape. On the unreleased branch this is fine but requires careful proto migration.
2. **Test assertion churn** — most tests inspect LinkedFrom values or LinkCompletionNotification fields. Every assertion needs review.
3. **Cross-partition read-modify-write on link establishment** — `on_link_request` now reads and mutates the child's `response_sinks`. The read-then-write happens within a single RocksDB transaction on the child's partition, so it's atomic. No new concurrency concerns.
4. **Failure encoding drift** — the `ServiceCompletion` dispatch must preserve Phase A's JSON failure encoding exactly. Regression test the existing onCompleted handler tests to verify bytes.
5. **`end_invocation` scan cost** — still need to scan `InvocationEdges(LinkedFrom)` for graph notifications even though callback dispatch now goes through `response_sinks`. The `has_links` flag from commit `82d650dbb` still gates the scan, so hot-path cost unchanged.

## Confirmation checkpoints

Design points confirmed through iteration:

1. ✓ `ServiceCompletionTarget` shape: `{ service_id: ServiceId, handler_name: ByteString }`
2. ✓ `ServiceInvocationResponseSink::ServiceCompletion(ServiceCompletionTarget)` variant name
3. ✓ `AttachServiceCommand` uses `PartitionProcessor(JournalCompletionTarget)` — same as `AttachInvocationCommand`
4. ✓ `VirtualObjectStatus` gains inline `response_sinks` field on non-terminal variants (`Locked`, `Unlocked`) — no wrapper struct. Sinks are type-enforced to be absent on `Completed(_)`.
5. ⚠ Scope: full refactor in one commit (awaiting final confirmation)
6. ⚠ Unlink sink cleanup: filter `ServiceCompletion` sinks by `parent.service_id` match (awaiting final confirmation)
7. ⚠ Unlink becomes read-modify-write on target state (awaiting final confirmation)
8. ✓ `LinkRequest` shape: `{ local: EntityId, caller_invocation_id, caller_completion_id, handler_sink: Option<ServiceCompletionTarget> }`
9. ✓ `ServiceInvocation.link_from: Option<EntityId>` for `StartLinkedCommand` piggyback path (separate from `LinkRequest` for `LinkServiceCommand` path)
10. ✓ Already-completed target short-circuits — link "succeeds" and fires handler immediately with stored result; emits `LinkCompletionNotification` for edge transition (mirrors `AttachInvocationCommand` semantics)

Points 1-4, 8, 9, 10 confirmed. Points 5-7 pending final go/no-go.
