# Implementation Plan: Phase A.2 — Link Completion Model Refactor

## Overview

Phase A.2 refactors the Phase A completion notification model. LinkedFrom records become completion sinks — each record carries the parent's identity and callback info. On child completion, a unified `LinkCompletionNotification` is sent to each parent. This phase is pure refactoring — no new user-visible features, just a cleaner foundation for Phase B.

## Prerequisites

- Phase A (on `linked-services` branch): ServiceEdges table, LinkServiceCommand, CompleteServiceCommand, UnlinkServiceCommand

## Current State (Phase A)

- **ServiceEdges table**: `(ServiceId, edge_label, ServiceNodeId)` keys
- **ServiceEdgeState**: `LinkedToActive { completion_handler_name }`, `LinkedToCompleted`, `LinkedFrom`
- **ServiceNodeId**: `Object(ServiceId)` only
- **Cross-partition protocol**: `LinkServiceRequest`/`LinkServiceResponse`, `UnlinkServiceRequest`, `ServiceCompletionNotification`
- **`CompleteServiceCommand`**: sets `VirtualObjectStatus::Completed(result)`, scans LinkedFrom parents, sends `ServiceCompletionNotification`
- **`on_service_completion_notification`**: transitions LinkedTo edge, fires onCompleted handler

## Desired End State

1. `ServiceNodeId` renamed to `EntityId` — `Object(ServiceId)` now, `WorkflowInvocation(InvocationId)` variant added (used in Phase B)
2. `ServiceEdgeState` restructured into three enums: `EdgeState { LinkedTo(LinkStatus), LinkedFrom(LinkCompletionSink) }`
3. LinkedFrom records store `LinkCompletionSink` — the completion handler or continuation info
4. `ServiceCompletionNotification` replaced by `LinkCompletionNotification { source, result, sink }`
5. `LinkServiceResponse` replaced by `LinkResponse` using `EntityId` fields
6. `ServiceInvocationResponseSink::Link` variant defined (unused in A.2, used in Phase B)
7. `CompleteServiceCommand` guard: reject if parent VO has active `LinkedTo(Active)` children

## Out of Scope

- **InvocationEdges table** — Phase B
- **StartLinkedCommand** — Phase B
- **`InvocationStatus::Completing`** — Phase B
- **`AttachLinkCommand`** — Phase B (requires InvocationEdges for full lifecycle tracking)
- **`LinkCompletionSink::Invocation` usage in LinkedFrom** — enum variant defined in A.2, written in Phase B
- **SDK implementation**

## Core Types

### EntityId (replaces ServiceNodeId)

```rust
pub enum EntityId {
    Object(ServiceId),
    WorkflowInvocation(InvocationId),  // variant added, used in Phase B
}
```

### EdgeState

```rust
pub enum EdgeState {
    LinkedTo(LinkStatus),
    LinkedFrom(LinkCompletionSink),
}

pub enum LinkStatus {
    Active,
    Completed,
}

pub enum LinkCompletionSink {
    /// VO parent — fire onCompleted handler on completion
    Service(ServiceId, Option<ByteString>),
    /// WI parent — deliver result to journal completion_id
    Invocation(InvocationId, Option<CompletionId>),
}
```

Phase A.2 only writes `Service` variant to LinkedFrom. Phase B adds `Invocation` writes.

### LinkCompletionNotification

```rust
pub struct LinkCompletionNotification {
    pub source: EntityId,           // child that completed
    pub result: ResponseResult,
    pub sink: LinkCompletionSink,   // parent identity + dispatch info
}
```

Replaces `ServiceCompletionNotification`. The child reads LinkedFrom records, packages each as a `LinkCompletionNotification`, and sends to the parent's partition.

### LinkResponse (replaces LinkServiceResponse)

```rust
pub struct LinkResponse {
    pub local: EntityId,            // parent (routes to parent's partition)
    pub remote: EntityId,           // child — IS the handle on success
    pub completion_id: CompletionId,
    pub result: Result<(), InvocationError>,
}
```

On success, the handle is `remote`. No redundant payload in `result`.

### ServiceInvocationResponseSink::Link

```rust
ServiceInvocationResponseSink::Link {
    sink: LinkCompletionSink,   // what to store in LinkedFrom on child
}
```

Variant defined in A.2 but unused — Phase B uses it for the `StartLinkedCommand` invoke path.

## Protocol: VO → VO (LinkServiceCommand)

### Step 1: Establish Link

**Parent partition (VO handler running):**

| Action | Table | Key | Value |
|--------|-------|-----|-------|
| Write edge | ServiceEdges | `(parent_sid, LinkedTo, EntityId::Object(child_sid))` | `EdgeState::LinkedTo(LinkStatus::Active)` |

- Validate: keyed caller, no self-link, direct cycle check
- Send `OutboxMessage::LinkServiceRequest` → child's partition

**Child partition (receives `Command::LinkServiceRequest`):**

| Action | Table | Key | Value |
|--------|-------|-----|-------|
| Write edge | ServiceEdges | `(child_sid, LinkedFrom, EntityId::Object(parent_sid))` | `EdgeState::LinkedFrom(LinkCompletionSink::Service(parent_sid, completion_handler))` |

- Send `LinkResponse(Ok)` → parent's partition

**Parent partition (receives `LinkResponse`):**
- Ok → deliver success completion to SDK (handle = `remote` field)
- Err → delete `LinkedTo(Active)`, deliver error completion

### Step 2: Child VO Completes

**Child partition (`CompleteServiceCommand` handler):**
- Set `VirtualObjectStatus::Completed(result)`
- Scan `ServiceEdges(child_sid, LinkedFrom, *)` — for each LinkedFrom record:
  - Extract `LinkCompletionSink` from `EdgeState::LinkedFrom(sink)`
  - Build `LinkCompletionNotification { source: EntityId::Object(child_sid), result, sink }`
  - Enqueue → parent's partition (route via `sink` variant's ServiceId or InvocationId partition key)

**Parent partition (receives `LinkCompletionNotification`):**
- Match on `sink`:
  - `Service(parent_sid, handler)`:
    - Update ServiceEdges `(parent_sid, LinkedTo, source)` → `LinkedTo(Completed)`
    - If `handler` is Some → enqueue handler invocation on parent VO
  - `Invocation(caller_iid, completion_id)` → placeholder for Phase B
- **Completion decision:** VO parent explicitly completes via `CompleteServiceCommand`. Guard rejects if any `LinkedTo(Active)` edges remain.

## Implementation Phases

### Phase 1: Foundation Types (no behavior change)

Rename Phase A types and define the new type hierarchy. No behavioral changes yet.

1. **`ServiceNodeId` → `EntityId`**: rename across codebase, add `WorkflowInvocation(InvocationId)` variant (unused until Phase B)
2. **Define `EdgeState`, `LinkStatus`, `LinkCompletionSink`** in `crates/types/src/invocation/mod.rs`
3. **`LinkCompletionNotification`**: define struct
4. **`LinkResponse`**: replace `LinkServiceResponse` — new field layout using `EntityId`, `result: Result<(), InvocationError>`
5. **`ServiceInvocationResponseSink::Link { sink: LinkCompletionSink }`**: add variant (unused in A.2 but defined for Phase B)
6. **Proto updates**: rename messages in `domain.proto`, add new types, update `protobuf_types.rs`

### Phase 2: Atomic Refactor — Edge Model + Notification Path

Single atomic commit — these changes are tightly coupled and cannot ship in an intermediate state.

1. **ServiceEdges table** uses `EdgeState` value type — update traits, partition-store implementation, proto
2. **`LinkServiceCommand` handler**: writes `EdgeState::LinkedTo(LinkStatus::Active)` (no more `completion_handler_name` on LinkedTo)
3. **`LinkServiceRequest`**: carries `completion_handler_name` so child can build the LinkedFrom sink
4. **`on_link_service_request`**: writes `EdgeState::LinkedFrom(LinkCompletionSink::Service(parent_sid, completion_handler))`
5. **`CompleteServiceCommand` handler**: scans LinkedFrom → builds `LinkCompletionNotification` from each sink
6. **`on_service_completion_notification` → `on_link_completion_notification`**: dispatches on `sink` variant, fires onCompleted handler from sink data
7. **WAL/outbox/shuffle**: rename `Command::ServiceCompletionNotification` → `Command::LinkCompletionNotification`, same for `OutboxMessage`
8. **Proto**: update `ServiceEdgeState` oneof, add `LinkCompletionNotification` message
9. **Update all existing Phase A tests** for new edge state shapes and notification types

### Phase 3: CompleteServiceCommand Guard

1. **`complete_service_command.rs`**: before setting `VirtualObjectStatus::Completed`, scan `ServiceEdges(service_id, LinkedTo, *)` for `LinkedTo(Active)` entries. If any exist, reject with error completion.

### Phase 4: Integration Tests

1. **Updated Phase A tests** (done as part of Phase 2 atomic refactor) — verify new edge shapes + notification types
2. **Guard test**: VO with active linked child → `CompleteServiceCommand` rejected → child completes → `CompleteServiceCommand` succeeds (1 happy + 1 unhappy path)

## Testing Strategy

- Extend `crates/worker/src/partition/state_machine/tests/linked_services.rs`
- Follow Phase A's `TestEnv` pattern — single instance applying commands from both "partitions"
- Fast TDD: 1 happy path + 1 unhappy path per new behavior
- Existing Phase A test updates are atomic with the Phase 2 refactor

## Risks

- **Phase 1/2 refactors touch many files**: `ServiceNodeId` → `EntityId`, `LinkServiceResponse` → `LinkResponse`, `ServiceCompletionNotification` → `LinkCompletionNotification`. Exhaustive matching catches missed sites.
- **LinkedFrom value change**: `LinkedFrom` goes from presence-only marker to carrying `LinkCompletionSink`. Existing Phase A tests updated in same commit as the refactor.
- **`CompleteServiceCommand` guard**: new rejection path — must not break existing completion flow when no children are linked.

## Critical Files for Implementation

| File | Role |
|------|------|
| `crates/types/src/invocation/mod.rs` | `EntityId`, `EdgeState`, `LinkStatus`, `LinkCompletionSink`, `LinkCompletionNotification`, `LinkResponse`, `ServiceInvocationResponseSink::Link` |
| `crates/storage-api/src/service_edges_table/mod.rs` | `ServiceEdges` table API using `EdgeState` value type |
| `crates/storage-api/proto/.../domain.proto` | Proto for new edge states, `LinkCompletionSink`, `LinkCompletionNotification` |
| `crates/worker/src/partition/state_machine/entries/link_service_command.rs` | Update for new edge state shape |
| `crates/worker/src/partition/state_machine/entries/complete_service_command.rs` | LinkedFrom scan → `LinkCompletionNotification` + guard |
| `crates/worker/src/partition/state_machine/mod.rs` | `on_link_completion_notification` handler (renamed) |
| `crates/worker/src/partition/state_machine/tests/linked_services.rs` | Updated + guard test |
