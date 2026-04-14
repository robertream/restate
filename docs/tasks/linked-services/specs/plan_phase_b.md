# Implementation Plan: Phase B — Workflow Invocation Linking + AttachLinkCommand

## Overview

Phase B adds the `InvocationEdges` table (invocation-scoped edges for WI parents), `StartLinkedCommand` (atomically starts a child workflow + establishes link), `AttachLinkCommand` (WI awaits a linked child's completion), and `InvocationStatus::Completing` (WI blocks until all linked children finish). Completes the full linking matrix: VO→VO, VO→WI, WI→VO, WI→WI.

## Prerequisites

- Phase A: ServiceEdges table, LinkServiceCommand, CompleteServiceCommand, UnlinkServiceCommand
- Phase A.2: `EntityId`, `EdgeState` value type, `LinkCompletionSink`, `LinkCompletionNotification`, `LinkResponse`, `ServiceInvocationResponseSink::Link` (defined), `CompleteServiceCommand` guard

## Current State (after A.2)

- **EntityId**: `Object(ServiceId)`, `WorkflowInvocation(InvocationId)`
- **EdgeState**: `LinkedTo(LinkStatus)`, `LinkedFrom(LinkCompletionSink)` — shared value type
- **LinkStatus**: `Active`, `Completed`
- **LinkCompletionSink**: `Service(ServiceId, Option<ByteString>)`, `Invocation(InvocationId, Option<CompletionId>)`
- **LinkCompletionNotification**: `{ source, result, sink }`
- **LinkResponse**: `{ local, remote, completion_id, result: Result<(), InvocationError> }` — `remote` is the handle
- **ServiceInvocationResponseSink::Link**: variant defined (Phase B wires it up)
- **CompleteServiceCommand guard**: rejects if active `LinkedTo(Active)` children

## Desired End State

1. `InvocationEdges` table — keyed by `(InvocationId, edge_label, EntityId)`, same `EdgeState` value type as ServiceEdges
2. `StartLinkedCommand` — atomically starts a child workflow invocation + establishes link
3. `ServiceInvocationResponseSink::Link` wired up — piggybacks LinkedFrom write + link validation on the invoke path
4. WI child completion (`end_invocation`) scans InvocationEdges LinkedFrom → sends `LinkCompletionNotification`
5. `AttachLinkCommand` — WI awaits a linked child's completion result
6. `InvocationStatus::Completing` — WI blocks until all linked children complete
7. Full link matrix: VO→VO, VO→WI, WI→VO, WI→WI

## Out of Scope

- **Cancellation cascade** — Phase C
- **Graph traversal API** — deferred
- **Shared ownership** — deferred
- **SDK implementation**

## Link Matrix

| Link | Parent edge table | Child edge table | Completion mechanism |
|------|------------------|------------------|---------------------|
| VO → VO | ServiceEdges | ServiceEdges | `CompleteServiceCommand` → LinkedFrom scan |
| VO → WI | ServiceEdges | InvocationEdges | `end_invocation` → LinkedFrom scan |
| WI → VO | InvocationEdges | ServiceEdges | `CompleteServiceCommand` → LinkedFrom scan |
| WI → WI | InvocationEdges | InvocationEdges | `end_invocation` → LinkedFrom scan |

## Technical Approach

### InvocationEdges Table

Mirrors ServiceEdges but keyed by `InvocationId`. Stores invocation-scoped edges for WI parents and WI children.

- **LinkedTo** entries: `EdgeState::LinkedTo(LinkStatus)` — same as ServiceEdges
- **LinkedFrom** entries: `EdgeState::LinkedFrom(LinkCompletionSink)` — same as ServiceEdges

Only the key type differs (`InvocationId` vs `ServiceId`). Value type is the shared `EdgeState` enum.

### StartLinkedCommand Flow

```
Parent partition                          Child partition
─────────────────                         ─────────────────
SDK emits StartLinkedCommand
  │
  ├─ Validate: caller is keyed service
  ├─ Validate: no self-link
  ├─ Write LinkedTo(Active)
  │   (ServiceEdges if VO parent,
  │    InvocationEdges if WI parent)
  ├─ Build ServiceInvocation with
  │  response_sink: Link { sink: LinkCompletionSink }
  ├─ Enqueue OutboxMessage::ServiceInvocation
  │                                       │
  │                              Command::Invoke received
  │                                ├─ Detect Link response sink
  │                                ├─ Validate: target not completed
  │                                ├─ Write LinkedFrom(sink) in
  │                                │   InvocationEdges (WI child)
  │                                ├─ Create invocation
  │                                ├─ Send LinkResponse(Ok) — handle = remote
  │     ◄─────────────────────────┘
  │
Command::LinkResponse received
  ├─ Ok: deliver success completion (handle = remote)
  ├─ Err: delete LinkedTo(Active), deliver error
```

### InvocationStatus::Completing Flow

```
Workflow run handler returns
  │
  ├─ end_invocation called
  ├─ Scan InvocationEdges(iid, LinkedTo, *) for LinkedTo(Active)
  │
  ├─ If no active children:
  │   └─ Complete normally (existing path)
  │
  ├─ If active children exist:
  │   ├─ Store InvocationStatus::Completing(metadata + result)
  │   ├─ Do NOT send responses or unlock
  │
  │  ... later, LinkCompletionNotification arrives ...
  │
  ├─ on_link_completion_notification:
  │   ├─ sink: Invocation(iid, completion_id)
  │   ├─ Update InvocationEdges LinkedTo → Completed
  │   ├─ Deliver to completion_id if Some
  │   ├─ Check: any remaining LinkedTo(Active)?
  │   │
  │   ├─ If still active: wait
  │   ├─ If all completed:
  │   │   ├─ Read InvocationStatus::Completing
  │   │   ├─ Send response to sinks
  │   │   ├─ Transition → Completed/Free
  │   │   └─ Unlock VirtualObjectStatus
```

### WI Child Completion

`end_invocation` gets a new step for WI children — scan LinkedFrom and send notifications:

1. Normal `send_response_to_sinks` (PartitionProcessor/Ingress sinks)
2. **New:** scan `InvocationEdges(child_iid, LinkedFrom, *)` — for each record:
   - Extract `LinkCompletionSink` from `EdgeState::LinkedFrom(sink)`
   - Build `LinkCompletionNotification { source: EntityId::WorkflowInvocation(child_iid), result, sink }`
   - Enqueue → parent's partition
3. Clean up InvocationEdges for this invocation

### AttachLinkCommand

A WI uses the handle from a prior link operation (the `remote` field of `LinkResponse`) to await the child's completion. Cross-partition message adds a continuation sink to the child's LinkedFrom record.

```rust
pub struct AttachLinkCommand {
    pub handle: EntityId,           // child identity (from LinkResponse)
    pub completion_id: CompletionId,
    pub name: ByteString,
}
```

**Flow:**

**Parent partition (WI handler running):**
- SDK emits `AttachLinkCommand { handle, completion_id }`
- Handler: send `OutboxMessage::AttachLink(AttachLinkRequest { caller_id, completion_id, target: handle })` → child's partition

**Child partition (receives `Command::AttachLink`):**
1. Determine target edge table from `target` variant (ServiceEdges if Object, InvocationEdges if WorkflowInvocation)
2. Check child status:
   - **Already completed** (`VirtualObjectStatus::Completed(result)` for VO, or InvocationEdges has no LinkedFrom because cleaned up) → immediately send `LinkCompletionNotification` with `Invocation(caller_iid, Some(completion_id))` sink
   - **Not completed** → write `EdgeState::LinkedFrom(LinkCompletionSink::Invocation(caller_iid, Some(completion_id)))` to target edge table

**Parent partition (receives `LinkCompletionNotification`):**
- `sink: Invocation(caller_iid, Some(completion_id))` → deliver result as `InvocationResponse` to `(caller_iid, completion_id)` → SDK receives the child's completion result

## Implementation Phases

### Phase 1: InvocationEdges Table

1. **Reuse `EdgeState` value type** from A.2 — no new enum needed
2. **`ReadInvocationEdgesTable` / `WriteInvocationEdgesTable`** traits — mirror ServiceEdges, keyed by `InvocationId`
3. **Partition-store**: `TableKind::InvocationEdges`, `KeyKind::InvocationEdges` (`b"ie"`), key encoding with `InvocationId`
4. **Proto**: reuse `EdgeState` proto (same value type), add `InvocationEdgesKey` encoding
5. **Transaction trait bounds**: add `ReadInvocationEdgesTable + WriteInvocationEdgesTable` to `Transaction`

### Phase 2: StartLinkedCommand Vertical Slice

1. **`StartLinkedCommand` struct**: `request: CallRequest`, `invoke_time: MillisSinceEpoch`, `completion_handler_name: Option<ByteString>`, `completion_id: CompletionId`, `name: ByteString`
2. **`Command::StartLinked`** + `CommandType::StartLinked`
3. **Proto**: `StartLinkedCommandMessage` + `StartLinkedCompletionNotificationMessage`
4. **Codec**: encode/decode in `entry_codec.rs`, message type in `message_codec`
5. **Handler** `start_linked_command.rs`:
   - Validate caller is keyed service, no self-link
   - Determine parent edge table: ServiceEdges if VO parent, InvocationEdges if WI parent
   - Write `EdgeState::LinkedTo(LinkStatus::Active)` to appropriate table
   - Build `ServiceInvocation` with `response_sink: Link { sink: LinkCompletionSink }` — `Service` variant for VO parent, `Invocation` variant for WI parent
   - Enqueue `OutboxMessage::ServiceInvocation`

### Phase 3: Child-Side Link Processing

1. **`on_service_invocation`**: detect `Link { sink }` in `response_sink`, validate target not completed, write `EdgeState::LinkedFrom(sink)` to InvocationEdges (WI child)
2. **If rejected**: send `LinkResponse(Err)`, do NOT create invocation
3. **If accepted**: create invocation, send `LinkResponse(Ok)` (handle = child EntityId in `remote`)

### Phase 4: WI Child Completion Path

1. **`end_invocation` extension**: after `send_response_to_sinks`, scan `InvocationEdges(child_iid, LinkedFrom, *)` → build and send `LinkCompletionNotification` for each
2. **`on_link_completion_notification` extension**: handle `Invocation(iid, completion_id)` sink variant — update InvocationEdges `LinkedTo → Completed`, deliver result to completion_id

### Phase 5: InvocationStatus::Completing

1. **`CompletingInvocation` struct**: `InFlightInvocationMetadata` fields + stored `ResponseResult`
2. **`InvocationStatus::Completing(CompletingInvocation)`** variant + proto
3. **`end_invocation`**: if workflow `run` handler AND active `LinkedTo(Active)` in InvocationEdges → store `Completing` instead of completing
4. **`on_link_completion_notification`**: if all children completed AND parent is `Completing` → resume completion (send responses, free/unlock)

### Phase 6: AttachLinkCommand Vertical Slice

1. **`AttachLinkCommand` struct** + `Command::AttachLink` variant + proto + codec
2. **`AttachLinkRequest`** type: `caller_id: InvocationId`, `completion_id: CompletionId`, `target: EntityId`
3. **`OutboxMessage::AttachLink`** + **`Command::AttachLink`** WAL variant
4. **State machine handler** `attach_link_command.rs`: build `AttachLinkRequest`, enqueue outbox
5. **`on_attach_link`** handler: determine target edge table, check completion status, either immediate response or write LinkedFrom with `Invocation` sink

### Phase 7: Integration Tests

Fast TDD — 1 happy path + 1 unhappy path per feature:

1. **StartLinked WI→WI**: parent starts linked workflow child → child completes → parent receives notification (happy)
2. **StartLinked rejection**: target already completed → `LinkResponse(Err)` → invocation not created (unhappy)
3. **VO→WI**: VO parent links to workflow child → WI completes → notification fires handler (happy)
4. **Completing single child**: WI starts linked child → run returns → Completing → child completes → parent completes (happy)
5. **Completing multi-child partial**: 2 children → first completes → still Completing (unhappy/edge case for Completing)
6. **AttachLink happy path**: WI starts linked child → AttachLink → child completes → result delivered to completion_id

## Testing Strategy

- Extend `crates/worker/src/partition/state_machine/tests/linked_services.rs`
- Follow Phase A's `TestEnv` pattern — single instance applying commands from both "partitions"
- Fast TDD: 1 happy + 1 unhappy path per feature; full coverage deferred to separate task

## Risks

- **`end_invocation` scan**: InvocationEdges LinkedFrom scan adds latency on WI completion. Empty for non-linked invocations — fast but nonzero.
- **Two edge tables**: `on_link_completion_notification` branches on `sink` variant to determine table. Incorrect routing loses notifications.
- **WAL compatibility**: `ServiceInvocationResponseSink::Link` must deserialize as `None` for older entries — `Option<ServiceInvocationResponseSink>` handles this.
- **`InvocationStatus::Completing` persistence**: new variant must be handled in all `InvocationStatus` match arms — exhaustive matching catches misses at compile time.

## Critical Files

| File | Role |
|------|------|
| `crates/storage-api/src/invocation_edges_table/mod.rs` | **New** — read/write traits, reuses `EdgeState` value type |
| `crates/partition-store/src/invocation_edges_table/mod.rs` | **New** — RocksDB implementation |
| `crates/types/src/journal_v2/command.rs` | `StartLinkedCommand`, `AttachLinkCommand`, `Command::StartLinked`, `Command::AttachLink` |
| `crates/worker/src/partition/state_machine/entries/start_linked_command.rs` | **New** — handler |
| `crates/worker/src/partition/state_machine/entries/attach_link_command.rs` | **New** — handler |
| `crates/worker/src/partition/state_machine/mod.rs` | `on_service_invocation` (Link sink), `end_invocation` (LinkedFrom scan + Completing), `on_link_completion_notification` (Invocation sink + Completing resume), `on_attach_link` |
| `crates/storage-api/src/invocation_status_table/mod.rs` | `InvocationStatus::Completing` |
| `crates/service-protocol-v4/src/entry_codec.rs` | Encode/decode for StartLinkedCommand + AttachLinkCommand |
| `crates/worker/src/partition/state_machine/tests/linked_services.rs` | Integration tests |
