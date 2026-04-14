# Task Context: Phase B — Workflow Invocation Linking (StartLinkedCommand)

## Goal

Implement Phase B of linked services: allow workflows to be nodes in the service graph by adding a `StartLinkedCommand` journal entry that atomically creates a child invocation AND establishes a link, extending the Phase A object-to-object linking to support invocation-to-object relationships.

## Motivation

Phase A established the service graph foundation — object-to-object linking with completion notification, GC cascade, and completed-state rejection. But workflows (the primary "unit of work" in Restate) can't yet participate as graph nodes. Phase B bridges this gap: a parent object/workflow starts a child workflow linked to itself, and the child's lifecycle is visible in the parent's ego graph.

## Architecture Patterns

### Phase A Foundation (On Branch)

Phase A implemented:
- **ServiceEdges table** (`crates/storage-api/src/service_edges_table/mod.rs`): stores parent→child (`LinkedTo`) and child→parent (`LinkedFrom`) edges keyed by `(ServiceId, edge_label, ServiceNodeId)`
- **ServiceNodeId** (`crates/types/src/invocation/mod.rs:615`): `Object(ServiceId)` only — Phase B adds `WorkflowInvocation` variant
- **ServiceEdgeState** (`service_edges_table/mod.rs:36`): `LinkedToActive { completion_handler_name }`, `LinkedToCompleted`, `LinkedFrom` — proto oneof in `domain.proto:774`
- **VirtualObjectStatus::Completed(ResponseResult)** (`service_status_table/mod.rs:23`): terminal state for objects, blocks new invocations
- **Commands**: `LinkServiceCommand`, `CompleteServiceCommand`, `UnlinkServiceCommand` — full vertical slices (journal → proto → codec → outbox → WAL → state machine)
- **Cross-partition protocol**: `LinkServiceRequest/Response`, `UnlinkServiceRequest`, `ServiceCompletionNotification`
- **GC cascade**: no LinkedFrom parents + Completed → delete state/promises/status/edges + propagate UnlinkServiceRequest

### Invocation Start Flow

Two paths create invocations:
1. **WAL `Command::Invoke`** (`mod.rs:555`): external/ingress → `on_service_invocation()` → dedup → preflight → inbox/schedule/invoke → `InvocationStatus::Invoked`
2. **Journal `CallCommand`** (`entries/call_commands.rs:89`): SDK call → construct `ServiceInvocation` with `Source::Service(caller_id, caller_target)` → `OutboxMessage::ServiceInvocation` → `Action::NewOutboxMessage`

For StartLinkedCommand, the flow is closer to `OneWayCallCommand` (fire-and-forget invocation) combined with `LinkServiceCommand` (edge establishment).

### Workflow Invocation Specifics

- `WorkflowHandlerType::Workflow` — main handler, single-occupancy, can write state
- `WorkflowHandlerType::Shared` — read-only concurrent handlers
- Workflows complete naturally via `end_invocation` when `run` returns (NOT via CompleteServiceCommand)
- `VirtualObjectStatus::Locked(invocation_id)` blocks concurrent workflow `run` invocations
- Workflow `run` returns `ResponseResult` — the completion value

### Key Difference: Workflow vs Object Completion

Objects complete via explicit `CompleteServiceCommand` (Phase A). Workflows complete via `end_invocation` when the invocation finishes. Phase B must hook into `end_invocation` to send `ServiceCompletionNotification` to linked parents when a workflow invocation completes.

### Cross-Partition Messaging

`OutboxMessage` variants → shuffle layer → `Command` on target partition via Bifrost. Phase A added: `LinkServiceRequest`, `LinkServiceResponse`, `UnlinkServiceRequest`, `ServiceCompletionNotification`. `OutboxMessage → Command` mapping in `types.rs:45-59`.

## Dependencies

### Crates Involved
- `restate-types`: ServiceNodeId, journal command struct, WAL types
- `restate-storage-api`: ServiceEdges table traits (possibly new InvocationEdges or extend existing)
- `restate-partition-store`: RocksDB implementation of edges
- `restate-service-protocol-v4`: entry codec, message codec
- `service-protocol` (proto): protocol.proto for SDK messages
- `restate-wal-protocol`: WAL Command enum
- `restate-worker`: state machine handlers, mod.rs dispatch

### Phase A Artifacts to Extend
- `ServiceNodeId` — add `WorkflowInvocation(InvocationId)` variant
- `ServiceEdges table` — extend key encoding for WorkflowInvocation remote type
- `ServiceCompletionNotification` — reuse for workflow completion notification
- `UnlinkServiceRequest` — reuse for GC of workflow-based links
- State machine `on_service_completion_notification` — may need to handle invocation-based nodes

## Implementation Approaches

### Approach A: Single Atomic Command (StartLinkedCommand)

A new journal command `StartLinkedCommand` that:
1. Creates the child `ServiceInvocation` (like OneWayCallCommand)
2. Writes `LinkedToActive` edge on parent (like LinkServiceCommand)
3. Sends `OutboxMessage::ServiceInvocation` to start the child
4. On the child's partition, `on_service_invocation()` also writes the `LinkedFrom` edge (piggybacked via a new field on ServiceInvocation)

**Trade-offs:**
- (+) Atomic — link and invocation established in single journal entry
- (+) No round-trip needed — piggyback LinkedFrom on the invoke path
- (-) Modifies `ServiceInvocation` struct (adds optional link metadata)
- (-) Couples invocation creation with link establishment

### Approach B: Compose Existing Primitives (OneWayCall + LinkService)

SDK emits two separate journal entries: `OneWayCallCommand` + `LinkServiceCommand`. No new command type.

**Trade-offs:**
- (+) No new command type — reuses existing infrastructure
- (-) Non-atomic — link and invocation are separate journal entries
- (-) Race condition: child could complete before link is established
- (-) ServiceNodeId for link target must be an Object, but we're linking to an invocation

### Approach C: StartLinkedCommand with Cross-Partition Link Round-Trip

New `StartLinkedCommand` that:
1. Creates the child invocation (outbox)
2. Writes `LinkedToActive` edge on parent
3. Sends separate `LinkServiceRequest` to child partition for `LinkedFrom` edge
4. Completion delivered when both invocation started AND link acknowledged

**Trade-offs:**
- (+) Clean separation — invocation and link are independent messages
- (+) Consistent with Phase A's round-trip pattern
- (-) Two cross-partition messages per start (invoke + link request)
- (-) More complex completion semantics (wait for both)

## Impact Summary

### Files to Modify
| File | Change |
|------|--------|
| `crates/types/src/invocation/mod.rs` | Add `ServiceNodeId::WorkflowInvocation(InvocationId)`, optional link metadata on `ServiceInvocation` |
| `crates/types/src/journal_v2/command.rs` | Add `StartLinkedCommand` struct + Command::StartLinked variant |
| `crates/storage-api/src/service_edges_table/mod.rs` | Key encoding for WorkflowInvocation remote type (already has `remote_type::WORKFLOW_INVOCATION`) |
| `crates/storage-api/proto/.../domain.proto` | Proto for ServiceNodeId WorkflowInvocation variant |
| `crates/storage-api/src/protobuf_types.rs` | Proto ↔ Rust conversions for new ServiceNodeId variant |
| `crates/partition-store/src/service_edges_table/mod.rs` | Key encoding/decoding for WorkflowInvocation |
| `service-protocol/.../protocol.proto` | `StartLinkedCommandMessage` proto |
| `crates/service-protocol-v4/src/entry_codec.rs` | Encode/decode StartLinkedCommand |
| `crates/service-protocol-v4/src/message_codec/mod.rs` | Message type mapping |
| `crates/wal-protocol/src/lib.rs` | (No change if piggybacking on existing Invoke path) |
| `crates/worker/.../entries/start_linked_command.rs` | **New** — state machine handler |
| `crates/worker/.../entries/mod.rs` | Register new handler + dispatch arm |
| `crates/worker/.../mod.rs` | Hook `end_invocation` to send ServiceCompletionNotification for linked workflows |
| `crates/worker/.../tests/linked_services.rs` | Integration tests for StartLinked flow |

### Files to Create
- `crates/worker/src/partition/state_machine/entries/start_linked_command.rs`

### Risk Assessment
- **Medium complexity**: 10-15 files modified, crosses 6 crates
- **No new table needed** if we reuse ServiceEdges with WorkflowInvocation remote type
- **Key risk**: Hooking into `end_invocation` to send completion notifications — must not break the existing invocation lifecycle
- **WAL compatibility**: Adding optional fields to ServiceInvocation must default to None

## External Research

### Design Docs (Local)
- `docs/design/linked_workflows_mvp.md` — Full vision: linked workflows, ServiceInvocationResponseSink::Link, InvocationStatus::Completing
- `docs/design/vo_child_link_on_completed.md` — onCompleted handler spec, LinkRelation/ChildStatus refactoring
- `docs/design/future_design_ideas_033126.md` — Parked ideas: supervision, shared ownership, bidirectional links

### Design vs Implementation Gap
The design docs describe a more ambitious model (Link table, LinkRelation, ChildStatus, InvocationStatus::Completing). Phase A implemented a simpler model (ServiceEdges, ServiceEdgeState, VirtualObjectStatus::Completed). Phase B should extend Phase A's model, not jump to the full design doc vision. Key simplification: reuse ServiceEdges table rather than creating a separate Link table.

## Key Files

| File | Role |
|------|------|
| `crates/types/src/invocation/mod.rs:615` | ServiceNodeId enum — add WorkflowInvocation variant |
| `crates/types/src/journal_v2/command.rs:47` | Command enum — add StartLinked variant |
| `crates/storage-api/src/service_edges_table/mod.rs` | ServiceEdges table API — extend for invocation nodes |
| `crates/partition-store/src/service_edges_table/mod.rs` | RocksDB edges implementation |
| `crates/worker/src/partition/state_machine/entries/call_commands.rs` | Pattern: how invocations are created from journal |
| `crates/worker/src/partition/state_machine/entries/link_service_command.rs` | Pattern: how edges are established |
| `crates/worker/src/partition/state_machine/mod.rs:555` | `on_service_invocation` — invoke path |
| `crates/worker/src/partition/state_machine/mod.rs:2956` | `end_invocation` — where to hook completion notification |
| `crates/worker/src/partition/state_machine/tests/linked_services.rs` | Existing linked services tests |
| `crates/service-protocol-v4/src/entry_codec.rs` | Entry encode/decode pattern |
| `service-protocol/dev/restate/service/protocol.proto` | SDK protocol messages |
| `crates/storage-api/proto/.../domain.proto:774` | ServiceEdgeState proto |
