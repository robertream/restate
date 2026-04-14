# Implementation Plan: Linked Workflows MVP

## Overview

Implement linked workflows — parent-to-child workflow links that make relationships explicit, block parent completion until children complete, enable observable state graphs, and propagate cancellation through the link tree. This is a cross-cutting feature touching storage, types, the partition state machine, and the SDK protocol.

**Source documents**: `docs/rfcs/rfc_state_graph_and_links_033126.md`, `docs/design/linked_workflows_mvp.md`

## Current State

### No Link Concept Exists

Today, invocations are independent. A workflow can call another via `CallCommand` (blocking) or `OneWayCallCommand` (fire-and-forget), but there is no stored relationship between them. Cancellation is point-to-point (`cancel.rs:59-114`). Completion is immediate when the handler returns — no waiting for children.

### Relevant Existing Infrastructure

- **Invocation status state machine** (`storage-api/src/invocation_status_table/mod.rs:141`): `Scheduled → Inboxed → Invoked ⇌ Suspended ⇌ Paused → Completed → Free`
- **Response sink fan-out** (`invocation_status_table/mod.rs`): `response_sinks: HashSet<ServiceInvocationResponseSink>` on `InFlightInvocationMetadata` — all sinks notified on completion
- **Outbox messages** (`storage-api/src/outbox_table/`): cross-partition communication via `OutboxMessage` variants, including `AttachInvocation` for cross-partition sink registration
- **Promises table** (`storage-api/src/promise_table/`, `partition-store/src/promise_table/`): template for adding new storage tables
- **Journal entries** (`types/src/journal_v2/command.rs`): `OneWayCallCommand`, `AttachInvocationCommand` — reused by linked workflows
- **Cancel propagation** (`worker/src/partition/state_machine/lifecycle/cancel.rs`): extend with link table traversal

## Desired End State

A parent (virtual object or workflow) can start a linked child workflow. The link is stored in a dedicated link table, cross-partition from day one. The parent does not complete until all linked children complete. Cancelling the parent cancels all linked children (each node cancels its own direct children). The link table stores the child's result directly, outliving invocation retention.

### Journal Entries for Starting a Linked Workflow

1. `OneWayCallCommand` (existing) — starts the child invocation
2. `CreateLink` (new) — writes link record, registers `Link` sink on child via `OutboxMessage::AttachInvocation`
3. `AttachInvocationCommand` (existing, optional) — workflow explicit await (`PartitionProcessor` sink)

### New Types Summary

- `InvocationStatus::Completing` — handler done, waiting for linked children. Carries `InFlightInvocationMetadata` and buffered `ResponseResult`.
- `ServiceInvocationResponseSink::Link` — registered on child, generates `LinkCompletionNotification` on completion
- `LinkCompletionNotification` — new outbox message type, child's partition → parent's partition
- `CreateLink` / `RemoveLink` — new journal command types

## Out of Scope / Deferred

- Graph traversal API (inspecting frozen state graph of completed children)
- SSE/ego graph live streaming for linked graphs
- Exclusive/shared handlers on workflows
- Framework-level timeout defaults
- Orphan GC policy
- Object-to-object links (State Introspection RFC)
- SDK implementation (this plan covers the runtime/server side)
- **`ServiceInvocationResponseSink::HandlerInvocation` / `onCompleted`** — deferred post-MVP. Objects can react to child completion via `AttachInvocationCommand` with a `PartitionProcessor` sink. The `HandlerInvocation` sink variant and its wiring through `AttachInvocationCommand` is ergonomic sugar that can be added once the core linking infrastructure is stable.
- **`ScanLinkTable` trait** — partition-level full link scan not needed by any MVP operation. Can be added when orphan GC or admin tooling requires it.

## Design Decisions (from plan review)

1. **No `active_children_count`** — instead of maintaining a denormalized counter on `Completing`, query the link table for any `Running` links when a `LinkCompletionNotification` arrives. The link table is the source of truth. Per-parent link counts are small (typically <20), making prefix scans negligible. This eliminates a consistency invariant across `CreateLink`, `RemoveLink`, and `LinkCompletionNotification` paths.

2. **Reuse `AttachInvocation` outbox for sink registration** — `CreateLink` registers its `Link` sink on the child's partition via the existing `OutboxMessage::AttachInvocation` path, which already handles cross-partition sink registration, routing, and dedup. Only `LinkCompletionNotification` requires a new `OutboxMessage` variant.

3. **Parent-keyed only link table** — the link table is keyed by parent `ServiceId` only. No child→parent secondary index for MVP. All operations are parent-centric. `LinkCompletionNotification` carries `owner_service_id` explicitly, so no reverse lookup is needed.

4. **No `child_invocation_id` in link record** — links are entity-to-entity (`ServiceId` → `ServiceId`). For workflows, there is at most one active invocation per `ServiceId`. Restart-as-new is disallowed on linked workflows to prevent stale notification issues. If invocation-level disambiguation is needed later, the field can be added.

5. **Vertical slice phases** — phases are structured as testable end-to-end slices rather than horizontal type/wiring layers. Each phase produces working, testable functionality.

6. **Strict TDD** — tests are written first (red), then implementation makes them pass (green). Thin storage CRUD test for the link table following the `promise_table_test` pattern. State machine integration tests: 1 happy path + 1 unhappy path, following the established `test_env.apply(Command::...)` pattern against real RocksDB.

## Implementation Phases

### Phase 1: Link Table + CreateLink (Storage → Journal → Sink Registration)

Write a thin link table CRUD test (red), then implement storage, then make it pass (green).

**1a. Storage API — Domain Types and Traits**

Create `crates/storage-api/src/link_table/mod.rs`:

```rust
pub struct Link {
    pub parent_service_id: ServiceId,
    pub child_service_id: ServiceId,
    pub label: ByteString,
    pub state: LinkState,
}

pub enum LinkState {
    Running,
    Completed { result: ResponseResult },
}
```

Traits:
- `ReadLinkTable` — `get_link(parent_service_id, label)`, `get_links_for_parent(parent_service_id)`
- `WriteLinkTable` — `put_link(...)`, `delete_link(parent_service_id, label)`, `update_link_state(parent_service_id, label, LinkState)`

Wire into `Transaction` supertrait in `storage-api/src/lib.rs:84-109`.

**1b. Protobuf Types**

Add `message Link` to `storage-api/proto/dev/restate/storage/v1/domain.proto`. Add `From`/`TryFrom` conversions in `storage-api/src/protobuf_types.rs`. Implement `PartitionStoreProtobufValue for Link`.

**1c. Partition Store Implementation**

Create `crates/partition-store/src/link_table/mod.rs`:
- Add `KeyKind::Link` with unique 2-byte prefix (e.g., `b"lk"`) in `keys.rs`
- Add `TableKind::Link` to `partition_store.rs`
- Use `define_table_key!` macro for key definition
- Key structure: keyed by parent `ServiceId` (service name + key) + label
- Implement `ReadLinkTable`, `WriteLinkTable` for both `PartitionStore` and `PartitionStoreTransaction`
- Register module in `partition-store/src/lib.rs`
- Thin CRUD test in `partition-store/src/tests/link_table_test/mod.rs` following `promise_table_test` pattern

**1d. `ServiceInvocationResponseSink::Link` Variant**

Add to `types/src/invocation/mod.rs`:

```rust
pub enum ServiceInvocationResponseSink {
    PartitionProcessor(JournalCompletionTarget),
    Ingress { request_id: ... },
    Link { owner_service_id: ServiceId },
}
```

Update serde compatibility layer (`serde_hacks` module), protobuf conversions in `protobuf_types.rs`, and proto definitions. The `Link` sink handler in `send_response_to_sinks` is wired in Phase 2.

**1e. `CreateLink` Journal Command**

Add to `types/src/journal_v2/command.rs`:

```rust
pub struct CreateLinkCommand {
    pub child_service_id: ServiceId,
    pub label: ByteString,
    pub completion_id: CompletionId,
    pub name: ByteString,
}
```

Processing in `worker/src/partition/state_machine/entries/create_link_command.rs`:
1. Write link record to link table (state: `Running`)
2. Send `OutboxMessage::AttachInvocation` with a `Link` sink to child's partition to register on child's `response_sinks`
3. Notify completion (link created confirmation)

### Phase 2: Completing + LinkCompletionNotification (Completion Path)

Write the happy path integration test (red): parent links child → child completes → parent transitions `Completing` → `Completed` → caller gets result. Then implement to make it pass (green).

**2a. `InvocationStatus::Completing` Variant**

Add variant to `storage-api/src/invocation_status_table/mod.rs:141`:

```rust
Completing {
    metadata: InFlightInvocationMetadata,
    result: ResponseResult,
}
```

This touches ~25 match sites on `InvocationStatus`. The compiler guides every site. Grouping guidance:

| Match site pattern | `Completing` groups with |
|---|---|
| Methods returning `InFlightInvocationMetadata` fields (`invocation_target`, `source`, `idempotency_key`, `journal_metadata`, `response_sinks`, `timestamps`) | `Invoked/Suspended/Paused` |
| `on_run_invocation` (scheduling) | `Completed/Free` — not runnable |
| `on_kill_invocation` | Unique — abort completing, reply Ok |
| `on_cancel_invocation` / `OnCancelCommand` | Unique — cancel linked children, no journal signal |
| `OnManualResumeCommand`, pause RPC | `Completed` — not resumable/pausable |
| Purge, purge journal, restart-as-new | Falls through to "not completed yet" |
| Journal entry processing guard | Excluded — no new journal entries during completing |
| Notification handler | Falls through to no-op |
| Invoker storage reader | Falls through to None |
| Protobuf encode/decode, discriminant | Unique — new variant |

Add protobuf variant for `Completing` in `domain.proto` and conversions in `protobuf_types.rs`.

**2b. `LinkCompletionNotification` Message**

Add new outbox message variant:

```rust
pub struct LinkCompletionNotification {
    pub owner_service_id: ServiceId,
    pub child_service_id: ServiceId,
    pub result: ResponseResult,
}
```

Wire into:
- `OutboxMessage` enum in `storage-api/src/outbox_table/mod.rs`
- `WithPartitionKey` impl (route to parent's partition via `owner_service_id`)
- `OutboxMessageExt::to_command()` in `worker/src/partition/types.rs`
- `Command` enum in `wal-protocol/src/lib.rs`
- State machine dispatch in `worker/src/partition/state_machine/mod.rs`

**2c. Completion Path — Transition to `Completing`**

Modify the completion path in `mod.rs` (where `OutputCommand` / end-of-invocation is processed, ~line 2707):
1. When the `run` handler returns, query link table for links with state `Running`
2. If no running links → `Completed` (existing behavior, unchanged)
3. If running links exist → `Completing { metadata, result }` — buffer the response, don't fan out to sinks yet

**2d. Handle `Link` Sink on Child Completion**

In `send_response_to_sinks` (`mod.rs:2805`), add handling for the `Link` variant:
- Generate `LinkCompletionNotification` with `owner_service_id`, `child_service_id`, and result
- Send via `handle_outgoing_message` to the parent's partition

**2e. Handle `LinkCompletionNotification` on Parent's Partition**

Add handler in the state machine for incoming `LinkCompletionNotification`:
1. Update link record in link table: `Running` → `Completed { result }`
2. Query link table for any remaining `Running` links for this parent
3. If none remaining and parent is `Completing`, transition parent `Completing` → `Completed`:
   - Fan out buffered result to parent's `response_sinks`
   - Run the full completion path (store `CompletedInvocation`, notify, free/retain, consume inbox, VQueues cleanup)

**2f. Disallow restart-as-new on linked workflows**

In `OnRestartAsNewCommand` and the restart-as-new RPC, check the link table. If the workflow has a parent link (is a linked child), reject the restart. This prevents stale `LinkCompletionNotification` issues without needing `child_invocation_id`.

### Phase 3: RemoveLink + Cancel Propagation

Write the unhappy path integration test (red): cancel parent with active linked child → cancel propagates → child fails → parent completes. Include `RemoveLink` during `Completing` as additional assertions. Then implement to make it pass (green).

**3a. `RemoveLink` Journal Command**

Add to `types/src/journal_v2/command.rs`:

```rust
pub struct RemoveLinkCommand {
    pub label: ByteString,
    pub name: ByteString,
}
```

Processing in `worker/src/partition/state_machine/entries/remove_link_command.rs`:
1. Delete link record from link table
2. If parent is `Completing`, query link table for remaining `Running` links
3. If none remaining, transition `Completing` → `Completed` (same path as 2e)

**3b. Cancel Propagation Through Links**

Extend `OnCancelCommand` in `worker/src/partition/state_machine/lifecycle/cancel.rs`:

For `Invoked`/`Suspended`/`Paused` states:
1. Existing behavior: append `CANCEL_SIGNAL` to journal
2. New: query link table for direct children
3. For each child: send `OutboxMessage::InvocationTermination` (cancel flavor) to child's partition

For `Completing` specifically:
- The handler code is done, so no journal signal needed
- Query link table for direct children, send cancel to each via outbox
- The parent remains in `Completing` until all children report back (with failure results via `LinkCompletionNotification`)

## Component Architecture

```
SDK / Ingress
    │
    ▼
Journal Entries (OneWayCallCommand, CreateLink, RemoveLink, AttachInvocationCommand)
    │
    ▼
State Machine (crates/worker/)
    ├── entries/create_link_command.rs    — CreateLink processing
    ├── entries/remove_link_command.rs    — RemoveLink processing
    ├── lifecycle/cancel.rs              — Cancel + link traversal
    ├── mod.rs                           — Completion path (Completing logic)
    │
    ▼
Storage (crates/storage-api/, crates/partition-store/)
    ├── link_table/                      — Link domain types, traits, RocksDB impl
    ├── invocation_status_table/         — Completing variant
    │
    ▼
Cross-Partition (Outbox)
    ├── OutboxMessage::LinkCompletionNotification  — child completed → parent
    ├── OutboxMessage::AttachInvocation             — register Link sink on child (reused)
    └── OutboxMessage::InvocationTermination        — cancel signals to children (reused)
```

## Testing Strategy (Strict TDD)

Tests written first (red), implementation makes them pass (green).

- **Partition store CRUD test** (`partition-store/src/tests/link_table_test/mod.rs`): Thin test following `promise_table_test` pattern — write link, read back, update state, delete. Validates RocksDB key/value encoding. Written in Phase 1.

- **Happy path integration test** (`worker/src/partition/state_machine/tests/linked_workflows.rs`): Full flow via `test_env.apply(Command::...)` against real RocksDB:
  1. Parent workflow invoked
  2. Parent spawns linked child (`OneWayCallCommand` + `CreateLink`)
  3. Parent returns — enters `Completing`
  4. Child completes — `LinkCompletionNotification` arrives
  5. Link updated, parent transitions `Completing` → `Completed`
  6. Caller gets parent's result
  Written in Phase 2 (red), passes after Phase 2 implementation (green).

- **Unhappy path integration test** (same file): Cancel + RemoveLink scenario:
  1. Parent links two children
  2. Parent returns — enters `Completing` (two running links)
  3. `RemoveLink` on child A — link deleted, parent still `Completing` (one running link)
  4. Cancel parent — cancel signal sent to child B
  5. Child B completes with failure — `LinkCompletionNotification` arrives
  6. Parent transitions `Completing` → `Completed`
  Written in Phase 3 (red), passes after Phase 3 implementation (green).

## Critical Files for Implementation

- `crates/storage-api/src/invocation_status_table/mod.rs` — Add `Completing` variant (~25 match sites, compiler-guided)
- `crates/types/src/invocation/mod.rs` — Add `Link` sink variant
- `crates/types/src/journal_v2/command.rs` — Add `CreateLinkCommand`, `RemoveLinkCommand`
- `crates/worker/src/partition/state_machine/mod.rs` — Completion path changes, `LinkCompletionNotification` handler, `Link` sink dispatch
- `crates/worker/src/partition/state_machine/lifecycle/cancel.rs` — Extend with link table traversal
- `crates/storage-api/src/outbox_table/mod.rs` — Add `LinkCompletionNotification` variant
- `crates/wal-protocol/src/lib.rs` — Add `LinkCompletionNotification` command variant
- `crates/storage-api/src/promise_table/mod.rs` — Pattern template for link table
- `crates/partition-store/src/keys.rs` — Add `KeyKind::Link` with 2-byte prefix
- `crates/storage-api/src/protobuf_types.rs` — Protobuf conversions for `Completing`, `Link` sink, link table
