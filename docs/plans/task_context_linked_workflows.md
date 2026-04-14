# Task Context: Linked Workflows MVP

## Feature Description

Implement linked workflows — workflows spawned by objects or other workflows, connected by unidirectional parent-to-child links. Links make workflow relationships explicit, workflow progress observable, and computation graphs inspectable. Full design in `docs/rfcs/rfc_state_graph_and_links_033126.md` and `docs/design/linked_workflows_mvp.md`.

## Architecture Patterns

### Storage Table Pattern (Promises Table as Template)

Adding a new storage table follows the promises table pattern:

**Storage API layer** (`crates/storage-api/`):
- `promise_table/mod.rs` — domain types (`Promise`, `PromiseState`, `OwnedPromiseRow`), read/write/scan traits
- Reads are `async fn`, writes are synchronous (buffered into write batch)
- `lib.rs:84-109` — traits wired into `Transaction` supertrait
- `proto/dev/restate/storage/v1/domain.proto:708-719` — protobuf message definition
- `protobuf_types.rs` — `From`/`TryFrom` conversions between Rust and proto types
- `PartitionStoreProtobufValue` trait ties Rust type to proto equivalent

**Partition store layer** (`crates/partition-store/`):
- `promise_table/mod.rs` — `define_table_key!` macro for key definition, free functions for get/put/delete, trait impls for both `PartitionStore` and `PartitionStoreTransaction`
- `keys.rs:36-68` — `KeyKind` enum with unique 2-byte prefix per table (e.g. `b"pr"` for promises)
- `partition_store.rs:241-258` — `TableKind` enum mapping to `KeyKind` slices
- `lib.rs` — module registration

**State machine layer** (`crates/worker/`):
- Entry handlers generic over table traits (e.g., `S: ReadPromiseTable + WritePromiseTable`)
- Cleanup on entity termination

### Journal Entry Pattern

Journal entries follow the pattern in `crates/types/src/journal_v2/command.rs`:
- Struct with `CallRequest` or similar payload + `CompletionId`(s)
- `impl_command_accessors!` macro for metadata/entry conversion
- `CommandMetadata` trait for related completion IDs
- Processing in `crates/worker/src/partition/state_machine/entries/` — `ApplyJournalCommandEffect<'e, T>` pattern

### Response Sink Pattern

`ServiceInvocationResponseSink` (`crates/types/src/invocation/mod.rs:636`) has variants:
- `PartitionProcessor(JournalCompletionTarget)` — delivers result to a journal completion slot
- `Ingress { request_id }` — delivers result to an HTTP client

Fan-out on completion: `InFlightInvocationMetadata.response_sinks: HashSet<ServiceInvocationResponseSink>` — all sinks notified when invocation completes.

### Outbox Message Pattern

Cross-partition communication uses `OutboxMessage` (`crates/storage-api/src/outbox_table/`). Existing variants include `ServiceInvocation`, `AttachInvocation`, `InvocationResponse`. Messages are written to the outbox table in the same transaction, then delivered asynchronously.

### Cancellation Pattern

`OnCancelCommand` (`crates/worker/src/partition/state_machine/lifecycle/cancel.rs:59-114`) dispatches on `InvocationStatus`:
- `Invoked/Suspended/Paused` → appends `CANCEL_SIGNAL` to journal
- `Inboxed` → `terminate_inboxed_invocation(Cancel)`
- `Scheduled` → `terminate_scheduled_invocation(Cancel)`
- Point-to-point only — no graph traversal today

## Key Files

### Types and Storage API
- `crates/types/src/invocation/mod.rs` — `ServiceInvocation`, `ServiceInvocationResponseSink`, `Source`
- `crates/types/src/journal_v2/command.rs` — journal command types (`CallCommand`, `OneWayCallCommand`, `AttachInvocationCommand`)
- `crates/storage-api/src/invocation_status_table/mod.rs:141` — `InvocationStatus` enum (needs new `Completing` variant)
- `crates/storage-api/src/promise_table/mod.rs` — template for link table
- `crates/storage-api/src/outbox_table/` — outbox message types
- `crates/storage-api/src/lib.rs:84-109` — `Transaction` supertrait bounds
- `crates/storage-api/proto/dev/restate/storage/v1/domain.proto` — protobuf definitions

### Partition Store
- `crates/partition-store/src/keys.rs:36-68` — `KeyKind` enum (add new 2-byte prefix)
- `crates/partition-store/src/partition_store.rs:241-258` — `TableKind` enum
- `crates/partition-store/src/promise_table/mod.rs` — template for link table impl
- `crates/partition-store/src/lib.rs` — module registration

### State Machine (Worker)
- `crates/worker/src/partition/state_machine/entries/call_commands.rs` — `CallCommand`/`OneWayCallCommand` processing
- `crates/worker/src/partition/state_machine/entries/attach_invocation_command.rs` — attach processing
- `crates/worker/src/partition/state_machine/lifecycle/cancel.rs` — cancellation (extend for link traversal)
- `crates/worker/src/partition/state_machine/mod.rs` — main state machine, completion logic
- `crates/worker/src/partition/state_machine/entries/mod.rs` — entry handler registration

### Protobuf Conversions
- `crates/storage-api/src/protobuf_types.rs` — Rust <-> proto conversions

## Implementation Approaches

### Approach: Incremental, Bottom-Up

Build from storage up through types to state machine to SDK:

1. **Link table** — storage API traits, protobuf types, partition store implementation
2. **New types** — `Completing` status variant, `Link`/`HandlerInvocation` sink variants, `LinkCompletionNotification` message
3. **Journal entries** — `CreateLink` and `RemoveLink` commands
4. **State machine** — `CreateLink`/`RemoveLink` handlers, completion path changes, cancel propagation
5. **Integration** — wire into SDK protocol, end-to-end test

### Cross-Partition Considerations

All link operations are cross-partition from day one:
- `CreateLink` sends outbox message to child's partition to register `Link` sink
- `LinkCompletionNotification` routes from child's partition to parent's partition via outbox
- Cancel propagation sends cancel signals cross-partition via outbox

## Impact Summary

### High Impact (Core Changes)
- `InvocationStatus` enum — new `Completing` variant, touches ~29 files / 298 occurrences (compiler-guided)
- Completion path in state machine — must check link table before finalizing
- New link table — full storage stack (API, proto, partition store)

### Medium Impact
- `ServiceInvocationResponseSink` — two new variants
- New outbox message type — `LinkCompletionNotification`
- Two new journal entry types — `CreateLink`, `RemoveLink`
- Cancel propagation — extend to read link table

### Low Impact
- Protobuf definitions — additive
- SDK protocol — new entry types

## Decisions Made (from Plan Review)

1. Completion delivers `Result<T, E>` only — no status field, no inline frozen graph
2. Children block parent completion — essential semantics
3. New `InvocationStatus::Completing` variant
4. Journal entries: `OneWayCallCommand` + `CreateLink` + optional `AttachInvocationCommand`
5. New sink variants: `Link` (generates `LinkCompletionNotification`), `HandlerInvocation` (for `onCompleted`)
6. `onCompleted` is object-only — workflows await via `AttachInvocationCommand`
7. Cross-partition from day one
8. No framework-level timeout defaults
9. Cancellation is local per node — each node cancels its own direct children
10. Link table stores the child's result directly — links outlive invocation retention
