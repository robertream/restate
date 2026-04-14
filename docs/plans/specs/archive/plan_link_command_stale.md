# Implementation Plan: LinkCommand + LinkedNotification

## Overview

Replace `OneWayCallCommand { linked: true }` with a dedicated `LinkCommand` journal entry that atomically starts a child workflow and establishes a link. The SDK receives a single completion when the child's partition confirms (or rejects) the link. This gives the SDK a clear success/failure signal and ensures the link is truly established before the SDK proceeds.

## Current State

`OneWayCallCommand` has a `linked: bool` field. When true, the handler writes a `ParentOf` link record and sends an `AttachInvocation` with a `Link` sink separately from the `ServiceInvocation`. Problems:
- Fire-and-forget semantics don't support failure reporting for linking
- The SDK gets an invocation ID completion immediately but no signal about whether the link was established
- Cycle rejection crashes the invocation (`Error::ApplyCommandEffect`) instead of returning a graceful failure
- Two outbox messages (`ServiceInvocation` + `AttachInvocation`) create a race window where the child exists but the `Link` sink isn't attached yet

## Desired End State

- `LinkCommand` — new journal entry that starts a child + writes a link atomically
- SDK gets one completion: `Ok(InvocationId)` or `Err(LinkError)`
- Completion is deferred until `LinkedNotification` arrives from the child's partition
- `OneWayCallCommand` reverts to fire-and-forget only (no `linked` field)
- No `AttachInvocation` for links — `Link` sink goes on `ServiceInvocation.response_sink` directly

## Design

### LinkCommand Journal Entry

```rust
pub struct LinkCommand {
    pub request: CallRequest,       // same as OneWayCall/Call
    pub invoke_time: MillisSinceEpoch,
    pub completion_id: CompletionId, // SDK awaits this
}
```

### LinkCommand Handler (parent's partition)

1. Validate: parent is keyed service, child is keyed service, not self-link, `are_linked` check
2. On validation failure (cycle detected, self-link, non-keyed service): deliver `Err(LinkError)` completion to SDK immediately — no `ApplyCommandEffect` crash
3. Write `ParentOf { state: Running }` link record
4. Enqueue `ServiceInvocation` with `Link` response sink on the `response_sink` field (not a separate `AttachInvocation`)
5. **Do not deliver completion yet** — SDK blocks until `LinkedNotification` arrives

### Child Invocation Creation (child's partition)

When `on_service_invocation` processes the `ServiceInvocation` and detects a `Link` sink in `response_sink`:
1. Write `ChildOf` record (same transaction as invocation creation — truly atomic)
2. Send `LinkedNotification { Ok }` to parent's partition

When the invocation is rejected (duplicate workflow, etc.):
1. Send `LinkedNotification { Err(reason) }` directly from the rejection path in `on_service_invocation`
2. `send_response_to_sinks` is not involved — the `Link` sink was never appended to the invocation's response sinks
3. `LinkCompletion` is reserved for actual execution completion (`Running` → `Completed`)

### LinkedNotification (child → parent)

```rust
pub struct LinkedNotification {
    pub owner_service_id: ServiceId,
    pub child_service_id: ServiceId,
    pub child_invocation_id: InvocationId,
    pub result: Result<(), InvocationError>,
}
```

Routes to parent's partition via `owner_service_id.partition_key()`.

Handler on parent:
- `Ok` → deliver completion to SDK: `Ok(InvocationId)` via `InvocationResponse` to `JournalCompletionTarget { parent_invocation_id, completion_id }`
- `Err` → delete `ParentOf` record, deliver completion to SDK: `Err(LinkError)` via same mechanism. If parent is in `Completing`, re-check children and potentially transition to `Completed`.
- **Stale message**: if `ParentOf` record is missing (parent purged/killed), log and return `Ok(())` — same pattern as `on_link_completion`

### LinkState

```rust
pub enum LinkState {
    Running,                           // child executing
    Completed { result: ResponseResult }, // child finished
}
```

No `Starting` state — `ParentOf { Running }` is written immediately. The deferred SDK completion (tracked by journal/completion machinery) is the "waiting" signal, not the link state.

### Link Sink Behavior

`send_response_to_sinks` is unchanged — the `Link` sink always generates `LinkCompletion` when it fires. It only fires for normal execution completion (child ran and finished).

Rejection is handled separately: `on_service_invocation` sends `LinkedNotification { Err }` directly from the rejection code path, before the `Link` sink is ever registered on any invocation.

### Completing Check

Parent enters `Completing` when handler returns with non-`Completed` links (unchanged from current behavior):
- `Running` → child executing, blocks completion
- `Completed` → child done, doesn't block

## Revert OneWayCallCommand

- Remove `linked: bool` from `OneWayCallCommand`
- Remove link logic from `_ApplyCallCommand`
- `_ApplyCallCommand` no longer needs `ReadLinkTable + WriteLinkTable` bounds
- OneWayCallCommand proto: remove `bool linked = 8`

## New Proto Messages

```proto
// Completable: Yes
// Fallible: Yes
// Type: 0x0400 + 15
message LinkCommandMessage {
    string service_name = 1;
    string handler = 2;
    string key = 3;
    bytes parameter = 4;
    repeated Header headers = 5;
    optional string idempotency_key = 6;
    uint64 invoke_time = 7;

    uint32 result_completion_id = 11;
}
```

Completion notification follows `NotificationTemplate`:
```proto
// Type: 0x8015
message LinkedNotificationMessage {
    reserved 2, 3, 4, 12, 16, 17;
    uint32 completion_id = 1;
    // Success: invocation_id as string
    // Failure: InvocationError
    oneof result {
        Value value = 5;
        Failure failure = 6;
    }
}
```

### Outbox: LinkedNotification

```proto
message OutboxLinkedNotification {
    ServiceId owner_service_id = 1;
    ServiceId child_service_id = 2;
    InvocationId child_invocation_id = 3;
    oneof result {
        google.protobuf.Empty success = 4;
        Failure failure = 5;
    }
}
```

## Out of Scope

- `GetLinkCommand` for handler access to link state
- Transitive cycle detection
- Orphan GC policy
- Graph traversal API
- SDK implementation

## Implementation Phases

### Phase 1: Revert OneWayCallCommand + Add LinkCommand Type

1. Remove `linked: bool` from `OneWayCallCommand` struct, proto, codec
2. Remove link logic from `_ApplyCallCommand`, remove `ReadLinkTable + WriteLinkTable` bounds
3. Remove `AttachInvocation` + `Link` sink code path (dead code after Link sink moves to `ServiceInvocation.response_sink`)
4. Add `LinkCommand` struct to `command.rs`
5. Add `LinkedNotification` outbox message + `Command::LinkedNotification` WAL variant
6. Add `LinkCommandMessage` and `LinkedNotificationMessage` to service protocol proto
7. Add codec encode/decode for `LinkCommand`

### Phase 2: LinkCommand Handler + LinkedNotification

1. `LinkCommand` handler: validate (return `Err(LinkError)` completion on cycle/self-link/non-keyed), write `ParentOf { Running }`, enqueue `ServiceInvocation` with `Link` sink on `response_sink`. No completion yet — store `completion_id` for later delivery.
2. `on_service_invocation`: when creating invocation with `Link` sink, write `ChildOf`, send `LinkedNotification { Ok }`
3. `on_service_invocation` rejection path: send `LinkedNotification { Err }` directly (do not involve `send_response_to_sinks`)
4. `LinkedNotification` handler on parent: on `Ok` deliver `Ok(InvocationId)` completion; on `Err` delete `ParentOf` record, deliver `Err(LinkError)` completion, re-check `Completing` if applicable. On stale (missing record): log and return `Ok(())`.

### Phase 3: Tests

1. Happy path: `LinkCommand` → child created → `LinkedNotification { Ok }` → SDK gets `Ok(InvocationId)` → child completes → parent transitions Completing → Completed
2. Rejection: `LinkCommand` → duplicate workflow → `LinkedNotification { Err }` → SDK gets `Err(LinkError)` → link deleted
3. Existing tests updated: replace `insert_outgoing_link` + `AttachInvocation` with direct storage setup

## Cross-Partition Messages (final set)

| Message | Direction | Purpose |
|---------|-----------|---------|
| `LinkedNotification` | child → parent | Link confirmed or rejected |
| `LinkCompletion` | child → parent | Child execution completed |
| `RemoveParentLink` | parent → child | Parent unlinks/purges/clears |

## Design Decisions

1. **Dedicated `LinkCommand` over flag on `OneWayCallCommand`** — linking is a failable operation with its own completion semantics. Fire-and-forget `OneWayCall` should stay simple.

2. **Link sink on `ServiceInvocation.response_sink`** — child's invocation is created with the Link sink already registered. No separate `AttachInvocation` needed. ChildOf record written atomically with invocation creation. Eliminates the race window from the previous two-message approach.

3. **`LinkedNotification` vs `LinkCompletion`** — different events at different times. `LinkedNotification` is the creation-time signal (link established or rejected). `LinkCompletion` is the execution-time signal (child ran and finished). Rejection sends `LinkedNotification { Err }` directly from `on_service_invocation`, not through `send_response_to_sinks`.

4. **One SDK completion** — `Ok(InvocationId)` or `Err(LinkError)`. SDK uses `AttachInvocationCommand` separately if it wants the child's result.

5. **No `Starting` state** — `ParentOf { Running }` is written immediately. The deferred SDK completion tracks the "waiting for confirmation" state. On rejection, the record is deleted. This keeps `LinkState` at two variants (`Running`, `Completed`) and the `Completing` check unchanged.

6. **Graceful validation errors** — cycle detection, self-link, and non-keyed service errors deliver `Err(LinkError)` via the completion instead of crashing with `Error::ApplyCommandEffect`.

7. **Stale message handling** — `LinkedNotification` arriving after parent is purged/killed is a no-op (log and return `Ok(())`), matching the existing `on_link_completion` pattern.
