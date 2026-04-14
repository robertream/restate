# Promise Objects — Server Implementation Plan

## Overview

Add object linking support to the server. Both workflow and object links use the same `LinkCommandMessage` (`0x0415`) on the wire — the server branches on whether `handler_name` is populated:

- **Workflow linking** (existing) — `handler_name` populated. Server invokes handler + creates link. Link result = handler return.
- **Object linking** (new) — `handler_name` empty. Server creates link only, no handler call. Link result = `ctx.resolve()`.

The existing `ServiceInvocationResponseSink::Link` is replaced by a new `LinkChild` outbox message that unifies both paths. Handler completion checks the `ChildOf` record to route `LinkCompletionNotification` — no response sink needed.

See `docs/design/promise_objects.md` for the full feature spec.

## Current State

- `ServiceInvocationResponseSink::Link` on `ServiceInvocation` — carries `owner_service_id`, `caller_invocation_id`, `caller_completion_id`
- `on_service_invocation()` writes `ChildOf { resolved: false }` and sends `LinkedNotification` when sink is `Link` (`mod.rs:750-774`)
- `send_response_to_sinks()` handles `Link` sink by sending `LinkCompletionNotification` (`mod.rs:2974-2998`)
- `ApplyLinkCommand` creates a `ServiceInvocation` with `Link` response sink (`link_command.rs:113-163`)
- `ResolveCommand` handler already implemented (`resolve_command.rs`) — checks `ChildOf`, updates to `resolved: true`, sends `LinkCompletionNotification`

## Changes

### Phase 1: `LinkChild` outbox message

New outbox message type replacing `ServiceInvocationResponseSink::Link`:

```rust
// crates/storage-api/src/outbox_table/mod.rs
enum OutboxMessage {
    // ...existing...
    LinkChild(LinkChild),
}

struct LinkChild {
    // Link establishment (always)
    owner_service_id: ServiceId,
    child_service_id: ServiceId,
    caller_invocation_id: InvocationId,
    caller_completion_id: CompletionId,
    completion_handler_name: Option<String>,

    // Handler invocation (workflow only, None for objects)
    invocation: Option<HandlerInvocation>,
}

struct HandlerInvocation {
    handler_name: String,
    argument: Bytes,
    headers: Vec<Header>,
    idempotency_key: Option<ByteString>,
    execution_time: Option<MillisSinceEpoch>,
}
```

**Files:**
- `crates/storage-api/src/outbox_table/mod.rs` — add `LinkChild`, `HandlerInvocation`

### Phase 2: `ApplyLinkCommand` emits `LinkChild`

Modify `link_command.rs` to emit `LinkChild` instead of creating a `ServiceInvocation`:

```
ApplyLinkCommand::apply:
1. Validate parent is keyed
2. Validate child is keyed
3. Self-link guard
4. Cycle check (are_linked)
5. Write ParentOf(Running(completion_handler_name)) on parent partition
6. If handler_name is populated:
   → enqueue OutboxMessage::LinkChild { invocation: Some(HandlerInvocation{...}), ... }
7. If handler_name is empty:
   → enqueue OutboxMessage::LinkChild { invocation: None, ... }
```

No `ServiceInvocation` created here. No `ServiceInvocationResponseSink::Link` used.

**Files:**
- `crates/worker/src/partition/state_machine/entries/link_command.rs` — replace `ServiceInvocation` creation with `LinkChild` outbox message

### Phase 3: `on_link_child` — child partition handler

New handler on the child partition. **Ordering: confirm link FIRST, then start invocation.** This avoids a race where the child completes before the parent has the link confirmed.

```
on_link_child(msg: LinkChild):
1. Write ChildOf { resolved: false }
2. Send LinkedNotification (success) → parent SDK's LinkHandle unblocks
3. If msg.invocation.is_some():
   a. Build ServiceInvocation from HandlerInvocation fields
      (plain invocation, no Link response sink, has_parent: true on metadata)
   b. Call on_service_invocation() to start the handler
```

For object links (`invocation: None`), only steps 1-2 execute — no handler is started.

**Files:**
- `crates/worker/src/partition/state_machine/mod.rs` — add `on_link_child()`, WAL dispatch for `LinkChild` command

### Phase 4: Handler completion checks `ChildOf`

Remove `ServiceInvocationResponseSink::Link` entirely. Handler completion now routes `LinkCompletionNotification` by checking the `ChildOf` record:

```
send_response_to_sinks / end_invocation:
  // Only check for linked children (gated by has_parent flag on invocation metadata)
  if invocation_metadata.has_parent {
      if let Some(parent_link) = get_first_parent(&child_service_id) {
          if !parent_link.resolved {
              send LinkCompletionNotification {
                  owner_service_id: parent_link.remote_service_id,
                  child_service_id,
                  result,
              }
          }
      }
  }
```

This unifies two paths:
- **Workflow child completes** → `get_first_parent` → `LinkCompletionNotification` with handler result
- **Object child resolves** → `ResolveCommand` already does `get_first_parent` → `LinkCompletionNotification` with resolve result

The `has_parent` flag avoids the `ChildOf` read for unlinked services (the vast majority).

**Changes to remove:**
- Remove `ServiceInvocationResponseSink::Link` variant from `crates/types/src/invocation/mod.rs`
- Remove `Link` sink handling from `send_response_to_sinks()` in `mod.rs:2974-2998`
- Remove inline `ChildOf` write + `LinkedNotification` from `on_service_invocation()` (`mod.rs:750-774`) — moved to `on_link_child`

**Files:**
- `crates/types/src/invocation/mod.rs` — remove `ServiceInvocationResponseSink::Link`
- `crates/worker/src/partition/state_machine/mod.rs` — modify `send_response_to_sinks()`, remove `on_service_invocation()` link handling

### Phase 5: Tests

**Happy paths (1 each):**
- Object link: `LinkCommand` with empty handler → `LinkChild { invocation: None }` → ChildOf created → LinkedNotification → parent confirmed → child resolves → parent receives result
- Workflow link: `LinkCommand` with handler → `LinkChild { invocation: Some(...) }` → ChildOf created → LinkedNotification → handler starts → handler completes → `get_first_parent` → parent receives result

**Unhappy paths (1 each):**
- Object link: child resolves with failure → parent receives failure notification
- Workflow link: handler target invalid → LinkedNotification with error (link not established)

**Edge cases (defer to coverage task):**
- Full GC cascade: link → resolve → remove → recursive cleanup
- Double resolve rejection
- Cycle detection
- Self-link guard
- Race: child completes before parent processes LinkedNotification

**Files:**
- `crates/worker/src/partition/state_machine/tests/linked_workflows.rs` — update existing tests, add new

## Implementation Order

| Phase | Description | Depends on |
|-------|-------------|-----------|
| 1 | `LinkChild` outbox message types | — |
| 2 | `ApplyLinkCommand` emits `LinkChild` | Phase 1 |
| 3 | `on_link_child` child partition handler | Phase 2 |
| 4 | Handler completion checks `ChildOf`, remove Link sink | Phase 3 |
| 5 | Tests | Phase 4 |

Sequential — each phase builds on the previous.

## Critical Files

| File | Change |
|------|--------|
| `crates/storage-api/src/outbox_table/mod.rs` | `LinkChild` + `HandlerInvocation` types |
| `crates/types/src/invocation/mod.rs` | Remove `ServiceInvocationResponseSink::Link` |
| `crates/worker/src/partition/state_machine/entries/link_command.rs` | Emit `LinkChild` instead of `ServiceInvocation` |
| `crates/worker/src/partition/state_machine/mod.rs` | `on_link_child()`, modify `send_response_to_sinks()`, remove `on_service_invocation()` link handling |
| `crates/worker/src/partition/state_machine/tests/linked_workflows.rs` | Tests |

## Dependencies

- **sdk-shared-core**: No changes needed. SDK calls existing `sys_link` with empty `handler_name` for object links.
- **Rust SDK**: `linked_object().link()` builder calls `sys_link` with empty handler, producing `LinkCommandMessage` with empty `handler_name`. See SDK plan at `../sdk-rust/docs/tasks/linked-workflows/specs/plan_v3.md`.
