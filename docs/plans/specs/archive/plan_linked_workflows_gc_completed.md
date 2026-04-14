# Implementation Plan: Link GC + Bidirectional Link Records

**Status**: COMPLETED — 2026-04-03. Implemented in commits 0fc81db3a, 757d1a316, b99c09586 on `linked-workflows` branch.

## Overview

Add link lifecycle management to the linked workflows feature. The link table becomes bidirectional — each link produces both an outgoing record on the parent's partition and an incoming record on the child's partition. This enables purge guards (linked children can't be purged), cycle prevention (key collision), and cross-partition cleanup via `RemoveParentLink` notifications.

**Prerequisite**: Linked workflows MVP is complete (11 commits on `linked-workflows` branch). This plan extends the existing implementation.

**Source documents**: `docs/tasks/linked-workflows/task_context.md`, `docs/tasks/linked-workflows/specs/link_gc_tasks.md`

## Current State (MVP Complete)

The linked workflows MVP is implemented with:
- Link table: `KeyKind::Link` (`b"lk"`), keyed by `(parent_service_id, child_service_id)`, stores `LinkState` (Running/Completed)
- `Link` struct: `parent_service_id`, `child_service_id`, `state`
- `InvocationStatus::Completing` variant with ~25 match sites
- `CreateLinkCommand` / `RemoveLinkCommand` journal entries with service protocol v4 codecs
- `LinkCompletionNotification` cross-partition outbox message
- `ServiceInvocationResponseSink::Link` and `HandlerInvocation` variants
- Cancel propagation through links (Invoked/Suspended/Paused + Completing states)
- `transition_completing_to_completed` and `cancel_linked_children` helpers
- 7 integration tests + 1 CRUD test

**What's missing**: Links have no lifecycle management. They persist forever. No purge guard. No incoming link tracking. No cycle prevention.

## Desired End State

- **Bidirectional link records**: Each link has an outgoing record on the parent's partition and an incoming record on the child's partition
- **Cycle prevention**: Key collision — `(A, B)` can only have one record per partition, so A→B and B→A can't coexist
- **Purge guard**: Linked children (with incoming links) cannot be purged
- **Cross-partition cleanup**: `RemoveParentLink` notification removes incoming records when parent purges, unlinks, or clears state
- **VO clear state**: Deleting a VO's state also deletes its links and notifies children

## Link Table Design

### Bidirectional Records

```rust
pub enum EdgeLabel {
    ParentOf,  // I am the parent of remote_service_id
    ChildOf,   // I am the child of remote_service_id
}

pub struct Link {
    pub local_service_id: ServiceId,   // "me" on this partition
    pub remote_service_id: ServiceId,  // "the other entity"
    pub edge_label: EdgeLabel,         // the relationship from my perspective
    pub state: LinkState,              // only meaningful for ParentOf
}
```

Key: `(local_service_id, remote_service_id)` — same prefix `b"lk"`, same table. `edge_label` is a stored field, not part of the key.

### Cycle Prevention

Key collision prevents cycles. If A links to B:
- A's partition has `(A, B)` with `edge_label: ParentOf`
- B's partition has `(B, A)` with `edge_label: ChildOf`

If B then tries to link to A:
- B's partition already has `(B, A)` — `CreateLink` checks for existing record and rejects

### Trait Methods

```
ReadLinkTable:
  get_link(local, remote) → Option<Link>
  get_children_of(service_id) → Vec<Link>     // filters edge_label=ParentOf
  has_parents(service_id) → bool              // short-circuit: return true on first ChildOf record

WriteLinkTable:
  put_link(link) → ()
  delete_link(local, remote) → ()
  delete_all_links_for(service_id) → ()       // prefix delete all records
```

## Cross-Partition Messages

### `RemoveParentLink` (new)

```rust
pub struct RemoveParentLink {
    pub child_service_id: ServiceId,
    pub parent_service_id: ServiceId,
}
```

- Routes to child's partition via `child_service_id.partition_key()`
- Handler: delete incoming link record `(child_service_id, parent_service_id)`
- Sent by: parent purge, `RemoveLinkCommand`, VO clear state

Follows `LinkCompletionNotification` pattern: new `OutboxMessage` variant → `Command` variant → state machine dispatch.

## Code Paths

### CreateLink (parent's partition)
1. Check for existing record `(parent, child)` — reject if exists (cycle prevention)
2. Write outgoing record: `Link { local=parent, remote=child, edge_label=ParentOf, state=Running }`
3. Send `AttachInvocation` to child's partition with `Link` sink

### AttachInvocation with Link sink (child's partition)
1. Existing: add `Link` sink to child's `response_sinks`
2. **New**: write incoming record `Link { local=child, remote=parent, edge_label=ChildOf, state=Running }`

### RemoveLink (parent's partition)
1. Delete outgoing record `(parent, child)`
2. **New**: send `RemoveParentLink` to child's partition

### Purge (workflow)
1. **New**: check `has_parents(service_id)` — reject if true
2. Read outgoing links
3. Delete all links for service
4. **New**: for each child, send `RemoveParentLink`
5. Continue with existing purge cleanup

### VO Clear State
1. Read outgoing links
2. **New**: delete all links for service
3. **New**: for each child, send `RemoveParentLink`

### RemoveParentLink handler (child's partition)
1. Delete incoming record `(child_service_id, parent_service_id)`
2. Silently ignore if record doesn't exist (stale notification)

## Implementation Phases

### Phase 1: Refactor Link Struct + RemoveParentLink Message

**1a. Refactor Link struct to bidirectional model**

Rename fields and add direction:
- `parent_service_id` → `local_service_id`
- `child_service_id` → `remote_service_id`
- Add `direction: LinkDirection` field
- Update proto, protobuf conversions, all callers

Rename and add trait methods:
- `get_links_for_parent` → `get_children_of` (filter `edge_label=ParentOf`)
- Add `has_parents` (check for any `edge_label=ChildOf`)
- Add `delete_all_links_for` (prefix delete)

Update partition store CRUD test.

**1b. RemoveParentLink outbox message and command**

- `RemoveParentLink` struct in `outbox_table/mod.rs`
- `OutboxMessage::RemoveParentLink` variant
- `Command::RemoveParentLink` in WAL protocol
- `to_command()` mapping

### Phase 2: State Machine Integration

**2a. Write incoming link on AttachInvocation with Link sink**

When child's partition processes `AttachInvocation` with a `Link` sink, also write:
`Link { local=child, remote=parent, edge_label=ChildOf, state=Running }`

**2b. Cycle prevention in CreateLink**

Before writing outgoing record, check `get_link(parent, child)`. If exists, reject.

**2c. Purge guard + link cleanup + RemoveParentLink on purge**

In `purge.rs`:
- Guard: `has_parents` → reject
- Read outgoing links, `delete_all_links_for`
- Send `RemoveParentLink` per child

**2d. RemoveLink sends RemoveParentLink + handler**

In `remove_link_command.rs`: after deleting outgoing record, send `RemoveParentLink`.
State machine dispatch: `Command::RemoveParentLink` → delete incoming record.

**2e. VO clear state link cleanup**

In `clear_all_state_command.rs` and `do_clear_all_state`: read outgoing links, delete all, send `RemoveParentLink` per child.

### Phase 3: Tests (TDD)

Three TDD tests drive all implementation:

1. **Happy path** (evolve existing `linked_child_cannot_be_purged_until_parent_unlinks`): Multi-level graph (grandparent → parent → child). Drives purge guard, RemoveParentLink handler, incoming link records. Verifies cascade: unlink parent, purge parent, purge child becomes possible, purge child cleans up grandchild's incoming record.
2. **Unhappy path**: Cycle prevention — A→B then B→A fails. Drives CreateLink guard.
3. **VO clear state**: VO with links clears state → links gone, RemoveParentLink sent. Drives clear_all_state integration (separate code path not covered by tests 1-2).

## Component Architecture

```
State Machine (crates/worker/)
    ├── entries/create_link_command.rs    — cycle check + outgoing record
    ├── entries/remove_link_command.rs    — delete outgoing + send RemoveParentLink
    ├── entries/clear_all_state_command.rs — link cleanup + RemoveParentLink
    ├── lifecycle/purge.rs               — purge guard + link cleanup + RemoveParentLink
    ├── mod.rs                           — handle AttachInvocation (incoming record)
    │                                      handle RemoveParentLink (delete incoming)
    │
    ▼
Storage (crates/storage-api/, crates/partition-store/)
    ├── link_table/                      — Link with EdgeLabel, get_children_of,
    │                                      has_parents, delete_all_links_for
    │
    ▼
Cross-Partition (Outbox)
    ├── OutboxMessage::RemoveParentLink   — parent purge/unlink → child
    ├── OutboxMessage::AttachInvocation   — register Link sink + write incoming record
    └── OutboxMessage::LinkCompletionNotification  — unchanged
```

## Design Decisions

1. **Single table, edge_label as field** — no separate key prefix for incoming vs outgoing. `EdgeLabel::ParentOf`/`ChildOf` enum stored as a field. Same `b"lk"` prefix.

2. **Key collision prevents cycles** — `(local, remote)` is the key. Both outgoing and incoming can't coexist for the same pair on the same partition. CreateLink checks before writing.

3. **`has_parents` on link table, not on invocation status** — incoming link records persist independently of invocation lifecycle. No field on `CompletedInvocation` needed.

4. **`local_service_id` / `remote_service_id` field names** — `edge_label` determines semantics. When `ParentOf`, local is parent, remote is child. Neutral naming avoids confusion.

5. **Incoming record written on AttachInvocation** — piggybacking on the existing cross-partition flow. No extra outbox message for the initial incoming record write.

6. **`RemoveParentLink` follows `LinkCompletionNotification` pattern** — same outbox → command → state machine dispatch wiring.
