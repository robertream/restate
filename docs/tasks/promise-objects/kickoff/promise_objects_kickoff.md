---
date: 2026-04-07
git_commit: 0e62b89aa
branch: promise-objects (forked from linked-workflows)
repo: robertream/restate
topic: Promise Objects — Resolvable Virtual Objects
tags: [linked-workflows, virtual-objects, resolve, state-machine]
status: ready-for-implementation
---

# Promise Objects Kickoff

## Project Context

**Design doc:** `docs/design/promise_objects.md`
**Branch:** `promise-objects` forked from `linked-workflows`
**Dependencies:** Linked Workflows MVP + onCompleted handler (both implemented on `linked-workflows`)

## What Exists

The linked-workflows branch provides the foundation:
- `LinkRelation::ChildOf` (unit variant) — `crates/storage-api/src/link_table/mod.rs:45`
- `ChildOf` written in `on_service_invocation` — `crates/worker/src/partition/state_machine/mod.rs:750`
- `on_remove_parent_link` — simple ChildOf deletion — `mod.rs:3207-3220`
- `has_parents` — prefix scan short-circuit — `crates/partition-store/src/link_table/mod.rs:176-190`
- Command dispatch in `entries/mod.rs:183-365` — last arm is `Command::Link`
- State mutation handlers: `set_state_command.rs`, `clear_state_command.rs`, `clear_all_state_command.rs`
- `CommandType` enum via `strum::EnumDiscriminants` — `crates/types/src/journal_v2/command.rs:47-70`

## What Needs to Change

### 1. `ChildOf { resolved: bool }` — link_table types + proto + partition-store
- Change `ChildOf` from unit variant to `ChildOf { resolved: bool }`
- Update proto: `ChildOf` message gets `bool is_resolved = 1`
- Update proto conversion in `protobuf_types.rs`
- Update `on_service_invocation` to write `ChildOf { resolved: false }`
- Replace `has_parents` with `get_first_parent` on `ReadLinkTable` trait + partition-store impl

### 2. `ResolveCommand` — types + proto + codec + dispatch
- Add `ResolveCommand { result: ResponseResult, completion_id: CompletionId }` to command.rs
- Add `ResolveCommandMessage` proto
- Add encode/decode in entry_codec.rs
- Add `CommandType::Resolve` dispatch in entries/mod.rs
- Add prometheus_label in journal_v2/mod.rs

### 3. `resolve_command.rs` — new handler
- Validate ChildOf exists via `get_first_parent`
- Reject if already resolved
- Reject if shared handler
- Update ChildOf to `resolved: true`
- Send `LinkCompletionNotification` to parent
- Deliver success completion

### 4. State mutation rejection
- Add resolved check at top of `set_state_command.rs`, `clear_state_command.rs`, `clear_all_state_command.rs`
- Point read via `get_first_parent` — fast O(1) for unlinked VOs
- NOT applied to `CompletePromise`

### 5. GC on `on_remove_parent_link`
- Read ChildOf before deleting to check resolved flag
- If resolved: delete all user state, delete all promises, propagate RemoveParentLink to children

## Implementation Approach

Single commit — the compiler guides the rename cascade. Two TDD tests:
- Happy: VO resolves → parent gets result
- Unhappy: SetState after resolve → OBJECT_RESOLVED error

## Code References

| File | Line | What |
|------|------|------|
| `crates/storage-api/src/link_table/mod.rs` | 42-48 | `LinkRelation` enum |
| `crates/storage-api/src/link_table/mod.rs` | 71-96 | `ReadLinkTable` trait |
| `crates/partition-store/src/link_table/mod.rs` | 176-190 | `has_parents` impl |
| `crates/worker/src/partition/state_machine/mod.rs` | 750-773 | ChildOf write in on_service_invocation |
| `crates/worker/src/partition/state_machine/mod.rs` | 3207-3220 | on_remove_parent_link |
| `crates/worker/src/partition/state_machine/entries/mod.rs` | 345-365 | Command dispatch (Link is last) |
| `crates/worker/src/partition/state_machine/entries/set_state_command.rs` | 19-66 | SetState handler pattern |
| `crates/worker/src/partition/state_machine/entries/clear_all_state_command.rs` | 29-85 | ClearAllState with link cleanup |
| `crates/types/src/journal_v2/command.rs` | 47-70 | Command enum |
| `crates/service-protocol-v4/src/entry_codec.rs` | 394-406 | RemoveLinkCommand codec pattern |
