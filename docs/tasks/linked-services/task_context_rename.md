# Task Context: Link Naming Convention Alignment

## Goal

Ensure all link-related code uses consistent `link_to`/`link_from` (commands/actions) or `linked_to`/`linked_from` (state/facts) terminology. Replace `parent`/`child`/`local`/`remote` semantics in VO/WI linking code.

## Naming Convention

| Context | Direction toward target | Direction toward initiator |
|---|---|---|
| **Link being created** (imperative, in-progress) | `link_to` | `link_from` |
| **Link already established** (fact, state, post-creation) | `linked_to` | `linked_from` |

The tense depends on whether the link exists at the point the identifier is used:
- `LinkRequest` creates a link → `link_to` / `link_from`
- `LinkResponse` confirms creation → `linked_to` / `linked_from` (link now exists)
- `UnlinkRequest` removes an existing link → `linked_to` / `linked_from` (link exists until removed)
- `LinkCompletionNotification` reports existing link's completion → `linked_to` / `linked_from`
- Stored sinks, edge state, counters → `linked_to` / `linked_from`

Also applies to local variables: use `linked_*` when the link is established, `link_*` when creating.

## Current State Audit

### Already Correct

These identifiers already follow the convention:
- `LinkServiceCommand.link_to` — command field (imperative action)
- `UnlinkServiceCommand.unlink_from` — command field (imperative action)
- `ServiceInvocation.link_from` — command field (piggybacked on invocation)
- `EdgeState::LinkedTo`, `EdgeLabel::LinkedTo` — state (past-tense fact)
- `linked_to_count`, `linked_from_count` — state counters on VOS and IFIM
- `get_service_linked_to`, `get_invocation_linked_to` — state scan methods
- All `on_link_*` / `on_unlink_*` handler functions — imperative actions
- All `Command::LinkRequest` / `LinkResponse` / etc. WAL and outbox variants

### Violations: Struct Fields (Public API)

No suffixes needed — Rust types (`EntityId`, `ServiceId`, `InvocationId`) carry the "what" — field names just need the role in the link graph.

| Current Name | Type | Proposed Name | Rationale |
|---|---|---|---|
| `LinkRequest.local` | `EntityId` | `link_to` | Creating link: target entity |
| `LinkRequest.parent` | `EntityId` | `link_from` | Creating link: initiator entity |
| `LinkResponse.local` | `EntityId` | `linked_from` | Link established: initiator |
| `LinkResponse.remote` | `EntityId` | `linked_to` | Link established: target |
| `UnlinkRequest.local` | `EntityId` | `linked_to` | Link exists: the entity that was linked to |
| `UnlinkRequest.parent` | `EntityId` | `linked_from` | Link exists: the entity that linked |
| `UnlinkResponse.local` | `EntityId` | `linked_from` | Link existed: initiator |
| `LinkCompletionNotification.local` | `EntityId` | `linked_from` | Link exists: initiator being notified |
| `LinkCompletionNotification.remote` | `EntityId` | `linked_to` | Link exists: target that completed |
| `ServiceLinkNotification { parent_service_id }` | `ServiceId` | `{ linked_from }` | Type carries "service" |
| `InvocationLinkNotification { parent_invocation_id }` | `InvocationId` | `{ linked_from }` | Type carries "invocation" |

### Violations: Internal Function Names

| Current Name | Location | Proposed Name |
|---|---|---|
| `retain_non_parent_sinks` | `worker/state_machine/mod.rs:327` | `retain_non_linked_from_sinks` |

### Violations: Local Variables

All `parent_*` and `child_*` local variables — rename to `link_from` / `link_to` (creating) or `linked_from` / `linked_to` (established). No suffixes unless two link-related values of different types share scope.

- `crates/worker/src/partition/state_machine/entries/link_service_command.rs` — creating: `link_from` / `link_to`
- `crates/worker/src/partition/state_machine/entries/unlink_service_command.rs` — established: `linked_from` / `linked_to`
- `crates/worker/src/partition/state_machine/mod.rs` — tense depends on handler context

### Proto Fields

Proto field **numbers** are unchanged (wire compatibility preserved). Only Rust-side struct field names change. The proto→Rust mapping in `protobuf_types.rs` must be updated:
- `outbox_message::LinkRequest { parent: ... }` → `{ link_from: ... }`
- `outbox_message::UnlinkRequest { parent: ... }` → `{ link_from: ... }`
- Response sink proto mappings for ServiceLinkNotification/InvocationLinkNotification

## Files Impacted

1. `crates/types/src/invocation/mod.rs` — struct field renames
2. `crates/storage-api/src/protobuf_types.rs` — proto↔Rust field mapping
3. `crates/worker/src/partition/state_machine/mod.rs` — handler code, function rename
4. `crates/worker/src/partition/state_machine/entries/link_service_command.rs` — local vars
5. `crates/worker/src/partition/state_machine/entries/unlink_service_command.rs` — local vars
6. `crates/worker/src/partition/state_machine/tests/linked_services.rs` — test code
7. `crates/storage-api/proto/dev/restate/storage/v1/domain.proto` — proto field name changes (optional, cosmetic)
8. `crates/wal-protocol/src/lib.rs` — if any field access

## Complexity Assessment

- Files impacted: ~8 (Med)
- Pattern match: Mechanical rename (Low)
- Components crossed: types, storage-api, worker (Med)
- Data model changes: Field names only, no field numbers (Low)
- No hard stops

**Tier: STANDARD**
