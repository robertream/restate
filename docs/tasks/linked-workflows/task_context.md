# Task Context: Link GC + Bidirectional Link Records

## Feature
Add link lifecycle management: bidirectional link records (ParentOf on parent, ChildOf on child), purge guard (cannot purge child with parent links), cross-partition `RemoveParentLink` on purge/unlink, GC links on VO clear state. Cyclic graphs prevented by key collision.

## Architecture Decisions

### Bidirectional link records in a single table
Each CreateLink produces two records:
- **Parent's partition**: `(parent_service_id, child_service_id)` with `edge_label=ParentOf`
- **Child's partition**: `(child_service_id, parent_service_id)` with `edge_label=ChildOf`

Same table, same key prefix (`b"lk"`). Key is `(local_service_id, remote_service_id)`. The `edge_label: EdgeLabel` field distinguishes direction.

### Link struct
```rust
pub enum EdgeLabel {
    ParentOf,  // I am the parent of remote_service_id
    ChildOf,   // I am the child of remote_service_id
}

pub struct Link {
    pub local_service_id: ServiceId,
    pub remote_service_id: ServiceId,
    pub edge_label: EdgeLabel,
    pub state: LinkState,  // only meaningful for ParentOf
}
```

### Trait methods
```
ReadLinkTable:
  get_link(local, remote) → Option<Link>
  get_children_of(service_id) → Vec<Link>     // filters edge_label=ParentOf
  has_parents(service_id) → bool              // short-circuit on first ChildOf

WriteLinkTable:
  put_link(link) → ()
  delete_link(local, remote) → ()
  delete_all_links_for(service_id) → ()       // prefix delete all records
```

### Cycle prevention
Key collision prevents cycles. `(local, remote)` key can only have one record. CreateLink checks for existing record and rejects if one exists.

### Purge guard
`has_parents(service_id)` checks for ChildOf records. If any exist, purge is rejected.

### Cross-partition parent removal
`RemoveParentLink { child_service_id, parent_service_id }` sent on parent purge/unlink/clear state. Routes to child's partition. Handler deletes incoming ChildOf record.

## Files to Modify
- `crates/storage-api/src/link_table/mod.rs` — rename fields, add EdgeLabel, new trait methods
- `crates/storage-api/src/outbox_table/mod.rs` — add `RemoveParentLink`
- `crates/storage-api/proto/dev/restate/storage/v1/domain.proto` — update Link proto, add RemoveParentLink
- `crates/storage-api/src/protobuf_types.rs` — conversions
- `crates/partition-store/src/link_table/mod.rs` — update impls, add new methods
- `crates/partition-store/src/tests/link_table_test/mod.rs` — update tests
- `crates/wal-protocol/src/lib.rs` — add `Command::RemoveParentLink`
- `crates/worker/src/partition/types.rs` — outbox→command mapping
- `crates/worker/src/partition/state_machine/mod.rs` — handle RemoveParentLink, write incoming link on AttachInvocation with Link sink
- `crates/worker/src/partition/state_machine/lifecycle/purge.rs` — purge guard + link cleanup + RemoveParentLink
- `crates/worker/src/partition/state_machine/entries/create_link_command.rs` — cycle check + renamed fields
- `crates/worker/src/partition/state_machine/entries/remove_link_command.rs` — send RemoveParentLink
- `crates/worker/src/partition/state_machine/entries/clear_all_state_command.rs` — link cleanup
- `crates/worker/src/partition/state_machine/tests/linked_workflows.rs` — update existing + new GC tests
