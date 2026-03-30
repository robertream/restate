# Task Context: Transparent Repartitioning for SSE State Router

*Scope doc: `docs/tasks/service-macro/concepts/scope.md`*

## Feature Description

When Restate repartitions (partition ownership transfers to a new replica set), SSE clients currently lose their connection because `on_partition_closed` evicts all ObjectState for the affected partition. The goal is to keep SSE connections alive across repartitioning by queuing clients in a pending map during the transition and promoting them when the new partition comes online.

---

## Architecture Patterns

### Current StateRouter structure (`crates/ingress-http/src/state_router.rs`)

```rust
pub struct StateRouter {
    objects: Arc<RwLock<HashMap<ServiceId, ObjectState>>>,
    partition_senders: Arc<RwLock<HashMap<PartitionId, mpsc::Sender<SubscriptionRequest>>>>,
}
```

`ObjectState` has:
- `partition_id: PartitionId` — for matching on `on_partition_closed`
- `cached_state: HashMap<String, Bytes>`
- `revision: u64`
- `initialized: bool` — distinguishes "waiting for first Replace" from "active"
- `history: VecDeque<Arc<StateChangeEvent>>` — 64-event ring buffer
- `clients: Vec<mpsc::Sender<Arc<StateChangeEvent>>>`
- `idle_since: Option<Instant>`

### Current SubscriptionRequest (`crates/partition-store/src/partition_store.rs:156`)

```rust
pub enum SubscriptionRequest {
    Subscribe { service_id: ServiceId },
    Unsubscribe { service_id: ServiceId },
}
```

### Current handle_watch_command (`partition_store.rs:361`)

`Subscribe`: reads all user states + revision via RocksDB, emits `StateChangeEvent { operation: Replace { state }, revision }` on `state_change_tx`. Adds to `subscribed_keys`.

`Unsubscribe`: removes from `subscribed_keys`.

### Repartitioning detection chain

1. `PartitionReplicaSetStates::changed()` fires (`partition_processor_manager.rs:372`)
2. OR `partition_table_version_watcher.changed()` fires (`partition_processor_manager.rs:349`)
3. Both call `self.on_replica_set_state_changes(&replica_set_states)` (`partition_processor_manager.rs:1272`)
4. That stops processors for removed partitions → processor task ends → `PartitionDb` drops → `state_change_tx` drops → relay task sees `None` → `router.on_partition_closed(partition_id)` is called
5. New processors start → `add_partition(partition_id, cmd_tx)` called in `spawn_processor_task.rs:216`

### ArcSwap ordering guarantee (`crates/core/src/metadata/update_task.rs:348-349`)

```rust
self.item.store(new_value);                        // ArcSwap stored first
self.write_watch.send_replace(maybe_new_version);  // watch fires after
```

`Metadata::with_current` is always current when `partition_table_version_watcher.changed()` fires.

### Current `on_partition_closed` behavior

Removes partition from `partition_senders` AND evicts all ObjectState for that partition from `objects`. Next subscriber for that service_id takes the cold path (reconnects to new partition).

---

## Implementation Approaches

### Option A — Subscribe-only two-map (simplest)

Replace single `objects` map with:
```rust
pending_subscriptions: Arc<RwLock<HashMap<ServiceId, Vec<mpsc::Sender<Arc<StateChangeEvent>>>>>>,
active_subscriptions:  Arc<RwLock<HashMap<ServiceId, ObjectState>>>,
```

`ObjectState` loses `initialized: bool` — it is only constructed when a Replace event arrives.

**Lifecycle**:
- `subscribe()`: if service_id in `active_subscriptions` → warm path (replay/send Replace). Else → add client to `pending_subscriptions`, send `Subscribe` to partition if not already pending.
- `on_partition_closed(partition_id)`: remove from `partition_senders`. For active service_ids with this partition_id: move client senders to `pending_subscriptions` (ObjectState evicted).
- `add_partition(partition_id, cmd_tx)`: add to `partition_senders`. Drain `pending_subscriptions` for service_ids on this partition — send `Subscribe` for each.
- `handle_state_change(event)` on Replace: if service_id in `pending_subscriptions` → move clients to new ObjectState in `active_subscriptions`, fan out Replace to all.

**Trade-off**: All repartitioned clients get a full Replace event even if they were up-to-date. Simple; no partition store changes needed beyond new map shape.

### Option B — Revision-aware Resubscribe (scope design)

Extends Option A: pending map stores `(last_revision: Option<u64>, clients)`.

Adds `SubscriptionRequest::Resubscribe { service_id, revision }`. Partition store's `handle_watch_command` checks current revision for service_id — if equal, emits nothing; if different, emits Replace.

**Lifecycle change from Option A**:
- `on_partition_closed`: preserve `obj.revision` in pending entry.
- `add_partition`: for pending entries with `Some(revision)`, send `Resubscribe { service_id, revision }` instead of `Subscribe`.

**Trade-off**: Up-to-date clients receive no Replace event (zero-overhead reconnect). Requires one RocksDB read per service_id in `handle_watch_command` to check revision. Small additional complexity.

### Option C — Subscribe-only, no structural change to pending (minimal MVP)

Keep the current single `objects` map with `initialized: bool`. Only change: `on_partition_closed` moves clients from evicted ObjectState to a new ObjectState with `initialized: false` (acting as the pending state) rather than dropping them.

Subscribe sends are still issued for cold service_ids. When Replace arrives, `initialized` flips to `true`.

**Trade-off**: Keeps the `initialized` flag (less clean) but minimizes refactor surface. The two-map design is cleaner but Option C ships sooner.

---

## Key Files

| File | Change |
|------|--------|
| `crates/ingress-http/src/state_router.rs` | Two-map design (Options A/B) or initialized-flag approach (Option C) |
| `crates/partition-store/src/partition_store.rs` | Add `SubscriptionRequest::Resubscribe` + handler (Option B only) |
| `crates/ingress-http/src/handler/objects.rs` | No change — `subscribe()` API unchanged |
| `crates/worker/src/partition_processor_manager/spawn_processor_task.rs` | No change — `add_partition()` already called |
| `crates/worker/src/partition_processor_manager.rs` | No change — existing lifecycle hooks handle orchestration |
| `crates/partition-store/src/lib.rs` | Re-export `SubscriptionRequest::Resubscribe` (Option B only) |

---

## Impact Summary

| Area | Impact |
|------|--------|
| SSE hot path (handle_state_change) | Map lookup changes; Replace triggers promotion |
| subscribe() | Checks two maps instead of one |
| on_partition_closed | Moves clients to pending instead of dropping |
| add_partition | Drains pending, issues Subscribe/Resubscribe per service_id |
| Partition store hot path | Unchanged (Option A). One revision check added (Option B) |
| Partition processor lifecycle | No change — existing spawn/stop hooks used |
