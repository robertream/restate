# Scope: Transparent Repartitioning for SSE State Router

*Generated: 2026-03-19*

## The Problem

When Restate repartitions (ownership of a partition transfers to a new replica set), SSE clients currently lose their connection. The partition store drops, the relay task detects channel close, `on_partition_closed` evicts the object state, and the next subscribe call starts cold. Clients must reconnect and re-receive the full Replace event.

This is observable and disruptive: the SSE connection breaks, and clients must handle reconnection themselves. For long-lived subscriptions during rolling updates or cluster rebalancing, this is a poor experience.

**Current state**: Repartitioning drops all active SSE subscriptions with no mechanism to transparently resume them.

**Impact**: Any client subscribed to virtual object state loses its connection during partition movement. No Restate-side replay or resubscription occurs.

## Target Users

**Primary**: SDK users and service authors using the SSE state endpoint (`GET /restate/objects/{service}/{key}/state`) who want live state streaming without handling reconnection.

**Secondary**: Restate operators triggering cluster rebalancing or rolling restarts — the experience degrades unless the SSE layer handles this transparently.

## Success Criteria

1. SSE clients survive a repartition without observable disconnection — no broken connection, no manual reconnect required.
2. Clients that are up-to-date (their `Last-Event-ID` matches the partition's current revision) receive no unnecessary Replace event.
3. Clients that are behind receive only the missing events (history replay) or a Replace if history doesn't cover the gap.
4. The pending subscription queue ensures no subscribe calls are dropped during the repartition window.
5. Existing `Subscribe` / `Unsubscribe` semantics are preserved for non-repartitioning paths.

## User Experience

**Happy path (no repartition)**: Same as today — Replace on subscribe, Patch/ClearAll events streamed. No change observable.

**During repartition**:
- New subscribe calls during the repartition window are queued (pending map) until the new partition registers.
- Existing active subscriptions receive a `Resubscribe` command sent to the new partition, which sends a Replace or confirms revision is current.
- The SSE connection stays open throughout. Clients see at most a Replace if they were behind; nothing if they were current.

**After repartition**: Normal streaming resumes through the new partition's relay task.

## Scope Boundaries

### ✅ In Scope

- **Two-map StateRouter**: Replace single `objects` map (with `initialized: bool`) with two maps:
  - `pending_subscriptions: HashMap<ServiceId, Vec<mpsc::Sender<Arc<StateChangeEvent>>>>` — clients awaiting first Replace
  - `active_subscriptions: HashMap<ServiceId, ObjectState>` — clients with current state (ObjectState loses `initialized` field)
- **`SubscriptionRequest::Resubscribe { service_id, revision }`**: New variant sent for existing active subscriptions when a partition re-registers. Partition store sends Replace only if its current revision differs from `revision`.
- **Pending queue drain per-partition**: When `add_partition()` is called, drain `pending_subscriptions` for service_ids on that partition and send `Subscribe` for each.
- **Promotion on Replace**: When a Replace event arrives for a service_id in `pending_subscriptions`, move those clients into `active_subscriptions` and fan out the Replace.
- **Coordinator task**: A task watching `replica_set_states` + `partition_table_version_watcher` that:
  1. Detects repartition (version change)
  2. Waits for Live view to be current (ArcSwap ordering guarantees this by the time the watcher fires)
  3. Issues `Resubscribe` for all active subscriptions on affected partitions
  4. Moves those ObjectStates to a transitioning state (or clears partition_senders so new subscribe calls go to pending)
  5. Shuts down old partition relay tasks (existing lifecycle: PartitionDb drop closes channel)
  6. New partitions register via `add_partition()`, draining pending queue

### ❌ Out of Scope

- Client-side reconnection behavior / SSE spec changes
- Re-ordering or deduplicating events across repartition boundary
- Support for multi-partition moves in a single atomic step (each partition transition is independent)
- Metrics or observability for repartition events
- Any changes to `StateChangeOperation` variants
- Unsubscribe during the repartition window (treated as best-effort)

### ⚠️ Maybe / Future

- Backpressure on the pending queue (currently unbounded per service_id)
- Timeout for pending subscriptions (if new partition never comes, clients wait indefinitely)
- `Resubscribe` optimization: partition compares revision before reading state, avoiding RocksDB read if up-to-date

## Architecture Decisions

### Two-map design eliminates `initialized: bool`

`initialized` exists solely to distinguish "waiting for first Replace" from "active". The two-map design makes this state structural:
- `pending_subscriptions` = not yet initialized
- `active_subscriptions` = initialized (ObjectState always has valid cached_state)

This is cleaner: ObjectState is only constructed when the first Replace arrives, so it is always valid.

### Promotion on Replace, not on Subscribe

Clients move from pending → active when the Replace event arrives, not when Subscribe is sent. This ensures ObjectState is only populated with real data. Multiple pending clients for the same service_id all receive the Replace and are promoted together.

### Pending queue drained per-partition on add_partition

When `add_partition(partition_id, cmd_tx)` is called, we look up all service_ids currently in `pending_subscriptions` whose partition (resolved via live metadata) matches `partition_id`, and send `Subscribe` for each. This avoids a global flush and correctly handles cases where only some partitions have changed.

### Resubscribe revision check in partition store

`handle_watch_command(Resubscribe { service_id, revision })` reads the current revision for `service_id` from the state table. If equal to `revision`, no event is emitted. If different, a Replace is sent. This is the only RocksDB read path; the router never reads state directly.

### ArcSwap ordering guarantees Live view is current

`update_task.rs:348-349` stores to ArcSwap before sending on the write watch channel. So when `partition_table_version_watcher.changed()` fires in the coordinator, `Metadata::with_current` already sees the new partition table. No additional wait needed.

### Coordinator owns the repartitioning window

The coordinator is the single task that knows when repartitioning is occurring. It transitions the router to "repartitioning mode" by clearing `partition_senders` (so new subscribe calls don't get sent to stale channels and instead accumulate in `pending_subscriptions`), then restores it as new partitions register. This serializes repartition handling without adding locks to the hot path.

## Constraints

- Must not break the non-repartitioning path — `Subscribe`/`Unsubscribe` semantics unchanged
- `StateRouter` must remain `Clone + Send + Sync` (it is `Arc<RwLock<...>>` fields)
- Coordinator watches `PartitionReplicaSetStates` — must integrate with existing `on_replica_set_state_changes` call site in `partition_processor_manager.rs`
- The pending queue must not hold the objects write lock while sending `Subscribe` commands (same lock ordering constraint as today)

## Integration Points

- **`crates/ingress-http/src/state_router.rs`**: Primary change — two-map design, `add_partition` drains pending queue, `handle_state_change` promotes pending clients on Replace
- **`crates/partition-store/src/partition_db.rs`** / **`partition_store.rs`**: Add `Resubscribe` variant handling in `handle_watch_command`
- **`crates/partition-store/src/lib.rs`**: Export `SubscriptionRequest::Resubscribe`
- **`crates/worker/src/partition_processor_manager.rs`**: Add coordinator task watching `replica_set_states` + `partition_table_version_watcher`
- **`crates/worker/src/partition_processor_manager/spawn_processor_task.rs`**: `add_partition` called when new partition starts — existing call site, no change needed
- **`crates/ingress-http/src/handler/objects.rs`**: No change needed — `subscribe()` API unchanged

## Risks

- **Pending queue starvation**: If a partition never comes back online, pending clients wait indefinitely. Acceptable for v1; timeout is a future concern.
- **Revision comparison correctness**: `Resubscribe { revision }` uses the router's last-known revision, which may be stale if events were lost during the repartition window. The conservative behavior (send Replace if revision doesn't match) is correct.
- **Lock contention during repartition**: The coordinator clears `partition_senders` under the write lock. All concurrent `subscribe()` calls will take the cold path into `pending_subscriptions`. This is brief and bounded.

## Next Steps

Complexity: **M** (medium) — isolated to 3 crates, clear data flow, no schema changes.

Suggested: `/spectre:plan docs/tasks/service-macro/concepts/scope.md` to produce implementation plan, then `/spectre:create_tasks` for task breakdown.
