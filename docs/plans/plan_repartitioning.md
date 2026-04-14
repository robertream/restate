# Implementation Plan: Transparent Repartitioning for SSE State Router

*Depth: comprehensive | Context: `task_context_repartitioning.md` | Scope: `concepts/scope.md`*

---

## Overview

SSE clients watching virtual object state currently lose their connection when partition ownership changes (repartitioning). This plan extends the existing `StateRouter` to keep SSE connections alive across partition transitions by introducing a `Subscription` enum (Pending/Active) in place of the single `objects` map, and issuing `Resubscribe` commands to newly registered partitions so active subscriptions resume without disruption.

---

## Current State

### `StateRouter` (`crates/ingress-http/src/state_router.rs`)

Single `objects: Arc<RwLock<HashMap<ServiceId, ObjectState>>>` map.

`ObjectState` fields: `partition_id`, `cached_state`, `revision`, `initialized`, `history`, `clients`, `idle_since: Option<Instant>`.

`on_partition_closed(partition_id)`: removes from `partition_senders` AND evicts all ObjectState entries with matching `partition_id`. Client connections are dropped.

`add_partition(partition_id, cmd_tx)`: adds to `partition_senders` only. No action on existing subscriptions.

### `SubscriptionRequest` (`crates/partition-store/src/partition_store.rs:156`)

```rust
pub enum SubscriptionRequest {
    Subscribe { service_id: ServiceId },
    Unsubscribe { service_id: ServiceId },
}
```

`handle_watch_command` at `partition_store.rs:361`: Subscribe reads full state + revision, emits Replace on `state_change_tx`. Unsubscribe removes from `subscribed_keys`.

### Repartitioning lifecycle

`on_replica_set_state_changes` (`partition_processor_manager.rs:1272`) → stops old processors → `PartitionDb` drops → relay task sees channel close → `router.on_partition_closed(partition_id)`. New processors start → `add_partition(partition_id, cmd_tx)` called inside `spawn_processor_task.rs:216`.

---

## Desired End State

### `StateRouter` — single-map enum design

```rust
enum Subscription {
    Pending(Vec<mpsc::Sender<Arc<StateChangeEvent>>>),
    Active(ObjectState),
}

pub struct StateRouter {
    objects:           Arc<RwLock<HashMap<ServiceId, Subscription>>>,
    partition_senders: Arc<RwLock<HashMap<PartitionId, mpsc::Sender<SubscriptionRequest>>>>,
}
```

`ObjectState` — simplified, no `partition_id`, no `initialized`, no `history`:

```rust
struct ObjectState {
    cached_state: HashMap<String, Bytes>,
    revision: u64,
    clients: Vec<mpsc::Sender<Arc<StateChangeEvent>>>,
    idle_since: Instant,  // passed in at construction; reset to caller-provided now when clients drain
}

impl ObjectState {
    fn new(cached_state: HashMap<String, Bytes>, revision: u64, now: Instant) -> Self { ... }
}
```

### Invariants

- `Subscription::Active` always has valid `cached_state` — only created when a `Replace` event arrives.
- `Subscription::Pending` holds clients waiting for their first Replace.
- Empty `Pending` entries (all clients disconnected) are removed immediately — no leaked entries.
- `idle_since` is only meaningful on `Active` with empty `clients`. Timer check: `clients.is_empty() && idle_since.elapsed() >= timeout`.

### Behavior changes

| Operation | Before | After |
|-----------|--------|-------|
| `on_partition_closed` | Evicts ObjectState; clients dropped | Removes from `partition_senders` only; subscriptions untouched |
| `add_partition` | Adds to `partition_senders` only | Sends `Resubscribe` for Active, `Subscribe` for Pending entries on that partition |
| Replace arrives | Sets `initialized = true`, fans out | Pending → Active promotion, fans out Replace to waiting clients |
| Partial reconnect replay | History ring buffer | Removed — Replace-or-skip only |

### New `SubscriptionRequest` variant

```rust
pub enum SubscriptionRequest {
    Subscribe { service_id: ServiceId },
    Unsubscribe { service_id: ServiceId },
    Resubscribe { service_id: ServiceId, revision: u64 },
}
```

`handle_watch_command(Resubscribe { service_id, revision })`:
1. Read current revision via `get_state_object_revision(service_id)`
2. If equal: return `Ok(())` — no event emitted, client is current
3. If differs: read full state, emit `Replace`, add to `subscribed_keys`

---

## Out of Scope

- History ring buffer / partial reconnect replay (deferred)
- Idle timeout configuration changes
- Metrics for repartition events

---

## System Architecture

```
SSE client connects
    │
    ▼
StateRouter::subscribe(service_id, last_event_id)
    │
    ├─ objects has Active(obj)?
    │     ├─ lei == revision → push client, idle_since reset
    │     └─ lei != revision → send Replace from cache, push client
    │
    ├─ objects has Pending(clients)?
    │     └─ push client (Subscribe already sent)
    │
    └─ cold path: insert Pending([client])
                  send Subscribe to partition sender

Partition store receives Subscribe
    └─ reads state, emits Replace on state_change_tx

Relay task receives Replace
    └─ StateRouter::handle_state_change(Replace)
          ├─ Pending entry? → promote to Active(ObjectState::new(...)), fan out Replace
          └─ Active entry? → apply patch, update revision, fan out

── Repartition ────────────────────────────────────────────────────

on_partition_closed(partition_id)
    └─ remove from partition_senders only
       (objects map untouched — clients keep open SSE connections)

add_partition(partition_id, cmd_tx)
    └─ add to partition_senders
       for each (service_id, sub) in objects:
           resolve partition via Metadata::with_current
           if matches partition_id:
               Active(obj)     → send Resubscribe { service_id, revision: obj.revision }
               Pending(_)      → send Subscribe { service_id }
       (sends happen outside objects lock)

── Timer / cleanup ─────────────────────────────────────────────────

on_timer()
    └─ iterate objects
       Active with clients non-empty → skip
       Active with clients empty, idle_since.elapsed() >= timeout → evict, send Unsubscribe
       Pending (all senders closed) → remove (already cleaned up at fan_out time)

── Client disconnect ────────────────────────────────────────────────

fan_out(clients, event, now):
    └─ try_send each client
       TrySendError::Full|Closed → drop sender
       if clients now empty:
           Active → idle_since = now
           Pending → remove entry from objects map
```

---

## API Design

### `StateRouter::subscribe` — simplified warm path

```
match objects.get_mut(service_id):
  Some(Active(obj)):
    idle_since = now
    lei == obj.revision → push client
    lei != obj.revision → try_send Replace from cache, push client

  Some(Pending(clients)):
    clients.push(client)   // Subscribe already in flight

  None:
    objects.insert(service_id, Pending(vec![client]))
    send Subscribe outside lock
```

---

## Implementation

Single phase. Both crates change together — `Resubscribe` variant and StateRouter refactor are shipped as one unit.

**Files**:
- `crates/partition-store/src/partition_store.rs` — add `Resubscribe` to enum and handler
- `crates/ingress-http/src/state_router.rs` — single-map enum refactor, all method rewrites

**Steps**:

1. Add `Resubscribe { service_id, revision }` to `SubscriptionRequest`; add arm to `handle_watch_command`
2. Replace `objects: HashMap<ServiceId, ObjectState>` with `HashMap<ServiceId, Subscription>`
3. Remove `partition_id`, `initialized`, `history` from `ObjectState`; change `idle_since: Option<Instant>` → `Instant`; add `ObjectState::new(cached_state, revision, now)`
4. Rewrite `subscribe()`: Active warm path → Replace-or-skip; Pending → push; cold → insert Pending + Subscribe
5. Rewrite `handle_state_change()`: Replace → promote Pending or apply to Active; Patch/ClearAll → Active only
6. Rewrite `on_partition_closed()`: remove from `partition_senders` only
7. Rewrite `add_partition()`: resolve partition per entry, send Resubscribe/Subscribe outside lock
8. Rewrite `on_timer()`: evict idle Active entries; Pending cleanup handled at fan_out time
9. Update `fan_out`: if empty after removals → Active sets `idle_since`, Pending removes entry from map

---

## Tests

**Happy path** — repartition round-trip (single test, multiple assertions):
subscribe (cold) → entry is Pending → Replace arrives → promoted to Active, Replace fanned out → `on_partition_closed` leaves Active untouched → `add_partition` sends Resubscribe → revision matches → no event → client stays connected

**Unhappy path** — Resubscribe revision mismatch:
active subscription, `add_partition` with changed partition state → Resubscribe sent → revision differs → Replace emitted, client receives updated state

---

## Critical Files

- `crates/ingress-http/src/state_router.rs` — primary change: enum refactor, all method rewrites
- `crates/partition-store/src/partition_store.rs` — `Resubscribe` variant + `handle_watch_command` arm
- `crates/partition-store/src/lib.rs` — verify `SubscriptionRequest` re-export
- `crates/worker/src/partition_processor_manager/spawn_processor_task.rs` — reference; `add_partition` call at line 216
- `crates/partition-store/src/tests/state_table_test/mod.rs` — reference for Resubscribe test pattern
