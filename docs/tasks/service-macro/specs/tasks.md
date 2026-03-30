# Tasks — Transparent Repartitioning for SSE State Router
*Generated: 2026-03-19 | Plan: `specs/plan_repartitioning.md`*

## Objective
Extend `StateRouter` to keep SSE client connections alive across partition ownership changes by introducing a `Subscription` enum (Pending/Active), issuing `Resubscribe` to newly registered partitions, and cleaning up empty pending entries automatically.

## Scope
- **In Scope**: `Subscription` enum replacing single `objects` map; `Resubscribe` SubscriptionRequest variant; `on_partition_closed` leaving subscriptions untouched; `add_partition` sending Resubscribe/Subscribe per entry; empty Pending cleanup; idle timeout on Active entries
- **Out of Scope**: History ring buffer, partial reconnect replay, idle timeout configuration, repartition metrics

## Requirements Traced
| ID | Description | Source | Tasks |
|----|-------------|--------|-------|
| REQ-001 | SSE clients survive repartition without disconnection | scope.md | 1.1, 1.2, 1.3 |
| REQ-002 | Up-to-date clients receive no Replace on repartition | plan | 1.1, 2.1 |
| REQ-003 | New subscribe calls during repartition window are queued | plan | 1.2 |
| REQ-004 | Empty pending entries are removed immediately | plan_review | 1.3 |
| REQ-005 | Idle Active subscriptions are evicted after timeout | scope.md | 1.4 |
| REQ-006 | `Resubscribe` variant in partition store emits nothing on revision match | plan | 2.1 |

---

## Architecture Context

### Where This Fits
- `StateRouter` lives in `crates/ingress-http/src/state_router.rs` — sits between partition store relay task and SSE HTTP handler
- `SubscriptionRequest` enum lives in `crates/partition-store/src/partition_store.rs:156` — consumed by the per-partition subscription task spawned in `spawn_processor_task.rs:196-212`
- `add_partition` is already called from `spawn_processor_task.rs:216` when a new partition starts — no change to call site needed
- `on_partition_closed` is called from the relay task when the partition's channel closes — no change to call site needed

### Technical Approach
- Replace `objects: Arc<RwLock<HashMap<ServiceId, ObjectState>>>` with `objects: Arc<RwLock<HashMap<ServiceId, Subscription>>>` where `Subscription` is `Pending(Vec<Sender>)` or `Active(ObjectState)`
- Single map means one lock, no lock ordering issues between pending and active
- `ObjectState` simplified: remove `partition_id`, `initialized`, `history`; change `idle_since: Option<Instant>` to plain `Instant` passed in at construction
- `Resubscribe` is sent only during `add_partition` (repartition), never during initial `subscribe()`

### Key Decisions
- Single map with enum preferred over two maps — eliminates lock ordering complexity
- `on_partition_closed` removes from `partition_senders` only — active subscriptions are untouched, clients keep open connections during repartition window
- `ObjectState::new(cached_state, revision, now: Instant)` takes `now` as parameter for testability
- Empty `Pending` entries removed immediately in fan_out equivalent — no timer cleanup needed for pending
- `Resubscribe` revision check: if current == provided, emit nothing; if differs, emit Replace and add to `subscribed_keys`

---

## Tasks

### Phase 1: StateRouter — Single-Map Enum Refactor

#### [1.1] Replace `objects` map with `Subscription` enum
- [ ] **1.1.1** Define `Subscription` enum with `Pending` and `Active` variants in `state_router.rs`
  - **Produces**: `enum Subscription { Pending(Vec<Sender>), Active(ObjectState) }` replacing `initialized: bool` pattern
  - **Consumed by**: all StateRouter methods via pattern match
  - **Replaces**: single `objects: HashMap<ServiceId, ObjectState>` with `initialized` flag
  - [ ] `Subscription::Pending` holds `Vec<mpsc::Sender<Arc<StateChangeEvent>>>`
  - [ ] `Subscription::Active` holds `ObjectState` (always valid cached_state by construction)
  - [ ] `objects` field type updated to `Arc<RwLock<HashMap<ServiceId, Subscription>>>`

- [ ] **1.1.2** Simplify `ObjectState` — remove `partition_id`, `initialized`, `history`; change `idle_since` to plain `Instant`
  - **Produces**: `ObjectState { cached_state, revision, clients, idle_since: Instant }` with `ObjectState::new(cached_state, revision, now: Instant)`
  - **Consumed by**: `Subscription::Active` variant; all ObjectState access sites
  - **Replaces**: `ObjectState` with `partition_id: PartitionId`, `initialized: bool`, `history: VecDeque<Arc<StateChangeEvent>>`, `idle_since: Option<Instant>`
  - [ ] `ObjectState::new` takes `now: Instant` — no `Instant::now()` calls inside ObjectState
  - [ ] `idle_since` is plain `Instant`, not `Option<Instant>` — timer check is `clients.is_empty() && idle_since.elapsed() >= timeout`
  - [ ] All fields that had `Option`-unwrap logic around `initialized` or `idle_since` simplified

#### [1.2] Rewrite `subscribe()` for Pending/Active two-state logic
- [ ] **1.2.1** Implement warm path: `Active` entry found — send Replace if `lei != revision`, push client
  - **Produces**: client added to `Active(obj).clients`; Replace sent from `obj.cached_state` if needed
  - **Consumed by**: SSE handler `handle_object_state` in `objects.rs` (rx end of mpsc)
  - **Replaces**: warm path that checked `obj.initialized` before deciding to send Replace
  - [ ] `lei == obj.revision` → push client, reset `idle_since` to `now`, no Replace sent
  - [ ] `lei != obj.revision` (or None) → try_send Replace from cached_state, push client
  - [ ] Lock released before any channel sends

- [ ] **1.2.2** Implement cold path: no entry or `Pending` — add client to pending, send `Subscribe` if new
  - **Produces**: client added to `Pending` entry; `Subscribe` sent to partition sender if entry newly created
  - **Consumed by**: partition store subscription task via `SubscriptionRequest::Subscribe`
  - **Replaces**: cold path that inserted `ObjectState` with `initialized: false`
  - [ ] `Pending` entry already exists → push client only (Subscribe already in flight)
  - [ ] No entry → insert `Pending([client])`, send `Subscribe` outside lock
  - [ ] Partition sender missing (repartition window) → client queued in Pending, no Subscribe sent until `add_partition`

#### [1.3] Rewrite `handle_state_change()` — Replace promotes Pending to Active
- [ ] **1.3.1** On `Replace` event: promote `Pending` entry to `Active`, fan out to all waiting clients
  - **Produces**: `Active(ObjectState::new(state, revision, now))` with all pending clients moved in
  - **Consumed by**: all pending client mpsc receivers (open SSE connections)
  - **Replaces**: logic that set `obj.initialized = true` and cleared history on Replace
  - [ ] Pending entry found → create ObjectState, move all pending senders into `obj.clients`, insert as `Active`, fan out Replace to all
  - [ ] Active entry found → apply Replace to `cached_state`, update `revision`, fan out (normal re-snapshot path)
  - [ ] Empty Pending (all senders disconnected) → entry removed, no ObjectState created

- [ ] **1.3.2** On `Patch`/`ClearAll` event: apply to `Active` only; remove empty `Active` entries after fan_out
  - **Produces**: updated `cached_state` and `revision` in Active entry; idle_since set if clients empty
  - **Consumed by**: active client mpsc receivers
  - **Replaces**: logic that also pushed to `history` ring buffer
  - [ ] Patch/ClearAll ignored if entry is Pending (no cached_state to apply to)
  - [ ] After fan_out: if `clients.is_empty()` → `idle_since = now`
  - [ ] Dropped clients (Full/Closed) removed from `clients` vec

#### [1.4] Rewrite `on_partition_closed()` — remove from `partition_senders` only
- [ ] **1.4.1** Remove eviction logic; only update `partition_senders`
  - **Produces**: `partition_senders` entry removed for `partition_id`
  - **Consumed by**: relay task lifecycle (called when partition channel closes)
  - **Replaces**: logic that evicted all ObjectState entries matching `partition_id`
  - [ ] Only `partition_senders.write().await.remove(&partition_id)` — no `objects` mutation
  - [ ] Active subscriptions for service_ids on this partition remain untouched in `objects`
  - [ ] New `subscribe()` calls during the gap go to Pending (no partition sender → cold path)

#### [1.5] Rewrite `add_partition()` — send Resubscribe/Subscribe per entry
- [ ] **1.5.1** After inserting into `partition_senders`, scan `objects` and issue commands for matching service_ids
  - **Produces**: `Resubscribe` sent for each Active entry on `partition_id`; `Subscribe` sent for each Pending entry
  - **Consumed by**: partition store subscription task via `handle_watch_command`
  - **Replaces**: `add_partition` that only inserted into `partition_senders` with no subscription recovery
  - [ ] Collect matching service_ids (Active + Pending) while holding `objects` read lock; release lock before any sends
  - [ ] `Active(obj)` → send `Resubscribe { service_id, revision: obj.revision }` on new `cmd_tx`
  - [ ] `Pending(_)` → send `Subscribe { service_id }` on new `cmd_tx`
  - [ ] Partition resolution via `Metadata::with_current(|m| m.partition_table_ref().find_partition_id(service_id.partition_key()))`

#### [1.6] Rewrite `on_timer()` — evict idle Active entries, skip Pending
- [ ] **1.6.1** Iterate `objects`, evict `Active` entries past idle timeout; ignore `Pending`
  - **Produces**: evicted service_ids removed from `objects`; `Unsubscribe` sent for each
  - **Consumed by**: partition store subscription task
  - **Replaces**: timer that iterated single `objects` map checking `idle_since.is_some_and(...)`
  - [ ] Skip any entry where `clients` is non-empty
  - [ ] Skip `Pending` entries entirely (no idle timeout for pending)
  - [ ] `Active` with `clients.is_empty() && idle_since.elapsed() >= SSE_SUBSCRIPTION_IDLE_TIMEOUT` → collect for eviction
  - [ ] Send `Unsubscribe` for each evicted entry outside the write lock

---

### Phase 2: Partition Store — `Resubscribe` Variant

#### [2.1] Add `Resubscribe` to `SubscriptionRequest` and handle in `handle_watch_command`
- [ ] **2.1.1** Add `Resubscribe { service_id: ServiceId, revision: u64 }` variant to `SubscriptionRequest` enum
  - **Produces**: new enum variant accessible to StateRouter via existing re-export
  - **Consumed by**: `handle_watch_command` in `partition_store.rs`; `add_partition` in `state_router.rs`
  - **Replaces**: N/A — new variant
  - [ ] Variant added at `partition_store.rs:156` alongside existing Subscribe/Unsubscribe
  - [ ] `SubscriptionRequest` re-export in `lib.rs` unchanged (enum re-exported as a whole)

- [ ] **2.1.2** Implement `Resubscribe` arm in `handle_watch_command`
  - **Produces**: emits Replace if revision differs; emits nothing if revision matches; adds to `subscribed_keys` in both cases
  - **Consumed by**: relay task's `state_change_tx` → `handle_state_change` in StateRouter
  - **Replaces**: N/A — new code path
  - [ ] Read current revision via `get_state_object_revision(&service_id)` — returns None if no state
  - [ ] `current_revision == provided_revision` → add to `subscribed_keys`, return `Ok(())`
  - [ ] `current_revision != provided_revision` → read full state, emit Replace on `state_change_tx`, add to `subscribed_keys`
  - [ ] Handles `None` revision (service_id has no state) as mismatch — emits Replace with empty state

---

### Phase 3: Tests

#### [3.1] Happy path — full repartition round-trip
- [ ] **3.1.1** Write single test covering: cold subscribe → Pending → Replace → Active promotion → `on_partition_closed` leaves Active → `add_partition` sends Resubscribe → revision matches → no Replace → client still connected
  - **Produces**: test in `state_router.rs` `#[cfg(test)]` block
  - **Consumed by**: `cargo nextest run -p restate-ingress-http`
  - **Replaces**: existing StateRouter tests that tested `initialized` flag behavior
  - [ ] Test asserts client receiver is still open after `on_partition_closed`
  - [ ] Test asserts no Replace sent when Resubscribe revision matches
  - [ ] Multiple assertions in single test per CLAUDE.md guidance

#### [3.2] Unhappy path — Resubscribe revision mismatch
- [ ] **3.2.1** Write test: active subscription, `add_partition` with changed state → Resubscribe sent → revision differs → Replace emitted, client receives updated state
  - **Produces**: test covering partition store `handle_watch_command(Resubscribe {...})`
  - **Consumed by**: `cargo nextest run -p restate-partition-store`
  - [ ] State changed between old partition and new partition
  - [ ] Replace event received by client with new state
  - [ ] Client revision updated after Replace

---

## Execution Strategies

### Sequential Execution
1. **2.1** — Add `Resubscribe` variant (no dependencies; unlocks 1.5)
2. **1.1** — `Subscription` enum + `ObjectState` simplification (foundation for all StateRouter tasks)
3. **1.2** — Rewrite `subscribe()`
4. **1.3** — Rewrite `handle_state_change()`
5. **1.4** — Rewrite `on_partition_closed()`
6. **1.5** — Rewrite `add_partition()` (depends on 2.1 for Resubscribe)
7. **1.6** — Rewrite `on_timer()`
8. **3.1, 3.2** — Tests (after all implementation complete)

### Parallel Execution

**Wave 1 (concurrent)**: 2.1, 1.1
- Rationale: partition store change and ObjectState/enum definition are independent; both are pure additions with no cross-dependency at definition time

**Wave 2 (after Wave 1)**: 1.2, 1.3, 1.4, 1.6
- Rationale: all StateRouter method rewrites depend on the Subscription enum from 1.1; they modify disjoint methods

**Wave 3 (after Wave 2)**: 1.5
- Rationale: `add_partition` depends on Resubscribe variant (2.1) and all existing entries being in the new enum shape

**Wave 4 (after Wave 3)**: 3.1, 3.2
- Rationale: tests require complete implementation

---

## Coverage Summary
- Total Requirements Extracted: 6
- Requirements with Task Coverage: 6 (100%)
- Phases: 3
- Parent Tasks: 7
- Sub-tasks: 13
