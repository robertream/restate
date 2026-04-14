# Task Context: Evolving the Linked Entity/Invocation Model

## Feature Description

Evolve the current linked workflows implementation into a unified linking model that supports:
- **Entity linking** (workflow↔object) — structural relationships between keyed services
- **Invocation linking** — tracking which invocations contributed to an entity's state
- **Fact accumulation** — workflows accrue facts per invocation, objects manage mutable state that transitions to immutable facts on resolve
- **Live observation** — the linked graph enables both live and retrospective analysis of application state
- **GC by reachability** — live ancestors anchor the tree; nothing is GCed while reachable from a live entity

## Architecture Patterns

### Current Link Table Model

Links are stored as directed edges in a dedicated RocksDB column family, keyed by `ServiceId` (not `InvocationId`):

```
LinkKey(partition_key, local_service_name, local_service_key, edge_label_byte, remote_service_name, remote_service_key)
```

- `edge_label_byte`: `0x00` = ParentOf (parent's partition), `0x01` = ChildOf (child's partition)
- `LinkRelation::ParentOf(ChildStatus)` where `ChildStatus` = `Running(Option<CompletionHandlerName>)` | `Completed(ResponseResult)`
- `LinkRelation::ChildOf { resolved: bool }` — resolved flag gates GC cascade

**Key fact**: Links are entity-to-entity (ServiceId), not invocation-to-invocation. There is no `InvocationId` in the link table schema.

### Current Invocation Metadata

`InFlightInvocationMetadata.response_sinks: HashSet<ServiceInvocationResponseSink>` — the `Link` variant carries `owner_service_id`, `caller_invocation_id`, `caller_completion_id`. This is the only record connecting a child invocation to its parent entity.

`Source::Service(InvocationId, InvocationTarget)` tracks the call-graph source but is separate from the link graph.

### Journal Storage

Journal is per-invocation, keyed by `(invocation_id, journal_index)`. Journal v2 entries have `record_created_at` timestamps. Journals are deleted at invocation completion unless `journal_retention_duration > 0`.

**Critical gap**: Journal data is ephemeral by default. For a "fact tree" model, journal entries (or their results) would need to survive invocation completion when linked to a live ancestor.

### Completion/Purge Lifecycle

- `InvocationStatus::Completing` — workflow handler done, waiting for linked children
- Only workflows enter Completing; VOs complete immediately
- Purge checks `get_first_parent` — blocks if parent link exists
- GC cascade: `on_remove_parent_link` → if `resolved: true`, deletes state/promises/links and propagates recursively

### Observation (DataFusion)

`storage-query-datafusion` provides SQL-queryable tables via 4-file pattern (schema.rs, row.rs, table.rs, mod.rs). Could expose a `sys_link` table for graph queries.

### WAL Command Dispatch

New command types require: WAL protocol variant + `record_keys()` arm + dispatch in `on_apply()` + handler method.

### Cross-Partition Communication

OutboxMessage enum handles all cross-partition messages. Existing link messages: `LinkCompletionNotification`, `RemoveParentLink`, `LinkedNotification`.

## Dependencies

### Existing Implementation (promise-objects branch)
- `LinkCommand` / `RemoveLinkCommand` / `ResolveCommand` — all wired through proto, codec, protocol runner, state machine
- `ChildOf { resolved: bool }` — stored, read during GC
- State mutation rejection after resolve (SetState/ClearState/ClearAllState)
- GC cascade on RemoveParentLink
- 19 passing tests

### Bugs to Fix First
- `ServiceInvocationResponseSink::Link` sends duplicate `LinkCompletionNotification` when VO resolves then handler returns
- This is addressed by the `LinkChild` outbox message plan (promise_objects_server_plan.md) which removes the Link sink entirely

## Implementation Approaches

### Approach A: Entity Links + Invocation Metadata (Minimal)

Keep entity links (ServiceId-keyed) as they are. Track invocation-to-entity relationships through metadata, not the link table.

- Fix the duplicate notification bug via `LinkChild` outbox message
- Object linking (empty handler_name) creates ChildOf without starting an invocation
- Invocation contributions tracked implicitly via `Source` field on invocation metadata
- Journal retention controlled by service-level config, not link ancestry
- Observation: `sys_link` DataFusion table for entity graph; `sys_invocation_status` for invocation history

**Trade-off**: No explicit invocation graph. Facts are service-level (state + promises), not invocation-level (journal). Simpler but less expressive for retrospective analysis.

### Approach B: Dual-Layer Graph (Entity + Invocation Edges)

Add invocation-level edges alongside entity edges. The link table gains a second edge type.

- Entity links: `ServiceId → ServiceId` (as today) — structural, long-lived
- Invocation links: `InvocationId → ServiceId` — records "this invocation contributed to this entity"
- When a handler runs on a linked object, an invocation edge is automatically created
- Journal retention tied to invocation edge reachability — if an invocation edge connects to a live entity, the journal is retained
- GC: entity unreachable → invocation edges deleted → journals purged

**Trade-off**: More expressive but adds a second key schema to the link table. Need to handle invocation edges at write time (every handler invocation on a linked entity creates an edge).

### Approach C: Fact-Anchored Graph (Objects as Scope Anchors)

Objects are the anchors. Workflows and their invocations produce facts that are scoped to their parent object.

- Object state = live mutable data
- Resolved object state = immutable facts (frozen snapshot)
- Workflow invocation journal = per-invocation facts (retained while parent object is live)
- Object → child object links form the structural graph
- Object → workflow links scope the workflow's facts to the object
- Workflow invocation → journal entries are automatically retained while the owning object is live
- When an object resolves: state freezes, all scoped facts (own state + children's facts + workflow journals) become immutable
- When the parent removes the link: the entire resolved subtree is GCed (state + facts + journals)

**Trade-off**: Most expressive. Objects become the natural scope boundaries for fact retention. But requires journal retention to be driven by link ancestry rather than per-service config, which is a deeper change.

## Impact Summary

| Concern | Approach A | Approach B | Approach C |
|---------|-----------|-----------|-----------|
| Files impacted | 6-8 | 10-15 | 15-20 |
| Data model changes | Modify existing | New edge type | New retention model |
| Observation | Entity graph only | Entity + invocation graph | Full fact graph |
| GC complexity | Current + fix | Two-layer reachability | Ancestry-driven retention |
| Journal handling | Unchanged | Retention by edge | Retention by scope |
| Breaking changes | None | Link table schema | Retention semantics |

## External Research

The model draws from:
- **Erlang/OTP process trees** — supervision and lifecycle propagation (adapted to durable execution)
- **Event sourcing** — journal entries as facts, with the link graph defining aggregate boundaries
- **CQRS projections** — the observation layer (DataFusion) as a read model over the fact graph
- **Reference counting GC** — live ancestor reachability as the GC root determination

Key insight from the future_design_ideas doc: OTP supervision doesn't map well because Restate processes replay from journal. But the structural linking (process trees as organizational units) maps very well.
