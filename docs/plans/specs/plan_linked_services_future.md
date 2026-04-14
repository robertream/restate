# Future Considerations: Linked Services

Items explicitly deferred from the initial linked-services implementation. None block the current PR.

## 1. AttachServiceCommand Confirmation

`AttachServiceCommand` currently has no confirmation step — it sends an `AttachServiceRequest` cross-partition and the caller blocks until the VO completes. If the target VO doesn't exist or is in an unexpected state, the caller hangs forever with no error.

In practice this is safe because `AttachServiceCommand` is only called on handles returned from `LinkServiceCommand`/`StartLinkedCommand`, so the VO is guaranteed to exist. But a malformed `ServiceId` would cause a silent hang.

**Fix**: Add an `AttachServiceResponse` cross-partition message (similar to `LinkResponse`) that confirms the sink was registered or reports failure. This changes the completion from single-step to two-step and is a protocol-level change.

## 2. GetLinkCommand

SDK-visible access to the link graph from within a handler. Currently there's no way for a running handler to query which links exist or their status (Active/Completed). A `GetLinkCommand` journal entry would let handlers inspect their own link graph — useful for conditional logic based on child completion status.

**Open questions:**
- Query by label? By entity type? Return all edges?
- Should this be a point read (specific linked_to entity) or a scan (all linked_to edges)?
- Does the SDK need a streaming/pagination model for large link sets?

## 2. Orphan GC Policy

When all linked_from entities unlink or are purged, the linked_to entity may have `linked_from_count == 0` with no remaining references. Currently there's no automatic cleanup — the entity persists until explicitly completed or purged.

**Options:**
- Do nothing (current) — linked_to entities are independent; links are observational, not ownership
- Reference-counted cleanup — when `linked_from_count` drops to 0, trigger cancellation or completion
- TTL-based — orphaned entities get a configurable grace period before cleanup

**Consideration:** The semantics depend on whether links imply ownership. Currently they don't — a VO can exist independently of who linked to it.

## 3. Graph Traversal API

External API (admin/CLI/dashboard) for inspecting the link graph. Useful for debugging, observability, and understanding system state.

**Possible surfaces:**
- Admin API endpoint: `GET /services/{name}/{key}/links` → list of linked_to edges with status
- CLI: `restate services links <name> <key>`
- Dashboard: visual graph rendering

**Depends on:** Deciding what information is useful to expose (edge state, linked_from_count, completion status, handler sinks).

## 4. WAL Format Version Bump

Adding `Option<T>` fields to WAL command payloads (e.g., `link_from`, `link_caller_completion_id` on `ServiceInvocation`) defaults to `None` on readers running older code. This is safe for the current implementation since `None` means "no link" which is the pre-feature behavior. However, a formal version bump may be warranted for:

- Rolling upgrade safety — ensuring mixed-version clusters handle new fields correctly
- Explicit feature gating — new fields only written when WAL version >= X

**Risk level:** Low for current implementation (additive optional fields with safe defaults).

## 5. Storage Redundancy Optimization

`LinkedFrom` edges were eliminated in Phase A.2 — all callback data lives in the target entity's `response_sinks`, and the linked_from count is tracked directly on `VirtualObjectStatus` and `InFlightInvocationMetadata`. The edge tables now only store `LinkedTo` edges.

**Potential optimizations:**
- The `EdgeLabel` discriminant byte in the key is always `LinkedTo` now — could be removed to save key space
- The `EdgeState` value only has `LinkedTo(Active)` and `LinkedTo(Completed)` — a single status byte would suffice instead of a protobuf-encoded value
- `linked_from_count` on the entity makes `LinkedFrom` edge scans unnecessary — confirmed working

**Trade-off:** Simplifying now locks out future edge labels. If `LinkedFrom` edges are ever re-added (e.g., for bidirectional traversal), the current flexible encoding is cheaper to extend.

## 6. Transitive Cycle Detection

Current implementation detects 1-hop cycles only (A links to B, B tries to link to A). Transitive cycles (A→B→C→A) are not detected and would manifest as deadlocked `Completing` invocations — the `Completing` lifecycle surfaces them operationally but doesn't prevent them.

**Options:**
- Bounded BFS at link creation time — walk up to N hops looking for the link_from entity in the linked_to entity's outgoing edges
- Async cycle detection — background scan of the edge graph
- Do nothing — `Completing` timeout/kill handles deadlocks operationally

**Deferred because:** Transitive cycles require cross-partition graph traversal at link creation time, adding latency and complexity to the hot path. The `Completing` lifecycle makes deadlocks observable and recoverable via kill/cancel.
