# Review Analysis: Task 1.1 — Foundation Types

**Date**: 2026-04-10
**Focus Area**: Comprehensive review of commit `4f7d999d7` — Phase A.2 Task 1.1 foundation types
**Context**: linked-services branch, Phase A.2 Task 1.1 (pure type plumbing, no behavior change)

## Summary

Commit `4f7d999d7` delivers Task 1.1 as specified: it introduces `EntityId`, `EdgeState`,
`LinkStatus`, `LinkCompletionSink`, `LinkCompletionNotification`, `LinkResponse`, and
`ServiceInvocationResponseSink::Link`, updates the proto schema, and wires conversions
through `protobuf_types.rs`. The WAL/outbox path is migrated from `LinkServiceResponse`
to `LinkResponse`. All Phase A integration tests (happy path, self-link, cycle detection,
GC cascade, double completion, external state mutation guard) still exercise the old
`ServiceEdgeState` shape and still pass.

Overall assessment: **the commit is correct for its stated scope, but it is NOT pure
type plumbing** — it silently changes the `on_link_response` protocol so that the parent
invocation is derived from `VirtualObjectStatus::Locked(iid)` instead of being carried on
the wire. This is the single most important risk in the diff. For Phase A's VO→VO use
case it is observationally safe because the parent VO is exclusively locked while its
invocation is suspended on the completion id, but the regression is subtle and the code
comment block (`mod.rs:1549–1580`) is a stream-of-consciousness rationalization that
should be cleaned up before this commit is considered "done."

Two acceptance criteria from `tasks_phase_a2_b.md:72` are also not fully met:
- `EdgeState::edge_label()` method is not present on `EdgeState` (only on the Phase A
  `ServiceEdgeState`). This is only a problem if Phase 2 expects it; for Task 1.1 itself
  `EdgeState` is unused, so this is a minor spec gap.
- The criterion `"serde hacks handle backward-compat (old entries have no Link)"` is
  structurally satisfied by the fact that `Link` is a brand-new variant (old serialized
  data simply can't contain it), but there is no test asserting that old wire bytes still
  deserialize with the new enum in place.

Recommendation: keep the commit as is for unblocking Wave 2 work, but file a
follow-up to (a) remove the narrative code comment, (b) add a targeted trace/metric
for the "parent no longer locked" drop path, and (c) add `EdgeState::edge_label()` when
Phase 2 begins touching it.

## Strengths

1. **Type hierarchy is clean and makes illegal states unrepresentable.**
   `EntityId` is a two-variant enum with a `partition_key()` delegation
   (`invocation/mod.rs:620–633`); `LinkCompletionSink` delegates partition routing via
   its inner id (`invocation/mod.rs:647–654`); `LinkCompletionNotification` routes via
   `sink.partition_key()` (`invocation/mod.rs:685–689`). All three `WithPartitionKey`
   impls are consistent and correct — the "where does this land" question has exactly
   one answer at every layer.

2. **`LinkResponse.result: Result<(), InvocationError>` is a well-chosen shape.**
   Making the handle the `remote` field (`invocation/mod.rs:731–740`) removes the old
   redundancy where `LinkServiceResponse` had to carry a separate handle and a success
   flag. The `Ok(())` form on the wire is encoded as `failure = None` in the proto
   (`domain.proto` `LinkResponse` oneof), which matches the Rust shape naturally and
   costs zero extra bytes on the happy path.

3. **Proto wire compatibility is handled correctly for a branch-only change.**
   - `LinkServiceRequest` field tags are preserved (`caller_invocation_id` still at tag 3).
   - `LinkResponse` reuses the old `LinkServiceResponse` oneof tag (10) inside
     `outbox_message`. Because Phase A is unreleased, tag reuse is a legitimate choice
     — it avoids tag sprawl and keeps the numeric space tidy.
   - `ServiceInvocationResponseSink::Link` is a new oneof arm at tag 4, which cannot
     conflict with anything.

4. **`protobuf_types.rs` conversions are symmetric and total.**
   `EntityId` try_from/from (`protobuf_types.rs:240–270`), `LinkCompletionSink`
   try_from/from (`protobuf_types.rs:272–300`), and the `ServiceInvocationResponseSink`
   conversions (both owned and borrowed variants, `protobuf_types.rs:2338–2449`) handle
   every enum variant with no `_ =>` catch-alls. The compiler will fail the build the
   day a new variant is added without an explicit case — this is exactly the right
   posture for wire types.

5. **serde_hacks handles the new variant without special-casing.**
   `invocation/mod.rs:1724–1783` adds `Link { sink: super::LinkCompletionSink }` to
   the serde-hack mirror enum and both `From` impls round-trip it cleanly. Because
   serde is driven by variant discriminant, old WAL payloads that never contained a
   `Link` variant will still deserialize — no backward-compat hack needed.

6. **Phase A regression guards are visibly preserved.**
   - Cycle detection: `link_service_command.rs:88–102` still scans
     `get_service_linked_from(&parent_service_id)` and checks whether the proposed
     child is already a parent-of-parent. Unchanged from Phase A.
   - Stale LinkedTo cleanup on error: `mod.rs:1603–1609` still deletes the LinkedTo
     edge when the child rejects the link. The `response.remote` field carries the
     child identity that Phase A used to get from `LinkServiceResponse.remote_node_id`,
     so the cleanup target is unchanged.
   - GC cascade: `on_unlink_service_request` (`mod.rs:1631–1707`) still walks
     `get_service_linked_from`, gates GC on `VirtualObjectStatus::Completed`, deletes
     state/promises/status, and enqueues grandchild unlinks. Zero structural change.
   - External state mutation guard: `handle_external_state_mutation`
     (`mod.rs:1802–1840`) still matches on
     `VirtualObjectStatus::Locked(_) | Completed(_) | Unlocked` and rejects mutations
     for `Completed` objects. Untouched by this commit.
   - Failure encoding: `on_service_completion_notification:1778–1788` still encodes
     `ResponseResult::Failure` as the JSON blob `{"error_code": N, "message": "..."}`
     when enqueuing the onCompleted handler invocation. Untouched.

7. **All Phase A tests still pass** (per user's validation results). The happy-path
   test at `tests/linked_services.rs:140–255` was updated to dispatch the new
   `Command::LinkResponse` variant (`:237–243`) and explicitly documents the new
   protocol assumption at `:235`: *"Parent VO is locked by parent_inv_id —
   on_link_response derives invocation from the lock."*

## Critical Issues

### C1 (Important) — `on_link_response` silently changes a wire-visible protocol while claiming "no behavior change"

Task 1.1 is specified as *"pure type plumbing; Phase 2 wires it up"*
(`tasks_phase_a2_b.md:78`). The diff delivers more than that. Specifically:

- Phase A's `LinkServiceResponse` carried `caller_invocation_id`, and
  `on_link_service_response` routed the completion directly to that invocation.
- `LinkResponse` drops `caller_invocation_id` (`invocation/mod.rs:731–740`).
- `on_link_response` (`mod.rs:1582–1596`) now derives the invocation id by reading
  `VirtualObjectStatus::Locked(iid)` on the parent service id.

This is not type plumbing — it is a protocol change with two observable consequences:

1. **Dying-parent silent drop.** If the parent invocation is killed/cancelled between
   issuing `LinkServiceRequest` and receiving `LinkResponse`, the parent VO transitions
   out of `Locked(parent_inv_id)` before the response lands. `mod.rs:1587–1595` then
   warns and returns `Ok(())`, silently dropping the completion. Phase A's protocol
   would have routed the response to the dead invocation, which `get_invocation_status`
   would reject downstream — but that path at least produced a deterministic failure
   log with an invocation id. The new path attributes the drop to "parent not locked,"
   which is a different and less actionable diagnostic.

2. **Lock-race misrouting (benign, but subtle).** If the parent invocation dies and a
   *new* invocation re-locks the same VO before the `LinkResponse` arrives, the new
   lock's invocation id will be read out. The stale edge cleanup on the error path
   still fires against `response.remote`, which is correct, but the
   `OnNotifyInvocationResponse` dispatch at `:1614–1621` will target the wrong
   invocation. In practice this is caught downstream: the new invocation has no
   journal entry for the given `caller_completion_id`, so
   `OnNotifyInvocationResponse` will reject or drop the completion. No data
   corruption, but again the diagnostic is confusing.

The test at `tests/linked_services.rs:140–255` exercises only the happy
parent-still-locked path; neither failure mode is regression-tested.

The code comment at `mod.rs:1549–1580` is a long block of "thinking out loud" —
it walks through the author's reasoning, contradicts itself ("we need to carry it" /
"we derive it from the lock"), and ends by rationalizing the current design. This
comment must not ship in its current form; it actively misleads a future reader
into believing the design is uncertain.

**Recommended action (pick one):**

- **Option A (preferred, conservative)**: add `caller_invocation_id: InvocationId`
  back to `LinkResponse` and route with it. This restores the Phase A protocol,
  keeps Task 1.1 truly behavior-preserving, and defers the "lock-derived routing"
  question until Phase B where it actually matters for WI parents.

- **Option B (accept the change)**: keep the current derivation, but (i) delete the
  narrative comment block and replace it with a 3-line invariant statement *("Parent
  VO is exclusively locked by the caller invocation for the duration of the suspended
  journal entry; any other lock state means the caller has been terminated and the
  completion is no longer needed."*); (ii) add a test that applies `LinkResponse` when
  the parent VO is `Unlocked` and asserts the drop is logged at `warn!` level with
  both the parent service id and the completion id; (iii) emit a metric counter for
  the drop so operators can see it.

Either option must land before Phase A.2 merges to main.

### C2 (Minor) — `tasks_phase_a2_b.md:72` acceptance criterion not met: `EdgeState::edge_label()` method

The spec requires `EdgeState` to expose an `edge_label()` method returning the
correct byte marker. `invocation/mod.rs:659–663` defines `EdgeState` without any
inherent impls. The current production code still uses the Phase A
`ServiceEdgeState` and its `edge_label::LINKED_TO` / `edge_label::LINKED_FROM`
constants (e.g. `mod.rs:1740`, `link_service_command.rs:72`), so nothing is broken
today. But the criterion is listed in Task 1.1's definition of done.

**Recommended action**: either (a) add the method now to satisfy the checklist, or
(b) move the criterion to Task 2.1 and mark it explicitly as deferred. I lean toward
(a) because the method is three lines and lets Task 2.1 diffs stay focused on
storage changes.

### C3 (Minor) — Borrowed-conversion unnecessarily clones `LinkCompletionSink`

`protobuf_types.rs:2437–2441` — the `From<&ServiceInvocationResponseSink>` impl for
the `Link` arm calls `LinkCompletionSink::from(sink.clone())` even though this impl
exists specifically to avoid owning the source. The `From<&LinkCompletionSink>` for
the proto type either exists or should exist; if it doesn't, add one. The current
clone will be cheap in practice (small enum, but holds a `ByteString`), but it
defeats the purpose of the borrowed overload.

**Recommended action**: add `impl From<&LinkCompletionSink> for proto::LinkCompletionSink`
and call it here.

## Recommendations

### Priority 1 (before merge)
- **R1**: Resolve C1 via Option A or Option B. The status quo (narrative comment +
  undocumented protocol change) is not acceptable as a foundation for Phase 2.
- **R2**: Delete or collapse `mod.rs:1549–1580` regardless of the C1 resolution.
  Stream-of-consciousness comments are a red flag in production code.

### Priority 2 (before Phase 2 touches these types)
- **R3**: Resolve C2 by adding `EdgeState::edge_label() -> u8` (or `&'static str`,
  matching the existing `edge_label::` constants' type).
- **R4**: Add a test for the "parent VO unlocked" drop path in `on_link_response` —
  apply `LinkResponse` against an unlocked service id and assert `Ok(())` with no
  state changes and no outbox messages. This is a 20-line test that closes the
  observability gap.

### Priority 3 (nice to have)
- **R5**: Resolve C3 to remove the unnecessary clone on the borrowed conversion path.
- **R6**: Add a unit test in `invocation/mod.rs` (or wherever serde_hacks is tested)
  that round-trips a `ServiceInvocationResponseSink::Link` through the serde hack,
  to lock the wire shape before any Phase 2 refactor touches it.
- **R7**: Consider adding a `#[deprecated]` or module-level doc on
  `ServiceCompletionNotification` (`invocation/mod.rs:750–763`) pointing at
  `LinkCompletionNotification` and noting that Task 2.1 is the replacement point.
  This is what makes "defined but unused" types discoverable.

## Alternative Approaches

- **On the caller-derivation question**: a third option is to have the child echo
  the `caller_invocation_id` in `LinkResponse` as an opaque "hint" field, with the
  parent's handler cross-checking it against the lock state. This gives you the
  diagnostic strength of Option A *and* the lock-based safety of Option B. Overkill
  for VO→VO, but worth noting since Phase B's WI parents don't have the
  "exclusively locked VO" invariant and will likely need to carry routing info
  explicitly anyway. If you're going to add that infrastructure in Phase B,
  pre-adding it in Phase A.2 is cheaper than a two-step migration.

- **On `EdgeState` vs `ServiceEdgeState` coexistence**: the current commit leaves
  `EdgeState` defined but completely unused — `ServiceEdgeState` (the Phase A type)
  is still what storage actually uses. This creates a window where a reader sees
  two edge-state types and has to figure out which is "real." Consider either
  (a) hiding `EdgeState` behind `#[cfg(test)]` or a module-level `pub(crate)` until
  Task 2.1 wires it in, or (b) adding a prominent doc comment on the Phase A
  `ServiceEdgeState` pointing at the replacement.

## Specific Answers to Review Foci

### Q: Does the code correctly deliver Task 1.1 as specified?

**Yes, with two caveats**: (1) the `on_link_response` protocol change is not pure
type plumbing — see C1; (2) `EdgeState::edge_label()` is missing — see C2.
Everything else on the spec checklist (`tasks_phase_a2_b.md:71–77`) is satisfied.

### Q: Are Phase A regression guards preserved?

**Yes, all five**:
- Cycle detection — `link_service_command.rs:88–102`, unchanged.
- GC cascade — `mod.rs:1631–1707`, unchanged.
- External state mutation guard — `mod.rs:1802–1840`, unchanged.
- Stale edge cleanup on error — `mod.rs:1603–1609`, logically equivalent to
  Phase A (now uses `response.remote` instead of `response.remote_node_id`).
- Failure encoding — `mod.rs:1778–1788`, unchanged.

The 6 existing Phase A tests all exercise these paths and all pass.

### Q: Is the `on_link_response` caller-derivation change safe?

**Observationally safe for Phase A's VO→VO use case, but fragile and unobservable.**
See C1 for the full analysis. Short version:
- The parent VO is held in `Locked(parent_inv_id)` for the duration of the suspended
  journal entry, so the lookup will normally return the correct invocation.
- If the parent is killed/cancelled mid-flight, the completion is silently dropped
  with a `warn!` and no metric. The happy path is tested; the drop path is not.
- If the parent dies and a new invocation re-locks the VO before the response
  arrives, the completion will be misrouted to the new invocation, but
  `OnNotifyInvocationResponse` will reject it downstream due to missing journal
  entry. No corruption, but a confusing diagnostic chain.
- Phase B's WI parents do not hold a VO lock and cannot use this derivation —
  carrying the caller id explicitly on `LinkResponse` (Option A in C1) avoids a
  second refactor in Phase B.

### Q: Is the exhaustive match of `ServiceInvocationResponseSink::Link` correct?

**Yes at the type-conversion layer**. `protobuf_types.rs:2338–2449` handles the new
variant in all three implementations (`TryFrom` owned, `From` owned, `From`
borrowed) without `_ =>` catch-alls, and serde_hacks (`invocation/mod.rs:1745–1783`)
handles it in both `From` directions. The Rust compiler guarantees any future
variant addition will fail to build until all sites are updated.

### Q: Proto wire compatibility — stable field numbers?

**Yes**:
- `LinkServiceRequest` field tags are unchanged (`domain.proto:636–641`).
- `LinkResponse` reuses the old oneof tag `10` in `outbox_message`. This is safe
  because the `linked-services` branch has not been released and there are no
  persisted WAL payloads with the old `LinkServiceResponse` shape in production.
- The new `Link` sub-message on `ServiceInvocationResponseSink` is at a fresh tag
  (`4`) and cannot conflict with existing variants.

### Q: `LinkCompletionSink` `Hash` derivation correct?

**Yes**. `invocation/mod.rs:639–645` derives `Hash, Eq, PartialEq, Clone, Debug`,
and every inner type is hashable (`ServiceId`, `InvocationId`, `ByteString`,
`CompletionId`, `Option<T: Hash>`). No reference types, no floats, no interior
mutability. The derivation is sound.

### Q: `LinkCompletionSink::WithPartitionKey` routing correct?

**Yes**. `invocation/mod.rs:647–654` routes `Service(sid, _)` to
`sid.partition_key()` and `Invocation(iid, _)` to `iid.partition_key()`. Both
delegate to existing, battle-tested `partition_key()` impls.
`LinkCompletionNotification::partition_key` delegates to
`self.sink.partition_key()` (`:685–689`) — notifications always land where the
sink lives, not where the source lives. Correct for "deliver to parent" semantics.

### Q: Are unused-yet types documented?

**Mostly yes**:
- `EntityId::WorkflowInvocation` — documented as "defined here for Phase B use."
- `EdgeState` — documented as "wired into storage in Phase 2 (Task 2.1)."
- `LinkCompletionNotification` — documented as "replaces `ServiceCompletionNotification`
  in Phase 2 (Task 2.1)."
- `ServiceInvocationResponseSink::Link` — documented as "defined in Phase A.2;
  wired in Phase B (StartLinkedCommand)."
- `LinkStatus` — has a brief doc comment but no explicit "wired in Phase 2" note.
- `ServiceCompletionNotification` — has **no** reverse pointer to its replacement.
  See R7.

## Comprehensive Scores (0–10)

- **Security Posture**: 10 — no attack surface changed; type-level plumbing only.
- **Logic Correctness**: 7 — correct for Phase A invariants, but the `on_link_response`
  derivation lacks a test for the drop path and has a confusing narrative comment.
- **Code Quality**: 8 — clean type hierarchy, symmetric conversions, good doc comments
  on most new types. The narrative comment at `mod.rs:1549–1580` drags this down.
- **Production Readiness**: 8 — all Phase A tests pass, no regressions, but C1
  (protocol change without observability) should land before merge.

## Prioritized Action Plan

1. **(Priority 1, before merge)** Resolve C1: either restore `caller_invocation_id`
   in `LinkResponse` (Option A) or delete the narrative comment + add the drop-path
   test (Option B).
2. **(Priority 1, before merge)** Delete or collapse the narrative comment block
   at `mod.rs:1549–1580`.
3. **(Priority 2)** Add `EdgeState::edge_label()` method to satisfy spec criterion.
4. **(Priority 2)** Add "parent VO unlocked" drop-path test if choosing C1 Option B.
5. **(Priority 3)** Remove unnecessary `sink.clone()` in borrowed `From` impl.
6. **(Priority 3)** Add serde round-trip test for the new `Link` variant.
7. **(Priority 3)** Add reverse pointer doc from `ServiceCompletionNotification`
   to `LinkCompletionNotification`.

## Headline Findings (for quick triage)

1. **C1 (Important)**: `on_link_response` derives parent invocation from
   `VirtualObjectStatus::Locked(iid)`, dropping `caller_invocation_id` from the
   wire. Safe for Phase A's VO→VO path by invariant, but fragile, untested on the
   drop path, and will need rework for Phase B's WI parents.
2. **C2 (Minor)**: `EdgeState::edge_label()` acceptance criterion not satisfied.
3. **C3 (Minor)**: Borrowed `From<&ServiceInvocationResponseSink>` needlessly clones.
4. All Phase A regression guards are **preserved**.
5. Proto wire compat, Hash/Eq derivations, partition-key routing, and exhaustive
   matches are **correct**.
