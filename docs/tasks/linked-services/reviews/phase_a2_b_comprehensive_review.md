# Review Analysis: Phase A.2 + Phase B Linked Services — Comprehensive Code Review

**Date**: 2026-04-10
**Focus Area**: Regression preservation, cross-partition routing, Completing lifecycle integration, end_invocation performance, proto compatibility, trait bound cascade, test coverage
**Context**: Task `linked-services`, branch `linked-services`, 22 commits (`4f7d999d7` → `154ff9494`), 50 files touched

## Summary

The implementation delivers Phase A.2 and Phase B cleanly on top of Phase A, landing a unified completion-sink model, a second edge table (`InvocationEdges`), the `StartLinkedCommand` / `AttachLinkCommand` journal commands, and an `InvocationStatus::Completing` lifecycle state. The design laid out in `plan_phase_a2.md` and `plan_phase_b.md` is faithfully followed — almost every acceptance criterion is verifiable in the code and covered by at least one integration test. Code organization is consistent with existing state-machine patterns: new journal commands live in `entries/`, new outbox commands are dispatched from `on_apply`, and the per-variant edge-table dispatch is localized in a handful of clearly scoped functions (`on_link_sink_invocation`, `on_link_service_request`, `on_link_response`, `on_link_completion_notification`, `on_attach_link`, `end_invocation`, `resume_completing_invocation`).

Overall quality is high. Phase A regression guards are all preserved, cross-partition routing correctly branches on sink-variant to the matching edge table, the Completing lifecycle integrates at every required match arm, and the test file (`crates/worker/src/partition/state_machine/tests/linked_services.rs`, ~2440 lines) exercises the major happy/unhappy paths end-to-end including the multi-child finalization path.

My review surfaced one code smell with a visible runtime symptom (stale `warn!` log on every linked child completion), a small correctness ambiguity around WI-child edge cleanup in the unlink cascade, one perf concern on `end_invocation`'s hot path, a couple of trait-bound leaks, and a handful of smaller hygiene suggestions. None rise to the level of blocking issues — the feature appears ready to merge once the misleading warning stub is resolved and the follow-ups are tracked.

## Strengths

1. **Faithful plan execution.** The two plan documents are precise and the code matches them step-for-step. `LinkCompletionSink` / `LinkCompletionNotification` / `EntityId` / `LinkResponse` all landed with the intended shape, and `ServiceEdgeState` unifies edge label + state as specified. The coupled-link semantics for `StartLinkedCommand` (link failure = invocation rejected) are implemented exactly as designed.

2. **Phase A regression preservation is solid.** Every one of the five Phase A guards from the prior commits is still in place:
   - Cycle detection (`link_service_command.rs:128-146`) — still runs for VO parents via `get_service_linked_from`.
   - GC cascade (`mod.rs:1754-1831`) — no-remaining-parents + Completed still triggers state/promise/status delete + grandchild unlink propagation.
   - External state mutation guard after completion — still rejects in `handle_external_state_mutation`.
   - Stale `LinkedTo` edge cleanup on `LinkResponse(Err)` — `on_link_response` now correctly deletes from `ServiceEdges` or `InvocationEdges` based on the parent `EntityId` variant.
   - Failure encoding as JSON bytes for `onCompleted` handler argument (`mod.rs:1907-1917`).

3. **Cross-partition routing is correct.** `on_link_completion_notification` (`mod.rs:1833-2017`) dispatches on `notification.sink` variant, using `ServiceEdges` for `LinkCompletionSink::Service` parents and `InvocationEdges` for `LinkCompletionSink::Invocation` parents. `on_link_sink_invocation` and `on_link_service_request` consistently derive the parent entity id from the sink variant. `link_service_command.rs:69-83` branches on `caller_target_ty` so WI-parent duplicates are checked against `InvocationEdges` and VO-parent duplicates against `ServiceEdges`.

4. **Completing lifecycle wiring is comprehensive.** `InvocationStatus::Completing` is handled in every exhaustive match in `invocation_status_table/mod.rs`. `on_kill_invocation` and `on_cancel_invocation` treat it as "already completed", `handle_attach_invocation_request` treats it as blockable-on-inflight, `do_append_response_sink` accepts it. `end_invocation` (`mod.rs:3459-3483`) correctly stores Completing only when `is_workflow_run && has_active_children`. `resume_completing_invocation` (`mod.rs:2021-2166`) re-runs the exact steps that `end_invocation` would have run.

5. **Test coverage is genuinely useful.** 16 integration tests cover the primary paths including the multi-child Completing mid-state assertion.

6. **Storage layer is clean.** `invocation_edges_table` mirrors `service_edges_table` with shared constants and the same trait shape.

7. **Fast-TDD discipline honored.** Each command task has exactly one happy + one unhappy path per the plan.

## Critical Issues

### CRITICAL-1 — Misleading "not yet wired (Phase B)" warning fires on every linked child completion

**Severity: Important (code smell with runtime symptom, not a functional bug)**

**Location**: `crates/worker/src/partition/state_machine/mod.rs:3658-3661` and `crates/storage-api/src/invocation_status_table/mod.rs:558`.

**Problem**: `ServiceInvocationResponseSink::Link` is not stripped from `response_sinks` when a `Link`-sinked `ServiceInvocation` transitions into `PreFlightInvocationMetadata`. In `on_link_sink_invocation` (`mod.rs:902-907`), the handler writes the `LinkedFrom` edge and sends `LinkResponse(Ok)`, then calls:

```rust
let pre_flight_invocation_metadata = PreFlightInvocationMetadata::from_service_invocation(
    self.record_created_at,
    service_invocation,
);
```

with the original `service_invocation` still carrying `Some(ServiceInvocationResponseSink::Link { .. })`. `PreFlightInvocationMetadata::from_service_invocation` collects it verbatim:

```rust
response_sinks: service_invocation.response_sink.into_iter().collect(),
```

Later, when the child completes via `end_invocation` → `send_response_to_sinks`, the `Link` arm at line 3658 fires and logs:

```
warn!("Received ServiceInvocationResponseSink::Link — not yet wired (Phase B)");
```

This warning will be emitted **on every successful linked child completion** in Phase B, which is misleading at best and alarming to operators at worst. The actual completion notification is correctly delivered via the separate `LinkedFrom` scan mechanism.

**Recommendation**: In `on_link_sink_invocation`, clear `service_invocation.response_sink` before constructing `PreFlightInvocationMetadata`:

```rust
let mut service_invocation = service_invocation;
let submit_notification_sink = service_invocation.submit_notification_sink.take();
service_invocation.response_sink = None; // Link sink already consumed: edge written, LinkResponse sent
```

Then replace the `ServiceInvocationResponseSink::Link { .. }` arm in `send_response_to_sinks` with `unreachable!("Link sink is consumed in on_link_sink_invocation before reaching send_response_to_sinks")`. Add a test assertion that after a linked invoke lands, the child's stored `InFlightInvocationMetadata.response_sinks` does not contain a `Link` variant.

---

### IMPORTANT-2 — `delete_all_invocation_edges` is guarded by `!linked_from.is_empty()` in `end_invocation`

**Severity: Minor (correctness edge case, not reachable in current Phase B tests)**

**Location**: `mod.rs:3498-3529`.

**Problem**: Only when `linked_from` is non-empty does `end_invocation` call `delete_all_invocation_edges`. If a WI child has finished linking to another (grand)child but has no parent `LinkedFrom` edges, its `LinkedTo(Completed)` or stale `LinkedTo(Active)` rows stay in `InvocationEdges` after it finishes via the non-Completing path.

**Recommendation**: Move the `delete_all_invocation_edges` call out of the `if !linked_from.is_empty()` block and run it unconditionally in `end_invocation`, mirroring `resume_completing_invocation`.

---

### IMPORTANT-3 — `end_invocation` now does an `InvocationEdges` scan + a `LinkedFrom` scan on *every* invocation completion

**Severity: Important (latency-sensitive path)**

**Location**: `mod.rs:3462-3529`.

**Problem**: The plan acknowledges that `end_invocation` is latency-critical. The current implementation adds up to two RocksDB prefix scans per completion:

1. **LinkedTo scan** (`mod.rs:3462-3466`) — only gated by `is_workflow_run`. Runs for every workflow run-handler completion, even when 99%+ have no linked children.
2. **LinkedFrom scan** (`mod.rs:3498-3501`) — gated by `needs_response_result`. Runs for every non-trivial workflow completion.

In the common case (no links), each scan returns empty, but each still costs a RocksDB iterator setup + seek + check.

**Recommendation**:
1. **Add a `has_links` flag to `InFlightInvocationMetadata`** set when an edge is first written; gate both scans on this flag.
2. **Use a `has_any_invocation_edges(invocation_id)` helper** with bounded single-key peek.
3. **Short-term: add a metric** (`histogram: end_invocation_scan_duration`) and a cheap `trace!` log before deciding on option 1 or 2.

At minimum option 3 (metric) before merging; option 1 as a follow-up task.

## Recommendations

### 1. (Important) Consolidate edge-label derivation
Both `service_edges_table` and `invocation_edges_table` expose identical `edge_label::LINKED_TO` / `LINKED_FROM` constants. Consider lifting to a single module.

### 2. (Minor) `on_unlink_service_request` doesn't clean up `InvocationEdges` for an unlinked WI child
At `mod.rs:1770`, the handler rejects non-`Object` `local_node_id` with a warning. Add a `// TODO(linked-services): extend to InvocationEdges when WI unlinking lands` comment.

### 3. (Minor) `vo_parent_wi_child_link_via_link_service_command` test has no corresponding unhappy path
Consider adding a test for `as_keyed_service_id()` guard branches.

### 4. (Minor) `CompleteServiceCommand` guard doesn't consult `InvocationEdges`
Add a comment explaining why `InvocationEdges` is intentionally not consulted (AttachLink reads the VO's result rather than driving it).

### 5. (Minor) `on_attach_link` stringifies `EntityId::WorkflowInvocation` status `Free` as an "internal" error
Consider using `InvocationError::new(410u16, ...)` (Gone) to give SDKs a typed signal.

### 6. (Minor) `AttachLinkCommand` target validation: no check that target partition_key matches
Add a one-line comment confirming "attach is unrestricted; any caller can attach to any known handle."

### 7. (Minor) `StartLinkedCommand` only supports workflow-run target
Add a test that asserts a `StartLinkedCommand` targeting a VO receives `StartLinkedResult::Failure`.

### 8. (Minor) Trait bound lists are now unwieldy
`on_link_completion_notification` requires 22 trait bounds. Consider defining a `trait LinkedServicesStorage` super-trait.

### 9. (Minor) Proto backward compatibility — not directly verified
Double-check proto field numbering for `CompletingInvocation`, `EdgeState`, `ServiceInvocationResponseSink::Link`, and `invoke_time` reservation.

### 10. (Minor) Test matcher silently ignores `completion_id` field
In `tests/linked_services.rs:1061`, add `completion_id: eq(completion_id)` to assert both fields.

## Alternative Approaches

### A1 — Push Link sink handling entirely out of `ServiceInvocationResponseSink`
Make `LinkCompletionSink` its own side channel via a dedicated `link_request: Option<LinkEstablishRequest>` field on `ServiceInvocation`. Would eliminate CRITICAL-1 entirely by construction. Bigger refactor; reasonable to defer.

### A2 — Store edge state as a compact bitfield instead of a proto oneof
Probably not worth the churn.

### A3 — Single edges table keyed by `EntityId`
Unified dispatch but mixed-length keys. Valid tradeoff for future consolidation.

## Specific Answers

**Q1 — Phase A regression preservation**: All five regression guards preserved and exercised.
**Q2 — Cross-partition routing correctness**: Correct. No cross-routing bugs identified.
**Q3 — Completing lifecycle integration**: Thorough. All match arms updated correctly.
**Q4 — `end_invocation` performance**: Two prefix scans per workflow completion — add a metric before merging.
**Q5 — Code organization and style**: Consistent with existing codebase.
**Q6 — Proto backward compatibility**: Not directly verified; see Recommendation 9.
**Q7 — Trait bound cascade**: Manageable but heavy; see Recommendation 8.
**Q8 — Test coverage**: 16 tests cover the primary matrix; gaps flagged as follow-ups.
**Q9 — "Not yet wired Phase B" warning**: Most visible issue; see CRITICAL-1.
**Q10 — Outstanding items**: Documented as follow-ups.

## Comprehensive Scores (0-10)

- **Security Posture**: 9 — no attack surface changes; type-level plumbing with proper validation.
- **Logic Correctness**: 8 — correct except for the CRITICAL-1 warning stub and IMPORTANT-2 edge-case cleanup.
- **Code Quality**: 8 — consistent with codebase patterns; trait bounds getting heavy.
- **Production Readiness**: 7 — blocked on CRITICAL-1 fix and IMPORTANT-3 metric addition.

## Prioritized Action Plan

1. **(Priority 1, before merge)** CRITICAL-1: Clear `response_sink = None` in `on_link_sink_invocation` and replace the `Link` arm in `send_response_to_sinks` with `unreachable!`. Add assertion test.
2. **(Priority 1, before merge)** IMPORTANT-3: Add a metric for `end_invocation` scan duration so production cost can be observed.
3. **(Priority 2)** IMPORTANT-2: Unconditionally call `delete_all_invocation_edges` in `end_invocation`.
4. **(Priority 2)** Recommendation 7: Add a test for `StartLinkedCommand` non-workflow target rejection.
5. **(Priority 2)** Recommendation 9: Visually verify proto field numbers.
6. **(Priority 3)** Recommendations 1, 2, 3, 4, 5, 6, 8, 10: Small hygiene improvements, can land as a follow-up cleanup commit.

## Final Verdict

**Ready to merge once CRITICAL-1 is fixed and IMPORTANT-3 has at least a metric added.** Recommendations are either tracked as follow-ups or small hygiene improvements. The core design is sound, regressions are preserved, cross-partition routing is correct, and the test coverage is honest about its gaps.
