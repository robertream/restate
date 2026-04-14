# Review Analysis: Unified Response Sinks Refactor (Commit `4c1ac82c8`)

**Date**: 2026-04-11
**Focus Area**: Correctness of the unified response-sinks model across VO/WI; WI unlinker preserving PartitionProcessor sinks; short-circuit path for already-completed targets; VirtualObjectStatus transitions; two link establishment paths; `fire_service_completion` failure encoding
**Context**: Linked services Phase A/A.2/B — branch `linked-services`, unified response sinks refactor per `docs/tasks/linked-services/specs/plan_unified_response_sinks.md`

## Summary

The refactor is materially sound and delivers on the plan. The core insight — unify "how a completion reaches an interested observer" into a single `response_sinks` collection on both `InvocationStatus` and `VirtualObjectStatus` — is implemented cleanly. Sinks survive lifecycle transitions that should preserve them (Locked ↔ Unlocked) and are dropped on the terminal transition to Completed only after being drained, which is structurally enforced by the enum shape (`Completed(ResponseResult)` has no sinks field at all).

The five areas highlighted as critical are all correctly handled:

1. **WI unlinker preserving PartitionProcessor sinks** — Correct. Filter only removes `ServiceCompletion` sinks matching a VO parent's service_id; for WI parents, `parent_service_id_opt` is `None` and the filter block is skipped entirely.
2. **Short-circuit for already-completed targets** — Correct. `on_link_request` fires the handler, emits a graph-only `LinkCompletionNotification`, sends `LinkResponse(Ok)`. No `LinkedFrom` is written.
3. **VO status transitions preserving sinks** — Correct. `do_unlock_service` ports sinks Locked → Unlocked. `CompleteServiceCommand` drains sinks before persisting `Completed`.
4. **Two link establishment paths converge** — Correct. `LinkServiceCommand` (via `on_link_request`) and `StartLinkedCommand` (via `on_link_from_invocation`) both reach the same end state.
5. **`fire_service_completion` failure encoding** — Functionally preserved (JSON shape), but see Critical #2 (unsafe against characters in `err.message()`).

However, there are **two real bugs**, one subtle correctness gap, and a handful of minor issues that should be addressed before the refactor ships.

## Strengths

1. **Type-enforced terminal state.** Making `VirtualObjectStatus::Completed(ResponseResult)` a tuple variant (no sinks field) forces the compiler to ensure sinks are drained prior to transition. `response_sinks_mut()` returning `None` for `Completed` is an additional safety net.

2. **Asymmetric unlink cleanup is correct.** The WI-parent branch deriving `parent_service_id_opt = None` at `crates/worker/src/partition/state_machine/mod.rs:1834-1837` is the single load-bearing line that preserves `PartitionProcessor` sinks during WI-parent unlinks.

3. **Short-circuit path is well-integrated.** The already-completed branch in `on_link_request` (`mod.rs:1681-1703`) correctly emits a `LinkCompletionNotification` to keep the parent's edge-state consistent.

4. **Test coverage is strong.** The test suite exercises:
   - Happy + unhappy paths for link establishment, completion, double-completion, unlink/GC cascade
   - The full `Completing` lifecycle with both single-child and multi-child scenarios
   - Both asymmetric unlink branches
   - VO→WI cross-type linking with both success and failure child outcomes
   - Both happy/active and already-completed AttachService paths

5. **`has_links` gate is preserved.** The presence flag still gates RocksDB scans on the latency-critical `end_invocation` path.

6. **Edge writes are idempotent and minimal.** `EdgeState::LinkedFrom` as a presence-only unit variant eliminates "two writes, which wins?" concerns.

## Critical Issues

### Critical #1 — `on_service_invocation` dedup runs before `link_from` detection

**Severity: Critical**

Location: `crates/worker/src/partition/state_machine/mod.rs:747-761`

```rust
// 1. Try deduplicate it first
let Some(mut service_invocation) =
    self.handle_duplicated_requests(service_invocation).await?
else {
    // Invocation was deduplicated, nothing else to do here
    return Ok(());
};

// Detect link_from — child-side link processing for StartLinkedCommand piggyback path.
// Must happen before from_service_invocation so we can write the LinkedFrom edge.
if service_invocation.link_from.is_some() {
    return self
        .on_link_from_invocation(invocation_id, service_invocation)
        .await;
}
```

**Problem**: When a parent issues `StartLinkedCommand` targeting an idempotent child that has already been deduplicated, the child partition's dedup path returns at line 752 without ever running `on_link_from_invocation`. Consequences:
- No `LinkedFrom` edge is written on the child.
- No `LinkResponse` is sent back to the parent.
- The parent's `LinkedTo(Active)` edge stays Active forever.
- The parent's SDK is never notified that its `StartLinkedCommand` completed → the workflow hangs.
- Because `LinkedFrom` is missing on the child, when the child eventually completes, no `LinkCompletionNotification` is emitted → `Completing` parents never finalize.

This is a cross-partition hang with no timeout. It is silent (no error is logged) and affects an important production scenario (idempotent workflows invoked via StartLinked).

**Recommendation**: Handle `link_from` before or alongside dedup:

- **Option A**: Move the `link_from` check before `handle_duplicated_requests`.
- **Option B**: In the dedup path (the `else` return), extract `link_from` and handle it separately: write `LinkedFrom`, optionally attach the sink, send `LinkResponse(Ok)`. This is the safer path since dedup has meaningful side-effects (e.g., response sinks attached to the existing invocation).
- **Option C**: If `link_from.is_some()` and the invocation is being deduplicated, log the condition and emit `LinkResponse(Err)` with a "cannot link to deduplicated invocation" message. Workaround, not a fix.

Add a test: seed an existing invocation, then dispatch a second `Invoke` with `link_from = Some(...)`, and assert that the parent receives `LinkResponse` (Ok or Err with a clear message).

### Critical #2 — `fire_service_completion` JSON encoding is unsafe

**Severity: Critical (correctness, data integrity)**

Location: `crates/worker/src/partition/state_machine/mod.rs:3775-3782`

```rust
ResponseResult::Failure(err) => bytes::Bytes::from(format!(
    r#"{{"error_code":{},"message":"{}"}}"#,
    u16::from(err.code()),
    err.message()
)),
```

**Problem**: `err.message()` is injected verbatim into a JSON string literal. If the message contains any of `"`, `\`, newlines, tabs, or control characters, the resulting bytes are not valid JSON and the handler code on the SDK side will fail to parse it. Error messages are a prime vector:
- `InvocationError` can carry upstream error text with quotes.
- Multi-line messages will break parsing.
- Minor security concern: if error messages ever include user-controlled strings, a malicious user could inject arbitrary JSON fields.

**Recommendation**: Use `serde_json` to serialize the error structure:

```rust
ResponseResult::Failure(err) => {
    #[derive(serde::Serialize)]
    struct FailurePayload<'a> {
        error_code: u16,
        message: &'a str,
    }
    let payload = FailurePayload {
        error_code: u16::from(err.code()),
        message: err.message(),
    };
    bytes::Bytes::from(serde_json::to_vec(&payload).expect("serialize failure payload"))
}
```

Add a failing-now test: `fire_service_completion` with a failure whose message contains `"` and `\n`, and assert the resulting argument bytes round-trip through `serde_json::from_slice::<serde_json::Value>`.

## Important Issues

### Important #1 — `fire_service_completion` drops retention information

**Severity: Important**

Location: `crates/worker/src/partition/state_machine/mod.rs:3784-3794`

The spawned handler invocation uses `ServiceInvocation::initialize(...)` defaults, yielding `completion_retention_duration = Duration::ZERO` and `journal_retention_duration = Duration::ZERO`. Consequences:
- Handler invocations have zero retention and are purged immediately after completion.
- Debugging a failed onCompleted handler becomes very difficult.

**Recommendation**: Decide on a retention policy explicitly:
- **Inherit from parent**: Pass the parent's retention into `fire_service_completion`.
- **Use a default fallback** from config.
- **Explicit in the spec**: Document the choice and add a test.

The `_invocation_target: Option<&InvocationTarget>` parameter is currently unused — either wire it up or remove it.

### Important #2 — `run_invocation` / `consume_inbox` drop sinks when relocking a Completed VO

**Severity: Important (defensive hygiene)**

Location: `crates/worker/src/partition/state_machine/mod.rs` around `run_invocation` (vqueues path) and `consume_inbox` (legacy path)

When an invocation transitions from `Completed` back to `Locked` (unreachable in current code but exists as a code path), these paths instantiate `VirtualObjectStatus::Locked { invocation_id, response_sinks: Default::default() }`. The `Default::default()` discards any sinks.

In practice unreachable because `on_service_invocation` rejects new invocations on a Completed VO. But if the invariant were broken elsewhere, sinks would be silently dropped.

**Recommendation**: Replace `Default::default()` with an `unreachable!("Completed VO should not be relocked")` to fail loudly if the invariant is violated.

### Important #3 — `VirtualObjectStatus::Unlocked` carries a misplaced `#[allow(clippy::exhaustive_enums)]`

**Severity: Minor**

Location: `crates/storage-api/src/service_status_table/mod.rs`

The attribute is placed on the `Unlocked` variant alone. `exhaustive_enums` is an enum-level lint, not a variant-level one.

**Recommendation**: Move to the enum definition or remove.

## Minor Issues

### Minor #1 — `response_sinks_mut()` silently returns `None` for `Completed`

Callers must remember to handle `None`. Correct behavior but footgun-prone.

**Recommendation**: Add rustdoc noting that `Completed` returns `None` and callers must not assume success. Consider returning a `Result` for stronger enforcement.

### Minor #2 — Test coverage gap for VO parent with `result_completion_handler` via `LinkServiceCommand`

The existing tests cover the `handler_sink: None` case (WI parent) and the write helpers exercise the Some case indirectly, but no test applies `LinkServiceCommand { result_completion_handler: Some("handleDone") }` end-to-end and asserts the sink lands in the child's `response_sinks`.

**Recommendation**: Add a VO→VO test with a handler, asserting the `ServiceCompletion` sink appears in `child_status.response_sinks()`.

### Minor #3 — `on_link_request` short-circuit only handles `EntityId::Object` children

Location: `mod.rs:1675-1728`

For `WorkflowInvocation` children, no short-circuit exists — the code falls through to "respond with success" without writing a `LinkedFrom` edge. If a `LinkRequest` is ever mistakenly routed to a WI child, the response will be an erroneous `Ok` with no side effects.

**Recommendation**: Add an explicit `match` arm for `EntityId::WorkflowInvocation(_)` that returns `LinkResponse(Err)` or logs a warning.

### Minor #4 — Conditional move of `request.handler_sink` in `on_link_request`

Location: `mod.rs:1685`

Compiles only because NLL handles the conditional partial move with the early return. Any future refactor could break this.

**Recommendation**: Extract `let handler_sink = request.handler_sink;` at the top of the function and use by move/reference as needed.

### Minor #5 — Two reads of `VirtualObjectStatus` in `on_link_request`

Short-circuit reads `status` once, normal path reads again. Minor inefficiency; consider holding the status across both branches.

## Recommendations

**Priority 1 (ship blockers)**:
1. Fix the `on_service_invocation` dedup-before-link_from bug (Critical #1). Add regression test.
2. Replace JSON string-interpolation in `fire_service_completion` with `serde_json::to_vec` (Critical #2). Add a test with embedded `"` and `\n` in error message.

**Priority 2 (should address before merge)**:
3. Decide and document retention policy for spawned handler invocations (Important #1).
4. Harden `run_invocation` / `consume_inbox` relock paths with `unreachable!` (Important #2).

**Priority 3 (nice to have)**:
5. Fix `#[allow(clippy::exhaustive_enums)]` placement (Important #3).
6. Add rustdoc for `response_sinks_mut()` (Minor #1).
7. Add VO-parent-with-handler `LinkServiceCommand` test (Minor #2).
8. Add explicit `LinkRequest` rejection for WI targets (Minor #3).
9. Refactor `handler_sink` conditional move (Minor #4).

## Comprehensive Scores (0-10)

- **Security Posture**: 7 — JSON injection risk (Critical #2) is the main concern. Otherwise no new attack surface.
- **Logic Correctness**: 7 — Critical #1 is a silent hang in a legitimate production scenario. Otherwise correct.
- **Code Quality**: 8 — Clean type hierarchy, good test coverage, minor hygiene issues.
- **Production Readiness**: 6 — Blocked by Critical #1 and #2. Acceptable after fixes.

## Specific Answers

**(a) WI unlinker preserving PartitionProcessor sinks** — **Correct.** The `parent_service_id_opt = None` early-exit for `EntityId::WorkflowInvocation` at `mod.rs:1836` ensures the filter block is skipped. The test `wi_unlink_preserves_attach_sink` at `linked_services.rs:2718-2804` tests the VO-child-with-WI-parent branch (uses `InvocationTarget::mock_virtual_object()`). **Gap**: no test for WI-parent → WI-child unlink preserving PartitionProcessor sinks.

**(b) Short-circuit for already-completed targets** — **Correct.** The short-circuit at `mod.rs:1681-1703` fires the handler, emits a graph-only `LinkCompletionNotification`, and returns `LinkResponse(Ok)`. The graph notification is essential — without it, the parent's `LinkedTo(Active)` edge would never transition. Test `link_service_command_on_completed_target_fires_handler_immediately` asserts all three outputs.

**(c) VirtualObjectStatus state transitions preserving sinks** — **Correct.** Unlocked → Locked ports sinks via `attempt_to_run` / `consume_inbox` / `on_pre_flight_invocation`. Locked → Unlocked: `do_unlock_service` at `mod.rs:5728-5756` explicitly preserves sinks. Locked → Completed: sinks drained before `put_virtual_object_status(Completed)` write. The soft spot `Unlocked { sinks: [] } → delete key` is handled correctly in `put_virtual_object_status` and round-trips via `unwrap_or_default()`.

**(d) Two link establishment paths reach consistent end states** — **Correct.** `LinkServiceCommand` → `on_link_request`, `StartLinkedCommand` → `on_link_from_invocation`. Both write the same edges via the same storage APIs and produce the same outbox outputs. Tests verify both paths against the same assertions.

**(e) `fire_service_completion` preserving Phase A failure encoding** — **Functionally preserved, but the encoding is unsafe.** See Critical #2. The JSON shape `{"error_code":N,"message":"..."}` matches Phase A, but `err.message()` is pasted into the literal without escaping.
