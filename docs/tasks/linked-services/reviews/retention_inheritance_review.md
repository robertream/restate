# Review Analysis: Retention Inheritance Fix (Commit `bfdab2411`)

**Date**: 2026-04-11
**Focus Area**: Correctness of retention inheritance for `fire_service_completion` spawned handler invocations; dead-param truth check at `Duration::ZERO` call sites; `CompleteServiceCommand` retention semantics; test coverage of happy + short-circuit paths
**Context**: Linked services Phase A.2 + B — branch `linked-services`, follow-up to IMPORTANT-1 from `unified_response_sinks_review.md`

## Summary

The fix correctly addresses the headline case of IMPORTANT-1: spawned `ServiceCompletion` handler invocations fired from `end_invocation`, `resume_completing_invocation`, `on_link_request` short-circuit, and `CompleteServiceCommand` now inherit the parent's `completion_retention_duration` and `journal_retention_duration` instead of defaulting to `Duration::ZERO`. The proto schema change is backward-compatible, the new test asserts real non-zero values on both the short-circuit and end-of-invocation paths, and the `FailurePayload` serde rework is preserved and safe.

However, the commit's implicit "dead param" claim at the eight remaining `Duration::ZERO` call sites is **not fully true**. Five of those sites can in principle dispatch `ServiceCompletion` sinks that arrived via the `StartLinkedCommand` piggyback path, meaning there is still a residual bug class — narrower than the original IMPORTANT-1, but not zero. Three of the eight sites are provably dead. Details in Critical #1.

The `CompleteServiceCommand` retention choice (inherit the completing handler invocation's retention) is semantically defensible but worth a short design note: it is "handler retention" not "VO service-level retention" (the latter doesn't exist), so spawned `onCompleted` handlers inherit the *terminating invocation's* retention, not the *parent caller's*. This is the correct default with the current type system, but it deserves documentation.

## Strengths

1. **Four primary dispatch paths correctly inherit retention.** `end_invocation` (`mod.rs:3706-3714`), `resume_completing_invocation` (`mod.rs:2283-2291`), `CompleteServiceCommand` (`complete_service_command.rs:123-131`), and `on_link_request` short-circuit (`mod.rs:1787-1792`, direct call to `fire_service_completion`) all pass real retention. These cover every "normal" completion path.

2. **Test is rigorous.** `fire_service_completion_inherits_parent_retention` (`tests/linked_services.rs:3084-3261`) uses realistic non-zero values (`3600s` / `1800s`), exercises both the short-circuit path (LinkRequest against an already-Completed VO) and the end-of-invocation path (child WI with ServiceCompletion sink completing normally), and asserts `assert_eq` on both `completion_retention_duration` and `journal_retention_duration` of the spawned handler invocation. No tautological shape checks.

3. **Proto change is backward-compatible.** New fields 6 and 7 on `LinkRequest` (`domain.proto:664-674`) with decode via `unwrap_or_default()` (`protobuf_types.rs:3951-3958`). Pre-fix WAL records decode to `Duration::ZERO`, which then propagates unchanged through `fire_service_completion` — i.e., the fix preserves the pre-fix behavior exactly for in-flight records during rolling upgrades. No tag collision: `LinkRequest` has no reserved tags.

4. **`fire_service_completion` struct-update syntax is clean.** The new implementation (`mod.rs:3943-3948`) uses `ServiceInvocation { completion_retention_duration, journal_retention_duration, ..ServiceInvocation::initialize(...) }`, which is the minimal diff from the prior code and preserves all other initialization defaults.

5. **`FailurePayload` encoding is preserved.** The serde-based JSON encoding (`mod.rs:3919-3933`) from the unified response sinks refactor is intact — the retention fix did not regress the special-character safety.

6. **`LinkRequest` seeding source is correct.** `link_service_command.rs:160-168` seeds the outgoing `LinkRequest` from the caller's `invocation_metadata.completion_retention_duration` / `journal_retention_duration`. This is the right source: `LinkServiceCommand` is invoked by the caller's handler, and its `invocation_metadata` is the caller's metadata. The retention thus propagates cross-partition to the child's short-circuit path with the parent's policy.

7. **Doc comment on `send_response_to_sinks` parameters is honest.** `mod.rs:3851-3854` explicitly labels the two new retention params as "dead param otherwise" — the commit author is aware of the narrow scope and documents the precondition rather than hiding it.

## Critical Issues

### Critical #1 — Residual `Duration::ZERO` leak via `StartLinkedCommand` piggyback

**Severity: Important** (narrower than the original IMPORTANT-1, but a real correctness gap)

The commit leaves eight call sites passing `Duration::ZERO, Duration::ZERO` into `send_response_to_sinks`. The implicit claim is that these sites cannot dispatch `ServiceCompletion` sinks. I verified each site against the `StartLinkedCommand` piggyback flow where `ServiceInvocation.response_sink` can be a `ServiceCompletion(ServiceCompletionTarget)` and flows via `on_link_from_invocation → PreFlightInvocationMetadata::from_service_invocation` into `metadata.response_sinks`.

**Provably dead (3 sites — safe):**

- `mod.rs:1461` and `mod.rs:1530` — `handle_service_invocation_exclusive_handler` rejection when target VO is Completed. Reachable via link_from? No: `on_link_from_invocation` itself checks for `VirtualObjectStatus::Completed` at `mod.rs:883` and rejects **before** calling `on_pre_flight_invocation`. Same-record apply, no interleaving. Safe.
- `mod.rs:5388`, `mod.rs:5410`, `mod.rs:5423` — `handle_attach_invocation_request` Free/NotReady/Completed branches. The response sinks here come from `AttachInvocationRequest.response_sink`, which at `mod.rs:4961` is always constructed as `ServiceInvocationResponseSink::partition_processor(...)`. Safe by construction.

**NOT provably dead (5 sites — residual bug class):**

- `mod.rs:1335` — `handle_duplicated_requests`, workflow duplicate branch. Drains `service_invocation.response_sink` (which can be `ServiceCompletion` on a `StartLinkedCommand` duplicate). Reachable if a workflow invocation is both (a) duplicated and (b) carries a linked parent via StartLinkedCommand. Edge case but not unreachable.
- `mod.rs:1373` — `handle_duplicated_requests`, idempotent replay against already-Completed invocation. Same mechanism. If the duplicate `ServiceInvocation` carries `response_sink = ServiceCompletion(...)`, the spawned handler inherits `Duration::ZERO`. Note: The deduplicated-link-from path at `mod.rs:747-784` (`on_link_from_deduplicated_invocation`) handles the *link* re-establishment but still hits `handle_duplicated_requests` first at line 1335/1373, which fires the sink. So this path is reachable specifically when a StartLinkedCommand is retried against a completed child.
- `mod.rs:2753` — `terminate_inboxed_invocation` (abort/kill). Drains `InboxedInvocation.metadata.response_sinks`, which was populated from `PreFlightInvocationMetadata::from_service_invocation`. If the original invocation was a StartLinkedCommand piggyback with a ServiceCompletion sink, the spawned error handler fires with ZERO retention and is immediately purged.
- `mod.rs:2863` — `terminate_scheduled_invocation`. Same mechanism, different state — a StartLinkedCommand-spawned invocation with an execution_time could end up Scheduled with a ServiceCompletion sink in its `response_sinks`.
- `mod.rs:4204` — `consume_inbox` drain branch when VO is Completed. Drains `inboxed_invocation.metadata.response_sinks`. Same mechanism — inboxed linked children of a now-Completed VO flush their sinks here.

**Impact assessment.** These leaks are strictly narrower than the pre-fix state: they only trigger on failure/cancel/GC cascades of linked children whose original `ServiceInvocation` carried a `response_sink = ServiceCompletion(...)` (i.e., StartLinkedCommand piggyback). The normal success path (IMPORTANT-1's headline case) is fully fixed. But the onCompleted error handler in these edge cases will still be purged before it can run, defeating the purpose of the retention inheritance entirely for those scenarios.

**Recommendation.** Either (a) thread retention into these five call sites by reading the originating invocation's `completion_retention_duration`/`journal_retention_duration` (available in all five cases — `InboxedInvocation` and `ScheduledInvocation` both carry `PreFlightInvocationMetadata`, and `handle_duplicated_requests` has the `service_invocation` in scope), or (b) downgrade the claim in the `send_response_to_sinks` doc comment from "dead param otherwise" to "carries retention for any ServiceCompletion sinks present; pass ZERO only when you have proven the sink cannot be `ServiceCompletion`" and explicitly enumerate which sites are provably dead. Option (a) is preferable — the data is right there at every call site.

### Critical #2 — `CompleteServiceCommand` retention semantics (documentation gap, not a bug)

**Severity: Minor / Design note**

Location: `complete_service_command.rs:123-131`

```rust
ctx.send_response_to_sinks(
    response_sinks.into_iter(),
    result.clone(),
    None, None,
    Some(&invocation_metadata.invocation_target),
    invocation_metadata.completion_retention_duration,
    invocation_metadata.journal_retention_duration,
)?;
```

The retention passed to spawned `onCompleted` handlers is that of the **completing handler invocation** (the VO handler that invoked `CompleteService`). This is semantically defensible — it's the only retention value in scope at this point — but it is not obviously "the right one" from a user's mental model:

- A linked parent might have a 24-hour retention expecting its `onCompleted` error handler to survive a long-running error investigation.
- The VO's completing handler is a short-lived exclusive invocation with whatever retention the client passed.
- Spawned handlers inherit the completing handler's retention, not the parent's.

In practice this usually works out: the client's `onCompleted` handler on the parent registers a `ServiceCompletion` sink whose spawn retention is gated by `fire_service_completion`'s args, which at `CompleteService` time are the completing VO handler's args. So a parent with 24-hour retention whose child VO is completed by a 1-hour-retention handler will get a 1-hour-retained `onCompleted` handler. That's a surprise.

The commit comment at `complete_service_command.rs:121-122` says: "Inherit retention from the completing handler invocation so spawned ServiceCompletion handler invocations are not immediately purged (Duration::ZERO default)." This is accurate about **what** it does but silent about **why** it's the right choice vs. inheriting from the parent.

**Recommendation.** Expand the comment to acknowledge the alternative (inherit parent retention per-sink) was considered and rejected because:
1. Parent retention is not available at this point without another table scan.
2. The completing handler's retention is a reasonable proxy since the handler is the entity that decided to terminate the VO.
3. For the linked parent use case, the parent is already awaiting the completion via its own `on_link_request` short-circuit path (if already completed) or `end_invocation` path (if the parent completes first), and those paths use the parent's own retention. `CompleteServiceCommand` only fires sinks that joined the VO *between* link and termination, which is an edge case.

No code change needed, just a clearer comment.

## Recommendations

### Important

1. **Fix or document the 5 residual `Duration::ZERO` sites.** See Critical #1. Threading retention through `handle_duplicated_requests`, `terminate_inboxed_invocation`, `terminate_scheduled_invocation`, and `consume_inbox` drain is mechanical — the data is in scope at every call. Estimated change: ~20 lines.

2. **Extend the retention-inheritance test to cover the 5 residual sites** (or at least the two most likely: `terminate_inboxed_invocation` via `TerminateInvocation` command on a linked inboxed child, and `consume_inbox` drain via CompleteService on a VO with inboxed linked children). Without test coverage on the residual sites, future refactors will silently reintroduce the ZERO default.

3. **Extend the test to cover `resume_completing_invocation` and `CompleteServiceCommand` paths.** The new test asserts only `end_invocation` and short-circuit; the other two paths that pass real retention are not covered by the assertion. A regression here would be silent. Easiest: parameterize `fire_service_completion_inherits_parent_retention` over the path.

### Minor

4. **Clarify the "dead param" comment at `mod.rs:3851-3852`.** Either enumerate the provably-dead sites, or reword to "retention for spawned ServiceCompletion handlers; caller is responsible for passing parent retention whenever a ServiceCompletion sink may be present."

5. **Consider a typed wrapper for the retention pair.** `(Duration, Duration)` at 7 call sites is error-prone (swap risk). A `SpawnedHandlerRetention { completion: Duration, journal: Duration }` newtype would make accidental swaps impossible and would carry a natural `::ZERO` constant. Minor ergonomic win, not required.

6. **Add `#[must_use]` or an invariant assert on the retention pair at the top of `fire_service_completion`.** If both are zero, the spawned handler is purged immediately — that's the "bug" case this commit fixes. A debug-only warn log (`if completion_retention == ZERO && journal_retention == ZERO { warn!("spawned handler will be purged immediately") }`) would make future regressions observable. Optional.

## Alternative Approaches

- **Store retention on `ServiceCompletionTarget`.** Instead of threading retention through 4+ call sites and 5 "dead" ones, add `completion_retention_duration: Duration` and `journal_retention_duration: Duration` fields to `ServiceCompletionTarget` itself. The sink captures the retention at the time it's registered (on the parent side, from the parent's metadata). `fire_service_completion` then consumes them directly from the target, no extra params needed. This is strictly more correct than inheriting from the terminating handler's context and eliminates Critical #1 entirely — the retention travels with the sink, and `send_response_to_sinks` no longer needs the two extra params. Estimated change: proto field addition + matching code changes at ~4 sink-registration sites; removes the new fields from `LinkRequest` and eliminates the 7-param signature. **Strongly recommended as a follow-up.** This was likely considered and deferred; it's the "right" long-term shape.

- **Use a default-retention configuration knob.** If sink-scoped retention is too invasive, fall back to a configured `default_spawned_handler_retention` in the worker config and use it as the floor. This avoids the "all ZERO" failure mode at the cost of introducing a new config option. Weaker than the above but less invasive.

## Comprehensive Scores (0-10)

- **Security Posture**: 9 — no new attack surface; proto change is backward-compatible
- **Logic Correctness**: 7 — headline case fixed, but 5 residual call sites leak `Duration::ZERO` for `ServiceCompletion` sinks in edge-case paths
- **Code Quality**: 8 — clean struct-update syntax; honest doc comment; 7-param signature is getting long
- **Production Readiness**: 7 — ship the headline fix, file a follow-up for the residual sites

## Prioritized Action Plan

1. **(Priority 1)** Fix the 5 residual `Duration::ZERO` sites (Critical #1). Threading retention is mechanical — 4 sites have PreFlightInvocationMetadata in scope, 1 has the ServiceInvocation directly. Estimated ~20 lines.
2. **(Priority 1)** Add test coverage for at least 2 of the 5 residual sites (`terminate_inboxed_invocation`, `consume_inbox` drain) to prevent regression.
3. **(Priority 2)** Extend `fire_service_completion_inherits_parent_retention` to also assert on `resume_completing_invocation` and `CompleteServiceCommand` paths.
4. **(Priority 2)** Clarify the `CompleteServiceCommand` retention choice in the comment (Critical #2).
5. **(Priority 3)** Clarify the `send_response_to_sinks` dead-param doc comment.
6. **(Priority 3)** Consider the `ServiceCompletionTarget`-carried retention refactor as a larger follow-up that would eliminate the whole class of threading bugs.

## Specific Answers

**(a) Are all `Duration::ZERO` call sites truly dead?**

**No, only 3 of 8 are provably dead.**

- **Provably dead (safe):** `mod.rs:1461`, `mod.rs:1530` (exclusive-handler Completed rejection — `on_link_from_invocation` short-circuits the Completed case at line 883 before reaching these); `mod.rs:5388`, `mod.rs:5410`, `mod.rs:5423` (attach-invocation — sinks are always `PartitionProcessor` by construction at line 4961).
- **NOT provably dead (residual bug):** `mod.rs:1335` (workflow dup), `mod.rs:1373` (idempotent replay against Completed), `mod.rs:2753` (terminate inboxed), `mod.rs:2863` (terminate scheduled), `mod.rs:4204` (inbox drain of Completed VO). All five can receive a `ServiceCompletion` sink via the `StartLinkedCommand` piggyback path, because `on_link_from_invocation:923-924` explicitly lets the ServiceCompletion sink flow into `response_sinks` via `from_service_invocation`. Spawned error handlers in these edge cases still inherit `Duration::ZERO`.

**(b) Is the `CompleteServiceCommand` retention choice semantically correct?**

**Defensible, but worth documenting.** Inheriting from the completing handler invocation is the only retention value in scope at that point and is a reasonable default. It is not exactly "the parent's retention" — for VO sinks that joined during the VO's lifetime, the retention is the *completing handler's*, not the *original linking parent's*. The two usually coincide in practice, but an explicit comment would help future readers. Alternative Approach #1 (`ServiceCompletionTarget`-carried retention) would eliminate the ambiguity entirely.

**(c) Does the test cover both the happy path (`end_invocation`) and the short-circuit path (`on_link_request`) with strong assertions?**

**Yes, both with strong assertions.** `fire_service_completion_inherits_parent_retention` (`tests/linked_services.rs:3084-3261`) exercises both paths with realistic non-zero retention values (`3600s` / `1800s`) and asserts `assert_eq!(spawned.completion_retention_duration, parent_completion_retention)` and the matching journal retention assertion on both paths. No tautological shape-only checks.

**Gap:** `resume_completing_invocation` and `CompleteServiceCommand` paths (both fixed) are not asserted by the new test. Also, none of the 5 residual `Duration::ZERO` sites are covered (but they arguably shouldn't be until they're fixed — an asserting test there would fail today).

---

**Overall verdict:** The commit materially improves the situation for the headline IMPORTANT-1 case and the test coverage is honest and strong for the fixed paths. It should land, but with a follow-up issue filed to (1) address the 5 residual `Duration::ZERO` sites or prove them dead, and (2) consider the `ServiceCompletionTarget`-carried retention refactor, which would collapse the whole parameter threading into a cleaner shape.
