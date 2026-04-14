# Code Review: Invoker Deployment Performance Metrics

**Date**: 2026-04-07
**Reviewer**: Independent code review agent
**Scope**: 8 modified files implementing #4553 + #4454

## Summary Assessment

Implementation is well-structured and follows established codebase patterns. One critical bug (gauge leak on abort), one important gap (HTTP total duration not recorded on error), and several minor issues.

**Scores (0-10):**
- Security Posture: 9 — No security concerns; metrics-only changes
- Logic Correctness: 7 — Critical gauge leak on abort paths; error path gap
- Code Quality: 8 — Clean design, follows patterns, minor naming issues
- Production Readiness: 7 — Must fix gauge leak before merge

## Critical Issues

### 1. Active invocations gauge leak on abort paths (CRITICAL)
**Files**: `lib.rs:1387-1413` (`handle_abort_invocation`), `lib.rs:1499-1521` (`handle_abort_partition`)

Neither abort handler calls `decrement_active_invocations(&ism)`. If an invocation has received `PinnedDeployment`, aborting it permanently leaks +1 on the gauge. Especially problematic for `handle_abort_partition` (leadership changes/shutdown) which can leak many at once.

**Fix**: Add `decrement_active_invocations(&ism);` after removing ISM from manager in both functions.

## Important Issues

### 2. HTTP total duration not recorded on error (IMPORTANT)
**File**: `mod.rs:713-748`

When `ResponseStream` returns an error, the `_ => {}` match arm drops without recording `INVOKER_HTTP_TOTAL_DURATION`. Operators can't see how long failed HTTP requests took.

**Fix**: Implement `Drop` for `InstrumentedResponseStream` to record total duration unconditionally, removing the inline recording to avoid double-count.

### 3. INVOKER_ENQUEUE label set inconsistency (IMPORTANT — pre-existing, deferred)
**File**: `lib.rs:472-487`

`Invoke` path: `{partition_id, service_name}`. `VQInvoke` path: `{status, partition_id, service_name}` with `status => TASK_OP_COMPLETED` which is semantically wrong for an enqueue event. Pre-existing issue; tracked for follow-up.

## Minor Issues

### 4. Throttle balance description says "sampled periodically" but implementation is event-driven
**File**: `metric_definitions.rs:135`

### 5. Anonymous tuple for active_invocation_labels
**File**: `invocation_state_machine.rs:64` — `Option<(String, String)>` less readable than named struct

### 6. Empty string deployment_id label when not pinned
**File**: `mod.rs:433`, `lib.rs:1813` — Acceptable but may cause dashboard confusion

## Prioritized Action Plan

1. **MUST FIX**: Gauge leak on abort paths (#1)
2. **SHOULD FIX**: HTTP total duration on error via Drop impl (#2)
3. **FOLLOW-UP**: INVOKER_ENQUEUE label inconsistency (#3)
4. **OPTIONAL**: Description fix (#4), naming (#5), empty string (#6)
