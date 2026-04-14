# Comprehensive Code Review: Invoker Deployment Metrics (R3)

**Date**: 2026-04-07
**Reviewer**: Claude Opus 4.6 (spectre:reviewer)
**Branch**: invoker-deployment-metrics
**Commit**: d925848a7

## Summary Assessment

Well-structured implementation. ServiceMetrics/TaskMetrics abstractions are clean and correct. Active invocations gauge is fully balanced across all paths. InstrumentedResponseStream TTFB/total duration measurement is correct. One allocation issue on the hot path and a cardinality question to resolve.

**Overall: Ready to merge with one targeted fix.**

## Scores (0-10)

- **Security Posture**: 9 — No vulnerabilities. Metrics don't expose sensitive data.
- **Logic Correctness**: 9 — Gauge balance verified across all paths. TTFB measurement correct.
- **Code Quality**: 9 — Clean abstractions, good encapsulation, zero-allocation design.
- **Production Readiness**: 8 — One hot-path allocation to fix (status_code.to_string).

## Findings

### 🚨 CRITICAL

**C1: `status_code.to_string()` allocates on hot path** (`metric_definitions.rs:178,185`)

`http_status_counter(status_code: u16)` calls `.to_string()` on every non-200 response. Allocates a String per call. `ID_LOOKUP` already handles `u16 -> &'static str` interning with fast path for values < 512 (all HTTP codes are 100-599).

**Fix**: `let code_str = ID_LOOKUP.get(status_code);` — one-line change, zero allocation.

**Cardinality note**: HTTP has ~60 defined codes. Proxies can return arbitrary values (499, 520-530). Consider bucketing into classes ("4xx"/"5xx") if cardinality is a concern, or accept bounded cardinality with exact codes.

### 🔥 HIGH

None.

### ⚠️ MEDIUM

**M1: TTFB metric description slightly misleading** (`metric_definitions.rs` describe call)

`started_at` is set after `ResponseStream::initialize()` spawns the HTTP task. The measurement includes connection setup (DNS/TLS). Description says "HTTP request send to response headers received."

**Fix**: Update to "Time from HTTP request initiation to response headers received (includes connection setup)".

**M2: Throttle balance only recorded at task start, not completion**

`record_throttle_balance` called in `handle_invoke` and `handle_vqueue_invoke` but not on task end. Gauge goes stale when system is idle. Getting token bucket reference to the release path requires structural changes.

**Recommendation**: Document as known limitation. Address in future iteration.

### 💡 LOW

**L1: VQInvoke enqueue has legacy "status" label** (`lib.rs:485-490`)

Already documented with inline comment. Deferred to future cleanup.

## Verified Correct

- **Active invocations gauge balance**: Every increment at PinnedDeployment has matching decrement on all terminal paths (closed, suspended, failed, abort, pause, yield). Empty-deployment_id guard prevents spurious operations.
- **ServiceMetrics Copy semantics**: Safe — mutations happen on separate copies in single-threaded event loop and spawned task.
- **Queue duration timing**: Correct — Instant::now() at construction, elapsed at dequeue.
- **deployment_id empty-string branching**: Correct — omits label when empty, separate time series.
- **InstrumentedResponseStream Drop**: Correct — records total duration on all exit paths including error/abort.

## Prioritized Action Plan

1. Replace `status_code.to_string()` with `ID_LOOKUP.get(status_code)` in `http_status_counter`
2. Update TTFB metric description (optional)
3. Document throttle balance staleness (optional)
