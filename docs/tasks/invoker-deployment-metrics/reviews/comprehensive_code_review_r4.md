# Code Review R4: invoker-deployment-metrics

**Branch**: `invoker-deployment-metrics`
**PR**: #4567
**Date**: 2026-06-08
**Reviewer**: spectre:reviewer (automated)

---

## Summary Assessment

The implementation is well-structured with good attention to performance (zero-allocation interning, cached metric handles). The `LazyIntern`, `ServiceMetrics`, and `InstrumentedResponseStream` abstractions are clean. However, there is one critical correctness bug introduced by the recent "unknown" placeholder change, and one recurring high-severity label inconsistency.

**Scores (0-10):**
- Security Posture: 9/10 — No secrets exposure. Intentional leaks documented.
- Logic Correctness: 6/10 — CRITICAL-1 (negative gauge) is production-visible.
- Code Quality: 8/10 — Clean abstractions, good separation of concerns.
- Production Readiness: 6/10 — CRITICAL-1 must be fixed before merge.

---

## Strengths

- **LazyIntern** — Clean, generic, thread-safe interning with documented cardinality warnings
- **ServiceMetrics** — `Copy` derive makes passing zero-cost; encapsulates label construction
- **InstrumentedResponseStream** — TTFB in `poll_next`, roundtrip in `Drop` ensures metrics always recorded
- **Network metrics hierarchy** — Clean swimlane-based scoping with legacy metric preservation
- **Discovery metrics** — Good outcome categorization
- **test_metrics() helpers** — Cleaner than `ServiceMetrics::EMPTY`

---

## 🚨 CRITICAL Issues

### CRITICAL-1: Active invocations gauge will go negative on early failures

**File**: `crates/invoker-impl/src/lib.rs:1844-1848`, `crates/invoker-impl/src/metric_definitions.rs:90`

`deployment_id` is now initialized to `"unknown"` (non-empty). `decrement_active_invocations` guards with `!ism.metric.deployment_id.is_empty()`. Since `"unknown"` is not empty, the decrement fires on every invocation termination, even if `handle_pinned_deployment` was never called (where the increment happens). Invocations that fail before deployment pinning will decrement without a prior increment, causing negative gauge values.

**Fix**: Change guard to `deployment_id != "unknown"`, or revert to initializing as `""` and use `"unknown"` only in the emission methods.

---

## 🔥 HIGH Priority

### HIGH-1: Inconsistent label sets for INVOKER_INVOCATIONS_QUEUED

**File**: `crates/invoker-impl/src/lib.rs:475-490`

`Invoke` path emits `{partition_id, service_name, deployment_id}` via `ServiceMetrics::counter()`. `VQInvoke` path emits `{status, partition_id, service_name}`. Different label sets on same metric cause Prometheus cardinality problems.

### HIGH-2: Redundant invoker_id label in concurrency slot metrics

**File**: `crates/invoker-impl/src/quota.rs:57-61`

Both `invoker_id` and `partition_id` labels emitted with identical values. Doubles cardinality unnecessarily.

---

## ⚠️ MEDIUM Priority

- **MEDIUM-1**: LazyIntern unbounded growth has no monitoring (`crates/core/src/metric_definitions.rs:34-61`)
- **MEDIUM-2**: HTTP status codes recorded only for non-success responses (`crates/invoker-impl/src/invocation_task/mod.rs:700-708`)
- **MEDIUM-3**: INVOKER_THROTTLING_BALANCE emitted with deployment_id="unknown" before pinning
- **MEDIUM-4**: Network legacy metrics now have additional labels — needs release note

---

## 💡 LOW Priority

- **LOW-1**: Module-level `#[allow(unused)]` in `crates/core/src/metric_definitions.rs:11`
- **LOW-2**: `IdLookup::SIZE = 512` pre-allocates/leaks for most deployments using 24-64 partitions
- **LOW-3**: Discovery `describe_metrics()` only called inside `ServiceDiscovery::new()`

---

## Prioritized Action Plan

1. **[MUST FIX]** Fix `decrement_active_invocations` guard for un-pinned deployments (CRITICAL-1)
2. **[MUST FIX]** Unify `INVOKER_INVOCATIONS_QUEUED` label sets (HIGH-1)
3. **[SHOULD FIX]** Add release note for legacy network metric label changes (MEDIUM-4)
4. **[SHOULD FIX]** Consider recording 2xx status codes (MEDIUM-2)
5. **[NICE TO HAVE]** Clean up redundant `invoker_id` label (HIGH-2)
6. **[NICE TO HAVE]** Add cache size monitoring to `LazyIntern` (MEDIUM-1)
7. **[NICE TO HAVE]** Remove `#[allow(unused)]` (LOW-1)
