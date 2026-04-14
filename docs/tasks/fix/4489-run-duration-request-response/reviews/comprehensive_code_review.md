# Comprehensive Code Review: Fix #4489 — Run Duration in Request-Response Mode

## Work & File Scope Boundary Validation

**Completed work**: Server-side support for SDK-reported `attempt_duration_ms` on run completions, fixing #4489 where `ctx.run` durations show 0ms in request-response (Lambda) mode.

**Files Modified:**
- `service-protocol/dev/restate/service/protocol.proto` — added `attempt_duration_ms` to `ProposeRunCompletionMessage` and `RunCompletionNotificationMessage`
- `crates/types/src/journal_v2/notification.rs` — added `attempt_duration: Option<Duration>` to `RunCompletion`
- `crates/service-protocol-v4/src/entry_codec.rs` — encode/decode `attempt_duration_ms` in RunCompletion codec
- `crates/invoker-impl/src/invocation_task/service_protocol_runner_v4.rs` — parse `attempt_duration_ms` from `ProposeRunCompletionMessage`
- `crates/worker/src/partition/state_machine/entries/notification.rs` — use SDK-provided duration for trace span start time

## Summary Assessment

**Overall**: Well-scoped, correctly implemented, backward-compatible fix. Minimal surface area (5 files), clean encode/decode symmetry, proper fallback for older SDKs. No security concerns. Minor style issue with `SystemTime::now()` usage.

**Risk Level**: Low

## Detailed Findings by Severity

### 🚨 CRITICAL Issues
None.

### 🔥 HIGH Priority
None.

### ⚠️ MEDIUM Priority

1. **`std::time::SystemTime::now()` usage** (`notification.rs:74`)
   - The project guidelines encourage `restate_clock::WallClock`/`MillisSinceEpoch::now()` over direct `SystemTime::now()`.
   - **Impact**: Minor inconsistency. Mitigated by being on a tracing-only path (guarded by `is_service_tracing_enabled()` + sampling).
   - **Recommendation**: Replace with `MillisSinceEpoch::now()` arithmetic and convert to `SystemTime`.

2. **Double decode of notification** (`notification.rs:69-73`)
   - The notification is decoded just to extract `attempt_duration`, adding one extra protobuf decode.
   - **Impact**: Negligible — only runs when tracing enabled + sampled + Run command type.
   - **Recommendation**: Acceptable as-is. Could be optimized later if profiling shows concern.

3. **No release notes entry**
   - CLAUDE.md instructs proposing changes to `release-notes/unreleased/` for behavior changes. Improved `ctx.run` duration reporting in Lambda mode is user-visible.
   - **Recommendation**: Add a brief release note mentioning improved run duration accuracy in request-response mode.

### 💡 LOW Priority

4. **Proto field number 2 gap in `ProposeRunCompletionMessage`**
   - Fields 1 and 3 are used, 2 is skipped. Valid protobuf but field 2 is effectively lost.
   - **Impact**: None. Field numbers don't need to be contiguous.

5. **Theoretical truncation in `as_millis() as u64`** (`entry_codec.rs:524`)
   - `Duration::as_millis()` returns `u128`, cast to `u64`. Truncates for durations > ~584M years.
   - **Impact**: Impossible in practice for run block durations.

## Comprehensive Scores (0-10)

| Dimension | Score | Notes |
|---|---|---|
| **Security Posture** | 10 | No security concerns; field is observability-only |
| **Logic Correctness** | 10 | Proper encode/decode symmetry, correct fallback |
| **Code Quality** | 8 | Minor `SystemTime::now()` style issue |
| **Production Readiness** | 9 | Missing release notes; otherwise ready |

## Prioritized Action Plan

1. Replace `std::time::SystemTime::now()` with `MillisSinceEpoch`-based arithmetic (MEDIUM)
2. Add release notes entry for improved run duration reporting (MEDIUM)
3. All other findings are informational — no action required
