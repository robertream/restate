# Review: Merge CreateLink into OneWayCallCommand

**Date**: 2026-04-03
**Scope**: Commit 32a483e44 — 15 files, +100/-243
**Verdict**: Approve. No critical issues.

**Scores**: Security 9/10 | Logic Correctness 9/10 | Code Quality 9/10 | Production Readiness 9/10

## No Critical or High Issues

The merge is clean. `linked: bool` correctly wired through proto (field 8), codec encode/decode, invoker, and state machine. All CreateLink remnants removed. `CallCommand` hardcodes `linked: false`. Link logic in `_ApplyCallCommand` is complete: keyed service validation, self-link guard, `are_linked` cycle check, `put_link`, AttachInvocation with Link sink.

## Minor Recommendations

1. **Invoker-side precondition check** for `linked=true` on non-keyed callers — would give SDK developers faster error feedback. Follow-up.
2. **`OneWayCallCommandLite` omits `linked` field** — linked calls indistinguishable from regular one-way calls in journal UI. Consider adding for observability.
3. **Error message "Link cycle detected"** is slightly misleading — it's really "these services already have a direct link." Cosmetic.
4. **No tests for `linked=true` path** — all tests use direct storage manipulation. A test exercising the linked OneWayCallCommand through the state machine would improve confidence.
