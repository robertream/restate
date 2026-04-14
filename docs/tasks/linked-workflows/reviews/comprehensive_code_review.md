# Linked Workflows MVP - Phases 1 & 2 Code Review

**Date**: 2026-04-02
**Scope**: Phase 1 (Link Table + CreateLink) + Phase 2 (Completing + LinkCompletionNotification)
**Status**: All findings resolved.

**Scores**: Security 9/10 | Logic Correctness 9/10 | Code Quality 8/10 | Production Readiness 9/10

---

## Findings (All Resolved)

### CRITICAL — RESOLVED

**C1: Virtual Object parent lookup uses wrong InvocationQuery variant**
- **Resolution**: VOs no longer enter Completing. Workflow-only guard added at `mod.rs:2772`. Commit `f9a47237a`.

### HIGH — RESOLVED

**C2: Code duplication between completion paths**
- **Resolution**: Extracted `transition_completing_to_completed` helper. Commit `6a5eb926d`.

**R1+R2: Double link table scan in notification handler**
- **Resolution**: Removed label, keyed by `child_service_id` for O(1) lookup. Single scan for remaining Running links. Commits `f9a47237a`, `0ad1f9c53`.

**R5: No test for virtual object parents**
- **Resolution**: VOs cannot enter Completing (by design), so VO parent test is not applicable. Design decision documented.

### MEDIUM — RESOLVED

**R4: Cancel on Completing returns AlreadyCompleted**
- **Resolution**: Phase 3 replaced with proper cancel propagation to children. Commit `6a5eb926d`.

**R3: update_link_state does unnecessary read-then-write**
- **Status**: Accepted as-is. The read-then-write is correct and the path is infrequent (only on child completion). Not a hot path concern.

**R6: Link sink carries minimal context**
- **Resolution**: Label removed, `child_service_id` is derived from `invocation_target`. The warn fallback is a defense-in-depth — the target is always present for keyed services.

### LOW — ACCEPTED

**R7: Completing + restart-as-new error message**
- **Status**: Accepted. `StillRunning` is technically correct (invocation hasn't externally completed). Cosmetic only.
