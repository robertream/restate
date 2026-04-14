# Full Branch Review — Linked Workflows

**Date**: 2026-04-03
**Scope**: 15 commits, 49 files, linked-workflows branch
**Status**: No critical bugs. Important design concerns flagged.

**Scores**: Security 9/10 | Logic Correctness 9/10 | Code Quality 8/10 | Production Readiness 8/10

## Important (address before merge)

**R1: CreateLinkCompletion lacks error signal for cycle rejection**
- `create_link_command.rs:64-68`, `notification.rs:328-330`
- SDK receives same completion for success and rejection — can't distinguish "link created" from "link rejected"
- Fix: add result field to CreateLinkCompletion (Success/AlreadyLinked)

**R2: Cancel logic duplicated between mod.rs and cancel.rs**
- Legacy path (mod.rs) uses `cancel_linked_children` helper. V4+ path (cancel.rs) inlines the same logic.
- Fix: refactor cancel.rs to use the helper

**R3: HandlerInvocation drops failure information**
- `mod.rs:2968-2971` — failure result passed as `Bytes::empty()` to onCompleted handler
- Handler can't distinguish success with empty body from failure
- Fix: encode InvocationError as argument, or add failure header

## Moderate (address soon after merge)

**R4: Double output entry read for workflows without links**
- `end_invocation` reads output entry twice: once for link check, once for response delivery
- Fix: read once, reuse

**R5: transition_completing_to_completed duplicates end_invocation logic**
- ~90 lines of shared completion logic in two places
- Fix: extract shared `finalize_invocation` helper

**R6: get_children_of scan on every LinkCompletionNotification**
- O(N) scan per notification, O(N²) total for N children
- Acceptable for small N (typical). Counter-based optimization deferred.

## Minor

- `delete_all_links_for` collects all keys before deleting (follows codebase pattern, fine)
- `LinkState::Completed` on ChildOf records is unused (type-level distinction not worth the complexity)
- `PurgeInvocationResponse::NotCompleted` reused for parent-linked rejection (imprecise but functional)
- Direct-only cycle prevention (transitive cycles A→B→C→A not detected — documented limitation)

## End-to-End Correctness

Lifecycle verified correct: CreateLink → Completing → LinkCompletionNotification → Completed. All race conditions handled (stale messages ignored, duplicate AttachInvocation guarded, concurrent completion/cancel paths converge correctly). Cross-partition ordering is safe — all operations are idempotent.

## Test Coverage

11 tests cover critical paths well. Missing edge cases for later: notification on already-Completed parent, CreateLink on non-keyed service, concurrent Kill + notification, self-link dedicated test.
