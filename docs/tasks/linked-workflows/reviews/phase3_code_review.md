# Phase 3 Code Review — RemoveLink + Cancel Propagation

**Date**: 2026-04-03
**Scope**: Phase 3 commit (6a5eb926d) — 17 files, +439/-97 lines
**Status**: All findings resolved.

**Scores**: Security 9/10 | Logic Correctness 9/10 | Code Quality 8/10 | Production Readiness 9/10

## Critical — RESOLVED

**C1: Cancel from Invoked/Suspended/Paused does NOT propagate to linked children**
- **Resolution**: Added `cancel_linked_children` helper called from all cancel paths (Invoked/Suspended/Paused + Completing). Test `cancel_invoked_parent_propagates_to_linked_children` verifies. Commit `9bfc013c6`.

**C2: RemoveLinkCommand does NOT trigger Completing→Completed**
- **Resolution**: Confirmed unreachable — entry processing guard prevents RemoveLink from running during Completing. By design, not a bug.

## Important — RESOLVED

**I1: Kill path for Completing has stale TODO and potential re-entry**
- **Resolution**: Kill on Completing now calls `transition_completing_to_completed` directly, bypassing `end_invocation` and its link barrier. Stale TODO removed. Linked children are killed via `cancel_linked_children`. Test `kill_completing_parent_finalizes_immediately` verifies. Commit `9bfc013c6`.

**I2: Duplicated cancel-propagation logic**
- **Resolution**: Extracted `cancel_linked_children` helper used by all 3 call sites (cancel from Invoked/Suspended/Paused, cancel from Completing, kill from Completing). Commit `9bfc013c6`.

**I3: Link barrier bypassed when no response sinks and zero retention**
- **Resolution**: Link check moved outside the sinks/retention guard. Workflows with running links always enter Completing. Test `completing_entered_with_zero_retention_and_no_sinks` verifies. Commit `9bfc013c6`.

## Low — ACCEPTED

- Clarifying comment for RemoveLinkCommand re: entry guard — accepted as-is, behavior is correct.
- Unused trait bounds on `ApplyRemoveLinkCommand` — come from `ApplyJournalCommandEffect` wrapper, not worth special-casing.
