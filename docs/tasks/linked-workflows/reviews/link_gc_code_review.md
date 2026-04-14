# Link GC Code Review

**Date**: 2026-04-03
**Scope**: Commit 0fc81db3a — 19 files, +858/-171
**Status**: Review complete. No critical blockers. Important items flagged.

**Scores**: Security 9/10 | Logic Correctness 8/10 | Code Quality 8/10 | Production Readiness 8/10

## Important

**I1: VO clear state deletes ChildOf records silently**
- `clear_all_state_command.rs:63` — `delete_all_links_for` removes ALL records including incoming ChildOf
- If VO is a child of a parent, parent's ParentOf record becomes orphaned — parent may stay in Completing forever
- Mitigated: uncommon for a VO to be both parent and child simultaneously

**I2: Self-link (A→A) not guarded**
- `create_link_command.rs:50-60` — no check for parent == child
- Self-link would deadlock: parent waits for itself

**I3: get_children_of scans all links then filters in memory**
- `partition-store/src/link_table/mod.rs:157-163` — scans ChildOf records unnecessarily
- Called on every workflow completion, cancel, purge
- Low impact for small link counts but latent performance issue

**I4: Purge guard only checks workflow invocations**
- `purge.rs:71-73` — guard scoped to `WorkflowHandlerType::Workflow`
- If VOs can create links, their purge path is unguarded

## Medium

- Cycle prevention only detects direct cycles (A→B→A), not transitive (A→B→C→A) — acceptable for MVP
- `PurgeInvocationResponse::NotCompleted` reused for link guard — imprecise but functional
- Proto `EdgeLabel` default = `PARENT_OF` (field 0) — correct for existing data but worth documenting

## Test Quality

- `multilevel_link_gc_cascade` — strong, covers full cascade
- `cycle_prevention_rejects_reverse_link` — partial, only verifies precondition (ChildOf record exists), doesn't exercise CreateLink rejection
- `vo_clear_state_deletes_links_and_notifies_children` — good coverage
