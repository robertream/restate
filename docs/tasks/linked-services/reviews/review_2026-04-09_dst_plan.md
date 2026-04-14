# Plan Review: DST Phase 0-1 — Simulation Testing

**Date**: 2026-04-09
**Reviewer**: Staff Engineer (Independent Review)
**Plan**: `docs/tasks/linked-services/specs/plan_dst_phase1.md`

## Verdict: Simplify Significantly

The plan targets the right area (`StateMachine::apply` is near-pure) but front-loads too much infrastructure before proving the approach works. The MVP should be **one file, ~150 lines, no oracle**.

## Recommendations (Prioritized)

### HIGH: Replace SimState with a HashSet

**What**: The `SimState` with `HashMap<InvocationId, SimInvocationState>` is an oracle — a second implementation of the state machine logic. Building a correct oracle is the most error-prone part of the plan.

**Simplification**: Track only which `InvocationId`s have been created (a `HashSet<InvocationId>`). Use this solely for generating valid follow-up commands. Check invariants against *real storage* via `TestEnv.storage`, not against a shadow model.

**Impact**: Removes the most fragile component. All requirements still met.

### HIGH: Start with 3-4 command types, uniform distribution

**What**: The precise weight system (40% invoker, 25% invoke, etc.) is premature. No data supports these ratios.

**Simplification**: Start with just:
- `Command::Invoke`
- `Command::InvokerEffect` (entry + end + suspended + pinned)
- `Command::TerminateInvocation`
- `Command::AnnounceLeader`

Uniform random. Add more types one at a time. Add weights when data shows certain mixes find more bugs.

### HIGH: Use storage-level invariants, not model-level

**What**: "Status consistency against SimState" requires a correct oracle.

**Simplification**: Check against real storage:
1. No panics (apply doesn't crash)
2. Valid invocation status (not corrupted)
3. Journal contiguity (entries 0..N all exist)
4. Completed invocations have results

No oracle needed.

### MEDIUM: Single file, not a subdirectory

**What**: 3 files in a `simulation/` subdirectory is over-sized for the MVP.

**Simplification**: Start with `simulation.rs`. Split when it exceeds ~500 lines.

### MEDIUM: Existing mock helpers use unseeded RNG

**What**: `InvocationId::mock_random()`, `ServiceInvocation::mock()` etc. use `rand::rng()` (global thread-local, not seedable). This silently breaks reproducibility.

**Fix**: Construct `InvocationId` and `ServiceInvocation` directly using the seeded RNG.

### MEDIUM: Parameterize `is_leader` in Phase 0a

**What**: `TestEnv::apply` hardcodes `is_leader: true` (line 166). The motivating bug (#4566) is about leadership transitions.

**Fix**: `apply_with` should accept `is_leader: bool` as well as time/LSN.

### LOW: Leadership transitions deserve targeted generation

**What**: Leadership flips at varied points in the invocation lifecycle (not just uniform) is where real bugs live.

**Suggestion**: After MVP works, bias `AnnounceLeader` to happen mid-lifecycle (between Invoke and InvokerEffect::End), not just uniformly.

## Testing Review

The plan describes one "property test" that runs many seeds. This is the right approach for simulation testing — it's not a traditional test with happy/unhappy paths, it's a fuzzer.

- **No over-testing**: The plan is appropriately scoped.
- **Missing critical path**: The `is_leader` parameter MUST be varied. Without it, the test can't catch the bug class that motivated this work.
- **Test complexity**: The SimState oracle adds unnecessary complexity. Storage-level invariants are simpler and more trustworthy.

## On proptest

Correctly avoided. proptest's shrinking doesn't work well for stateful sequences where removing a command changes the meaning of all subsequent commands.
