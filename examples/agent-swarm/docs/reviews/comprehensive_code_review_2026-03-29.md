# Agent Swarm Demo — Comprehensive Code Review

**Date**: 2026-03-29
**Scope**: Full implementation review (correctness, architecture, readability, test coverage)
**Context**: Example project for Restate repo demonstrating SSE state streaming with agent swarm visualization

## Summary Assessment

Well-structured demo that effectively showcases the Restate SSE state streaming feature. The architecture — one virtual object as centralized state store, workflow agents writing back to it, single SSE connection driving the frontend — is clean and pedagogically sound. Code is readable, test mock is solid, separation of concerns is appropriate for a demo.

**Overall**: Good. Ready to ship with a small number of targeted fixes.

## Scores (0-10)

| Area | Score | Notes |
|------|-------|-------|
| Security Posture | 7 | Local demo, no auth needed. LLM input not sanitized but acceptable for demo. |
| Logic Correctness | 7 | C1 (stale detail panel) and C2 (fragile parent derivation) are the main issues. |
| Code Quality | 8 | Clean, readable, good separation. Minor rough edges. |
| Production Readiness | N/A | This is a demo, not production code. Appropriate for its purpose. |

## Findings

### 🚨 CRITICAL Issues

None.

### 🔥 HIGH Priority

**1. [C1] Detail panel shows stale data** — `ui/index.html:223`

Clicking a node copies its state into `$selData` as a snapshot. The detail panel never updates as SSE pushes new state. A user clicking a "running" node will see "running" forever even after the node completes.

- **Impact**: Misleading UI during the demo's most important moment
- **Fix**: Read the selected node live from `sf.records` instead of copying into `$selData`

**2. [C2] Parent derivation via string splitting is fragile** — `agents.ts:133,143,175,185`

`nodeId.split("--").slice(0, -1).join("--")` derives the parent ID. Works today but breaks if topic IDs contain `--`. Unnecessary since each agent already knows its parent.

- **Impact**: Could break with certain LLM-generated topic names
- **Fix**: Pass `parentId` explicitly in workflow request payloads

### ⚠️ MEDIUM Priority

**3. [R1] Sequential search execution limits visual "swarm" effect** — `agents.ts:51-72`

ResearchAgent awaits each SearchAgent sequentially. Search nodes appear one at a time instead of fanning out. Undercuts the visual impact for a demo meant to showcase parallel execution.

- **Benefit**: Much more visually compelling demo
- **Effort**: Medium — requires fire-and-forget + callback pattern instead of sequential await

**4. [R7] Test count assertion is brittle** — `test/agent-swarm.e2e.test.ts:63`

Magic number `14` (13 nodes + 1 badge) breaks if mock timeline changes. Extract as named constant.

- **Benefit**: Test maintainability
- **Effort**: Low

### 💡 LOW Priority

**5. [R3] Journaled ctx.sleep() calls for visual effect** — `agents.ts:137,179`

Sleep calls exist for streaming visual effect but are journaled by Restate. Add a comment noting these are demo-only.

**6. [R4] LLM ID parsing edge case** — `llm.ts:73`

Post-regex empty string not handled. Add `|| "area-" + index` fallback.

**7. [R5] `frame({ size: 200 })` is arbitrary** — `index.html:174`

Add comment explaining the large page size avoids pagination for expected node counts.

**8. [R6] Vendored idb.ts referenced but unused** — `lib/data-graph/data.ts:95`

Dynamic import of `./idb.js` works because it's never called, but add a comment for anyone trying to use IDB().

**9. [C3] Hardcoded `demo-1` key** — `ui/index.html:172,189,190`

Extract to a single variable or URL parameter for instructiveness.

## Prioritized Action Plan

1. **Fix C1** — Live detail panel (HIGH, prevents stale UI)
2. **Fix C2** — Explicit parentId in workflow payloads (HIGH, prevents fragile string parsing)
3. **Consider R1** — Parallel search execution (MEDIUM, visual improvement)
4. **Fix R7** — Extract test magic number (LOW, maintainability)
5. **Add comments** — R3, R5, R6 (LOW, documentation)
