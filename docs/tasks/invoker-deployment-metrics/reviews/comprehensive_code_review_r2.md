# Code Review Round 2: Invoker Deployment Performance Metrics

**Date**: 2026-04-07 | **Scores**: Security 9, Correctness 9, Quality 8, Production Readiness 9

## Summary

All round 1 issues fixed. No critical or high-priority issues. Ready to merge.

## Verified Fixes
- Gauge leak: all terminal paths decrement (abort, abort_partition, pause, error, yield, shutdown)
- Drop impl: no double-counting, covers all exit paths
- LabelLookup<K>: generic design sound, no leak races under concurrency
- ServiceLabels: correctly threaded ISM → TaskRunner → Task → protocol runners
- No remaining .to_string()/.clone() on hot-path labels (only status code on error path)

## Findings

### Important (design consideration, not blocker)
1. **V4 drain inflates http_total_duration** — v4 runner drains response stream for up to 5s after protocol completes. Drop fires after drain, inflating total duration. Acceptable for initial ship; document if operators report confusion.

### Minor
2. **Throttle balance description says "sampled periodically"** — should say "recorded on acquire/release"
3. **INVOKER_ENQUEUE label inconsistency** — pre-existing, deferred

## Action Plan
1. OPTIONAL: Fix throttle balance description (one-line)
2. DOCUMENT: Add comment near Drop impl noting v4 drain inclusion
3. DEFER: Status code allocation, INVOKER_ENQUEUE inconsistency
