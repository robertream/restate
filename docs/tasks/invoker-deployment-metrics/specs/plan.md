# Implementation Plan: Invoker Deployment Performance Metrics

## Overview

Add deployment-level observability to the invoker subsystem, combining issues #4553 (deployment performance metrics) and #4454 (more invoker metrics) into a single PR. The goal is to let operators distinguish slow user services from internal Restate bottlenecks.

Six new metrics and label enrichment on seven existing metrics, all following established `metrics` crate patterns.

## Desired End State

Operators can answer:
- **"Is my service slow?"** → `http_request_duration.seconds` (TTFB) and `http_total_duration.seconds` per service/deployment
- **"Is Restate the bottleneck?"** → `queue_duration.seconds` shows internal wait time
- **"What's the concurrency?"** → `active_invocations` gauge per service/deployment
- **"Are deployments returning errors?"** → `http_status_code.total` per service/deployment/status
- **"Is the invoker throttled?"** → `throttle_balance` gauge shows token bucket debt
- **"Which service is affected?"** → `service_name` label on all relevant existing metrics

## Out of Scope

- Per-deployment connection pool metrics (depends on #4451)
- Existing label cleanup/normalization (`invoker_id` → `partition_id`)
- Config flags for label cardinality control
- Composite "deployment health" metrics
- VQueue path queue duration (bypasses segment queue)

## Review Decisions

Decisions from plan review (2026-04-07):

1. **Active invocations gauge**: Track only after `PinnedDeployment` when both `service_name` and `deployment_id` are known. One increment, one decrement. Slight undercount during deployment resolution is operationally irrelevant.

2. **ResponseStream instrumentation**: Use a wrapper struct (`InstrumentedResponseStream`) around the inner enum. Shared metadata (`started_at`, `service_name`, `deployment_id`) lives once on the wrapper, not duplicated across variants.

3. **INVOKER_ENQUEUE fix**: Switch the `Invoke` path (lib.rs:467) from `.to_string()` to `ID_LOOKUP.get()` to eliminate per-invocation allocation. The `VQInvoke` path already uses `ID_LOOKUP`. Leave `status` label as-is on both paths for now — note inconsistency for future cleanup.

4. **Throttle balance**: Event-driven recording at slot acquire/release in `quota.rs`, not periodic polling. Avoids adding a timer arm to the hot-path `select!` loop. Note: if dashboard smoothing requires periodic sampling, this can be revisited — but event-driven is correct and zero-cost when idle.

5. **Phase compression**: 6 phases compressed to 3 (Definitions → New Metrics → Label Enrichment).

6. **Testing**: No dedicated metric unit tests. Validation via `cargo check` (compilation) + manual `curl localhost:9070/metrics` smoke test.

## Technical Approach

### Phase 1: Metric Definitions + Label Infrastructure

Add all new metric constants and `describe_*` calls to `metric_definitions.rs`. This is the foundation — all subsequent phases reference these constants.

**Files**: `crates/invoker-impl/src/metric_definitions.rs`

**Work**:
1. Add 6 new metric name constants:
   - `INVOKER_THROTTLE_BALANCE`
   - `INVOKER_QUEUE_DURATION`
   - `INVOKER_ACTIVE_INVOCATIONS`
   - `INVOKER_HTTP_REQUEST_DURATION`
   - `INVOKER_HTTP_TOTAL_DURATION`
   - `INVOKER_HTTP_STATUS_CODE`
2. Add label name constants: `SERVICE_NAME_LABEL`, `DEPLOYMENT_ID_LABEL`, `STATUS_CODE_LABEL`
3. Add `describe_*` calls in `describe_metrics()`

### Phase 2: New Metrics

All new metric instrumentation: queue duration, HTTP latency/status, active invocations, throttle balance.

**Files**: `crates/invoker-impl/src/input_command.rs`, `crates/invoker-impl/src/lib.rs`, `crates/invoker-impl/src/invocation_task/mod.rs`, `crates/invoker-impl/src/invocation_task/service_protocol_runner.rs`, `crates/invoker-impl/src/invocation_task/service_protocol_runner_v4.rs`, `crates/invoker-impl/src/quota.rs`

**Work**:

#### Queue Duration
1. Add `enqueued_at: Instant` field to `InvokeCommand` struct
2. Set `enqueued_at = Instant::now()` at construction (where `InputCommand::Invoke` is created)
3. At `handle_invoke` (~lib.rs:688), record `enqueued_at.elapsed()` as `INVOKER_QUEUE_DURATION` histogram with `partition_id` label

#### HTTP Latency + Status Codes
1. Create `InstrumentedResponseStream` wrapper struct around inner `ResponseStream` enum, carrying `started_at: Instant`, `service_name: ByteString`, `deployment_id: DeploymentId`
2. In the wrapper's `poll_next()`, when inner transitions `WaitingHeaders` → `ReadingBody`:
   - Record `started_at.elapsed()` as `INVOKER_HTTP_REQUEST_DURATION` (TTFB)
   - Extract HTTP status code from response headers
   - If status != 200, increment `INVOKER_HTTP_STATUS_CODE` counter
3. When inner `ResponseStream` reaches `Terminated`, record total duration as `INVOKER_HTTP_TOTAL_DURATION`
4. `service_name` and `deployment_id` are available on `InvocationTask` fields at the point `ResponseStream` is created (after deployment resolution)

#### Active Invocations Gauge
1. Track only after `PinnedDeployment` when both `service_name` and `deployment_id` are known
2. Increment `INVOKER_ACTIVE_INVOCATIONS` gauge when `PinnedDeployment` arrives in the main loop
3. Decrement on task end (completed/failed/suspended)
4. Label with `service_name` + `deployment_id`

#### Throttle Balance
1. In `quota.rs`, record `INVOKER_THROTTLE_BALANCE` gauge at slot acquire and slot release (event-driven, no polling timer)
2. Label with `partition_id`

### Phase 3: Label Enrichment on Existing Metrics

Add `service_name`, `deployment_id`, and `partition_id` labels to existing metrics.

**Files**: `crates/invoker-impl/src/lib.rs`, `crates/invoker-impl/src/invocation_task/mod.rs`, `crates/invoker-impl/src/quota.rs`

**Work**:
1. **`INVOKER_ENQUEUE`**: Add `service_name` label at both recording sites. Fix `Invoke` path to use `ID_LOOKUP.get()` instead of `.to_string()` (allocation fix)
2. **`INVOKER_INVOCATION_TASKS`**: Add `partition_id`, `service_name`, `deployment_id` labels at all recording sites (started/completed/suspended/failed)
3. **`INVOKER_TASK_DURATION`**: Add `service_name`, `deployment_id` labels at mod.rs:412
4. **`INVOKER_EAGER_STATE_TRUNCATED`**: Add `service_name` and `deployment_id` labels at ~mod.rs:131
5. **Concurrency metrics** (`INVOKER_CONCURRENCY_LIMIT`, `INVOKER_CONCURRENCY_SLOTS_ACQUIRED`, `INVOKER_CONCURRENCY_SLOTS_RELEASED`): Add `partition_id` label (keep existing `invoker_id` label)

## Critical Files for Implementation

- `crates/invoker-impl/src/metric_definitions.rs` — All new constants and describe calls (Phase 1)
- `crates/invoker-impl/src/lib.rs` — Invoker main loop: queue duration, active invocations, enqueue label fix (Phases 2, 3)
- `crates/invoker-impl/src/invocation_task/mod.rs` — InstrumentedResponseStream wrapper, task duration labels, eager state labels (Phases 2, 3)
- `crates/invoker-impl/src/invocation_task/service_protocol_runner.rs` — Status code capture in v1-v3 path (Phase 2)
- `crates/invoker-impl/src/invocation_task/service_protocol_runner_v4.rs` — Status code capture in v4 path (Phase 2)
- `crates/invoker-impl/src/input_command.rs` — `enqueued_at` field on InvokeCommand (Phase 2)
- `crates/invoker-impl/src/quota.rs` — Throttle balance gauge, `partition_id` label on concurrency metrics (Phases 2, 3)

## Verification

1. `cargo check` — compilation confirms all metric calls are valid
2. `cargo clippy --all-features --all-targets --workspace -- -D warnings`
3. `cargo fmt --all -- --check`
4. `cargo nextest run --all-features`
5. Manual smoke test: `curl localhost:9070/metrics | grep restate.invoker` against a running instance
