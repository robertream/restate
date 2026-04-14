# Task Context: Invoker Deployment Performance Metrics

## Issues
- [#4553](https://github.com/restatedev/restate/issues/4553) — Add invoker deployment performance metrics
- [#4454](https://github.com/restatedev/restate/issues/4454) — More invoker metrics
- Parent: [#4450](https://github.com/restatedev/restate/issues/4450) — Invoker improvements (milestone 1.7)

## Selected Architecture

Combined single PR for both issues. All metrics use the `metrics` crate (v0.24) following existing patterns in `metric_definitions.rs`.

## Metric Spec

### New metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `restate.invoker.throttle_balance` | gauge | `partition_id` | Invocation token bucket balance, sampled ~5s. Negative = throttled. |
| `restate.invoker.queue_duration.seconds` | histogram | `partition_id` | Wall-clock time from enqueue to task start. Deployment unknown at enqueue time. |
| `restate.invoker.active_invocations` | gauge | `service_name` `deployment_id` | Current in-flight invocations per service/deployment. |
| `restate.invoker.http_request_duration.seconds` | histogram | `service_name` `deployment_id` | Time-to-first-byte (TTFB): HTTP request send → response headers received. |
| `restate.invoker.http_total_duration.seconds` | histogram | `service_name` `deployment_id` | Full HTTP request duration including response body streaming. |
| `restate.invoker.http_status_code.total` | counter | `service_name` `deployment_id` `status_code` | Count of non-200 HTTP status codes from deployments. |

### Existing metrics gaining labels

| Metric | Type | New Labels | Description |
|--------|------|------------|-------------|
| `restate.invoker.enqueue.total` | counter | `partition_id` `service_name` | Number of invocations added to the queue. |
| `restate.invoker.invocation_tasks.total` | counter | `partition_id` `service_name` `deployment_id` | Invocation task lifecycle events. |
| `restate.invoker.task_duration.seconds` | histogram | `partition_id` `service_name` `deployment_id` | Total single-attempt invocation task duration. |
| `restate.invoker.eager_state_truncated.total` | counter | `service_name` `deployment_id` | Invocations where eager state was truncated. |
| `restate.invoker.concurrency_limit` | gauge | `partition_id` | Configured concurrency limit. |
| `restate.invoker.concurrency_slots.acquired` | counter | `partition_id` | Concurrency slots acquired. |
| `restate.invoker.concurrency_slots.released` | counter | `partition_id` | Concurrency slots released. |

> Existing labels (e.g. `invoker_id`, `status`) preserved. Cleanup deferred.

## Label Strategy
- `service_name`: primary label on deployment-related metrics
- `deployment_id`: on HTTP metrics and active invocations gauge
- `partition_id`: standardized for new metrics over `invoker_id`

## Architecture Patterns
- Constants in `metric_definitions.rs`, `describe_metrics()` called at init
- `IdLookup` for partition ID label caching (avoids `.to_string()` on hot path)
- Pre-allocate `Counter`/`Histogram` handles in structs for hot paths
- `metrics` crate v0.24, `metrics-exporter-prometheus` v0.18.1
- Naming: `restate.{subsystem}.{metric_name}.{unit_suffix}`

## Key Files

| Component | File | Key Lines |
|-----------|------|-----------|
| Invoker event loop | `crates/invoker-impl/src/lib.rs` | `step()` at 450, `handle_invoke` at 688, `start_invocation_task` at 1699 |
| Metric definitions | `crates/invoker-impl/src/metric_definitions.rs` | 66-121 |
| InvocationTask::run | `crates/invoker-impl/src/invocation_task/mod.rs` | 369, 373, 412 |
| ResponseStream | `crates/invoker-impl/src/invocation_task/mod.rs` | 620-691 |
| Protocol runner (v1-v3) | `crates/invoker-impl/src/invocation_task/service_protocol_runner.rs` | 155-169, 342-390 |
| Protocol runner (v4) | `crates/invoker-impl/src/invocation_task/service_protocol_runner_v4.rs` | Similar |
| Concurrency quota | `crates/invoker-impl/src/quota.rs` | Full file |
| InvokeCommand | `crates/invoker-impl/src/input_command.rs` | 105+ |
| Token bucket config | `crates/invoker-api/src/capacity.rs` | 17, 49-54 |

## Implementation Approach

1. **metric_definitions.rs**: Add 6 new constants + describe calls
2. **InvokeCommand** (`input_command.rs`): Add `enqueued_at: Instant` field
3. **ResponseStream** (`invocation_task/mod.rs`): Add `Instant` to `WaitingHeaders`, record TTFB at transition, total at `Terminated`
4. **InvocationTask::run** (`invocation_task/mod.rs`): Record `service_name` + `deployment_id` on existing `task_duration` histogram
5. **Invoker main loop** (`lib.rs`): Record queue duration at `handle_invoke`, add throttle balance gauge on timer, manage active_invocations gauge, add labels to existing metrics
6. **quota.rs**: Add `partition_id` label to concurrency metrics
7. **Protocol runners**: Capture HTTP status code from response headers, emit counter for non-200

## Dependencies
- gardal `SharedTokenBucket::balance()` for throttle gauge (confirmed available)
- `InvocationTarget` available at enqueue time for `service_name`
- `DeploymentId` available after `PinnedDeployment` message

## Impact Summary
- Primary crate: `restate-invoker-impl`
- Minor touch: `restate-invoker-api` (InvokeCommand struct)
- No schema/config changes
- No public API changes
- No breaking changes (additive labels only)
