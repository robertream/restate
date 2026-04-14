---
date: 2026-04-07
git_commit: b0a7a90d8
branch: main
repo: restatedev/restate
topic: "Add invoker deployment performance metrics (issues #4553 + #4454)"
tags: [invoker, metrics, observability, performance]
status: kickoff
---

# Kickoff: Invoker Deployment Performance Metrics

## Project Context

**Issues**:
- [restatedev/restate#4553](https://github.com/restatedev/restate/issues/4553) — *Add invoker deployment performance metrics*
- [restatedev/restate#4454](https://github.com/restatedev/restate/issues/4454) — *More invoker metrics*

**Parent**: [restatedev/restate#4450](https://github.com/restatedev/restate/issues/4450) — *Invoker improvements* (milestone 1.7)

**Goal**: Surface deployment-level metrics (request latency, deployment concurrency, queueing delays, status codes, throttling) so operators can differentiate slow user services from internal Restate bottlenecks. Combined single PR for both issues since the label threading and instrumentation points overlap significantly.

### Sibling Issues (same parent #4450)

| Issue | Title | Status | Relevance |
|-------|-------|--------|-----------|
| #4451 | Connection pooling | Open | Per-deployment connection pools would provide natural concurrency dimension |
| #4453 | Safety net against stuck invocations | Open | Stuck detection could surface through metrics |
| #4456 | Invoker should await response EOS | **Done** | N/A |

---

## Metric Spec

### New metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `restate.invoker.throttle_balance` | gauge | `partition_id` | Invocation token bucket balance, sampled ~5s. Negative = throttled. |
| `restate.invoker.queue_duration.seconds` | histogram | `partition_id` | Wall-clock time from enqueue to task start. Deployment unknown at enqueue time. |
| `restate.invoker.active_invocations` | gauge | `service_name` `deployment_id` | Current in-flight invocations per service/deployment. |
| `restate.invoker.http_request_duration.seconds` | histogram | `service_name` `deployment_id` | Time-to-first-byte (TTFB): HTTP request send → response headers received. Isolates deployment response time. |
| `restate.invoker.http_total_duration.seconds` | histogram | `service_name` `deployment_id` | Full HTTP request duration including response body streaming. |
| `restate.invoker.http_status_code.total` | counter | `service_name` `deployment_id` `status_code` | Count of non-200 HTTP status codes from deployments. |

### Existing metrics gaining labels

| Metric | Type | New Labels | Description |
|--------|------|------------|-------------|
| `restate.invoker.enqueue.total` | counter | `partition_id` `service_name` | Number of invocations added to the queue. |
| `restate.invoker.invocation_tasks.total` | counter | `partition_id` `service_name` `deployment_id` | Invocation task lifecycle events (started/completed/suspended/failed). |
| `restate.invoker.task_duration.seconds` | histogram | `partition_id` `service_name` `deployment_id` | Total single-attempt invocation task duration. |
| `restate.invoker.eager_state_truncated.total` | counter | `service_name` `deployment_id` | Invocations where eager state was truncated due to size limit. |
| `restate.invoker.concurrency_limit` | gauge | `partition_id` | Configured concurrency limit (slots) for invoker tasks. |
| `restate.invoker.concurrency_slots.acquired` | counter | `partition_id` | Number of concurrency slots acquired. |
| `restate.invoker.concurrency_slots.released` | counter | `partition_id` | Number of concurrency slots released. |

> **Note:** Existing labels (e.g. `invoker_id`, `status`) are preserved on existing metrics. Label cleanup/normalization is deferred to a future PR.

### Label strategy

- **`service_name`**: Primary label on all deployment-related metrics. Bounded, dashboarding-friendly.
- **`deployment_id`**: Added to HTTP-related metrics and active invocations gauge. Cardinality is manageable since deployment count is small in practice (10-50 per cluster).
- **`partition_id`**: Used on queue/invoker-level metrics. Standardized over `invoker_id` for new metrics.
- Deployment-level drill-down also available via tracing spans (`restate.deployment.id` already on invocation task span).

---

## Research Summary

### Invocation Lifecycle (timing measurement points)

```
Partition Processor                    Invoker Main Loop                     InvocationTask
        |                                     |                                    |
        |--- InputCommand::Invoke ----------->|                                    |
        |                                     |-- enqueue to SegmentQueue -------->|
        |                                     |   [QUEUE WAIT TIME starts]         |
        |                                     |                                    |
        |                                     |-- (quota slot available?)           |
        |                                     |-- (memory budget available?)        |
        |                                     |-- (token bucket allows?)            |
        |                                     |                                    |
        |                                     |-- handle_invoke() --------------->|
        |                                     |   [QUEUE WAIT TIME ends]           |
        |                                     |   [TASK DURATION starts]           |
        |                                     |                                    |
        |                                     |                   resolve deployment
        |                                     |                   prepare HTTP request
        |                                     |                   [HTTP LATENCY starts]
        |                                     |                   client.call(req)
        |                                     |                   ...response headers...
        |                                     |                   [HTTP LATENCY: time-to-first-byte]
        |                                     |                   ...stream body...
        |                                     |                   [HTTP LATENCY: total]
        |                                     |                   [TASK DURATION ends]
        |                                     |<-- InvocationTaskOutput -----------|
```

### Concurrency Model

The invoker uses a **global** concurrency slot system (`InvokerConcurrencyQuota` in `quota.rs`), not per-deployment. A single `AtomicUsize` tracks available slots across all deployments for a given partition's invoker. There is no per-deployment semaphore, connection pool quota, or concurrency counter.

The HTTP layer (`hyper_util::client::legacy::Client`) has its own per-authority connection pool, but this is opaque — no metrics are exposed from it. Issue #4451 proposes adding explicit connection pooling, which would create a natural place for per-deployment concurrency metrics.

### Token Bucket Throttling

Two token buckets from `gardal` crate (v0.0.1-alpha.9):
- **`invocation_token_bucket`**: Gates the segment queue dequeue rate (applied via `.throttle()` at `lib.rs:349`)
- **`action_token_bucket`**: Passed into each `InvocationTask` for SDK action rate limiting (v4 protocol only)

Gardal exposes **zero metrics or observability hooks**. No callbacks, no rejection signals, no wait queue. The `ThrottledStream` delays item delivery via GCRA borrow model — never drops or rejects. Observable state:
- `available() -> f64` — tokens available (clamped to 0)
- `balance() -> f64` — current balance, may go negative when in debt
- `limit() -> &Limit` — configured rate/burst

**Decision**: Sample `balance()` on a ~5s timer as a gauge. Negative = throttled; magnitude = severity. Zero hot-path overhead (single atomic read).

### Deployment Identification

Deployments are identified by `DeploymentId` (UUID). The deployment is resolved inside `InvocationTask::select_protocol_version_and_run` (`mod.rs:441-494`). The `DeploymentId` is sent back to the invoker loop via `PinnedDeployment` message and stored in `InvocationStatusStore`.

The `InvocationTarget` (service name + handler) is available at task creation time. The URI authority is derived from `deployment.ty` at HTTP call time.

### Metrics Patterns in Codebase

- **Library**: `metrics` crate v0.24 + `metrics-exporter-prometheus` v0.18.1
- **Pattern**: Constants in `metric_definitions.rs`, `describe_metrics()` called at init, `IdLookup` for partition ID label caching
- **Hot-path optimization**: Pre-allocate `Counter`/`Histogram` handles in structs (see `quota.rs`, `writer.rs`)
- **Global labels**: `cluster_name`, `node_name` applied at recorder level
- **Quantiles**: 0.5, 0.9, 0.99, 1.0
- **Naming convention**: `restate.{subsystem}.{metric_name}.{unit_suffix}`

---

## Implementation Approach

### HTTP Latency (TTFB + Total)

Instrument `ResponseStream` (`invocation_task/mod.rs:620-691`):
- Capture `Instant::now()` at `ResponseStream::initialize()` (line 633) when `client.call(req)` is spawned
- Record TTFB when `WaitingHeaders` → `ReadingBody` transition occurs (line 652-670)
- Record total duration when `ResponseStream` reaches `Terminated`
- Both protocol runner versions (v1-v3, v4+) share `ResponseStream`, so both get instrumented automatically

### Queue Duration

- Add `enqueued_at: Instant` field to `InvokeCommand` (`input_command.rs`)
- Record at enqueue time (`lib.rs:467-468`)
- Compute `enqueued_at.elapsed()` at `handle_invoke` (`lib.rs:688`)
- VQueue path (`VQInvoke`) bypasses the segment queue — skip queue duration for these

### Active Invocations Gauge

- Increment when task starts (at `start_invocation_task`, `lib.rs:1699`)
- Decrement when task ends (at task output handlers)
- Labeled by `service_name` (known at task creation) and `deployment_id` (known after `PinnedDeployment`)

### Status Codes

- Capture HTTP status code in `handle_response_headers` (`service_protocol_runner.rs:364-366`)
- Emit counter for non-200 status codes, labeled by `service_name` `deployment_id` `status_code`

### Throttle Balance

- Add `tokio::time::Interval` (~5s) to the invoker main loop `step()` select
- On tick, read `invocation_token_bucket.balance()` and emit gauge

### Service Name Label Enrichment

- `InvocationTarget` is available on `InvokeCommand` at enqueue time → `service_name` for `enqueue.total`
- `InvocationTask` carries `invocation_target` → `service_name` for task lifecycle and duration metrics
- `DeploymentId` available after `PinnedDeployment` → `deployment_id` for task lifecycle metrics

---

## Code References

| Component | File | Key Lines |
|-----------|------|-----------|
| Invoker event loop | `crates/invoker-impl/src/lib.rs` | `step()` at 450, `handle_invoke` at 688, `start_invocation_task` at 1699 |
| Metric definitions | `crates/invoker-impl/src/metric_definitions.rs` | 66-121 (all constants + describe) |
| InvocationTask::run | `crates/invoker-impl/src/invocation_task/mod.rs` | 369 (entry), 373 (start timer), 412 (record duration) |
| ResponseStream | `crates/invoker-impl/src/invocation_task/mod.rs` | 620-691 (state machine for HTTP response) |
| Protocol runner (v1-v3) | `crates/invoker-impl/src/invocation_task/service_protocol_runner.rs` | 155-169 (prepare + initialize), 342-390 (replay loop) |
| Protocol runner (v4) | `crates/invoker-impl/src/invocation_task/service_protocol_runner_v4.rs` | Similar structure |
| Concurrency quota | `crates/invoker-impl/src/quota.rs` | Full file — global slot management |
| ServiceClient | `crates/service-client/src/lib.rs` | 46-54 (client struct, TODO about pooling) |
| HttpClient | `crates/service-client/src/http.rs` | 59-72 (hyper clients) |
| Status store | `crates/invoker-impl/src/status_store.rs` | `on_start` records SystemTime |
| InvokeCommand | `crates/invoker-impl/src/input_command.rs` | 105+ (handle impl, command structs) |
| IdLookup cache | `crates/invoker-impl/src/metric_definitions.rs` | 20-63 |
| Prometheus setup | `crates/tracing-instrumentation/src/prometheus_metrics.rs` | 33-68 |
| Token bucket config | `crates/invoker-api/src/capacity.rs` | 17, 49-54 |
| Throttling options | `crates/types/src/config/worker.rs` | 892-908 |

---

## Decisions Made

1. **Scope**: Combined single PR for #4553 and #4454.
2. **Labels**: `service_name` + `deployment_id` on deployment-related metrics. `partition_id` standardized for new metrics (existing `invoker_id` preserved, cleanup deferred).
3. **HTTP latency**: Both TTFB and total HTTP duration as separate histograms.
4. **Queue time**: Wall-clock enqueue-to-start (inclusive of all waits). VQueue path excluded.
5. **Throttle signal**: Sample `balance()` on ~5s timer as a gauge. Zero hot-path overhead.
6. **v4 consistency**: Both protocol versions share `ResponseStream` — automatic coverage.

## Open Questions

1. Should we consider a "deployment health" composite metric (combining latency + error rate + concurrency)?
2. Are there downstream consumers of these metrics (alerting, dashboards) that constrain naming?
3. What's the timeline relationship with #4451 (connection pooling)? If pooling lands first, the concurrency metrics design might differ.
