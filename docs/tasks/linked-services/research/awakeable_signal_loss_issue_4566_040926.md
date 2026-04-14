---
date: "2026-04-09T00:00:00-07:00"
git_commit: 53518d24dc8da287576245d30c3e18adc9fd6c68
branch: linked-services
repository: restate
topic: "Awakeable signals lost during partition leadership transitions (issue #4566)"
tags: [research, codebase, partition-processor, deduplication, bifrost, leadership, awakeable]
status: complete
last_updated: "2026-04-09"
last_updated_by: Researcher
last_updated_note: "Added follow-up: performance impact analysis for Strategy A fix"
---

# Research: Awakeable Signal Loss During Partition Leadership Transitions (#4566)

| Field | Value |
|-------|-------|
| Date | 2026-04-09 |
| Git Commit | `53518d24` |
| Branch | `linked-services` |
| Repository | `restate` |
| Issue | [restatedev/restate#4566](https://github.com/restatedev/restate/issues/4566) |

## Research Question

How and why are awakeable resolve/reject signals silently lost during partition leadership transitions? What is the root cause in the dedup mechanism, which code paths are affected, and what are possible fix strategies?

## Summary

Awakeable signals (and other operations using `self_propose_and_respond_asynchronously`) can be permanently lost during partition leadership transitions. The root cause is a **timing gap between Bifrost commit confirmation and state machine execution**, combined with **epoch-based deduplication fencing** that drops entries from previous leader epochs.

The ingress HTTP API returns HTTP 202 (accepted) when Bifrost durably commits the signal, but the partition processor may never execute it if a new leader's `AnnounceLeader` entry updates the dedup high-water mark to a newer epoch first.

## Detailed Findings

### 1. The Dedup Mechanism — How Records Are Filtered

Every entry written to Bifrost carries a `DedupInformation` (producer ID + sequence number). When the partition processor reads records from the log, `apply_record` checks each entry against the dedup table.

**`apply_record` dedup gate** — `crates/worker/src/partition/mod.rs:954-969`:
1. Extracts `DedupInformation` from the envelope header
2. Calls `is_outdated_or_duplicate(dedup_information, transaction)`
3. If outdated → silently drops the record (`return Ok(None)` at line 962)
4. If fresh → stores the new sequence number as the high-water mark via `put_dedup_seq_number`
5. This update happens **unconditionally before the command is dispatched**, including for `AnnounceLeader`

**`is_outdated_or_duplicate`** — `crates/worker/src/partition/mod.rs:1040-1064`:
```rust
(DedupSequenceNumber::Esn(last_esn), DedupSequenceNumber::Esn(esn)) => {
    last_esn >= *esn  // greater-than-or-equal check
}
```

### 2. `EpochSequenceNumber` Ordering — The Fencing Mechanism

**`EpochSequenceNumber`** — `crates/storage-api/src/deduplication_table/mod.rs:89-136`:
```rust
pub struct EpochSequenceNumber {
    pub leader_epoch: LeaderEpoch,
    pub sequence_number: MessageIndex,
}
```

`PartialOrd` is **lexicographic** — leader epoch is the primary key (lines 128-136):
```rust
self.leader_epoch
    .cmp(&other.leader_epoch)
    .then_with(|| self.sequence_number.cmp(&other.sequence_number))
```

An ESN from epoch N+1 is **always** greater than any ESN from epoch N, regardless of sequence number. Once `AnnounceLeader(eN+1)` stores `{eN+1, 0}` in the dedup table, every self-proposal from epoch eN is considered outdated.

### 3. Producer ID Space

**`ProducerId`** — `crates/storage-api/src/deduplication_table/mod.rs:58-71`:

| Producer | Dedup Type | Used By |
|---|---|---|
| `ProducerId::Other("SELF")` | `Esn` (epoch-fenced) | Self-proposals from the partition's own leader via `SelfProposer` |
| `ProducerId::Partition(id)` | `Sn` (monotonic, **not** epoch-fenced) | Cross-partition outbox messages via Shuffle |
| `ProducerId::Producer(u128)` | `Sn` (monotonic) | Ingress producers |

Only `ProducerId::Other("SELF")` uses `EpochSequenceNumber`, making it the only producer type subject to epoch-based fencing.

### 4. `AnnounceLeader` — How It Poisons the Dedup Table

**`announce_leadership`** — `crates/worker/src/partition/leadership/mod.rs:244-274`:
1. Creates a new `SelfProposer` with `EpochSequenceNumber::new(leader_epoch)` → `{leader_epoch, seq: 0}`
2. Calls `self_proposer.propose(partition_key, announce_leader)` — stamped with ESN `{leader_epoch, 0}`
3. Transitions to `State::Candidate`

**`SelfProposer::create_header`** — `crates/worker/src/partition/leadership/self_proposer.rs:196-211`:
- ESN is consumed pre-increment: `AnnounceLeader` gets `{epoch, 0}`, next proposal gets `{epoch, 1}`

When `apply_record` processes `AnnounceLeader`:
- `put_dedup_seq_number("SELF", Esn{epoch, 0})` — **high-water mark set**
- Returns early at lines 977-979 (no state machine apply)
- Any subsequent self-proposal from a prior epoch: `{epoch, 0} >= {old_epoch, K}` → **TRUE** → **DROPPED**

### 5. The Two Response Mechanisms — The Root Cause

#### `self_propose_and_respond_asynchronously` (VULNERABLE)

**Defined:** `crates/worker/src/partition/leadership/leader_state.rs:428-450`

1. Calls `self_proposer.propose_with_notification()` → enqueues to Bifrost background appender
2. Gets a `CommitToken` — a oneshot that fires when **Bifrost durably commits the bytes**
3. Wraps in `SelfAppendFuture` with the RPC response callback
4. When `CommitToken` fires → sends `Appended` to the caller (e.g., HTTP 202)

**The gap:** `CommitToken` = "Bifrost wrote the bytes", **NOT** "the partition processor executed the command." The caller gets confirmation before the state machine ever sees the entry.

#### `handle_rpc_proposal_command` (SAFE)

**Defined:** `crates/worker/src/partition/leadership/leader_state.rs:398-426`

1. Stores the response channel in `awaiting_rpc_actions: HashMap<RequestId, Reciprocal>`
2. Proposes via `self_proposer.propose()` — **without** a commit notification
3. Response is only sent when the **state machine executes the command** and emits an action
4. On leadership loss → all pending reciprocals get `LostLeadership` error → caller retries

This is why `/send` is not affected — it uses `handle_rpc_proposal_command`.

### 6. Affected Operations

All operations that use `self_propose_and_respond_asynchronously`:

| Operation | Handler | File |
|---|---|---|
| `append_signal` (awakeable resolve/reject) | `NotifySignal` | `crates/worker/src/partition/rpc/append_signal.rs:29-51` |
| `append_invocation_response` (old protocol v3) | `InvocationResponse` | `crates/worker/src/partition/rpc/append_invocation_response.rs:27-45` |
| `restart_as_new_invocation` (simple case) | `Invoke` | `crates/worker/src/partition/rpc/restart_as_new_invocation.rs:236-243` |

**NOT affected** — `/send` invocation path uses `handle_rpc_proposal_command`:
- `crates/worker/src/partition/rpc/append_invocation.rs:56-73`

### 7. The Race Condition — Step by Step

```
Timeline:

1. Old leader (eN) receives awakeable resolve via ingress HTTP RPC
2. append_signal handler calls self_propose_and_respond_asynchronously
3. SelfProposer enqueues NotifySignal{eN, K} to BackgroundAppender mpsc channel
4. New leader candidate (eN+1) wins campaign
5. announce_leadership() creates new SelfProposer, enqueues AnnounceLeader{eN+1, 0}
6. become_follower() cancels old leader's tasks (but doesn't drain in-flight appends)

   --- Both records race to Bifrost loglet ---

7. AnnounceLeader{eN+1, 0} committed to Bifrost at LSN N
8. NotifySignal{eN, K} committed to Bifrost at LSN N+1
   (or possibly in a batch where AnnounceLeader is processed first)

9. Old leader's BackgroundAppender confirms commit → CommitToken fires
10. SelfAppendFuture resolves → reciprocal.send(Ok(Appended))
11. HTTP ingress receives Appended → returns HTTP 202 to client

12. Partition processor reads log batch:
    a. AnnounceLeader{eN+1,0}: dedup passes, stores ("SELF", Esn{eN+1,0})
    b. NotifySignal{eN,K}: dedup check Esn{eN+1,0} >= Esn{eN,K} → TRUE → DROPPED

13. Signal permanently lost. Invocation stays suspended.
```

### 8. Leadership Step-Down — No Synchronization with In-Flight Writes

**`become_follower()`** — `crates/worker/src/partition/leadership/mod.rs:580-594`:
- For `State::Leader`: calls `leader_state.stop(invoker_tx)`
- For `State::Candidate`: just drops the `self_proposer`

**`LeaderState::stop()`** — `crates/worker/src/partition/leadership/leader_state.rs:217-266`:
- Cancels Shuffle task handle (async, non-blocking)
- Calls `self_proposer.mark_as_non_leader()` (preference hint only)
- Does **NOT** drain the `self_proposer`'s `AppenderHandle`

**`AppenderHandle::drop`** — `crates/bifrost/src/background_appender.rs:210-217`:
- Calls `handle.cancel()` via cancellation token
- Does **NOT** wait for in-flight appends to complete
- Records already in the mpsc channel or in-flight to the loglet can still be committed

### 9. Ingress Retry Behavior

**`execute_rpc`** — `crates/ingress-http/src/rpc_request_dispatcher.rs:151-163`:
- `append_signal` is called with `is_idempotent=true`
- Retry logic: `retry = is_idempotent || e.is_safe_to_retry()` — always true for idempotent ops
- Signals **will** be retried on `LostLeadership` errors
- But in the bug scenario, no error occurs — `Appended` is returned successfully

## Code References

| Concern | File | Lines |
|---|---|---|
| `apply_record` dedup gate | `crates/worker/src/partition/mod.rs` | 954-969 |
| Silent drop on duplicate | `crates/worker/src/partition/mod.rs` | 957-962 |
| `AnnounceLeader` early return | `crates/worker/src/partition/mod.rs` | 977-979 |
| `is_outdated_or_duplicate` | `crates/worker/src/partition/mod.rs` | 1040-1064 |
| `EpochSequenceNumber` struct | `crates/storage-api/src/deduplication_table/mod.rs` | 89-112 |
| `EpochSequenceNumber::PartialOrd` | `crates/storage-api/src/deduplication_table/mod.rs` | 128-136 |
| `ProducerId::Other("SELF")` | `crates/storage-api/src/deduplication_table/mod.rs` | 58-71 |
| `DedupInformation::self_proposal` | `crates/storage-api/src/deduplication_table/mod.rs` | 36-41 |
| `self_propose_and_respond_asynchronously` | `crates/worker/src/partition/leadership/leader_state.rs` | 428-450 |
| `handle_rpc_proposal_command` | `crates/worker/src/partition/leadership/leader_state.rs` | 398-426 |
| `SelfAppendFuture` | `crates/worker/src/partition/leadership/leader_state.rs` | 733-781 |
| Leadership-loss cleanup | `crates/worker/src/partition/leadership/leader_state.rs` | 253-265 |
| `AnnounceLeader` proposal | `crates/worker/src/partition/leadership/mod.rs` | 244-274 |
| `SelfProposer::propose_with_notification` | `crates/worker/src/partition/leadership/self_proposer.rs` | 137-152 |
| `SelfProposer::create_header` (ESN stamping) | `crates/worker/src/partition/leadership/self_proposer.rs` | 196-211 |
| `BackgroundAppender` main loop | `crates/bifrost/src/background_appender.rs` | 81-157 |
| `CommitToken` | `crates/bifrost/src/background_appender.rs` | 455-468 |
| `AppenderHandle::drop` (cancel-only) | `crates/bifrost/src/background_appender.rs` | 210-217 |
| `append_signal` handler | `crates/worker/src/partition/rpc/append_signal.rs` | 29-51 |
| `append_invocation_response` handler | `crates/worker/src/partition/rpc/append_invocation_response.rs` | 27-45 |
| `/send` uses safe path | `crates/worker/src/partition/rpc/append_invocation.rs` | 56-73 |
| Ingress retry policy | `crates/ingress-http/src/rpc_request_dispatcher.rs` | 151-163 |
| Shuffle independent appender | `crates/worker/src/partition/shuffle.rs` | 407 |
| `become_follower` | `crates/worker/src/partition/leadership/mod.rs` | 580-594 |
| `LeaderState::stop` | `crates/worker/src/partition/leadership/leader_state.rs` | 217-266 |

## Architecture Insights

1. **Two-tier confirmation model**: Bifrost provides "durable commit" guarantees (bytes on disk), but the partition processor provides "execution" guarantees (state machine applied). `self_propose_and_respond_asynchronously` conflates these — it returns "accepted" at the Bifrost tier, not the execution tier.

2. **Epoch-based fencing is intentional**: The dedup mechanism correctly prevents stale leader proposals from being applied after a new leader takes over. The bug is not in the fencing itself but in the **premature confirmation** to the caller before fencing has occurred.

3. **No cross-appender ordering**: Bifrost guarantees FIFO ordering within a single appender but not across independent appenders. The old leader's self-proposer and the new candidate's self-proposer are separate appenders racing for LSN slots.

4. **Step-down is not a synchronization barrier**: `become_follower()` cancels tasks but does not drain in-flight Bifrost writes. Records already in the background appender's mpsc channel or in-flight to the loglet can still commit after the processor steps down.

5. **Ingress retry cannot help**: The bug occurs in the success path — the caller receives `Appended` (HTTP 202), not an error. Retry logic only activates on errors.

## Possible Fix Strategies

### A. Migrate affected RPCs to `handle_rpc_proposal_command` (Recommended)

Convert `append_signal`, `append_invocation_response`, and `restart_as_new_invocation` (simple case) to use `handle_rpc_proposal_command` instead of `self_propose_and_respond_asynchronously`. This delays the response until the state machine actually executes the command. On leadership loss, the caller gets `LostLeadership` and retries.

**Pros:** Proven pattern (used by `/send`), no architectural changes needed.
**Cons:** Higher latency for signal acknowledgment (must wait for state machine execution, not just Bifrost commit). Requires adding action-emission for these command types.

### B. Drain in-flight appends before stepping down

Make `become_follower()` synchronously drain the old leader's background appender before the new candidate proposes `AnnounceLeader`.

**Pros:** Preserves the fast-ack behavior.
**Cons:** Adds latency to leadership transitions. Complex to implement correctly (must handle appender failures). Does not address the fundamental issue that Bifrost commit != execution.

### C. Separate dedup namespaces per epoch

Instead of sharing `ProducerId::Other("SELF")` across epochs, use `ProducerId::Other("SELF-{epoch}")` so each leader epoch has its own dedup space. Old-epoch entries would not be fenced by the new epoch's `AnnounceLeader`.

**Pros:** Old-epoch entries survive leadership transitions.
**Cons:** Introduces a new problem — old-epoch entries could be replayed by the new leader, potentially causing duplicate execution. Would need additional idempotency tracking.

### D. Track pending self-proposals and re-propose on new leadership

When the new leader takes over, check for self-proposed entries from the old epoch that were committed to Bifrost but not yet executed, and re-propose them under the new epoch.

**Pros:** No latency impact on the happy path.
**Cons:** Complex implementation. Requires correlating Bifrost LSNs with execution status.

## Related Research

- Issue: [restatedev/restate#4566](https://github.com/restatedev/restate/issues/4566)
- Linked services design: `docs/design/linked_services.md` (shares partition processor infrastructure)

## Open Questions

1. **Latency impact of Strategy A**: How much additional latency would waiting for state machine execution add to awakeable resolve/reject? This is the signal path used by external systems integrating with Restate.

2. **`restart_as_new_invocation` dual path**: This handler uses *both* response mechanisms depending on the code path. Should both be migrated, or only the `self_propose_and_respond_asynchronously` path?

3. **Existing signals in production**: Are there permanently suspended invocations in production clusters caused by this bug? Is there a detection/recovery mechanism?

4. **Cross-partition signals via Shuffle**: The issue description says `append_signal` is affected, but Shuffle uses `ProducerId::Partition(id)` with `Sn` (not epoch-fenced). Are cross-partition awakeable completions routed through Shuffle (and therefore safe) or through the self-proposer?

---

## Follow-up Research: Strategy A Implementation Plan (2026-04-09)

### Research Question

What are the concrete code changes required to implement Strategy A — migrating all `self_propose_and_respond_asynchronously` callers to `handle_rpc_proposal_command`?

### How `handle_rpc_proposal_command` Works End-to-End

The safe response pattern follows this flow:

1. **RPC arrives** → `PartitionProcessorRpcRequestId` (ULID) generated at ingress (`rpc_request_dispatcher.rs:85`)
2. **`request_id` embedded in command payload** — e.g., `ServiceInvocation.submit_notification_sink = Some(SubmitNotificationSink::Ingress { request_id })`
3. **Proposed to Bifrost** via `self_proposer.propose()` (without commit notification)
4. **Reciprocal stored** in `awaiting_rpc_actions: HashMap<PartitionProcessorRpcRequestId, RpcReciprocal>` (`leader_state.rs:80`)
5. **State machine executes command** → extracts `request_id` from payload → emits `Action` carrying `request_id`
6. **`handle_actions`** (`leader_state.rs:472-684`) matches the action → `awaiting_rpc_actions.remove(&request_id)` → sends response via reciprocal
7. **On leadership loss** → all `awaiting_rpc_actions` drained with `LostLeadership` error (`leader_state.rs:253-265`) → caller retries

The response is only sent **after** `transaction.commit()` at `mod.rs:674`, guaranteeing the state machine has durably applied the command.

### All `self_propose_and_respond_asynchronously` Call Sites (Migration Scope)

| # | File | Line | Command | Response | Severity |
|---|------|------|---------|----------|----------|
| 1 | `rpc/append_signal.rs` | 38 | `Command::NotifySignal` | `Appended` | **High** — primary bug path |
| 2 | `rpc/append_invocation_response.rs` | 35 | `Command::InvocationResponse` | `Appended` | **High** — old protocol path |
| 3 | `rpc/append_invocation.rs` | 47 | `Command::Invoke` (Appended mode) | `Appended` | **Medium** — fire-and-forget sends |
| 4 | `rpc/restart_as_new_invocation.rs` | 237 | `Command::Invoke` (simple restart) | `Ok { new_invocation_id }` | **Low** — old journal workaround |

### Per-Call-Site Migration Analysis

#### Call Site 1: `append_signal` (NotifySignal) — High Priority

**Current flow:** `append_signal.rs:37-47` → `self_propose_and_respond_asynchronously(partition_key, Command::NotifySignal(...), replier, Appended)`

**Required changes:**

1. **Add `request_id` to `NotifySignalRequest`** (`crates/types/src/invocation/mod.rs:1415-1418`):
   ```rust
   pub struct NotifySignalRequest {
       pub invocation_id: InvocationId,
       pub signal: Signal,
       pub request_id: Option<PartitionProcessorRpcRequestId>,  // NEW
   }
   ```
   `Option` allows backward compatibility — signals proposed by internal systems (timers, shuffle) set `None`.

2. **Add `Action::ForwardSignalAppliedResponse`** (`crates/worker/src/partition/state_machine/actions.rs`):
   ```rust
   ForwardSignalAppliedResponse {
       request_id: PartitionProcessorRpcRequestId,
   },
   ```

3. **Emit action from state machine** — in `mod.rs:647-657`, after `OnNotifySignalCommand::apply(self)` succeeds, check `if self.is_leader` and `request_id.is_some()`:
   ```rust
   if let Some(request_id) = notify_signal_request.request_id {
       if self.is_leader {
           self.action_collector.push(Action::ForwardSignalAppliedResponse { request_id });
       }
   }
   ```

4. **Handle action in `leader_state.rs`** — add match arm in `handle_action`:
   ```rust
   Action::ForwardSignalAppliedResponse { request_id } => {
       if let Some(response_tx) = self.awaiting_rpc_actions.remove(&request_id) {
           response_tx.send(Ok(PartitionProcessorRpcResponse::Appended));
       }
   }
   ```

5. **Switch RPC handler** — `append_signal.rs`:
   ```rust
   // Before: self.proposer.self_propose_and_respond_asynchronously(...)
   // After:
   let request_id = request.request_id;
   let cmd = Command::NotifySignal(NotifySignalRequest {
       invocation_id,
       signal,
       request_id: Some(request_id),
   });
   self.proposer.handle_rpc_proposal_command(partition_key, cmd, request_id, replier).await;
   ```

**Serialization concern:** `NotifySignalRequest` is serialized via `Command` into Bifrost. Adding `request_id: Option<PartitionProcessorRpcRequestId>` requires updating the protobuf/serde for `Command` in `wal-protocol`. Old entries without `request_id` will deserialize as `None` — safe for rolling upgrades.

#### Call Site 2: `append_invocation_response` (InvocationResponse) — High Priority

**Current flow:** `append_invocation_response.rs:34-41` → `self_propose_and_respond_asynchronously(partition_key, Command::InvocationResponse(...), replier, Appended)`

**Required changes:** Nearly identical to call site 1:

1. **Add `request_id` to `InvocationResponse`** (`crates/types/src/invocation/mod.rs:573-576`):
   ```rust
   pub struct InvocationResponse {
       pub target: JournalCompletionTarget,
       pub result: ResponseResult,
       pub request_id: Option<PartitionProcessorRpcRequestId>,  // NEW
   }
   ```

2. **Add `Action::ForwardInvocationResponseAppliedResponse`** (or reuse `ForwardSignalAppliedResponse` if we want a single "command applied" action).

3. **Emit action from state machine** — in `mod.rs:552-573`, after the `InvocationResponse` handling succeeds.

4. **Handle action + switch RPC handler** — same pattern as call site 1.

#### Call Site 3: `append_invocation` (Invoke, Appended mode) — Medium Priority

**Current flow:** `append_invocation.rs:44-53` — when `AppendInvocationReplyOn::Appended`, uses `self_propose_and_respond_asynchronously(partition_key, Command::Invoke(...), replier, Appended)`.

**Options:**
- **Option A:** Reuse `SubmitNotificationSink::Ingress { request_id }` and switch to `handle_rpc_proposal_command`. The state machine already emits `Action::IngressSubmitNotification` for `Invoke` commands with this sink. This means the "Appended" mode becomes semantically equivalent to "Submitted" — response arrives after state machine execution rather than Bifrost commit. The response type changes from `Appended` to `Submitted(...)`.
- **Option B:** Add a new `AppendedNotificationSink` to `ServiceInvocation` and corresponding action. More complex, preserves exact `Appended` response semantics.

**Recommendation:** Option A is simpler. The `Appended` reply mode is already a fire-and-forget semantic — callers don't distinguish between "Bifrost committed" and "state machine processed." The slightly higher latency is acceptable for correctness.

#### Call Site 4: `restart_as_new_invocation` (Simple Path) — Low Priority

**Current flow:** `restart_as_new_invocation.rs:236-243` — the old journal V1 workaround proposes `Command::Invoke` and responds with `Ok { new_invocation_id }` at Bifrost commit time.

**This path is being deprecated:** The condition `use_old_journal_workaround` (lines 104-125) only triggers for invocations that used protocol < V4 or have no journal V2 entries. As the fleet upgrades, this path becomes dead code.

**Options:**
- **Defer:** Accept the risk for the legacy path since it will naturally disappear.
- **Migrate:** Thread `new_invocation_id` through the state machine via `SubmitNotificationSink` on the `ServiceInvocation`. The `IngressSubmitNotification` action doesn't currently carry `new_invocation_id`, so the response would need to be mapped.

**Recommendation:** Defer. Document the known vulnerability in a code comment and prioritize the high-priority call sites.

### Consolidated Action Type Design

Rather than adding per-command action variants, consider a single generic action:

```rust
Action::ForwardAppendedResponse {
    request_id: PartitionProcessorRpcRequestId,
}
```

This works because all three high-priority call sites return `PartitionProcessorRpcResponse::Appended` — the response payload is identical. The action just signals "the command associated with this `request_id` was successfully applied."

**Files requiring this new action:**
- `crates/worker/src/partition/state_machine/actions.rs` — add variant
- `crates/worker/src/partition/leadership/leader_state.rs` — add match arm in `handle_action`

### State Machine Emission Points

| Command | State machine handler | Emit point |
|---------|----------------------|------------|
| `NotifySignal` | `mod.rs:647-657` | After `OnNotifySignalCommand::apply(self).await?` succeeds |
| `InvocationResponse` | `mod.rs:552-573` | After both V1 (`handle_completion`) and V2 (`OnNotifyInvocationResponse::apply`) succeed |
| `Invoke` (Appended mode) | Uses existing `IngressSubmitNotification` via `SubmitNotificationSink` | Already emitted at `mod.rs:4435-4457` |

### WAL Protocol / Serialization Changes

Adding `request_id: Option<PartitionProcessorRpcRequestId>` to command payloads requires updating:

1. **`NotifySignalRequest`** in `crates/types/src/invocation/mod.rs:1415-1418`
2. **`InvocationResponse`** in `crates/types/src/invocation/mod.rs:573-576`
3. **WAL protobuf definitions** in `crates/wal-protocol/` — the `Command` serialization format
4. **Backward compatibility:** `Option<T>` defaults to `None` on deserialization of old entries — safe for rolling upgrades

### Implementation Order

1. **Phase 1 — Add `Action::ForwardAppendedResponse`**: Add the action variant, handle it in `leader_state.rs`. No behavioral change yet.

2. **Phase 2 — Migrate `append_signal`**: Add `request_id` to `NotifySignalRequest`, emit `ForwardAppendedResponse` from state machine, switch handler to `handle_rpc_proposal_command`. This fixes the primary bug.

3. **Phase 3 — Migrate `append_invocation_response`**: Same pattern. Fixes the old protocol path.

4. **Phase 4 — Migrate `append_invocation` Appended mode**: Switch to `SubmitNotificationSink` + `handle_rpc_proposal_command`. Response type changes from `Appended` to `Submitted`.

5. **Phase 5 (Optional) — Remove `self_propose_and_respond_asynchronously`**: If all callers migrated (including restart_as_new), remove the method, `SelfAppendFuture`, `CommitToken` usage, and `propose_with_notification` from `SelfProposer`.

### Latency Impact Assessment

The additional latency from Strategy A is:
- **Bifrost read latency**: Time for the partition processor to read the entry back from the log (~single-digit ms for local loglet, potentially higher for replicated loglet)
- **State machine apply time**: Time to process the command (~microseconds for signal application)
- **Transaction commit**: Time to commit the RocksDB transaction (~sub-ms)

Total additional latency: likely **5-20ms** for the common case (local loglet), up to **50-100ms** for replicated loglet with high load. This is the same latency profile as `/send` (which already uses `handle_rpc_proposal_command`), so it's a well-understood trade-off.

### Additional Code References

| Concern | File | Lines |
|---|---|---|
| `Actuator` trait definition | `crates/worker/src/partition/rpc/mod.rs` | 45-70 |
| `Action` enum (all variants) | `crates/worker/src/partition/state_machine/actions.rs` | 30-107 |
| `handle_action` dispatch | `crates/worker/src/partition/leadership/leader_state.rs` | 472-684 |
| `IngressSubmitNotification` emission | `crates/worker/src/partition/state_machine/mod.rs` | 4435-4457 |
| `IngressResponse` emission | `crates/worker/src/partition/state_machine/mod.rs` | 2836-2877 |
| `awaiting_rpc_actions` type | `crates/worker/src/partition/leadership/leader_state.rs` | 80 |
| `RpcReciprocal` type alias | `crates/worker/src/partition/leadership/leader_state.rs` | 62-63 |
| `PartitionProcessorRpcRequestId` | `crates/types/src/identifiers.rs` | 1079 |
| `NotifySignalRequest` struct | `crates/types/src/invocation/mod.rs` | 1415-1418 |
| `InvocationResponse` struct | `crates/types/src/invocation/mod.rs` | 573-576 |
| `restart_as_new` old workaround condition | `crates/worker/src/partition/rpc/restart_as_new_invocation.rs` | 104-125 |
| Transaction commit before handle_actions | `crates/worker/src/partition/mod.rs` | 674-679 |
| Leadership-loss drain of awaiting_rpc_actions | `crates/worker/src/partition/leadership/leader_state.rs` | 253-265 |

### Open Questions (Updated)

1. **Response type for Appended→Submitted migration**: If `append_invocation` Appended mode is migrated to use `SubmitNotificationSink`, the HTTP response changes from `Appended` (202) to `Submitted` (with execution time info). Is this a breaking API change? Check how ingress maps these responses.

2. **Unified vs per-command action**: Is a single `ForwardAppendedResponse` action clean enough, or do reviewers prefer per-command action variants for type safety?

3. **WAL versioning**: Does adding `Option<PartitionProcessorRpcRequestId>` to `NotifySignalRequest` require a WAL format version bump, or is the protobuf evolution sufficient?

4. **Test coverage**: What existing tests exercise the signal/invocation-response paths? Can the fix be verified with existing integration tests, or do we need a new test that simulates leadership transitions during signal delivery?

---

## Follow-up Research: Performance Impact Analysis (2026-04-09)

### Research Question

What are the expected negative performance impacts of migrating from `self_propose_and_respond_asynchronously` to `handle_rpc_proposal_command` for awakeable signals and invocation responses?

### Summary

The migration adds **one additional RocksDB write** (partition store transaction commit) and **batch coalescing delay** to the response path. Under typical conditions, this adds **2-10ms** to awakeable signal response latency. Under load with full batches, tail latency could increase by up to **20-30ms**. This is the same latency profile as `/send` (which already uses `handle_rpc_proposal_command`), and awakeables are a low-to-medium volume path, so the impact is acceptable.

### Latency Path Comparison

#### Current Path: `self_propose_and_respond_asynchronously`

Response sent after Bifrost commit only:

| Step | Operation | Typical Latency |
|------|-----------|----------------|
| 1 | RPC received, command proposed to SelfProposer mpsc channel | ~0.01ms |
| 2 | BackgroundAppender batches and calls `append_batch_erased()` | ~0.01ms |
| 3 | Local loglet `enqueue_batch` → RocksDB `write_batch()` (with WAL fsync) | **1-5ms** |
| 4 | `CommitToken` fires → `SelfAppendFuture` resolves → response sent | ~0.01ms |
| | **Total (local loglet, fsync enabled)** | **~1-5ms** |
| | **Total (replicated loglet)** | **~5-50ms** |

#### New Path: `handle_rpc_proposal_command`

Response sent after state machine execution + partition store commit:

| Step | Operation | Typical Latency |
|------|-----------|----------------|
| 1 | RPC received, command proposed to SelfProposer mpsc channel | ~0.01ms |
| 2 | BackgroundAppender batches and calls `append_batch_erased()` | ~0.01ms |
| 3 | Local loglet `enqueue_batch` → RocksDB `write_batch()` (with WAL fsync) | **1-5ms** |
| 4 | `notify_readers` fires `tail_watch` → Tokio waker propagation | ~0.01ms |
| 5 | Partition processor `select!` wakes, `read_entries` returns batch | ~0.01ms |
| 6 | State machine applies command (+ up to 31 coalesced records) | **0.01-1ms** |
| 7 | RocksDB partition store `transaction.commit()` | **1-5ms** |
| 8 | `handle_actions` dispatches response synchronously | ~0.001ms |
| | **Total (local loglet, fsync, low load)** | **~2-10ms** |
| | **Total (local loglet, fsync, full batch)** | **~5-15ms** |
| | **Total (replicated loglet)** | **~7-55ms** |

#### Added Latency

| Scenario | Current | New | Delta |
|----------|---------|-----|-------|
| Local loglet, low load | ~1-5ms | ~2-10ms | **+1-5ms** |
| Local loglet, full batch (32 records) | ~1-5ms | ~5-15ms | **+4-10ms** |
| Replicated loglet, low load | ~5-50ms | ~7-55ms | **+2-5ms** |
| Replicated loglet, full batch | ~5-50ms | ~10-60ms | **+5-10ms** |

### Batch Coalescing — The Main Tail Latency Concern

The partition processor reads up to `max_command_batch_size` (default: **32**) records per iteration (`mod.rs:1068-1099`). All records in a batch are processed in a single RocksDB transaction, and `handle_actions` is called **once per batch**, not per record.

**Impact:** If an awakeable signal is the first record in a batch of 32, its RPC response waits for all 31 subsequent records to be state-machine-applied and then a single RocksDB commit. Under high load (deep Bifrost queue), this means the signal response latency includes the processing time of up to 31 unrelated commands.

**Mitigation:** This is identical to how `/send` already works. The system is designed around this batching model. The `max_command_batch_size = 32` default was chosen as a balance between throughput (larger batches = fewer RocksDB commits) and latency (smaller batches = faster per-record response).

### Self-Read Path — No Round-Trip Penalty

When the leader writes to Bifrost and reads back its own entry, there is **no network round-trip**. The local loglet uses a `tokio::watch` channel (`TailOffsetWatch`) that fires the reader's waker in the **same async step** as the RocksDB write completing (`local_loglet/mod.rs:192-198`). The read stream's iterator reads from the RocksDB block cache (the just-written block is hot). The dominant cost is Tokio task scheduling (~microseconds).

For replicated loglet, the read stream uses a background `ReadStreamTask` that fetches records from log-servers via network RPC, adding network latency. However, the replicated loglet's read-ahead buffer (`readahead_records` config) prefetches records, amortizing network cost.

### Backpressure and Resource Impact

**Proposal backpressure is identical:** Both paths use the same `self_proposer.propose()` / `propose_with_notification()` which goes through the same `mpsc::channel(BIFROST_QUEUE_SIZE=50)`. The channel `.reserve().await` suspends when full — same backpressure regardless of which response mechanism is used.

**Connection holding:** `awaiting_rpc_actions` holds the RPC reciprocal (and thus the HTTP connection) longer — from proposal time through state machine execution, rather than just Bifrost commit. Each open RPC keeps one `tokio::spawn` responder task alive in the network reactor (`reactor.rs:455`). No timeout is configured on partition processor RPCs.

**Memory:** `awaiting_rpc_actions` is an unbounded `HashMap<PartitionProcessorRpcRequestId, RpcReciprocal>` (`leader_state.rs:80`). Each entry holds a `Reciprocal` wrapping a `oneshot::Sender`. At low-to-medium volumes (typical for awakeables), this is negligible.

**Volume assessment:** Awakeables are resolved by external HTTP `POST` requests — inherently a **low-to-medium volume path** driven by external events (payment callbacks, user approvals, etc.), not machine-speed fan-out. Holding connections for an extra 2-10ms at this volume has negligible resource impact.

### Risk Assessment

| Risk | Severity | Likelihood | Mitigation |
|------|----------|------------|------------|
| +2-10ms p50 latency for awakeable resolve | **Low** | Certain | Same as `/send` which is already accepted |
| +5-15ms p99 latency under batch coalescing | **Low** | Under load | Tunable via `max_command_batch_size` |
| Longer HTTP connection hold time | **Negligible** | Certain | Awakeables are low volume |
| No timeout on partition processor RPC | **Medium** | Edge case | Pre-existing issue, not introduced by this change |
| `awaiting_rpc_actions` unbounded growth | **Negligible** | Under extreme load only | Pre-existing for `/send`, low awakeable volume |

### Comparison with Existing `/send` Path

The `/send` invocation path already uses `handle_rpc_proposal_command` and experiences the exact same latency profile. This is the most heavily used RPC path in Restate. If the latency is acceptable for `/send` (which processes every single invocation), it is certainly acceptable for awakeable signals (which are external event injections at much lower volume).

### Configuration Parameters That Affect Latency

| Parameter | Default | Location | Effect |
|-----------|---------|----------|--------|
| `max_command_batch_size` | 32 | `types/src/config/worker.rs:153` | Max records per state machine batch. Lower = faster per-record response, higher = better throughput |
| `rocksdb_disable_wal_fsync` | false | `types/src/config/bifrost.rs:199` | Disabling fsync reduces Bifrost write from ~1-5ms to ~0.1ms |
| `writer_batch_commit_count` | 5000 | `types/src/config/bifrost.rs:300` | Local loglet writer batch size |
| `BIFROST_QUEUE_SIZE` | 50 | `self_proposer.rs:30` | Background appender channel depth |

### Conclusion

The performance impact of Strategy A is **minimal and well-understood**:
- **+2-10ms typical latency** — equivalent to what `/send` already experiences
- **No throughput impact** — backpressure mechanism is identical
- **No meaningful resource impact** — awakeables are low-volume
- **No new architectural risks** — uses a proven, battle-tested response mechanism

The correctness gain (eliminating permanent invocation suspension) far outweighs the minor latency increase. The latency profile is already accepted for the `/send` path, which handles orders of magnitude more traffic than awakeable signals.
