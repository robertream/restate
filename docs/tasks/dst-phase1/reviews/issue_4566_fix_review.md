# Review: Issue #4566 Fix — Migration to `handle_rpc_proposal_command`

**Date**: 2026-04-10
**Focus**: Justification for execution-gated response, latency concern, correctness of the fix
**Context**: dst-phase1 task — ad-hoc fix for awakeable signal loss during partition leadership transitions
**User concern (verbatim)**: *"I'd like you to focus on the fix, why do we need a synchronous response? if this alters latency from ~1ms to ~5ms thats a 5x slowdown, which seems extreme"*

---

## A. Direct Answer to the User's Concern — "Why do we need a synchronous response?"

### A.1 "Synchronous" is a slight misread — nothing blocks a thread

Both the old and new paths are fully async. Both use `tokio::oneshot` through `Reciprocal` (`crates/worker/src/partition/rpc/mod.rs:161`). Nothing blocks an HTTP connection handler thread, nothing holds a lock across `.await`, and ingress HTTP keeps using the same non-blocking request flow in `crates/ingress-http/src/rpc_request_dispatcher.rs:143-148`. The difference is **when** the oneshot is fulfilled — not whether a thread is parked.

- **Old path (`self_propose_and_respond_asynchronously`)** — the reply is sent from a `SelfAppendFuture` polled in `LeaderState::run` as soon as Bifrost's `CommitToken` fires (`crates/worker/src/partition/leadership/leader_state.rs:428-450`).
- **New path (`handle_rpc_proposal_command`)** — the reciprocal is stashed in `awaiting_rpc_actions` (`leader_state.rs:80, 417-423`) and fulfilled later by `handle_action` when the state machine emits `ForwardAppendedResponse` (`leader_state.rs:652-656`).

So what actually changes is the **promise semantics**: "Bifrost wrote the bytes" vs. "the partition processor executed the command." Neither is "synchronous" in the blocking sense.

### A.2 Why waiting for execution is required — the race in plain terms

The dedup mechanism (`crates/worker/src/partition/mod.rs:954-969`, `crates/storage-api/src/deduplication_table/mod.rs:89-136`) uses lexicographic `(leader_epoch, seq)` ordering for `ProducerId::Other("SELF")`. Any record from epoch `N` is silently dropped once the high-water mark is set to `{N+1, 0}` (`mod.rs:957-962`). This is **intentional and correct** — it prevents split-brain execution by old leaders.

The bug is a layering violation in the old handler:

1. Old leader (epoch `N`) enqueues `NotifySignal{N, K}` to `BackgroundAppender`.
2. `become_follower()` does not drain in-flight appends (`leader_state.rs:217-266` — no drain call on `self_proposer`; also `crates/bifrost/src/background_appender.rs:210-217`).
3. New leader (epoch `N+1`) proposes `AnnounceLeader{N+1, 0}` concurrently.
4. Both land on Bifrost. `BackgroundAppender` confirms the commit → `CommitToken` fires → old leader's `SelfAppendFuture` sends `Ok(Appended)` to the caller.
5. Partition processor reads the batch, applies `AnnounceLeader` first → dedup high-water = `{N+1, 0}` → the `NotifySignal{N, K}` is silently filtered at `mod.rs:957-962`.
6. Client got HTTP 202, signal is gone, invocation is stuck forever.

The research doc's step-by-step is accurate (`awakeable_signal_loss_issue_4566_040926.md` lines 137-164). I verified the dedup gate, epoch ordering, and step-down behavior against the current code. All citations hold.

The **root cause** is that `CommitToken` only proves "the bytes are durably in Bifrost." It does **not** prove "the partition processor applied the command and the caller's intent took effect." `handle_rpc_proposal_command` closes that gap because it waits for the state machine (which runs the dedup gate before the command handler) to emit an action — no action, no reply, and on leadership loss `awaiting_rpc_actions` drains with `LostLeadership` (`leader_state.rs:253-262`), giving the caller a clean retry signal.

### A.3 Is Strategy A the right fix? (Against B/C/D)

Agree with the research doc's alternative analysis (lines 232-261), with a sharper framing:

- **Strategy B (drain in-flight on step-down)** is a local band-aid. It adds coupling between `become_follower()` and Bifrost appender internals, and would have to handle appender failures, shutdown race, and the fact that a record already handed to the loglet can't be un-queued cleanly. Worse, it doesn't fix the fundamental conflation of "durable in log" with "executed by state machine" — this same conflation could bite other future features.
- **Strategy C (per-epoch producer namespaces)** is strictly worse. It would permit old-epoch commands to execute under a new leader, which is exactly the split-brain the dedup fence was designed to prevent. Adding idempotency-at-the-command-layer to compensate is a huge increase in surface area.
- **Strategy D (re-propose on takeover)** requires correlating LSNs with execution status across leadership transitions and introduces a new source of duplicate execution if the correlation is imperfect. It's the most invasive of the four.

**Strategy A is the right call**: it reuses a proven code path (`/send` has used `handle_rpc_proposal_command` forever — `append_invocation.rs:56-73`), it makes the response contract match what the user actually cares about ("my signal was delivered"), and it cleanly composes with the existing `LostLeadership` retry at the ingress layer (`rpc_request_dispatcher.rs:62` — `is_idempotent || e.is_safe_to_retry()` is always true for these RPCs).

**Critique of the research doc's framing**: it calls Strategy A "higher latency for signal acknowledgment" without stressing that this is the semantically correct definition of acknowledgment. The old path's low latency was not a feature — it was a bug that leaked "durable" to mean "done."

### A.4 Is there a cheaper fix that preserves fast-ack?

Short answer: **no**, not without introducing a different bug or a large new subsystem. Any "fast-ack" scheme still has to guarantee that the ack implies the dedup gate was cleared, and the dedup gate is by design a property of the state machine's apply loop. Trying to move that guarantee upstream of the apply loop means either serializing step-down with proposals (Strategy B — complex) or breaking the fence (Strategy C — dangerous). You can't get "Bifrost-commit latency" and "post-fence correctness" at the same time without structural changes to the fencing model.

---

## B. Honest Latency Assessment

### B.1 Fact-check on the 5x framing

The user's "1ms → 5ms is 5x" is derived from the research doc's Performance Impact Analysis (lines 499-600). The cited numbers:

| Scenario | Current | New | Delta |
|---|---|---|---|
| Local loglet, low load | 1-5ms | 2-10ms | +1-5ms |
| Local loglet, full batch | 1-5ms | 5-15ms | +4-10ms |
| Replicated loglet, low load | 5-50ms | 7-55ms | +2-5ms |

The "5x" framing is **technically defensible but misleading** because:

1. It picks the *lowest* "current" number (1ms, hypothetical with fsync off, low load) and the *middle* of the "new" range (5ms). If you use the realistic local-loglet-with-fsync midpoint (3ms current → 6ms new), it's 2x. If you compare full-batch to full-batch (5ms → 15ms), it's 3x. If you compare replicated-loglet (which is what a real cluster runs), it's closer to 1.1x (55ms → 55-60ms).
2. It's not a "slowdown" in the fair sense. The old number was wrong — it acknowledged something that wasn't yet guaranteed. Comparing a broken-but-fast number to a correct-but-slightly-slower number as "slowdown" is not a fair frame.

The numbers in the doc look plausible after cross-checking: the local loglet uses `TailOffsetWatch` (`tokio::watch`) for self-reads so there's no network round-trip, and the extra cost is dominated by the partition store's `transaction.commit()` (`partition/mod.rs:674`) plus potentially waiting for up to 31 other records in a batch (`max_command_batch_size = 32`, `types/src/config/worker.rs:153`).

### B.2 Absolute vs relative — what matters

Awakeables are **external HTTP callbacks**. A payment webhook, a user approval, a human-in-the-loop decision. The caller is almost certainly across the public internet, which adds 10-200ms of round-trip latency that dwarfs any local processing delta. In that context:

- 5x relative but 4ms absolute = **noise**.
- The client already has to handle retries on network glitches, so an occasional `LostLeadership` → retry at ingress is in the same class of events it already tolerates.
- There is no workload where "awakeable resolution throughput" is a hot path. These are by definition rate-limited by external event arrival.

### B.3 Is there a workload where the 5x matters?

No direct internal caller exists. `append_signal` / `append_invocation_response` are reached via ingress HTTP (`crates/ingress-http/src/handler/awakeables.rs:78-107`) and the `RpcRequestDispatcher` (`rpc_request_dispatcher.rs:138-163`). There's no internal machine-speed path that needs sub-millisecond signal fan-out. The research doc's volume assessment (lines 567-577) is correct.

### B.4 Cross-reference with `/send` — the decisive argument

`/send` already uses `handle_rpc_proposal_command` (`append_invocation.rs:66-73`) and has this exact latency profile. `/send` is the single most used partition processor RPC in Restate — every invocation the system handles goes through it. If the 2-10ms delta were unacceptable, it would have been a problem years ago. It is not.

**Corollary**: the "new" latency for awakeables is not a regression from some theoretical baseline — it is a convergence with the baseline that the rest of the system already uses.

### B.5 Mitigations, if ever needed

If the latency ever genuinely hurt someone, the mitigations are pre-existing knobs, not new code:

- **Lower `max_command_batch_size`** (`types/src/config/worker.rs:153`) from 32 to 8 or 16. Reduces head-of-line blocking inside a batch at the cost of more per-record RocksDB commits.
- **Disable WAL fsync** on Bifrost (`types/src/config/bifrost.rs:199` — `rocksdb_disable_wal_fsync`). This drops Bifrost write latency from 1-5ms to ~0.1ms.

Neither should ship with this fix. They are levers for a problem that does not yet exist.

### B.6 Verdict

**The latency penalty is justified and the fix should ship as-is.** The "5x" is a real but misleading framing: it compares a broken number to a correct number in the most favorable arithmetic for the old path. In absolute terms the delta is single-digit milliseconds on a path dominated by external HTTP latency, and it exactly matches `/send`'s long-standing profile.

---

## C. Correctness Review of the Fix

### C.1 Does the fix actually close the bug?

Walking through the same race with the new code:

1. Old leader (epoch `N`) receives `append_signal`. Handler calls `handle_rpc_proposal_command` (`append_signal.rs:39-50`) which stashes the reciprocal in `awaiting_rpc_actions` keyed by `request_id` (`leader_state.rs:417-423`) and proposes `NotifySignal { …, request_id: Some(request_id) }` via `SelfProposer::propose` (no commit notification).
2. Race: old proposal `{N, K}` and new `AnnounceLeader{N+1, 0}` both land in Bifrost.
3. Partition processor reads the batch. `AnnounceLeader` is processed first, dedup mark advances to `{N+1, 0}`.
4. Old `NotifySignal{N, K}` hits the dedup gate (`mod.rs:957-962`) → **dropped silently** → no state machine apply → **no `ForwardAppendedResponse` action emitted**.
5. Meanwhile, `become_follower` calls `LeaderState::stop` (`leadership/mod.rs:591`) which drains `awaiting_rpc_actions` with `PartitionProcessorRpcError::LostLeadership` (`leader_state.rs:253-262`).
6. Ingress receives `LostLeadership`, which is treated as safe-to-retry because `is_idempotent=true` (`rpc_request_dispatcher.rs:143, 62`).
7. The retry lands on the new leader (epoch `N+1`), proposes under `{N+1, M}` which is past the fence, state machine applies it, action fires, reply sent.

**The bug is closed.** The fix relies on three existing invariants, all of which hold in the code I read:
- `LeaderState::stop` drains `awaiting_rpc_actions` on every leadership loss.
- Ingress retries idempotent RPCs on `LostLeadership`.
- The state machine only emits `ForwardAppendedResponse` when the command actually applied.

### C.2 WAL backward compatibility

**`NotifySignalRequest`** (`crates/types/src/invocation/mod.rs:1327-1334`) is serialized *directly* (no `serde_hacks` wrapper). The new field is correctly annotated `#[serde(default)]`. Old WAL entries will deserialize to `request_id: None`, which flows through the state machine without emitting the action. **Safe.**

**`InvocationResponse`** (`crates/types/src/invocation/mod.rs:567-578`) uses `#[serde(from/into = "serde_hacks::InvocationResponse")]`. The outer struct has `#[serde(default)]` on `request_id` (line 576), but that annotation is **dead** — the `from`/`into` attributes mean serde always goes through the intermediate type. The intermediate type (`serde_hacks::InvocationResponse` at `mod.rs:1507-1517`) also has `#[serde(default)]` on `request_id` (line 1515), and that one is the effective one. The round-trip impls (lines 1519-1541) correctly forward the field.

**Minor cleanup (not blocking)**: drop the dead `#[serde(default)]` on the outer `InvocationResponse.request_id` (line 576) or add a comment noting it's handled by the intermediary.

### C.3 Protobuf round-trip / outbox path

`crates/storage-api/src/protobuf_types.rs:3308-3349` (for `InvocationResponse`) and `3351-3436` (for `NotifySignalRequest`) explicitly set `request_id: None` on the decode path and ignore it on the encode path. This is **correct and intentional**:

- The outbox is used for cross-partition messaging via shuffle (`OutboxMessage::ServiceResponse(InvocationResponse)`, `OutboxMessage::NotifySignal(NotifySignalRequest)` in `outbox_table/mod.rs`).
- Outbox messages carry `ProducerId::Partition(id)` dedup, *not* `ProducerId::Other("SELF")`. They are not subject to the epoch-fence bug in the first place.
- The receiving partition's state machine receives the command with `request_id = None` → the `if let Some(request_id)` check simply skips the action emission.
- There is no RPC reciprocal to fulfill on the receiving partition side — the RPC originally landed on whichever partition the caller first reached (via `partition_processor_rpc_client.rs:371-374`), and that partition's reciprocal was already closed.

**Safe.**

### C.4 Action emission point and leader gating

The action emission is placed **after** the `?`-propagating `apply` calls → only emitted on successful state machine execution. Actions are collected in `self.action_collector`, which is drained by `leadership_state.handle_actions(...)` at `partition/mod.rs:679` — only after the outer `transaction.commit().await?` at line 674.

So the ordering is: state machine succeeds → transaction commits → actions dispatched → reply sent. This is the correct invariant for "reply implies executed."

**Leader gating**: The action is emitted unconditionally (no `if self.is_leader` gate). This is **correct by design**:

- `action_collector` is drained and dispatched via `LeadershipState::handle_actions`.
- That method pattern-matches on `State`: for `Follower` and `Candidate` it is a no-op. Only `Leader` forwards to `LeaderState::handle_actions`.
- Followers will collect the action and then discard it at the outer handle layer.

This is consistent with how every other `Forward*` action in the enum works — none of them gate on `is_leader` at the emission site. For consistency, **don't add a gate**.

**Confirmed**: the `action_collector.clear()` logic at `partition/mod.rs:574, 627` correctly handles mid-batch `AnnounceLeader` transitions without any new code. This protects `ForwardAppendedResponse` for free.

**Verdict**: action emission is correct.

### C.5 Leadership-loss drain

`LeaderState::stop` at `leader_state.rs:253-262` drains `awaiting_rpc_actions` with `LostLeadership`. The new `ForwardAppendedResponse` path uses this exact map, so it **inherits the drain for free**. One of the nicer properties of this refactor.

### C.6 TDD regression test quality — the weakest part of the fix

Let me be direct: `issue_4566_uses_handle_rpc_proposal_command` in `append_signal.rs:69-99` is a **shape test against a mock**, not a regression test for the race. It proves:
- The handler calls `handle_rpc_proposal_command` and not `self_propose_and_respond_asynchronously`.
- The `request_id` is threaded into the `NotifySignalRequest`.

It does **not** prove:
- That the state machine actually emits `Action::ForwardAppendedResponse` when processing `Command::NotifySignal` with `request_id = Some(...)`.
- That the action dispatcher actually fulfills the reciprocal.
- That the drain on leadership loss hits this path.

A motivated developer refactoring later could accidentally remove the action emission in `state_machine/mod.rs` and the only test that would catch it is — nothing I could find.

**What's missing** (in priority order):

1. **A state machine test at `TestEnv` level** that applies `Command::NotifySignal { request_id: Some(x), … }` to a valid invocation and asserts `Action::ForwardAppendedResponse { request_id: x }` is in the returned actions. Catches regressions in the emission point. Same for `Command::InvocationResponse`.
2. **A symmetric mock-shape test for `append_invocation_response.rs`**. It's missing entirely. The existing file has no `#[cfg(test)]` module.
3. **A simulation test** that exercises the full race (old-epoch self-proposal + AnnounceLeader from new epoch) and asserts the RPC reciprocal receives `LostLeadership`. The existing `issue_4566_dedup_drops_old_epoch_self_proposal` in `tests/simulation.rs:553-673` proves the dedup drops the record, but does not reach the `awaiting_rpc_actions` drain layer.

**Recommendation**: before merging, add at least (1) — a TestEnv action-emission assertion for both `NotifySignal` and `InvocationResponse`. This is cheap and directly exercises the load-bearing lines.

### C.7 Missed migrations — is partial OK?

The fix migrates `append_signal` and `append_invocation_response` but leaves:

- **`append_invocation` Appended mode** (`append_invocation.rs:45-55`) still on `self_propose_and_respond_asynchronously`.
- **`restart_as_new_invocation` simple path** (deferred per task context).

Searching callers of `AppendInvocationReplyOn::Appended`: the only production callers in `partition_processor_rpc_client.rs` use `Submitted` (line 263) and `Output` (line 290). **There is no production caller that invokes the `Appended` branch.** It is effectively dead code. That means:

- Shipping the partial fix is **safe**. The `append_invocation::Appended` branch is unreachable in practice.
- However, it is **confusing** — a reader might not realize it's dead.

**Recommendation**: add a short comment at `append_invocation.rs:45` noting that Appended mode has no production caller and is deferred to a follow-up migration that removes `self_propose_and_respond_asynchronously` entirely.

---

## D. Residual Gaps & Recommendations

### D.1 Release notes entry — **MUST add**

Per project `CLAUDE.md`, this is a bug fix with a behavior change (latency profile shifts, response semantics tighten). Warrants an entry in `release-notes/unreleased/`. Suggested name: `4566-fix-awakeable-signal-loss-on-leadership-change.md`.

Skeleton:
- **Bug Fix**: Awakeable signals and invocation responses could be silently lost during partition leadership transitions.
- **What Changed**: `append_signal` and `append_invocation_response` now reply after state machine execution rather than Bifrost commit, matching `/send`.
- **Impact**: Response latency increases by ~2-10ms (same profile as `/send`). HTTP 202 now correctly implies the signal was applied. On leadership transitions, callers may observe transient `LostLeadership` errors (retried automatically by the ingress dispatcher).
- **Migration**: None required.

### D.2 Dead `#[serde(default)]` on outer `InvocationResponse.request_id`

Minor. `crates/types/src/invocation/mod.rs:576` — the annotation is never evaluated because of the `from/into` serde hacks. Either drop it or comment why it's there.

### D.3 Deprecation comment on `self_propose_and_respond_asynchronously`

This method is still referenced by `append_invocation` (Appended, dead) and `restart_as_new_invocation` (legacy). A future contributor might reach for it as a pattern. Add a doc comment at `leader_state.rs:428`:

```rust
/// DEPRECATED: Do not use for new code. This path responds to the caller when
/// Bifrost commits, which is not a sufficient acknowledgment — the record
/// can still be silently dropped by the dedup gate during leadership
/// transitions (see issue #4566). New RPCs must use
/// `handle_rpc_proposal_command` which replies only after the state machine
/// has executed the command. This method is retained only for call sites
/// that have not yet been migrated.
```

### D.4 No test coverage for `InvocationResponse` action emission

Highlighted in C.6. The `append_invocation_response.rs` file has no `#[cfg(test)]` module. Add a symmetric mock-shape test plus a TestEnv test that applies `Command::InvocationResponse { request_id: Some(x), … }` and asserts the action is in the returned actions.

### D.5 Edge case: `request_id` reused across retries

The ingress dispatcher generates `request_id` once outside the retry loop (`rpc_request_dispatcher.rs:142, 156`). If a retry after `LostLeadership` lands on a new leader that still has the old `request_id` in `awaiting_rpc_actions` (unlikely but possible during rapid transitions), `handle_rpc_proposal_command` handles it via `Entry::Occupied` (`leader_state.rs:407-415`) by replacing the reciprocal and failing the old one with `"retried"`. **Safe, no change needed.**

### D.6 No RPC timeout on the partition processor side

Not introduced by this fix but worth flagging: `awaiting_rpc_actions` is an unbounded `HashMap` with no timeout (`leader_state.rs:80`). If a proposal is committed but somehow the state machine apply never happens, the reciprocal remains until leadership loss. Pre-existing property of `handle_rpc_proposal_command`. Not a blocker for this fix; file as a follow-up observability improvement.

---

## Summary of Action Items (prioritized)

**Must-do before ship:**
1. Add a release note in `release-notes/unreleased/4566-*.md` (D.1).
2. Add a TestEnv action-emission test for both `NotifySignal` and `InvocationResponse` with `request_id: Some(x)` → asserts `Action::ForwardAppendedResponse` in the actions (C.6).

**Should-do:**
3. Add a symmetric mock-shape test for `append_invocation_response.rs` matching `append_signal.rs`'s test (C.6).
4. Add the deprecation doc comment to `self_propose_and_respond_asynchronously` (D.3).
5. Add the dead-branch comment at `append_invocation.rs:45` noting that Appended mode has no production caller and is deferred (C.7).

**Nice-to-have:**
6. Drop the dead `#[serde(default)]` on the outer `InvocationResponse.request_id` or comment why it's there (D.2).
7. File a follow-up for the pre-existing no-timeout / unbounded-map observation on `awaiting_rpc_actions` (D.6).
8. File a follow-up for migrating `append_invocation` Appended mode and `restart_as_new_invocation` for symmetry.

---

## Scores

- **Security Posture**: 9/10 — No security-relevant surface area touched.
- **Logic Correctness**: 9/10 — Fix closes the bug; minor gap is in regression test depth, not the fix itself.
- **Code Quality**: 8/10 — Clean, minimal, follows existing patterns; dead `#[serde(default)]` and missing release note are small blemishes.
- **Production Readiness**: 8/10 — Ready modulo release note and deeper action-emission test.

**Verdict**: The fix is **correct** and **ready to ship** subject to the must-do items (release note + state-machine-level regression test).
