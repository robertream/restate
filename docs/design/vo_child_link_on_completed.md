# Spec: onCompleted Handler Registration for Virtual Objects

## Context

Linked workflows allow virtual objects (VOs) and workflows to form parent-child lifecycle relationships. When a parent creates a link to a child workflow, the child's lifecycle is bound to the parent — completion, cancellation, and state retention are coordinated.

**Problem:** Workflows can `await` a linked child's future directly because their handlers are long-running. Virtual objects cannot — their handlers are short-lived exclusive handlers that run to completion and release the lock. VOs currently have no SDK-level mechanism to react to a linked child's completion.

**Solution:** Allow VOs to register an `onCompleted` handler name at link creation time. When the child completes (success, failure, or cancellation), Restate automatically invokes the named Exclusive handler on the parent VO with the child's result as the argument. The handler name is stored on the `ParentOf` link record — no changes to response sinks, outbox messages, or the child's completion fan-out path.

## Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Client type | SDK handler context only | External clients don't create links |
| Mechanism | Handler name on ParentOf record, fired from `on_link_completion_notification` | Piggybacks on existing cross-partition notification; no sink changes needed |
| Registrant | Parent only | Only the handler creating the link registers the callback |
| Timing | At link creation time | Atomic — stored in ParentOf record alongside the link |
| SDK API | Options bag: `ctx.linkWorkflow(target, { onCompleted: "handlerName" })` | Matches existing Restate SDK conventions; scales to future options |
| Target | Same service only | Handler must be defined on the creating VO |
| Handler type | Always Exclusive | Consistent with VO handler model |
| Parent scope | No server-side validation | SDK controls the API surface — only exposes `onCompleted` to VOs. Server accepts the field from any parent type. |
| Child scope | VOs and workflows | Children can be either type |
| Required? | Optional | Links can exist purely for state retention or synchronized cancellation |
| Per-child handlers | Yes | Each `linkWorkflow()` can specify a different handler name |
| Handler arguments | Success: raw output bytes. Failure: proto-encoded `Failure` message bytes. | Reuses the existing `Value`/`Failure` pattern from the service protocol. Applied consistently to both `HandlerInvocation` sink and onCompleted. |
| Fires on cancel/kill? | Yes, always | Handler receives proto-encoded `Failure` with ABORTED code |
| Already-completed child | Link establishment fails | LinkCommand targeting an already-running/completed workflow is rejected via `LinkedNotification Err` — handler doesn't fire |
| Handler error semantics | Fire and forget | Normal retry semantics apply; result is discarded |
| RemoveLink semantics | Deleting ParentOf is sufficient | `LinkCompletionNotification` for a deleted ParentOf is stale and ignored — handler never fires. No sink removal needed. |
| Storage | Handler name in `ChildStatus::Running` on `ParentOf` record | Consumed at `Running → Completed` transition; no leftover fields |
| Observability | Deferred | Handler invocation uses `SpanContext::empty()` for now. Linked traces (parent + child) can be added in follow-up. |
| Admin API | Deferred | Handler name visible in link/sink inspection can be added in follow-up. |

## Type Refactoring

Replace `EdgeLabel` + `LinkState` with `LinkRelation` + `ChildStatus`:

```rust
// crates/storage-api/src/link_table/mod.rs

type CompletionHandlerName = ByteString;

/// The parent's view of the child's lifecycle. Only on ParentOf records.
pub enum ChildStatus {
    Running(Option<CompletionHandlerName>),
    Completed(ResponseResult),
}

impl ChildStatus {
    /// Transition Running → Completed, yielding the completion handler name if registered.
    fn complete(self, result: ResponseResult) -> (ChildStatus, Option<CompletionHandlerName>) {
        let handler = match self {
            ChildStatus::Running(h) => h,
            _ => None,
        };
        (ChildStatus::Completed(result), handler)
    }
}

/// The relationship between local and remote service.
pub enum LinkRelation {
    ParentOf(ChildStatus),
    ChildOf,
}

pub struct Link {
    pub local_service_id: ServiceId,
    pub remote_service_id: ServiceId,
    pub relation: LinkRelation,
}
```

`LinkRelation` provides the key byte for RocksDB prefix scans:

```rust
impl LinkRelation {
    pub fn key_byte(&self) -> u8 {
        match self {
            LinkRelation::ParentOf(_) => 0x00,
            LinkRelation::ChildOf => 0x01,
        }
    }
}
```

Benefits:
- `ChildOf` records carry no state — can't accidentally have a `ChildStatus` on a `ChildOf` record
- `CompletionHandlerName` lives only in `Running` — consumed at the `Running → Completed` transition via `ChildStatus::complete()`
- No "only meaningful for ParentOf" comments — the type system enforces it

## Server-Side Changes

### 1. Proto Changes

**`service-protocol/dev/restate/service/protocol.proto` — LinkCommandMessage**

Add optional field:
```protobuf
message LinkCommandMessage {
  // ... existing fields ...
  optional string completion_handler_name = 13;
}
```

**`crates/storage-api/proto/dev/restate/storage/v1/domain.proto` — Link**

Restructure to match new types:
```protobuf
message Link {
  message ParentOfRunning {
    optional string completion_handler_name = 1;
  }
  message ParentOfCompleted {
    ResponseResult result = 1;
  }
  message ChildOf {}

  ServiceId remote_service_id = 1;

  oneof relation {
    ParentOfRunning parent_of_running = 2;
    ParentOfCompleted parent_of_completed = 3;
    ChildOf child_of = 5;
  }
}
```

### 2. Type Changes

**`crates/types/src/journal_v2/command.rs` — LinkCommand**

Add field:
```rust
pub struct LinkCommand {
    pub request: CallRequest,
    pub invoke_time: MillisSinceEpoch,
    pub completion_id: CompletionId,
    pub name: ByteString,
    pub completion_handler_name: Option<ByteString>,  // NEW
}
```

**`crates/types/src/invocation/mod.rs` — ServiceInvocation**

Add factory method for internal handler invocations:
```rust
impl ServiceInvocation {
    pub fn internal_handler_invocation(
        service_id: &ServiceId,
        handler_name: ByteString,
        argument: Bytes,
    ) -> Self {
        let target = InvocationTarget::virtual_object(
            service_id.service_name.clone(),
            service_id.key.clone(),
            handler_name,
            VirtualObjectHandlerType::Exclusive,
        );
        let id = InvocationId::generate(&target, None);
        Self {
            argument,
            ..Self::initialize(id, target, Source::Internal)
        }
    }
}
```

**No changes to `ServiceInvocationResponseSink::Link`** — the handler name lives on the ParentOf record, not the response sink.

### 3. LinkCommand Handler Changes

**`crates/worker/src/partition/state_machine/entries/link_command.rs`**

Write the `completion_handler_name` into the `ParentOf` record:

```rust
ctx.storage.put_link(&Link {
    local_service_id: parent_service_id.clone(),
    remote_service_id: child_service_id,
    relation: LinkRelation::ParentOf(
        ChildStatus::Running(self.entry.completion_handler_name.clone())
    ),
})?;
```

The `ServiceInvocation` and `Link` response sink are unchanged.

### 4. Handler Invocation from `on_link_completion_notification`

**`crates/worker/src/partition/state_machine/mod.rs`**

In `on_link_completion_notification`, use `ChildStatus::complete()` to transition and extract the handler name:

```rust
async fn on_link_completion_notification(&mut self, notification: LinkCompletionNotification) -> Result<(), Error> {
    // ... existing: read ParentOf record, check stale ...

    // Transition Running → Completed, extracting handler name.
    let handler_name = if let LinkRelation::ParentOf(status) = link.relation {
        let (completed, handler) = status.complete(child_result.clone());
        link.relation = LinkRelation::ParentOf(completed);
        handler
    } else {
        None
    };
    self.storage.put_link(&link).map_err(Error::Storage)?;

    // If a completion handler was registered, emit the handler invocation.
    if let Some(handler_name) = handler_name {
        self.handle_outgoing_message(OutboxMessage::ServiceInvocation(Box::new(
            ServiceInvocation::internal_handler_invocation(
                &owner_service_id,
                handler_name,
                encode_handler_argument(&child_result),
            ),
        )))?;
    }

    // ... existing: check remaining children, transition Completing → Completed ...
}
```

### 5. Fix `HandlerInvocation` Sink — Consistent Failure Encoding

**`crates/worker/src/partition/state_machine/mod.rs` — `send_response_to_sinks()`**

Update the existing `HandlerInvocation` arm to use the shared factory and consistent failure encoding:

```rust
ServiceInvocationResponseSink::HandlerInvocation {
    service_id,
    handler_name,
} => {
    let argument = encode_handler_argument(&result);
    self.handle_outgoing_message(OutboxMessage::ServiceInvocation(Box::new(
        ServiceInvocation::internal_handler_invocation(
            &service_id,
            handler_name,
            argument,
        ),
    )))?;
}
```

The shared `encode_handler_argument` helper (in `crates/worker/src/partition/state_machine/mod.rs` — `service-protocol-v4` is already a worker dependency):
```rust
fn encode_handler_argument(result: &ResponseResult) -> Bytes {
    match result {
        ResponseResult::Success(bytes) => bytes.clone(),
        ResponseResult::Failure(err) => {
            use prost::Message;
            let proto_failure = proto::Failure::from(Failure::from(err.clone()));
            Bytes::from(proto_failure.encode_to_vec())
        }
    }
}
```

This is a behavior change from the previous `Bytes::new()` on failure, but `HandlerInvocation` is unreleased code on this branch.

### 6. Codec Changes

**`crates/service-protocol-v4/src/entry_codec.rs`**

Add `completion_handler_name` to LinkCommand encode/decode:

- Encode: write `entry.completion_handler_name` into `LinkCommandMessage.completion_handler_name`
- Decode: read `msg.completion_handler_name` into `LinkCommand.completion_handler_name`

## What Does NOT Change

- `ServiceInvocationResponseSink::Link` — no new fields
- `send_response_to_sinks` Link arm — untouched (only HandlerInvocation arm updated for consistency)
- `RemoveParentLink` — no new fields
- `on_remove_parent_link` — no sink removal logic
- `LinkedNotification` / `on_linked_notification` — unchanged

## RemoveLink Behavior

When a parent removes a link:
1. `RemoveLinkCommand` deletes the `ParentOf` record (handler name gone with it)
2. `RemoveParentLink` sent to child → deletes `ChildOf` record
3. Child completes later → `LinkCompletionNotification` arrives at parent → no `ParentOf` record found → stale message, logged and ignored
4. Handler never fires

No sink removal needed. The existing stale-message handling in `on_link_completion_notification` covers this case.

## Key Files to Modify

| File | Change |
|------|--------|
| `crates/storage-api/src/link_table/mod.rs` | Replace `EdgeLabel` + `LinkState` with `LinkRelation` + `ChildStatus` + `complete()` method |
| `crates/storage-api/proto/dev/restate/storage/v1/domain.proto` | Restructure `Link` proto to match new types |
| `crates/storage-api/src/protobuf_types.rs` | Update proto ↔ Rust conversion for `Link` |
| `crates/partition-store/src/link_table/mod.rs` | Update key byte derivation from `LinkRelation` |
| `crates/types/src/invocation/mod.rs` | Add `ServiceInvocation::internal_handler_invocation` factory |
| `service-protocol/dev/restate/service/protocol.proto` | Add `completion_handler_name` to `LinkCommandMessage` |
| `crates/types/src/journal_v2/command.rs` | Add field to `LinkCommand` |
| `crates/service-protocol-v4/src/entry_codec.rs` | Encode/decode `completion_handler_name` |
| `crates/worker/src/partition/state_machine/entries/link_command.rs` | Write handler name into `ParentOf` record |
| `crates/worker/src/partition/state_machine/mod.rs` | Emit handler invocation from `on_link_completion_notification`; update `HandlerInvocation` sink for consistent failure encoding; add `encode_handler_argument` helper |
| `crates/worker/src/partition/state_machine/tests/linked_workflows.rs` | Update existing tests for new types + new onCompleted tests |

## Edge Cases

1. **Child already running/completed when link establishes:** LinkCommand targeting an already-running workflow is rejected via `LinkedNotification Err`. The handler doesn't fire because the link was never established. This is consistent — you can't retroactively link to an existing workflow.

2. **RemoveLink before child completes:** ParentOf deleted (handler name gone). `LinkCompletionNotification` arrives later → stale, ignored. Handler never fires.

3. **Parent VO cleared state:** Clear state deletes ParentOf records and sends RemoveParentLink to children. Handler names are deleted with the ParentOf records. Same stale-message handling applies.

4. **Handler name doesn't exist on VO:** The `ServiceInvocation` for the handler will fail at the deployment with "handler not found." Normal retry semantics apply. This is a programming error — no special handling needed.

5. **Multiple links with same handler name:** Allowed. Each child completion fires the same handler independently. If the handler needs to distinguish which child, the VO should store child IDs in state.

6. **Cancellation/kill of child:** Produces an InvocationError with ABORTED code. `LinkCompletionNotification` carries the failure. `on_link_completion_notification` fires the handler with the proto-encoded `Failure` as argument bytes.

7. **Parent killed/canceled while child running:** Parent's cancel path cancels linked children. Children complete with ABORTED. `LinkCompletionNotification` arrives at parent — if parent is already terminated, the `ParentOf` lookup fails (stale). Handler doesn't fire. Clean.

## Verification

1. **Tests in `linked_workflows.rs`:**
   - Happy path: LinkCommand with `completion_handler_name` → child completes → `on_link_completion_notification` emits both state update to `Completed` AND `ServiceInvocation` (handler) with child's result bytes
   - Unhappy path: LinkCommand with `completion_handler_name` → child canceled → handler fires with proto-encoded `Failure` (ABORTED)
   - Note: updating `HandlerInvocation` sink (Section 5) may require adjusting the existing `handler_invocation_sink_queues_callback_on_child_completion` test if it asserts failure arguments
   - Note: "no handler registered" case is implicitly covered by all existing link tests (which don't set `completion_handler_name`) — no dedicated test needed

2. **Build & lint:**
   - `cargo check`
   - `cargo clippy --all-features --all-targets --workspace -- -D warnings`
   - `cargo fmt --all -- --check`

3. **Full test suite:**
   - `cargo nextest run --all-features`
