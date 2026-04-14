# Implementation Plan: LinkCommand in sdk-shared-core

## Overview

Add `LinkCommand` support to the SDK shared core (`sdk-shared-core`). This replaces the `linked: bool` flag on `sys_send` with a dedicated `sys_link` method that returns a `LinkHandle`. The SDK awaits a `LinkedNotificationMessage` completion confirming the link was established (or rejected) before proceeding.

## Current State

`sys_send(..., linked: true)` sets `linked = true` on `OneWayCallCommandMessage`. The SDK gets back a `CallInvocationIdCompletionNotificationMessage` with the child's invocation ID — but this fires immediately, before the link is confirmed. There's no failure signal if linking fails (cycle, non-keyed, duplicate workflow).

## Desired End State

- `sys_link(target, input, options) -> LinkHandle` — new VM method
- `LinkHandle` wraps a single `NotificationHandle` for the `LinkedNotificationMessage` completion
- Completion carries `Ok(InvocationId)` or `Err(LinkError)` — SDK knows whether the link was established
- `linked: bool` removed from `sys_send` / `OneWayCallCommandMessage`
- `RemoveLinkCommandMessage` updated: `name` field removed

## Changes Required

### 1. Proto: Add LinkCommandMessage + LinkedNotificationMessage

File: `service-protocol/dev/restate/service/protocol.proto`

```proto
// Completable: Yes
// Fallible: Yes
// Type: 0x0400 + 21 (0x0415)
message LinkCommandMessage {
    string service_name = 1;
    string handler = 2;
    string key = 3;
    bytes parameter = 4;
    repeated Header headers = 5;
    optional string idempotency_key = 6;
    uint64 invoke_time = 7;

    uint32 result_completion_id = 11;
}

// Type: 0x8015
message LinkedNotificationMessage {
    reserved 2, 3, 4, 12, 16, 17;
    uint32 completion_id = 1;
    // Success: invocation_id as string
    // Failure: InvocationError
    oneof result {
        Value value = 5;
        Failure failure = 6;
    }
}
```

Remove from `OneWayCallCommandMessage`:
- `bool linked = 8`

Remove from `RemoveLinkCommandMessage`:
- `string name = 3`

### 2. MessageType Registration

File: `src/service_protocol/header.rs`

Add to `gen_message_type_enum!` macro:
- `LinkCommand = 0x0415` (command)
- `LinkedNotification = 0x8015` (notification)

### 3. Message Trait Implementations

File: `src/service_protocol/messages.rs`

- `impl_message_traits!(LinkCommand: command)` — or `command eq` if field-by-field comparison is sufficient
- Implement `CommandMessageHeaderEq for LinkCommandMessage` — compare `service_name`, `handler`, `key`, `invoke_time`, `result_completion_id`
- Implement `CommandMessageHeaderDiff for LinkCommandMessage` — diff display for replay mismatch
- `impl_message_traits!(LinkedNotification: notification)`
- Remove `linked` from `CommandMessageHeaderEq` and `CommandMessageHeaderDiff` for `OneWayCallCommandMessage`
- Remove `name` from `CommandMessageHeaderEq` and `CommandMessageHeaderDiff` for `RemoveLinkCommandMessage`

### 4. CommandType Enum

File: `src/lib.rs`

Add variant to `CommandType`:
```rust
LinkCommand,
```

Add `LinkHandle`:
```rust
pub struct LinkHandle {
    pub invocation_id_notification_handle: NotificationHandle,
}
```

### 5. CommandType ↔ MessageType Conversions

File: `src/error.rs`

Add mappings:
- `CommandType::LinkCommand` → `MessageType::LinkCommand`
- `MessageType::LinkCommand` → `CommandType::LinkCommand`

### 6. Display Name

File: `src/fmt.rs`

Add: `CommandType::LinkCommand => "LinkCommand"`

### 7. VM Trait + Implementation

File: `src/lib.rs` (trait) + `src/vm/mod.rs` (impl)

Add to `VM` trait:
```rust
fn sys_link(
    &mut self,
    target: Target,
    input: Bytes,
    execution_time_since_unix_epoch: Option<Duration>,
    options: SendOptions,
) -> VMResult<LinkHandle>;
```

Remove `linked: bool` parameter from `sys_send`.

Implementation in `CoreVM::sys_link`:
1. Allocate one completion ID: `invocation_id_completion_id`
2. Construct `LinkCommandMessage` with `result_completion_id` set
3. Call `do_transition(SysSimpleCompletableEntry(message, completion_id, options))`
4. Return `LinkHandle { invocation_id_notification_handle }`

The pattern is identical to `sys_send` but uses `LinkCommandMessage` instead of `OneWayCallCommandMessage` and doesn't set a `linked` flag.

Update `CoreVM::sys_send`:
- Remove `linked` parameter
- Remove `linked` field from `OneWayCallCommandMessage` construction

Update `CoreVM::sys_remove_link`:
- Remove `name` parameter and field from `RemoveLinkCommandMessage` construction

### 8. Tests

File: `src/tests/linked_workflows.rs`

1. **Happy path**: `sys_link(target, input, options)` → VM emits `LinkCommandMessage` → runtime sends `LinkedNotificationMessage { Ok(invocation_id) }` → `take_notification` returns invocation ID
2. **Rejection**: `sys_link(target, input, options)` → VM emits `LinkCommandMessage` → runtime sends `LinkedNotificationMessage { Err(failure) }` → `take_notification` returns error
3. Update existing `sys_send` tests: remove `linked: true` parameter
4. Update `sys_remove_link` tests: remove `name` parameter

## Implementation Order

### Phase 1: Proto + Registration
1. Update proto: add `LinkCommandMessage`, `LinkedNotificationMessage`, remove `linked` from `OneWayCallCommandMessage`, remove `name` from `RemoveLinkCommandMessage`
2. Regenerate prost types
3. Register `MessageType::LinkCommand` and `MessageType::LinkedNotification` in header.rs
4. Add `impl_message_traits!` and header eq/diff impls in messages.rs
5. Add `CommandType::LinkCommand` + conversions in lib.rs, error.rs, fmt.rs

### Phase 2: VM Methods
1. Add `sys_link` to VM trait and CoreVM impl
2. Remove `linked` from `sys_send` signature and message construction
3. Remove `name` from `sys_remove_link` signature and message construction
4. Add `LinkHandle` type

### Phase 3: Tests
1. Happy path: link established
2. Rejection: link error
3. Update existing linked workflow and remove_link tests

## Out of Scope

- Language SDK wrappers (TypeScript, Java, Python) — they consume the shared core VM trait
- GetLinkCommand
- Any runtime-side changes (covered by separate plan)
