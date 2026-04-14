# LinkCommand + LinkedNotification — Tasks

Source: `docs/plans/specs/plan_link_command.md`

## Wave 1: Phase 1 — Revert OneWayCallCommand + Add LinkCommand Type (Complete — prior commits)

- [x] **1.1** Remove `linked: bool` from OneWayCallCommand struct, proto, codec
- [x] **1.2** Remove link logic from `_ApplyCallCommand`, remove `ReadLinkTable + WriteLinkTable` bounds
- [x] **1.3** Add `LinkCommand` struct to types crate + `LinkedNotification` outbox message + WAL variant
- [x] **1.4** Add `LinkCommandMessage` and `LinkedNotificationMessage` to service protocol proto
- [x] **1.5** Add codec encode/decode for `LinkCommand` and `LinkedNotificationMessage`

## Wave 2: Phase 2 — LinkCommand Handler + LinkedNotification Handler (Complete)

- [x] **2.1** `LinkCommand` handler: validate, write ParentOf, enqueue ServiceInvocation with Link sink on response_sink, defer completion
- [x] **2.2** `on_service_invocation`: when Link sink present, write ChildOf + send LinkedNotification Ok; rejection path sends LinkedNotification Err directly
- [x] **2.3** `LinkedNotification` handler on parent: Ok delivers completion, Err deletes ParentOf + delivers error + re-checks Completing
- [x] **2.4** [SKIPPED — not dead code yet] AttachInvocation Link path kept for backward compat; existing tests still use it

## Wave 3: Phase 3 — Tests (Complete)

- [x] **3.1** Happy path test: LinkCommand → LinkedNotification Ok → SDK completion → child completes → Completing → Completed
- [x] **3.2** Rejection test: LinkCommand → duplicate workflow → LinkedNotification Err → SDK error → link deleted
- [x] **3.3** [SKIPPED — not needed] Existing tests already use direct storage setup via insert_outgoing_link helper

## Review Fixes (Complete)

- [x] F1: Graceful completion for parent-not-keyed (was ApplyCommandEffect crash)
- [x] F2: Use caller_invocation_id directly in LinkedNotification Err path
- [x] F3: Add warn log for impossible non-keyed service case
- [x] F4: Fix import ordering in test file
