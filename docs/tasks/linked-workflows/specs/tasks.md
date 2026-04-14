# Linked Workflows — Tasks

Source: `docs/plans/specs/plan_linked_workflows.md`

## Wave 1: Phase 1 — Link Table + CreateLink (Complete)

- [x] **1.1** Storage API domain types and traits
- [x] **1.2** Protobuf types
- [x] **1.3** Partition store implementation + CRUD test
- [x] **1.4** `ServiceInvocationResponseSink::Link` variant
- [x] **1.5** `CreateLinkCommand` journal entry + state machine handler

## Wave 2: Phase 2 — Completing + LinkCompletionNotification (Complete)

- [x] **2.1** Happy path integration test (TDD red)
- [x] **2.2** `InvocationStatus::Completing` variant + ~25 match sites + protobuf
- [x] **2.3** `LinkCompletionNotification` message + outbox/command wiring
- [x] **2.4** Completion path: transition to `Completing` when active links exist
- [x] **2.5** `Link` sink handler in `send_response_to_sinks`
- [x] **2.6** `LinkCompletionNotification` handler on parent's partition
- [x] **2.7** Disallow restart-as-new on linked workflows
- [x] **2.8** Happy path integration test passes (TDD green)

## Wave 3: Phase 3 — RemoveLink + Cancel Propagation (Complete)

- [x] **3.1** Unhappy path integration test (TDD red)
- [x] **3.2** `RemoveLinkCommand` journal entry + state machine handler
- [x] **3.3** Cancel propagation through links
- [x] **3.4** Unhappy path integration test passes (TDD green)

## Post-MVP (Complete)

- [x] **4.1** `ServiceInvocationResponseSink::HandlerInvocation` (onCompleted for VOs)
- [x] **4.2** CreateLink/RemoveLink service protocol v4 codec (replaced unimplemented!() stubs)

## Review Fixes (Complete)

- [x] Workflow-only Completing guard (VOs never enter Completing)
- [x] Key links by child_service_id (removed label)
- [x] Cancel from Invoked/Suspended/Paused propagates to linked children
- [x] Kill on Completing bypasses link barrier via transition_completing_to_completed
- [x] Link barrier check moved outside sinks/retention guard
- [x] Extracted cancel_linked_children and transition_completing_to_completed helpers
- [x] Test consolidation (8 → 7 focused integration tests)
