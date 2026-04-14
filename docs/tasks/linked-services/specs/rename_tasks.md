# Tasks — Link Naming Convention Alignment
*Generated on 2026-04-14*

## Objective
Replace `parent`/`child`/`local`/`remote` field names with `link_to`/`link_from` (creating) or `linked_to`/`linked_from` (established) across all linked-services code. No suffixes — Rust types carry the "what."

## Scope
- **In Scope**: Struct field renames, function renames, local variable renames, proto field name updates (cosmetic), protobuf_types.rs mapping updates
- **Out of Scope**: Proto field number changes, wire format changes, behavioral changes, test logic changes (only identifier renames in tests)

## Requirements Traced
| ID | Description | Source | Tasks |
|----|-------------|--------|-------|
| REQ-001 | `link_to`/`link_from` for link creation context | User request | 1.1, 1.2, 1.3 |
| REQ-002 | `linked_to`/`linked_from` for established link context | User request | 1.1, 1.2, 1.3 |
| REQ-003 | No `parent`/`child`/`local`/`remote` in link code | User request | 1.1, 1.2, 1.3 |
| REQ-004 | No unnecessary suffixes — types carry the "what" | User request | 1.1, 1.2, 1.3 |

---

## Architecture Context

### Where This Fits
Mechanical rename across the linked-services feature code. All changes are in the `linked-services` branch, not yet merged to main.

### Technical Approach
Bottom-up: rename type definitions first (`crates/types`), then fix compilation errors in storage-api and worker. Each struct rename propagates deterministically via compiler errors.

### Key Decisions
- Tense encodes lifecycle: `link_*` = creating, `linked_*` = established
- No suffixes: `linked_from: ServiceId` not `linked_from_service_id: ServiceId`
- Proto field numbers unchanged — wire compatibility preserved

---

## Tasks

### Phase 1: Rename Identifiers

#### [1.1] Rename struct fields in `crates/types/src/invocation/mod.rs`

- [ ] **1.1.1** Rename `LinkRequest` fields: `.local` → `.link_to`, `.parent` → `.link_from`
  - [ ] Field names updated
  - [ ] Doc comments updated to remove "parent"/"child" wording

- [ ] **1.1.2** Rename `LinkResponse` fields: `.local` → `.linked_from`, `.remote` → `.linked_to`
  - [ ] Field names updated
  - [ ] Doc comments updated

- [ ] **1.1.3** Rename `UnlinkRequest` fields: `.local` → `.linked_to`, `.parent` → `.linked_from`
  - [ ] Field names updated
  - [ ] Doc comments updated

- [ ] **1.1.4** Rename `UnlinkResponse` fields: `.local` → `.linked_from`
  - [ ] Field name updated
  - [ ] Doc comment updated

- [ ] **1.1.5** Rename `LinkCompletionNotification` fields: `.local` → `.linked_from`, `.remote` → `.linked_to`
  - [ ] Field names updated
  - [ ] Doc comments updated

- [ ] **1.1.6** Rename `ServiceInvocationResponseSink` variant fields: `ServiceLinkNotification { parent_service_id }` → `{ linked_from }`, `InvocationLinkNotification { parent_invocation_id }` → `{ linked_from }`
  - [ ] Field names updated
  - [ ] Doc comments updated

#### [1.2] Fix compilation in downstream crates

- [ ] **1.2.1** Update `crates/storage-api/src/protobuf_types.rs` — proto↔Rust field mappings for all renamed fields
  - [ ] All `parent` / `local` / `remote` / `parent_service_id` / `parent_invocation_id` references updated
  - [ ] Proto field numbers unchanged

- [ ] **1.2.2** Update `crates/worker/src/partition/state_machine/mod.rs` — all field access sites, rename `retain_non_parent_sinks` → `retain_non_linked_from_sinks`
  - [ ] All struct field accesses compile
  - [ ] Function renamed

- [ ] **1.2.3** Update `crates/worker/src/partition/state_machine/entries/link_service_command.rs` — field access + local variables (`parent_*` → `link_from`, `child_*` → `link_to`)
  - [ ] Compiles with new field names
  - [ ] Local variables use link convention

- [ ] **1.2.4** Update `crates/worker/src/partition/state_machine/entries/unlink_service_command.rs` — field access + local variables (`parent_*` → `linked_from`, `child_*` → `linked_to`)
  - [ ] Compiles with new field names
  - [ ] Local variables use link convention

- [ ] **1.2.5** Update `crates/wal-protocol/src/lib.rs` if any field access exists
  - [ ] Compiles

- [ ] **1.2.6** Update local variables in `mod.rs` handlers: `on_link_request`, `on_link_response`, `on_unlink_request`, `on_unlink_response`, `on_link_completion_notification`, `on_link_from_invocation`, `on_link_from_deduplicated_invocation`
  - [ ] All `parent_*` / `child_*` locals renamed per tense convention
  - [ ] No `parent`/`child`/`local`/`remote` remains in link handler code

#### [1.3] Update tests and proto

- [ ] **1.3.1** Update `crates/worker/src/partition/state_machine/tests/linked_services.rs` — field access in test construction
  - [ ] All tests compile and pass

- [ ] **1.3.2** (Optional) Update proto field names in `domain.proto` for cosmetic consistency
  - [ ] Proto field names match Rust names
  - [ ] Field numbers unchanged

### Phase 2: Validate

#### [2.1] Full validation

- [ ] **2.1.1** `cargo check` passes
- [ ] **2.1.2** `cargo fmt --all -- --check` passes
- [ ] **2.1.3** `cargo clippy` passes
- [ ] **2.1.4** `cargo nextest run -p restate-worker` — all linked-services tests pass

---

## Execution Strategies

### Sequential Execution
1. Task 1.1 — Rename struct fields in types crate (source of truth)
2. Task 1.2 — Fix compilation errors (compiler-guided)
3. Task 1.3 — Update tests and proto
4. Task 2.1 — Validate

### Parallel Execution

**Wave 1**: 1.1 (struct field renames)
**Wave 2**: 1.2.1, 1.2.2, 1.2.3, 1.2.4, 1.2.5 (all compiler-error fixes, can be done file-by-file)
**Wave 3**: 1.3 (tests + proto)
**Wave 4**: 2.1 (validation)

---

## Coverage Summary
- Total Requirements: 4
- Requirements with Task Coverage: 4 (100%)
- Phases: 2
- Parent Tasks: 4
- Sub-tasks: 13
