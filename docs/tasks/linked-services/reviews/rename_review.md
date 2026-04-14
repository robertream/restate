# Review: Naming Convention Refactor (link_to/link_from/linked_to/linked_from)

**Date**: 2026-04-14
**Commits**: 8033c2259, 7e5151c3d
**Scope**: Struct field renames, proto field renames, function/variable renames across linked-services code

## Summary Assessment

Good execution. The tense-based convention is self-documenting and consistently applied at the domain-type level. Proto wire format preserved. 36/36 tests pass. Residual `remote` naming at the storage layer is the main gap.

**Scores (0-10):**
- Security Posture: 10 — no security-relevant changes
- Logic Correctness: 10 — behavioral no-op, all tests pass
- Code Quality: 8 — clean at domain level, storage layer has leftover `remote` naming
- Production Readiness: 9 — wire compatible, well-tested

## Findings

### 🚨 CRITICAL: None

### 🔥 HIGH: None

### ⚠️ MEDIUM

**M1. `remote` parameter name survives in storage-api trait signatures**

6 public trait methods still use `remote: &EntityId`:
- `service_edges_table/mod.rs` — `get_service_edge`, `put_service_edge`, `delete_service_edge`
- `invocation_edges_table/mod.rs` — `get_invocation_edge`, `put_invocation_edge`, `delete_invocation_edge`

Plus their partition-store implementations. Options: `target` (direction-neutral) or `entity` (matches type name).

**M2. `remote_type`/`remote_key` in define_table_key! macros and decode_entity params**

- `service_edges_table/mod.rs:35-36` — key struct fields `remote_type`, `remote_key`
- `invocation_edges_table/mod.rs:36-37` — same
- `edge_encoding.rs:37` — `fn decode_entity(remote_type: u8, remote_key: Bytes)`

Should be `entity_type`/`entity_key` to match function/type naming.

**M3. `remote_type` module re-export**

`service_edges_table/mod.rs` exports a `remote_type` module with `OBJECT`/`WORKFLOW_INVOCATION` constants, re-exported by `invocation_edges_table`. Consider `entity_type` module name.

### 💡 LOW

**L1. Doc comments still use parent/child terminology**

~10 doc comments reference "parent"/"child" as conceptual terms (e.g., `types/invocation/mod.rs:784`, `link_service_command.rs:54`, `outbox_table/mod.rs:41`). Arguably fine as prose but could be updated for full consistency.

**L2. edge_encoding.rs null-byte separator**

`encode_entity` uses `\0` as separator between service_name and key. If a service name contained `\0`, decoding would split incorrectly. Extremely unlikely in practice (SDK names are ASCII identifiers). Consider a `debug_assert!` guard.

## Proto Compatibility: Verified

All field numbers preserved across LinkRequest, LinkResponse, UnlinkRequest, UnlinkResponse, LinkCompletionNotification, ServiceLinkNotification, InvocationLinkNotification, InvocationStatusV2, VirtualObjectStatus. **No wire format breaks.**

## Convention Consistency: Verified

| Struct | Fields | Convention | Correct? |
|--------|--------|-----------|----------|
| LinkRequest | link_to, link_from | imperative (creating) | ✅ |
| LinkResponse | linked_from, linked_to | fact (established) | ✅ |
| UnlinkRequest | linked_to, linked_from | fact (existing link) | ✅ |
| UnlinkResponse | linked_from | fact | ✅ |
| LinkCompletionNotification | linked_from, linked_to | fact | ✅ |
| ServiceInvocation.link_from | link_from | imperative (piggybacking) | ✅ |
| ServiceLinkNotification | linked_from | fact (stored sink) | ✅ |
| InvocationLinkNotification | linked_from | fact (stored sink) | ✅ |
| retain_non_linked_from_sinks | — | fact (established sinks) | ✅ |
