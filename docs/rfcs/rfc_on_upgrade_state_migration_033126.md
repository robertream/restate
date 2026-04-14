---
date: "2026-03-31T18:00:00-07:00"
git_commit: b1766c03b914aec16875d1a1c1cbe240f84255da
branch: service-macro
repository: restatedev/restate
topic: "RFC: onUpgrade State Migration for Virtual Objects"
tags: [rfc, architecture, state-migration, virtual-objects, deployment]
status: draft
last_updated: "2026-03-31"
last_updated_by: Claude Opus 4.6
last_updated_note: "Extracted from State Graph RFC into standalone RFC"
---

# RFC: onUpgrade State Migration for Virtual Objects

| Field | Value |
|-------|-------|
| Date | 2026-03-31 |
| Git Commit | `b1766c03b` |
| Branch | `service-macro` |
| Repository | `restatedev/restate` |
| Status | Draft |
| Authors | Robert Ream |

---

## Motivation

When deploying a new service version that changes the state schema of a virtual object, developers handle migration defensively: check for old format, convert on read. There is no structured migration path. Every handler must guard against stale schema shapes, and there is no way to know which objects have been migrated and which haven't.

This RFC proposes an `onUpgrade` handler — a structured, crash-safe, lazy migration mechanism for virtual object state.

---

## Design

### The `onUpgrade` Handler

A virtual object can declare an `onUpgrade` handler that runs on first access after a deployment version change:

```typescript
const counter = restate.object({
    name: "Counter",
    handlers: {
        increment: async (ctx) => { /* ... */ },
        onUpgrade: async (ctx, { fromVersion, toVersion }) => {
            const old = await ctx.get("count");
            if (typeof old === "number") {
                await ctx.set("count", { value: old, lastModified: Date.now() });
            }
        },
    },
});
```

### Semantics

- **Lazy execution**: `onUpgrade` runs on first access to the object after the deployment version changes. No bulk migration — each object migrates individually when it is first invoked.
- **Exclusive lock**: `onUpgrade` runs under the object's exclusive lock, before the actual handler that triggered the access. No concurrent handler can observe partially-migrated state.
- **Journaled**: The `onUpgrade` execution is journaled like any other handler invocation. If the runtime crashes mid-migration, it replays from the journal. Migration is crash-safe.
- **Version tracking**: The partition store tracks the last deployment version per object key. `onUpgrade` only fires when the stored version differs from the current deployment version.
- **Idempotent by contract**: Developers should write `onUpgrade` to be safe to run multiple times (in case of partial replay). The framework guarantees at-most-once execution per version transition, but defensive coding is good practice.

### Version Identity

The deployment version is derived from the service deployment registration. When a new deployment is registered for a service, the runtime records the new version. On next access to any object of that service, if the object's stored version differs from the current deployment version, `onUpgrade` fires.

The version is opaque to the runtime — it is whatever the deployment system provides (semantic version, git hash, deployment ID). The `fromVersion` and `toVersion` parameters let the handler decide what migration logic to apply.

### Multi-Version Jumps

If an object hasn't been accessed across multiple deployments (v1 → v2 → v3), `onUpgrade` fires once with `fromVersion: v1, toVersion: v3`. The handler is responsible for handling multi-version jumps. A switch/case pattern works well:

```typescript
onUpgrade: async (ctx, { fromVersion, toVersion }) => {
    if (fromVersion < 2) {
        // v1 → v2: rename "count" to "counter"
        const old = await ctx.get("count");
        if (old !== null) {
            await ctx.set("counter", old);
            await ctx.clear("count");
        }
    }
    if (fromVersion < 3) {
        // v2 → v3: wrap counter in object
        const val = await ctx.get("counter");
        if (typeof val === "number") {
            await ctx.set("counter", { value: val, lastModified: Date.now() });
        }
    }
}
```

---

## Optional: Eager Bulk Migration

For deployments that need to migrate all objects proactively (not lazily), an admin API endpoint triggers bulk migration:

```
POST /restate/admin/services/{service}/migrate
```

This iterates all object keys for the service and invokes `onUpgrade` for any that haven't been migrated to the current version. This is a background operation — it does not block the deployment.

Eager migration is optional. Lazy migration is the default and covers most cases.

---

## Open Questions

1. **Version comparison semantics**: Should the runtime compare versions as opaque strings (changed = migrate) or ordered values (newer = migrate, older = error)? Opaque is simpler.

2. **Interaction with repartitioning**: When partitions are reassigned, the version-per-object tracking must be partition-local. Does the partition store's existing key metadata support this?

3. **onUpgrade failure**: If `onUpgrade` throws, the triggering handler call fails. Should there be a retry policy specific to migration, or does the standard invocation retry apply?

4. **Workflow state migration**: Workflows have transient state. Does `onUpgrade` make sense for workflows, or is it only meaningful for persistent virtual object state? (Likely objects only — workflow state dies with the workflow.)

---

## References

### Codebase

- Deployment registration: service deployment metadata in the meta service
- Partition store state: `crates/partition-store/src/partition_store.rs`
- Object state access: `crates/storage-api/src/state_table/mod.rs`
- Invocation lifecycle: `crates/types/src/invocation/mod.rs`

### Related

- Future Design Ideas: `future_design_ideas_033126.md` — OTP hot code reloading analogy (section 9)
