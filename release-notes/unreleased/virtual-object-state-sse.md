# Virtual Object State SSE Stream

## New Feature

### What Changed

Added a new ingress endpoint `GET /restate/objects/{service}/{key}/state` that streams real-time
virtual object state changes as a Server-Sent Events (SSE) stream.

**Event types:**

| Data prefix | Meaning |
|-------------|---------|
| `RPL {…}`  | Full state snapshot (sent on initial connect or after receiver lag) |
| `ASN {…}`  | One or more keys were assigned new values in one transaction |
| `DEL […]`  | One or more keys were deleted in one transaction |
| `CLR`       | All state was cleared atomically |

Each event carries an `id` equal to the per-object state revision number. Clients may pass
`Last-Event-ID` on reconnect; if the revision is stale the server sends a fresh `RPL` snapshot.
The stream includes SSE keep-alive comments and a `retry: 3000` hint on snapshot events.

**Admin API change — `POST /services/{service}/state`:**

- `new_state: null` now performs an atomic clear-all, emitting a single `CLR` event on any open
  SSE streams. Previously, an empty `new_state` would delete keys individually.
- `new_state: {}` (empty object) continues to delete existing keys one by one (per-key `DEL`
  events), or is a no-op when no state exists.
- `new_state` is now a required field; omitting it from the request body is a deserialization
  error. Pass `null` explicitly to clear all state.

### Why This Matters

Enables clients to subscribe to live state changes for a virtual object key without polling the
admin API.

### Impact on Users

- **New deployments**: the endpoint is available immediately on any ingress node that co-locates
  the partition store.
- **Existing deployments**: the `POST /services/{service}/state` admin endpoint behaviour for
  `new_state: null` has changed (now CLR instead of per-key DEL). Callers passing `null` or
  omitting the field should be updated to pass an explicit empty object `{}` if per-key delete
  semantics were intended.
- The endpoint returns `503 Service Unavailable` if the partition is not locally managed by the
  ingress node.
- State keys that are not valid UTF-8 are omitted from the SSE stream with a server-side warning
  log. In practice this only affects services that write raw-byte keys via the service protocol;
  all official SDKs use string keys.
