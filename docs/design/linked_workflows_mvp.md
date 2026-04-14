# Linked Workflows MVP Design

## What It Is

- A linked workflow is a child workflow connected to a parent (object or workflow) via a unidirectional link
- Links are stored in a dedicated link table — first-class metadata, not state keys
- Links make workflow relationships explicit, enable observable state graphs, and block parent completion
- Cross-partition from day one — most links are cross-partition since parent and child have different service types

## Client API

Three operations:
1. **Start a linked workflow** — creates the child invocation + link
2. **Remove a linked workflow** — detaches child from parent's graph (child continues running)
3. **Read/traverse links** — query link status and graph structure (details deferred)

## Journal Entries

Starting a linked workflow produces up to three journal entries:

1. **`OneWayCallCommand`** (existing) — starts the child invocation, no response sink
2. **`CreateLink`** (new) — writes link record to link table, sends outbox message to child's partition to register a `ServiceInvocationResponseSink::Link` sink on the child
3. **`AttachInvocationCommand`** (existing, optional) — two cases:
   - Workflow explicitly awaits the child → `PartitionProcessor` sink (existing)
   - Object registers `onCompleted` → `HandlerInvocation` sink (new variant)

Removing a link: **`RemoveLink`** (new) — deletes link from link table, child is orphaned

## New Types

- **`ServiceInvocationResponseSink::Link`** — registered on child's response_sinks by CreateLink. On child completion, generates a `LinkCompletionNotification` routed to parent's partition
- **`ServiceInvocationResponseSink::HandlerInvocation`** — for `onCompleted` on virtual objects. Enqueues a handler invocation on the parent object when child completes
- **`LinkCompletionNotification`** — new message type (distinct from `InvocationResponse`). Sent from child's partition to parent's partition. Updates link table and checks parent completion condition
- **`InvocationStatus::Completing`** — new variant: handler code is done, waiting for linked children

## Completion Semantics

- Completion delivers `Result<T, E>` — no separate status field, no inline frozen graph
- Children block parent completion — when you wait for completion, all associated work completes
- Parent enters `Completing` when `run` returns with active linked children
- Each `LinkCompletionNotification` updates the link table and checks if all children are done
- When last child completes, parent transitions `Completing` → `Completed`
- If no linked children, skips directly to `Completed` (existing behavior unchanged)

## Invocation Lifecycle with Links

```
Scheduled → Inboxed → Invoked ⇌ Suspended ⇌ Paused → Completing → Completed → Free
```

## `onCompleted` (Virtual Objects Only)

- Objects register a completion handler via `AttachInvocationCommand` with `HandlerInvocation` sink
- When child completes, handler is queued as normal object handler invocation
- Receives `Result<T, E>` — the child's return value or error
- Workflows don't need this — they explicitly await children via `AttachInvocationCommand`

## Cancellation

- Cancelling a workflow cancels its linked subtree
- Each node cancels its own direct children — not a recursive tree walk from the root
- Parent reads its link table, sends cancel signal to each direct child via outbox
- Each child does the same for its own children
- Composes naturally with cross-partition — each partition processor reads its own link table
- No framework-level timeout defaults — owner controls timeout via cancellation

## Failure Propagation

- Child failure updates link status to `failed` but parent continues
- Parent completes when all linked children complete (including failed ones)
- Parent decides what to do about failures via application logic
- Advanced failure propagation (supervision) deferred

## Storage Model

- Links table (dedicated, separate from state table) stores graph topology and results
- Link entries store the child's result directly — links outlive invocation retention
- Written atomically alongside journal entries in the same transaction
- Graph traversal: link → child's result + child's links → recursively down

## Durability Model

- Durability is rooted at virtual objects — the persistent anchor
- A link chain persists as long as its object ancestor does
- Object clears its link → subtree becomes orphan/GC-eligible
- Workflow-only trees with no object ancestor are transient — retained for the workflow's retention duration

## Ego Graph

- Links define the graph edges — which entities are connected
- The state table provides the node data
- The ego graph is: my state + my links + each linked child's state (recursively)
- Frozen state graph = all nodes completed, nothing still running
- Graph traversal API deferred

## Graph Curation

- Because linked children block completion, curation determines what you wait for
- Remove links to helper/disposable children before returning
- Remaining links = what you wait for + what's in the frozen graph
- Forgetting to remove a link to a hanging child = parent never completes (by design)

## Deferred

- Graph traversal API (inspecting frozen state graph of completed children)
- SSE/ego graph live streaming
- Exclusive/shared handlers on workflows
- Framework-level timeout defaults
- Orphan GC policy
- `onCompleted` / `ServiceInvocationResponseSink::HandlerInvocation` — ergonomic sugar for virtual objects reacting to child completion. Objects can use `AttachInvocationCommand` with a `PartitionProcessor` sink in the interim.
