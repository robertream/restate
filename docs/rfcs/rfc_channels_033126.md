---
date: "2026-03-31T15:22:00-07:00"
git_commit: b1766c03b914aec16875d1a1c1cbe240f84255da
branch: service-macro
repository: restatedev/restate
topic: "RFC: Broadcast Channels (Placeholder)"
tags: [rfc, architecture, channels, broadcast, bifrost]
status: placeholder
last_updated: "2026-03-31"
last_updated_by: Claude Opus 4.6
---

# RFC: Broadcast Channels

**Status**: Placeholder — separated from the State Graph RFC for independent design.

## Summary

Broadcast channels are partition-aware communication infrastructure for efficient one-to-many message delivery. They are structurally distinct from links (which model relationships) — channels model message delivery topology.

## Key Design Insights (from session)

- **Edges vs channels**: Links model structure (relationships, ownership, graph traversal). Channels model communication (message delivery, fan-out). Two distinct primitives.
- **Partition-aware delivery**: Publisher sends one message per partition that has subscribers. Each partition maintains a local subscriber list. Fan-out is partition-batched, not per-subscriber.
- **Backed by Bifrost**: Channels use Restate's existing durable log infrastructure. Publisher appends once. Subscribers consume independently at their own offset.
- **Adaptive batching**: Low-frequency channels deliver immediately. High-frequency channels coalesce within a time window. Configurable per channel.
- **Publisher journal is O(1)**: One journal entry regardless of subscriber count. The runtime handles fan-out.

## Deferred Until

- The State Graph and Links RFC is proven and implemented
- A concrete user hits the scaling wall where fan-out-via-handler is insufficient
- Or: the AI agent / swarm patterns demand efficient broadcast at >1000 subscribers

## Related

- State Graph RFC: `rfc_state_graph_and_links_033126.md`
- Future Design Ideas: `future_design_ideas_033126.md` (sections on backpressure, reactive edges)
- Original combined RFC: `rfc_process_composition_primitives_033126.md`
