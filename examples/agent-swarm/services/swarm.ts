import * as restate from "@restatedev/restate-sdk";

import { Agent } from "./agents.js";
import { decomposeQuery } from "./llm.js";

/** Upper bounds. */
const MAX_RESEARCHERS = 4;

/**
 * AgentSwarm — the root virtual object.
 *
 * One instance per research run (keyed by run ID).
 * Owns all state — every agent in the tree writes back here.
 * SSE: GET /restate/objects/AgentSwarm/{runId}/state
 */
export const AgentSwarm = restate.object({
  name: "AgentSwarm",
  handlers: {
    /** User-facing: start a research swarm. */
    start: async (ctx: restate.ObjectContext, query: string) => {
      // Clear previous run
      ctx.clearAll();

      // Unique run ID so workflow IDs don't collide with previous runs
      const runId = ctx.rand.uuidv4().slice(0, 8);

      ctx.set("root", { query, status: "planning", message: "Analyzing query with LLM..." });

      // LLM decomposes query
      const topics = await ctx.run("decompose", () => decomposeQuery(query, MAX_RESEARCHERS));
      console.log("[swarm] Topics:", topics.map((t) => t.label).join(", "));

      ctx.set("root", {
        query,
        status: "delegating",
        message: `Spawning ${topics.length} agents: ${topics.map((t) => t.label).join(", ")}`,
        researchers: topics.length,
      });

      // Spawn agent workflows — each writes its state here via updateNode
      for (const t of topics) {
        // Register the node immediately so the UI sees it
        ctx.set(t.id, {
          status: "spawned", type: "agent", label: t.label,
          desc: t.desc, parent: "root", message: "Starting...",
        });

        // Fire-and-forget: agent runs independently (unique ID per run)
        ctx.workflowSendClient(Agent, `${ctx.key}--${runId}--${t.id}`).run({
          swarmKey: ctx.key,
          nodeId: t.id,
          parentId: "root",
          topic: t.label,
          query,
          budget: 32,
          depth: 0,
        });
      }

      ctx.set("root", {
        query,
        status: "running",
        message: `${topics.length} agents active`,
        researchers: topics.length,
        complete: 0,
      });
    },

    /** Called by workflows to update their node state. */
    updateNode: async (
      ctx: restate.ObjectContext,
      req: { nodeId: string; state: Record<string, unknown> },
    ) => {
      ctx.set(req.nodeId, req.state);
    },

    /** Called by agent workflows when they complete. */
    agentDone: async (
      ctx: restate.ObjectContext,
      req: { nodeId: string; parentId: string; findings: string[] },
    ) => {
      // Update the completing node
      const current = (await ctx.get<Record<string, unknown>>(req.nodeId)) || {};
      ctx.set(req.nodeId, {
        ...current,
        status: "complete",
        message: `${req.findings.length} findings`,
        findings: req.findings.length,
      });

      // Propagate completion upward
      if (req.parentId === "root") {
        // Root-level agent completed
        const root = (await ctx.get<Record<string, unknown>>("root")) || {};
        const complete = ((root.complete as number) || 0) + 1;
        const total = (root.researchers as number) || 1;
        if (complete >= total) {
          ctx.set("root", { ...root, status: "complete", message: `Research complete — all ${total} areas finished`, complete });
        } else {
          ctx.set("root", { ...root, status: "running", message: `${complete} of ${total} agents complete`, complete });
        }
      } else {
        // Nested agent completed — update parent's counter.
        // Note: We do NOT propagate further here.
        // The parent Agent workflow is waiting on RestatePromise.all() —
        // it will call agentDone when IT finishes (after collecting child results).
        const parent = (await ctx.get<Record<string, unknown>>(req.parentId)) || {};
        const complete = ((parent.complete as number) || 0) + 1;
        ctx.set(req.parentId, { ...parent, complete });
      }
    },
  },
});
