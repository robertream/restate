import * as restate from "@restatedev/restate-sdk";

import { AgentSwarm } from "./swarm.js";
import { LLMRouter } from "./llm-router.js";
import * as tools from "./tools.js";

const HARD_CAP = 10;

/** Helper: update a node in the swarm state object. */
function update(ctx: restate.WorkflowContext, swarmKey: string, nodeId: string, state: Record<string, unknown>) {
  ctx.objectSendClient(AgentSwarm, swarmKey).updateNode({ nodeId, state });
}

// Tool registry — maps tool names to executor functions
const TOOL_REGISTRY: Record<string, (arg: string) => Promise<string>> = {
  "wikipedia": async (arg) => {
    const result = await tools.wikipediaSummary(arg);
    return result.extract || "No Wikipedia article found.";
  },
  "wikipedia-search": async (arg) => {
    const results = await tools.wikipediaSearch(arg);
    return results.map((r) => `${r.title}: ${r.excerpt}`).join("\n") || "No results.";
  },
  "jina-search": async (arg) => {
    return await tools.jinaSearch(arg);
  },
  "jina-read": async (arg) => {
    const content = await tools.jinaRead(arg);
    return content.slice(0, 2000);
  },
  "arxiv": async (arg) => {
    const results = await tools.arxivSearch(arg);
    return results.map((r) => `${r.title}\n${r.summary}`).join("\n---\n") || "No papers found.";
  },
  "hacker-news": async (arg) => {
    const results = await tools.hackerNewsSearch(arg);
    return results.map((r) => `${r.title} (${r.points} pts) ${r.url}`).join("\n") || "No stories found.";
  },
  "google-news": async (arg) => {
    const results = await tools.googleNewsSearch(arg);
    return results.map((r) => `${r.title} — ${r.source}`).join("\n") || "No news found.";
  },
  "reddit": async (arg) => {
    const results = await tools.redditSearch(arg);
    return results.map((r) => `${r.title} (r/${r.subreddit}, ${r.upvotes} upvotes)`).join("\n") || "No discussions found.";
  },
};

/** Call the LLM via the router (queued, one-at-a-time). */
async function llm(
  ctx: restate.WorkflowContext,
  systemPrompt: string,
  history: Array<{ role: "user" | "model"; text: string }>,
  userPrompt: string,
  maxTokens: number,
  useToolGrammar = false,
  useBatchGrammar = false,
): Promise<string> {
  return await ctx.objectClient(LLMRouter, "default").prompt({
    systemPrompt, history, userPrompt, maxTokens, useToolGrammar, useBatchGrammar,
  });
}

export const Agent = restate.workflow({
  name: "Agent",
  handlers: {
    run: async (
      ctx: restate.WorkflowContext,
      req: { swarmKey: string; nodeId: string; parentId: string; topic: string; query: string; budget: number; depth: number },
    ) => {
      const { swarmKey, nodeId, parentId, topic, query, budget, depth } = req;
      const maxSteps = Math.min(budget, HARD_CAP);

      const systemPrompt = `Research: "${topic}" (query: "${query}")
Pick 3-6 tools most likely to have USEFUL answers for this specific topic.
- wikipedia: established concepts, definitions, history
- arxiv: scientific research, quantitative data, models
- jina-search: current analysis, reports, diverse perspectives
- google-news: breaking developments, recent policy changes
- hacker-news: tech/startup perspective, expert commentary
- reddit: public opinion, debate, real-world experiences
- delegate: ONLY if topic is too broad for one agent
- done: when you have enough findings

Choose tools that COMPLEMENT each other — don't pick 3 similar sources.
{"tools":[{"tool":"wikipedia","query":"${topic.toLowerCase().slice(0, 25)}"},{"tool":"arxiv","query":"${query.slice(0, 25)}"}]}
{"tools":[{"tool":"done","query":""}]}`;

      const history: Array<{ role: "user" | "model"; text: string }> = [];
      const findings: string[] = [];

      try {
        let stepsUsed = 0;
        while (stepsUsed < maxSteps) {
          // 1. LLM picks a BATCH of tools (grammar-enforced)
          const remaining = maxSteps - stepsUsed;
          const pickPrompt = findings.length > 0
            ? `Research "${topic}". ${remaining} steps left. Findings:\n${findings.slice(-4).join("\n")}\nPick 3-${Math.min(6, remaining)} tools to run in parallel:`
            : `Research "${topic}". ${remaining} steps left. Pick 3-${Math.min(6, remaining)} tools to run in parallel:`;

          const pickRaw = await llm(ctx, systemPrompt, history, pickPrompt, 200, false, true);
          history.push({ role: "user", text: pickPrompt });
          history.push({ role: "model", text: pickRaw });

          // Parse batch JSON
          let toolPicks: Array<{ tool: string; query: string }> = [];
          try {
            const parsed = JSON.parse(pickRaw);
            toolPicks = (parsed.tools || []).map((t: any) => ({
              tool: (t.tool || "done").toLowerCase(),
              query: t.query || topic,
            }));
          } catch {
            console.log(`[${nodeId}] batch parse failed: ${pickRaw}`);
            break;
          }

          // Filter to valid tools, cap at remaining budget
          const validPicks = toolPicks
            .filter((p) => p.tool !== "done")
            .filter((p) => TOOL_REGISTRY[p.tool] || p.tool === "delegate")
            .slice(0, remaining);

          // Check if LLM wants to stop
          if (validPicks.length === 0 || toolPicks[0]?.tool === "done") {
            update(ctx, swarmKey, nodeId, {
              status: "synthesizing", type: "agent", label: topic,
              parent: parentId, message: `Synthesizing ${findings.length} findings...`,
              depth, budget: maxSteps, budgetUsed: stepsUsed,
            });
            break;
          }

          console.log(`[${nodeId}] batch: ${validPicks.map((p) => `${p.tool}|${p.query}`).join(", ")}`);

          // Check for delegate in the batch
          const delegatePick = validPicks.find((p) => p.tool === "delegate");
          if (delegatePick) {
            const decomposeResult = await llm(ctx, systemPrompt, history,
              `Break "${topic}" into 2-3 specific sub-topics. One per line, format: ID|LABEL\nExample: carbon-tax|Carbon Tax Mechanisms`,
              150);
            history.push({ role: "user", text: `Break "${topic}" into sub-topics...` });
            history.push({ role: "model", text: decomposeResult });

            const subTopics = decomposeResult.split("\n")
              .map((l) => l.trim())
              .filter((l) => l.includes("|"))
              .map((l) => {
                const [id, label] = l.split("|").map((s) => s.trim());
                return { id: (id || "sub").toLowerCase().replace(/[^a-z0-9-]/g, "-"), label: label || id || "Sub-topic" };
              })
              .slice(0, 3);

            if (subTopics.length > 0) {
              const remBudget = maxSteps - stepsUsed - 2;
              const equalBudget = Math.max(2, Math.floor(remBudget / subTopics.length));

              update(ctx, swarmKey, nodeId, {
                status: "delegating", type: "agent", label: topic,
                parent: parentId, message: `Delegating to ${subTopics.length} sub-agents...`,
                depth, budget: maxSteps, budgetUsed: stepsUsed + 1,
                findings: [...findings], childCount: subTopics.length,
              });

              const wfPrefix = ctx.key;
              const childPromises = subTopics.map((sub) => {
                const childNodeId = `${nodeId}--${sub.id}`;
                const childBudget = Math.min(equalBudget, HARD_CAP);

                update(ctx, swarmKey, childNodeId, {
                  status: "spawned", type: "agent", label: sub.label,
                  parent: nodeId, message: "Starting...",
                  depth: depth + 1, budget: childBudget, budgetUsed: 0,
                });

                return ctx.workflowClient(Agent, `${wfPrefix}--${sub.id}`).run({
                  swarmKey, nodeId: childNodeId, parentId: nodeId,
                  topic: sub.label, query, budget: childBudget, depth: depth + 1,
                });
              });

              const childResults = await restate.RestatePromise.all(childPromises);
              findings.push(...childResults.flatMap((r) => r.findings));
            }
            break;
          }

          // Execute real tools in PARALLEL
          const realPicks = validPicks.filter((p) => TOOL_REGISTRY[p.tool]);

          // Register tool nodes
          for (const pick of realPicks) {
            const toolNodeId = `${nodeId}--${pick.tool}-${stepsUsed}`;
            update(ctx, swarmKey, toolNodeId, {
              status: "running", type: "tool", label: pick.tool,
              parent: nodeId, message: pick.query,
              tool: pick.tool, toolQuery: pick.query,
            });
          }

          update(ctx, swarmKey, nodeId, {
            status: "searching", type: "agent", label: topic,
            parent: parentId, message: `Running ${realPicks.length} tools in parallel...`,
            depth, budget: maxSteps, budgetUsed: stepsUsed + realPicks.length,
            findings: [...findings],
          });

          // Fan out tool execution — all run concurrently
          const toolResults = await Promise.all(
            realPicks.map((pick, i) =>
              ctx.run(`exec-${stepsUsed}-${i}`, () => TOOL_REGISTRY[pick.tool]!(pick.query)),
            ),
          );

          // Summarize all results in one LLM call
          const allResults = realPicks.map((pick, i) =>
            `[${pick.tool}] ${pick.query}:\n${toolResults[i]!.slice(0, 800)}`
          ).join("\n\n");

          const summaryPrompt = `Tools returned:\n${allResults}\n\nExtract 2-4 key findings as concise sentences:`;
          const resultSummary = await llm(ctx, systemPrompt, history, summaryPrompt, 200);
          history.push({ role: "user", text: summaryPrompt });
          history.push({ role: "model", text: resultSummary });

          const newFindings = resultSummary.split("\n")
            .map((l) => l.trim().replace(/^[-•*\d.)\s]+/, ""))
            .filter((l) => l.length > 10);
          findings.push(...newFindings);

          // Update tool nodes with results
          for (let i = 0; i < realPicks.length; i++) {
            const pick = realPicks[i]!;
            const toolNodeId = `${nodeId}--${pick.tool}-${stepsUsed}`;
            // Truncate raw result for display, show as findings
            const rawLines = (toolResults[i] || "").split("\n")
              .map((l) => l.trim())
              .filter((l) => l.length > 5)
              .slice(0, 5);
            update(ctx, swarmKey, toolNodeId, {
              status: "complete", type: "tool", label: pick.tool,
              parent: nodeId, message: rawLines[0]?.slice(0, 60) || "No results",
              tool: pick.tool, toolQuery: pick.query,
              findings: rawLines,
            });
          }

          stepsUsed += realPicks.length;

          update(ctx, swarmKey, nodeId, {
            status: "searching", type: "agent", label: topic,
            parent: parentId, message: `${findings.length} findings from ${stepsUsed} tools`,
            depth, budget: maxSteps, budgetUsed: stepsUsed,
            findings: [...findings],
          });
        }

        // Synthesize conclusions
        if (findings.length > 0) {
          const synthNodeId = `${nodeId}--synthesis`;
          update(ctx, swarmKey, synthNodeId, {
            status: "running", type: "tool", label: "synthesis",
            parent: nodeId, message: `Synthesizing ${findings.length} findings...`,
          });

          update(ctx, swarmKey, nodeId, {
            status: "synthesizing", type: "agent", label: topic,
            parent: parentId, message: `Synthesizing ${findings.length} findings...`,
            depth, budget: maxSteps, budgetUsed: maxSteps,
            findings: [...findings],
          });

          const conclusions: string[] = [];
          for (let i = 0; i < 3; i++) {
            const cPrompt = `Based on all findings, generate conclusion ${i + 1} of 3. One clear sentence:`;
            const conclusion = await llm(ctx, systemPrompt, history, cPrompt, 100);
            history.push({ role: "user", text: cPrompt });
            history.push({ role: "model", text: conclusion });
            const parsed = conclusion.trim().replace(/^[-•*\d.)\s]+/, "");
            if (parsed.length > 10) conclusions.push(parsed);
          }
          findings.push(...conclusions);

          update(ctx, swarmKey, synthNodeId, {
            status: "complete", type: "tool", label: "synthesis",
            parent: nodeId, message: `${conclusions.length} conclusions`,
            findings: conclusions,
          });
        }

        // Complete
        update(ctx, swarmKey, nodeId, {
          status: "complete", type: "agent", label: topic,
          parent: parentId, message: `${findings.length} findings`,
          depth, budget: maxSteps, budgetUsed: maxSteps,
          findings,
        });

        ctx.objectSendClient(AgentSwarm, swarmKey).agentDone({
          nodeId, parentId, findings,
        });

        return { findings };
      } catch (e) {
        update(ctx, swarmKey, nodeId, {
          status: "complete", type: "agent", label: topic,
          parent: parentId, message: `Error: ${(e as Error).message?.slice(0, 50)}`,
          depth, budget: maxSteps, budgetUsed: maxSteps,
          findings,
        });
        ctx.objectSendClient(AgentSwarm, swarmKey).agentDone({
          nodeId, parentId, findings,
        });
        return { findings };
      }
    },
  },
});
