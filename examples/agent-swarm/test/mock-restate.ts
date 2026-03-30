import type { Plugin } from "vite";
import type { ServerResponse } from "http";

/**
 * Mock Restate SSE server for e2e tests.
 * Simulates AgentSwarm — single object with all node state.
 */

type State = Record<string, unknown>;

interface ObjectState {
  data: State;
  revision: number;
  clients: Set<ServerResponse>;
}

const objects = new Map<string, ObjectState>();

function getObject(service: string, key: string): ObjectState {
  const id = `${service}/${key}`;
  if (!objects.has(id)) objects.set(id, { data: {}, revision: 0, clients: new Set() });
  return objects.get(id)!;
}

function broadcast(obj: ObjectState, event: string) {
  obj.revision++;
  for (const res of obj.clients) {
    res.write(`id: ${obj.revision}\ndata: ${event}\n\n`);
  }
}

function setNode(swarmKey: string, nodeId: string, value: unknown) {
  const obj = getObject("AgentSwarm", swarmKey);
  obj.data[nodeId] = value;
  broadcast(obj, `ASN ${JSON.stringify({ [nodeId]: value })}`);
}

// --- Mock swarm timeline ---

async function runSwarmTimeline(runId: string) {
  const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

  // Clear
  const obj = getObject("AgentSwarm", runId);
  obj.data = {};
  broadcast(obj, "CLR");

  // Root planning
  setNode(runId, "root", { query: "climate change economics", status: "planning", message: "Analyzing query..." });
  await sleep(500);

  setNode(runId, "root", { query: "climate change economics", status: "delegating", message: "Spawning 3 agents", researchers: 3, complete: 0 });

  // Spawn agents
  const topics = [
    { id: "economics", label: "Economics", desc: "Economic impacts" },
    { id: "policy", label: "Policy", desc: "Government regulations" },
    { id: "science", label: "Climate Science", desc: "Physical science" },
  ];

  for (const t of topics) {
    setNode(runId, t.id, { status: "spawned", type: "agent", label: t.label, desc: t.desc, parent: "root", message: "Starting..." });
    await sleep(200);
  }

  setNode(runId, "root", { query: "climate change economics", status: "running", message: "3 agents active", researchers: 3, complete: 0 });

  // Economics agent delegates to 2 sub-agents; policy and science search directly

  // All agents start searching
  for (const t of topics) {
    setNode(runId, t.id, {
      status: "searching", type: "agent", label: t.label, desc: t.desc, parent: "root",
      message: `wikipedia: ${t.label.toLowerCase()}...`,
      tool: "wikipedia", toolQuery: t.label.toLowerCase(),
      depth: 0, budget: 8, budgetUsed: 1,
    });
    await sleep(200);
  }

  // Economics agent delegates
  setNode(runId, "economics", {
    status: "delegating", type: "agent", label: "Economics", desc: "Economic impacts", parent: "root",
    message: "Delegating to 2 sub-agents...",
    depth: 0, budget: 8, budgetUsed: 2, findings: ["Finding 1"], childCount: 2,
  });
  await sleep(200);

  // Spawn economics sub-agents
  const subTopics = [
    { id: "economics--carbon-markets", label: "Carbon Markets" },
    { id: "economics--gdp-impact", label: "GDP Impact" },
  ];
  for (const s of subTopics) {
    setNode(runId, s.id, {
      status: "spawned", type: "agent", label: s.label, parent: "economics",
      message: "Starting...", depth: 1, budget: 3, budgetUsed: 0,
    });
    await sleep(150);
  }

  // Policy and science continue searching then complete
  for (const t of [topics[1]!, topics[2]!]) {
    setNode(runId, t.id, {
      status: "searching", type: "agent", label: t.label, desc: t.desc, parent: "root",
      message: "2 findings from 1 tools",
      tool: "wikipedia", toolQuery: t.label.toLowerCase(),
      depth: 0, budget: 8, budgetUsed: 1,
      findings: ["Finding 1", "Finding 2"],
    });
    await sleep(150);
    setNode(runId, t.id, {
      status: "synthesizing", type: "agent", label: t.label, desc: t.desc, parent: "root",
      message: "Synthesizing 2 findings...", depth: 0, budget: 8, budgetUsed: 8,
      findings: ["Finding 1", "Finding 2"],
    });
    await sleep(200);
    setNode(runId, t.id, {
      status: "complete", type: "agent", label: t.label, desc: t.desc, parent: "root",
      message: "5 findings", findings: 5, depth: 0, budget: 8, budgetUsed: 8,
    });
    await sleep(150);
  }

  // Sub-agents search and complete
  for (const s of subTopics) {
    setNode(runId, s.id, {
      status: "searching", type: "agent", label: s.label, parent: "economics",
      message: `wikipedia: ${s.label.toLowerCase()}...`,
      tool: "wikipedia", depth: 1, budget: 3, budgetUsed: 1,
    });
    await sleep(200);
    setNode(runId, s.id, {
      status: "synthesizing", type: "agent", label: s.label, parent: "economics",
      message: "Synthesizing 1 finding...", depth: 1, budget: 3, budgetUsed: 3,
      findings: ["Sub-finding 1"],
    });
    await sleep(200);
    setNode(runId, s.id, {
      status: "complete", type: "agent", label: s.label, parent: "economics",
      message: "2 findings", findings: 2, depth: 1, budget: 3, budgetUsed: 3,
    });
    await sleep(150);
  }

  // Economics agent resumes after children complete, synthesizes, completes
  setNode(runId, "economics", {
    status: "synthesizing", type: "agent", label: "Economics", desc: "Economic impacts", parent: "root",
    message: "Synthesizing 3 findings...", depth: 0, budget: 8, budgetUsed: 7,
    findings: ["Finding 1", "Sub-finding 1", "Sub-finding 2"],
  });
  await sleep(200);
  setNode(runId, "economics", {
    status: "complete", type: "agent", label: "Economics", desc: "Economic impacts", parent: "root",
    message: "5 findings", findings: 5, depth: 0, budget: 8, budgetUsed: 8,
  });
  await sleep(150);

  // Root tracks completion: policy=1, science=2, economics=3
  setNode(runId, "root", { query: "climate change economics", status: "running", message: "1 of 3 agents complete", researchers: 3, complete: 1 });
  await sleep(50);
  setNode(runId, "root", { query: "climate change economics", status: "running", message: "2 of 3 agents complete", researchers: 3, complete: 2 });
  await sleep(50);
  setNode(runId, "root", { query: "climate change economics", status: "complete", message: "Research complete — all 3 areas finished", researchers: 3, complete: 3 });
}

export function mockRestateServer(): Plugin {
  return {
    name: "mock-restate-server",
    configureServer(server) {
      server.middlewares.use((req, res, next) => {
        if (req.method === "POST" && req.url === "/api/reset") {
          objects.clear();
          res.writeHead(200, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ ok: true }));
          return;
        }
        next();
      });

      // POST /AgentSwarm/{key}/start/send
      server.middlewares.use((req, res, next) => {
        const match = req.url?.match(/^\/AgentSwarm\/([^/]+)\/start\/send$/);
        if (req.method === "POST" && match) {
          runSwarmTimeline(match[1]!);
          res.writeHead(200, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ invocationId: `inv_mock_${Date.now()}`, status: "Accepted" }));
          return;
        }
        next();
      });

      // GET /restate/objects/{service}/{key}/state
      server.middlewares.use((req, res, next) => {
        const match = req.url?.match(/^\/restate\/objects\/([A-Z][a-zA-Z]*)\/([^/]+)\/state$/);
        if (req.method === "GET" && match) {
          const obj = getObject(match[1]!, match[2]!);
          res.writeHead(200, { "Content-Type": "text/event-stream", "Cache-Control": "no-cache", Connection: "keep-alive" });
          res.write(`id: ${obj.revision}\ndata: RPL ${JSON.stringify(obj.data)}\nretry: 3000\n\n`);
          obj.clients.add(res);
          req.on("close", () => obj.clients.delete(res));
          return;
        }
        next();
      });
    },
  };
}
