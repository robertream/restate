# Agent Swarm Demo

Live visualization of an AI research agent swarm using Restate durable execution + SSE state streaming.

**Architecture:** One `AgentSwarm` virtual object holds all agent state. Workflow agents (`ResearchAgent`, `SearchAgent`, `SynthesisAgent`) write progress back to it. The frontend connects to one SSE stream and renders a live graph.

## Quick Start (Docker)

```bash
# From the repo root:
docker build -t agent-swarm-demo -f examples/agent-swarm/Dockerfile .
docker run -p 5173:5173 -p 8080:8080 -p 9070:9070 agent-swarm-demo

# Open http://localhost:5173 and click "Start Demo"
# Admin UI at http://localhost:9070/ui/
```

## Manual Setup

**Prerequisites:** Restate server (built from this repo), Node.js 20+

```bash
cd examples/agent-swarm

# Install dependencies
npm install

# Download LLM model (~800MB, one time)
npm run pull-model

# Start Restate server (in another terminal, from repo root)
cargo run --bin restate-server

# Start agent services
npm run services

# Register services with Restate
npm run register

# Start the frontend
npm run dev
# Open http://localhost:5173

# Click "Start Demo" or trigger manually:
npm run start-demo
```

## How It Works

```
AgentSwarm/demo-1 (virtual object — root state store)
  │
  ├─ start(query)         ← user triggers
  ├─ updateNode(id, val)  ← workflows report progress
  ├─ researcherDone(...)  ← workflows report completion
  │
  │  state (streamed via SSE):
  │    root: { status, message, query }
  │    economics: { status, type: researcher, parent: root }
  │    economics--scholar: { status, type: search, parent: economics, findings: [...] }
  │    economics--synthesis: { status, type: synthesis, parent: economics }
  │    ...
  │
  GET /restate/objects/AgentSwarm/demo-1/state → SSE stream → data-graph → graph UI
```

Each workflow agent:
1. Receives `{ swarmKey, nodeId, ... }` as input
2. Calls `AgentSwarm/{swarmKey}.updateNode()` to write progress
3. Uses in-process LLM (Llama 3.2 1B via node-llama-cpp) for real inference
4. Returns results to parent via Restate's durable call mechanism

## LLM Integration

The demo uses `node-llama-cpp` to run a local LLM in-process — no API key needed. Each LLM call is wrapped in `ctx.run()` so Restate journals it; on replay, the LLM isn't called again.

- **Director**: LLM decomposes the query into 2-4 research areas
- **Researcher**: LLM decides what sources to search (1-3 per topic)
- **Search**: LLM generates findings for each source (streamed incrementally)
- **Synthesis**: LLM combines findings into conclusions

## Frontend

The UI uses `data-graph` (vendored in `lib/data-graph/`) to consume the SSE KV-sync stream. The graph is rendered as:
- Flat `v-for` over all records (no DOM nesting, no depth limit)
- Pre-computed layout map assigns `{x, y}` to each node
- SVG lines connect parent → child edges
- Click any node to open a draggable detail panel

## Tests

```bash
npm test
```

5 Playwright e2e tests validate the full pipeline using a mock Restate server (no real Restate needed for tests).
