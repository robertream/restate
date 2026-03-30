import * as restate from "@restatedev/restate-sdk";

import { AgentSwarm } from "./swarm.js";
import { Agent } from "./agents.js";
import { LLMRouter } from "./llm-router.js";

restate.serve({
  services: [AgentSwarm, Agent, LLMRouter],
  port: 9080,
});

console.log("Agent swarm services listening on :9080");
