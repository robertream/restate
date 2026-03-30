import * as restate from "@restatedev/restate-sdk";
import { LlamaChatSession } from "node-llama-cpp";

import { getSharedLlama } from "./llm.js";

// Singleton model + context — loaded once, reused for all requests
let sharedSession: LlamaChatSession | null = null;
let sessionDispose: (() => Promise<void>) | null = null;

const TOOL_NAMES = [
  "wikipedia", "wikipedia-search", "jina-search", "jina-read",
  "arxiv", "hacker-news", "google-news", "reddit", "delegate", "done",
] as const;

let toolGrammarPromise: Promise<any> | null = null;
let batchToolGrammarPromise: Promise<any> | null = null;

async function getToolGrammar() {
  if (!toolGrammarPromise) {
    toolGrammarPromise = (async () => {
      const llama = await getSharedLlama();
      return await llama.createGrammarForJsonSchema({
        type: "object" as const,
        properties: {
          tool: { type: "string" as const, enum: [...TOOL_NAMES] },
          query: { type: "string" as const, maxLength: 50 },
        },
        required: ["tool", "query"] as const,
      });
    })();
  }
  return toolGrammarPromise;
}

async function getBatchToolGrammar() {
  if (!batchToolGrammarPromise) {
    batchToolGrammarPromise = (async () => {
      const llama = await getSharedLlama();
      return await llama.createGrammarForJsonSchema({
        type: "object" as const,
        properties: {
          tools: {
            type: "array" as const,
            items: {
              type: "object" as const,
              properties: {
                tool: { type: "string" as const, enum: [...TOOL_NAMES] },
                query: { type: "string" as const, maxLength: 50 },
              },
              required: ["tool", "query"] as const,
            },
            minItems: 1,
            maxItems: 6,
          },
        },
        required: ["tools"] as const,
      });
    })();
  }
  return batchToolGrammarPromise;
}

async function ensureSession() {
  if (!sharedSession) {
    const llama = await getSharedLlama();
    // ensureLoaded() was already called by getSharedLlama, model is loaded
    // Get the model from llama
    const { createSession } = await import("./llm.js");
    const result = await createSession("You are a research assistant.");
    sharedSession = result.session;
    sessionDispose = result.dispose;
    console.log("[llm-router] Shared session created");
  }
  return sharedSession;
}

/**
 * LLMRouter — virtual object that serializes all LLM access.
 *
 * Single-writer on key "default" means requests queue naturally.
 * Uses one persistent model context to avoid VRAM churn.
 * History is rebuilt per-request via setChatHistory.
 */
export const LLMRouter = restate.object({
  name: "LLMRouter",
  handlers: {
    prompt: async (
      ctx: restate.ObjectContext,
      req: {
        systemPrompt: string;
        history: Array<{ role: "user" | "model"; text: string }>;
        userPrompt: string;
        maxTokens: number;
        useToolGrammar: boolean;
        useBatchGrammar?: boolean;
      },
    ): Promise<string> => {
      return await ctx.run("inference", async () => {
        const session = await ensureSession();

        const chatHistory: any[] = [
          { type: "system", text: req.systemPrompt },
        ];
        for (const turn of req.history) {
          if (turn.role === "user") {
            chatHistory.push({ type: "user", text: turn.text });
          } else {
            chatHistory.push({ type: "model", response: [turn.text] });
          }
        }
        session.setChatHistory(chatHistory);

        let grammar;
        if (req.useBatchGrammar) {
          grammar = await getBatchToolGrammar();
        } else if (req.useToolGrammar) {
          grammar = await getToolGrammar();
        }

        return await session.prompt(req.userPrompt, {
          maxTokens: req.maxTokens,
          temperature: 0.5,
          grammar,
        });
      });
    },
  },
});

// No separate LLMWorker needed — the router handles inference directly
// with a persistent shared session (no context creation/disposal per call)
