import { getLlama, LlamaChatSession, type Llama, type LlamaModel } from "node-llama-cpp";
import { fileURLToPath } from "url";
import path from "path";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const MODEL_PATH = path.join(__dirname, "../models/hf_ggml-org_gemma-3-4b-it.Q4_K_M.gguf");

let llama: Llama | null = null;
let model: LlamaModel | null = null;

async function ensureLoaded() {
  if (!model) {
    console.log("[llm] Loading model...");
    llama = await getLlama();
    model = await llama.loadModel({ modelPath: MODEL_PATH });
    console.log("[llm] Model loaded");
  }
  return model;
}

/** Get the shared Llama instance (for grammar creation, etc.) */
export async function getSharedLlama() {
  await ensureLoaded();
  return llama!;
}

async function prompt(text: string, maxTokens = 256): Promise<string> {
  const m = await ensureLoaded();
  const context = await m.createContext({ contextSize: { min: 128, max: 2048 } });
  try {
    const session = new LlamaChatSession({ contextSequence: context.getSequence() });
    return await session.prompt(text, { maxTokens, temperature: 0.7 });
  } finally {
    await context.dispose();
  }
}

/** Parse pipe-delimited lines from LLM output. */
function parseLines(response: string, minFields: number): string[][] {
  return response
    .trim()
    .split("\n")
    .map((l) => l.trim())
    .filter((l) => l.includes("|"))
    .map((l) => l.split("|").map((s) => s.trim()))
    .filter((parts) => parts.length >= minFields);
}

// --- Agent prompts ---

/** Director: decompose a query into 2-N research areas. */
export async function decomposeQuery(
  query: string,
  maxTopics: number,
): Promise<Array<{ id: string; label: string; desc: string }>> {
  const response = await prompt(
    `You are a research planner. Given the query below, identify 2 to ${maxTopics} distinct research areas to investigate. Choose the number based on how broad the query is.

For each area, respond with one line in this exact format:
ID|LABEL|DESCRIPTION

Where ID is a short lowercase slug (no spaces), LABEL is a 1-3 word title, and DESCRIPTION is one sentence explaining what to research.

Query: "${query}"

Respond with only the lines, nothing else:`,
    200,
  );

  const lines = parseLines(response, 3);

  if (lines.length === 0) {
    return [
      { id: "primary", label: "Primary Research", desc: `Research on: ${query}` },
      { id: "secondary", label: "Supporting Analysis", desc: `Supporting analysis for: ${query}` },
    ];
  }

  return lines.slice(0, maxTopics).map(([id, label, desc]) => ({
    id: (id || "area").toLowerCase().replace(/[^a-z0-9]/g, "-").replace(/-+/g, "-"),
    label: label || "Research",
    desc: desc || "Research area",
  }));
}

/** Researcher: decide what sources to search for a topic. */
export async function planSources(
  query: string,
  topic: string,
  topicDesc: string,
  maxSources: number,
): Promise<Array<{ id: string; label: string; searchQuery: string }>> {
  const response = await prompt(
    `You are a research agent investigating "${topic}" (${topicDesc}) for the query: "${query}".

Choose 1 to ${maxSources} information sources to search. For each, respond with one line:
ID|SOURCE_NAME|SEARCH_QUERY

Where ID is a short slug, SOURCE_NAME is the database/source name, and SEARCH_QUERY is the specific search string to use.

Respond with only the lines:`,
    200,
  );

  const lines = parseLines(response, 3);

  if (lines.length === 0) {
    return [{ id: "search-1", label: "Web Search", searchQuery: `${topic} ${query}` }];
  }

  return lines.slice(0, maxSources).map(([id, label, searchQuery]) => ({
    id: (id || "src").toLowerCase().replace(/[^a-z0-9]/g, "-").replace(/-+/g, "-"),
    label: label || "Search",
    searchQuery: searchQuery || query,
  }));
}

/** Create a persistent chat session with a system prompt. */
export async function createSession(
  systemPrompt: string,
): Promise<{ session: LlamaChatSession; dispose: () => Promise<void> }> {
  const m = await ensureLoaded();
  const context = await m.createContext({ contextSize: { min: 128, max: 2048 } });
  const session = new LlamaChatSession({
    contextSequence: context.getSequence(),
    systemPrompt,
  });
  return { session, dispose: () => context.dispose() };
}

/** Parse the first non-empty line from an LLM response, stripping list markers. */
function parseFirstLine(response: string): string {
  const lines = response
    .trim()
    .split("\n")
    .map((l) => l.trim().replace(/^[-•*\d.)\s]+/, ""))
    .filter((l) => l.length > 0);
  return lines[0] ?? response.trim();
}

/** Prompt constants — shared between generation and replay ingestion. */
export const FINDING_PROMPT =
  "Generate the next research finding. One concise sentence with a specific claim, number, or fact.";
export const CONCLUSION_PROMPT =
  "Generate the next key conclusion that synthesizes the findings above. One clear sentence.";

/** Generate one research finding from a persistent session. */
export async function generateOneFinding(session: LlamaChatSession): Promise<string> {
  const response = await session.prompt(FINDING_PROMPT, { maxTokens: 100, temperature: 0.7 });
  return parseFirstLine(response);
}

/** Generate one synthesis conclusion from a persistent session. */
export async function generateOneConclusion(session: LlamaChatSession): Promise<string> {
  const response = await session.prompt(CONCLUSION_PROMPT, { maxTokens: 100, temperature: 0.7 });
  return parseFirstLine(response);
}

/**
 * Inject a replayed turn into session history so subsequent prompts have correct context.
 * Call this when ctx.run() returned a journaled value without actually calling the LLM.
 */
export function ingestReplayedTurn(
  session: LlamaChatSession,
  userText: string,
  modelResponse: string,
): void {
  const history = session.getChatHistory();
  history.push({ type: "user", text: userText });
  history.push({ type: "model", response: [modelResponse] });
  session.setChatHistory(history);
}
