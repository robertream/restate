import { test, expect } from "@playwright/test";
import { createServer, type ViteDevServer } from "vite";

let server: ViteDevServer;
let baseUrl: string;

test.beforeAll(async () => {
  server = await createServer({
    configFile: "./test/vite.config.test.ts",
    server: { port: 0 },
  });
  await server.listen();
  const address = server.httpServer?.address();
  const port = typeof address === "object" && address ? address.port : 5199;
  baseUrl = `http://localhost:${port}`;
});

test.afterAll(async () => {
  await server?.close();
});

test.beforeEach(async () => {
  await fetch(`${baseUrl}/api/reset`, { method: "POST" });
});

test("page loads, connects to SSE, and shows empty state", async ({
  page,
}) => {
  await page.goto(baseUrl);
  await expect(page.locator("text=Live")).toBeVisible({ timeout: 10000 });
  await expect(page.locator(".empty")).toBeVisible();
  await expect(
    page.getByRole("button", { name: "Start Demo" }),
  ).toBeVisible();
});

test("clicking Start Demo builds the full agent tree", async ({ page }) => {
  await page.goto(baseUrl);
  await expect(page.locator("text=Live")).toBeVisible({ timeout: 10000 });

  await page.getByRole("button", { name: "Start Demo" }).click();

  // Director status appears
  await expect(page.locator(".badge")).toBeVisible({ timeout: 5000 });

  // 3 agent nodes appear
  await expect(page.locator(".node-label", { hasText: "Economics" })).toBeVisible({ timeout: 10000 });
  await expect(page.locator(".node-label", { hasText: "Policy" })).toBeVisible({ timeout: 10000 });
  await expect(page.locator(".node-label", { hasText: "Climate Science" })).toBeVisible({ timeout: 10000 });

  // Sub-agents from delegation appear
  await expect(page.locator(".node-label", { hasText: "Carbon Markets" })).toBeVisible({ timeout: 10000 });
  await expect(page.locator(".node-label", { hasText: "GDP Impact" })).toBeVisible({ timeout: 10000 });

  // Agent nodes show type="agent"
  await expect(
    page.locator(".node-type", { hasText: "agent" }).first(),
  ).toBeVisible({ timeout: 10000 });

  // All node cards + 1 badge reach complete
  // root(1) + agents(3) + sub-agents(2) = 6 nodes + 1 badge = 7
  await expect(page.locator(".st-complete")).toHaveCount(7, {
    timeout: 15000,
  });

  // Node messages visible
  await expect(page.locator(".node-message").first()).toBeVisible();
});

test("reconnect restores state snapshot", async ({ page }) => {
  await page.goto(baseUrl);
  await expect(page.locator("text=Live")).toBeVisible({ timeout: 10000 });

  await page.getByRole("button", { name: "Start Demo" }).click();
  await expect(page.locator(".st-complete")).toHaveCount(7, {
    timeout: 15000,
  });

  await page.reload();
  await expect(page.locator("text=Live")).toBeVisible({ timeout: 10000 });
  await expect(
    page.locator(".node-label", { hasText: "Economics" }),
  ).toBeVisible({ timeout: 5000 });
  await expect(page.locator(".st-complete")).toHaveCount(7, { timeout: 5000 });
});

test("SVG edges connect parent to child nodes", async ({ page }) => {
  await page.goto(baseUrl);
  await expect(page.locator("text=Live")).toBeVisible({ timeout: 10000 });

  await page.getByRole("button", { name: "Start Demo" }).click();
  await expect(page.locator(".st-complete")).toHaveCount(7, {
    timeout: 15000,
  });

  // SVG edges: root→3 agents (3) + economics→2 sub-agents (2) = 5
  const edges = page.locator(".graph-edges line");
  await expect(edges).toHaveCount(5, { timeout: 5000 });
});

test("delegation creates depth > 1 nodes", async ({ page }) => {
  await page.goto(baseUrl);
  await expect(page.locator("text=Live")).toBeVisible({ timeout: 10000 });

  await page.getByRole("button", { name: "Start Demo" }).click();
  await expect(page.locator(".st-complete")).toHaveCount(7, { timeout: 15000 });

  // Delegation status appears during the run (Carbon Markets is a sub-agent of Economics)
  await expect(page.locator("text=Carbon Markets")).toBeVisible({ timeout: 5000 });
  await expect(page.locator("text=GDP Impact")).toBeVisible({ timeout: 5000 });

  // SVG edges span 2 levels (5 total = 3 root→agent + 2 agent→sub-agent)
  await expect(page.locator(".graph-edges line")).toHaveCount(5, { timeout: 5000 });
});

test("clicking a node opens the detail panel", async ({ page }) => {
  await page.goto(baseUrl);
  await expect(page.locator("text=Live")).toBeVisible({ timeout: 10000 });

  await page.getByRole("button", { name: "Start Demo" }).click();
  await expect(page.locator(".st-complete")).toHaveCount(7, {
    timeout: 15000,
  });

  // Click a node
  await page.locator(".node").first().click();

  // Detail panel opens
  await expect(page.locator(".detail-panel")).toBeVisible({ timeout: 3000 });
  await expect(page.locator(".detail-header")).toBeVisible();

  // Close it
  await page.locator(".detail-close").click();
  await expect(page.locator(".detail-panel")).not.toBeVisible({
    timeout: 3000,
  });
});
