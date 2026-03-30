import { defineConfig } from "vite";
import path from "path";
import { mockRestateServer } from "./mock-restate.js";

/** Vite config for e2e tests — uses mock Restate server instead of proxy. */
export default defineConfig({
  root: path.resolve(__dirname, "../ui"),
  plugins: [mockRestateServer()],
  resolve: {
    alias: {
      "data-graph": path.resolve(__dirname, "../lib/data-graph"),
    },
  },
  server: {
    fs: { allow: [path.resolve(__dirname, "..")] },
  },
});
