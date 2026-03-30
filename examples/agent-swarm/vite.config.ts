import { defineConfig } from "vite";
import path from "path";

export default defineConfig({
  root: "ui",
  appType: "spa",
  server: {
    fs: { allow: [path.resolve(__dirname)] },
    proxy: {
      "/restate": {
        target: "http://localhost:8180",
        changeOrigin: true,
      },
      "/AgentSwarm": { target: "http://localhost:8180", changeOrigin: true },
    },
  },
  resolve: {
    alias: {
      "data-graph": path.resolve(__dirname, "lib/data-graph"),
    },
  },
});
