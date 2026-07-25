import { defineConfig } from "vitest/config";
import react from "@vitejs/plugin-react";

export default defineConfig({
  base: "/admin/",
  plugins: [react()],
  build: {
    // ECharts is isolated in the lazy-loaded live-monitor route.
    chunkSizeWarningLimit: 650,
  },
  server: {
    port: 4173,
    strictPort: true,
    proxy: {
      "/v1": process.env.VITE_API_TARGET ?? "http://127.0.0.1:8792",
    },
  },
  test: {
    environment: "jsdom",
    globals: true,
    setupFiles: "./src/test/setup.ts",
  },
});
