import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import { resolve } from "node:path";

// Build of the benchmarking dashboard SPA, embedded into the benchui Go server
// (internal/benchui/embed.go embeds `all:dist`). Separate from vite.config.ts (the embedded console)
// and vite.docs.config.ts (the standalone docs site).
//
//   npm run build:bench   →   ../internal/benchui/dist/   (served by the Go benchui server)
//
// `base: "./"` makes every asset URL relative so the SPA works regardless of the mount path. The entry
// is bench.html; the build script renames it to index.html so the server serves it by default.
export default defineConfig({
  plugins: [react()],
  base: "./",
  build: {
    outDir: "../internal/benchui/dist",
    emptyOutDir: true,
    rollupOptions: {
      input: resolve(__dirname, "bench.html"),
    },
  },
});
