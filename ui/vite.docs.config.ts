import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import { resolve } from "node:path";

// Standalone static build of the in-app documentation (the admin UI's "Documentation" tab), exported
// as a self-contained website with no backend. Separate from vite.config.ts (which builds the full
// embedded console into internal/ui/dist).
//
//   npm run build:docs   →   ../docs-site/   (deploy this folder to any static host)
//
// `base: "./"` makes every asset URL relative, so the site also works when served from a subpath
// (e.g. https://user.github.io/wavespan/). The entry is docs.html; the build script renames it to
// index.html so static hosts serve it by default.
export default defineConfig({
  plugins: [react()],
  base: "./",
  build: {
    outDir: "../docs-site",
    emptyOutDir: true,
    rollupOptions: {
      input: resolve(__dirname, "docs.html"),
    },
  },
});
