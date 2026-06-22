import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// The build output feeds internal/ui/dist, which the node embeds via go:embed (design/26).
export default defineConfig({
  plugins: [react()],
  build: {
    outDir: "../internal/ui/dist",
    emptyOutDir: true,
  },
  server: { port: 5173 },
});
