import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// The build output feeds internal/ui/dist, which the node embeds via go:embed (design/26).
//
// In dev (`npm run dev`) the SPA is served by Vite on :5173, but the ConnectRPC backend lives on a
// node's admin port. The transport posts to window.location.origin, so without a proxy those calls
// hit Vite (which has no backend) and fail with "unimplemented / HTTP 404". We proxy the Connect
// service paths to a running node — point WAVESPAN_DEV_NODE at any node's admin port (default node1
// from docker-compose, host-mapped to :7901). Streaming RPCs (InspectLocal, StreamGossip, Cypher)
// ride the same proxy.
const DEV_NODE = process.env.WAVESPAN_DEV_NODE ?? "http://localhost:7901";

export default defineConfig({
  plugins: [react()],
  build: {
    outDir: "../internal/ui/dist",
    emptyOutDir: true,
  },
  server: {
    port: 5173,
    proxy: {
      // All wavespan.v1.* Connect services (Observability, Cypher, …) → a live node admin port.
      "/wavespan.v1.": { target: DEV_NODE, changeOrigin: true },
      // Admin/diagnostic HTTP endpoints the UI links to.
      "/admin": { target: DEV_NODE, changeOrigin: true },
      "/metrics": { target: DEV_NODE, changeOrigin: true },
    },
  },
});
