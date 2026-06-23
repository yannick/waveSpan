// Entry point for the benchmarking dashboard SPA (`vite build --config vite.bench.config.ts`).
//
// It mounts the BenchApp inside the same theme shell as the node console, so the dashboard shares the
// admin UI's look. The resulting build is embedded into and served by the benchui Go server.
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { ThemeProvider } from "./theme/ThemeProvider";
import { BenchApp } from "./bench/BenchApp";
import "./theme/tokens.css";
import "./theme/base.css";
import "./theme/components.css";

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <ThemeProvider>
      <BenchApp />
    </ThemeProvider>
  </StrictMode>,
);
