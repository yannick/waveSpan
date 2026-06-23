// Entry point for the STANDALONE documentation website (`vite build --config vite.docs.config.ts`).
//
// It mounts only the in-app Documentation view inside the same theme shell as the node console, so
// the exported static site is byte-identical in look to the admin UI's Docs tab — but needs no
// backend (every page is markdown bundled at build time). Deploy the resulting ./docs-site folder to
// any static host.
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { ThemeProvider } from "./theme/ThemeProvider";
import { ThemeToggle } from "./components";
import { Documentation } from "./views/Documentation";
import "./theme/tokens.css";
import "./theme/base.css";
import "./theme/components.css";

// The Documentation view keeps the open page in the URL hash (#/docs?doc=<slug>). Seed the screen so
// deep links and the prev/next navigation produce clean, shareable URLs on a static host.
if (!window.location.hash || window.location.hash === "#" || window.location.hash === "#/") {
  window.history.replaceState(null, "", "#/docs");
}

function DocsSite() {
  return (
    <div className="ws-app">
      <header className="ws-appbar">
        <div className="ws-wordmark">
          <h1 className="ws-headline">
            wave<span className="ws-wordmark__glyph">·</span>span
          </h1>
          <span className="ws-wordmark__sub">documentation</span>
        </div>
        <ThemeToggle />
      </header>

      <main className="ws-view">
        <Documentation />
      </main>
    </div>
  );
}

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <ThemeProvider>
      <DocsSite />
    </ThemeProvider>
  </StrictMode>,
);
