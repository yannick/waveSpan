// Placeholder shell for the benchmarking dashboard. The real dashboard (target probe, dataset load,
// run control, live charts, profiling reports) lands in a follow-up task. For now this renders the
// theme shell so the bench build is wired end-to-end and succeeds.
import { Panel, ThemeToggle } from "../components";

export function BenchApp() {
  return (
    <div className="ws-app">
      <header className="ws-appbar">
        <div className="ws-wordmark">
          <h1 className="ws-headline">WaveSpan Benchmarks</h1>
          <span className="ws-wordmark__sub">dashboard</span>
        </div>
        <ThemeToggle />
      </header>

      <main className="ws-view">
        <Panel>
          <p>The benchmarking dashboard is coming soon.</p>
        </Panel>
      </main>
    </div>
  );
}
