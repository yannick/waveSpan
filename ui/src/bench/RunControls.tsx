// Run controls: Start / Pause / Resume / Stop, gated by the current run state, plus a state badge and
// an elapsed mm:ss clock. The actual API calls live in the parent (BenchApp); this is a pure,
// state-driven control surface.
import { Badge, Button, Panel } from "../components";
import type { Tone } from "../components";

/** Run lifecycle states the dashboard distinguishes. Unknown strings fall back to a neutral display. */
export type RunPhase = "idle" | "running" | "paused" | "done" | string;

interface RunControlsProps {
  state: RunPhase;
  onStart(): void;
  onPause(): void;
  onResume(): void;
  onStop(): void;
  /** Elapsed wall-clock seconds since the run started. */
  elapsedSec: number;
}

const STATE_TONE: Record<string, Tone> = {
  idle: "neutral",
  running: "success",
  paused: "warning",
  done: "info",
};

function mmss(totalSec: number): string {
  const s = Math.max(0, Math.floor(totalSec));
  const m = Math.floor(s / 60);
  const r = s % 60;
  return `${String(m).padStart(2, "0")}:${String(r).padStart(2, "0")}`;
}

export function RunControls({
  state,
  onStart,
  onPause,
  onResume,
  onStop,
  elapsedSec,
}: RunControlsProps) {
  const isRunning = state === "running";
  const isPaused = state === "paused";
  const isActive = isRunning || isPaused;

  return (
    <Panel
      title="Run"
      actions={
        <Badge tone={STATE_TONE[state] ?? "neutral"} dot>
          {state}
        </Badge>
      }
    >
      <div style={{ display: "flex", flexWrap: "wrap", gap: "var(--ws-space-sm)" }}>
        <Button variant="primary" onClick={onStart} disabled={isActive}>
          Start
        </Button>
        <Button variant="secondary" onClick={onPause} disabled={!isRunning}>
          Pause
        </Button>
        <Button variant="secondary" onClick={onResume} disabled={!isPaused}>
          Resume
        </Button>
        <Button variant="danger" onClick={onStop} disabled={!isActive}>
          Stop
        </Button>
      </div>

      <div
        style={{
          marginTop: "var(--ws-space-lg)",
          display: "flex",
          alignItems: "baseline",
          gap: "var(--ws-space-sm)",
        }}
      >
        <span className="ws-field-label">elapsed</span>
        <span className="ws-mono" style={{ fontSize: "var(--ws-text-title-size)", fontWeight: 800 }}>
          {mmss(elapsedSec)}
        </span>
      </div>
    </Panel>
  );
}
