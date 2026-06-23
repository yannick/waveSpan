// The benchmarking dashboard. Composes the target probe, workload selection, run controls, live
// charts, and profiling into one Linea-themed layout, and owns the run state machine:
//
//   idle ──Start──▶ running ──Pause──▶ paused ──Resume──▶ running
//                      │                   │
//                      └────────Stop───────┴──▶ done  (stream closed, summary fetched)
//
// On Start we createRun → startRun → open the sample stream; each Sample updates the headline cards
// and is pushed into the uPlot charts. An interval ticks the elapsed clock. Stop closes the stream and
// fetches the final summary. The stream and timer are always torn down on unmount.
import { useEffect, useRef, useState } from "react";
import { EmptyState, InlineMessage, Panel, Spinner, ThemeToggle } from "../components";
import { Target } from "./Target";
import { Workloads, type WorkloadConfig } from "./Workloads";
import { RunControls, type RunPhase } from "./RunControls";
import { Charts, type ChartsHandle } from "./Charts";
import { Profiling } from "./Profiling";
import {
  createRun,
  getRun,
  openSampleStream,
  pauseRun,
  resumeRun,
  startRun,
  stopRun,
  type NodeRef,
  type ProbeResult,
  type Sample,
  type StreamHandle,
} from "./api";

const DEFAULT_WORKLOADS: WorkloadConfig = {
  graph: "social",
  workloads: [],
  concurrency: 32,
  durationMs: 0,
};

export function BenchApp() {
  const [probe, setProbe] = useState<ProbeResult | null>(null);
  const [probedNodes, setProbedNodes] = useState<NodeRef[]>([]);
  const [config, setConfig] = useState<WorkloadConfig>(DEFAULT_WORKLOADS);

  const [runId, setRunId] = useState<string | null>(null);
  const [phase, setPhase] = useState<RunPhase>("idle");
  const [sample, setSample] = useState<Sample | null>(null);
  const [elapsedSec, setElapsedSec] = useState(0);
  const [err, setErr] = useState<string | null>(null);
  const [starting, setStarting] = useState(false);

  const chartsRef = useRef<ChartsHandle | null>(null);
  const streamRef = useRef<StreamHandle | null>(null);
  const timerRef = useRef<number | null>(null);
  const startedAtRef = useRef<number>(0);
  // Accumulated active time when paused, so resume continues the clock rather than resetting it.
  const accumRef = useRef<number>(0);

  const stopTimer = () => {
    if (timerRef.current !== null) {
      window.clearInterval(timerRef.current);
      timerRef.current = null;
    }
  };
  const startTimer = () => {
    startedAtRef.current = Date.now();
    stopTimer();
    timerRef.current = window.setInterval(() => {
      setElapsedSec((accumRef.current + (Date.now() - startedAtRef.current)) / 1000);
    }, 250);
  };

  const closeStream = () => {
    streamRef.current?.close();
    streamRef.current = null;
  };

  // Always tear down stream + timer on unmount.
  useEffect(() => {
    return () => {
      closeStream();
      stopTimer();
    };
  }, []);

  const dataAddr = probe?.dataAddr ?? "localhost:7811";
  const canStart =
    !!probe &&
    config.workloads.length > 0 &&
    (phase === "idle" || phase === "done") &&
    !starting;

  const onStart = async () => {
    if (!probe) return;
    setErr(null);
    setStarting(true);
    try {
      const { id } = await createRun({
        dataAddr: probe.dataAddr,
        graph: config.graph,
        workloads: config.workloads,
        concurrency: config.concurrency,
        durationMs: config.durationMs,
      });
      setRunId(id);
      await startRun(id);

      // Reset chart + clock for the new run.
      chartsRef.current?.clear();
      setSample(null);
      accumRef.current = 0;
      setElapsedSec(0);
      startTimer();
      setPhase("running");

      streamRef.current = openSampleStream(
        id,
        (s) => {
          setSample(s);
          chartsRef.current?.push(s);
        },
        () => {
          // Stream errors are usually the run ending; surface softly, don't hard-fail the UI.
        },
      );
    } catch (e) {
      setErr(String(e instanceof Error ? e.message : e));
      setPhase("idle");
      stopTimer();
    } finally {
      setStarting(false);
    }
  };

  const onPause = async () => {
    if (!runId) return;
    try {
      await pauseRun(runId);
      accumRef.current += Date.now() - startedAtRef.current;
      stopTimer();
      setPhase("paused");
    } catch (e) {
      setErr(String(e instanceof Error ? e.message : e));
    }
  };

  const onResume = async () => {
    if (!runId) return;
    try {
      await resumeRun(runId);
      startTimer();
      setPhase("running");
    } catch (e) {
      setErr(String(e instanceof Error ? e.message : e));
    }
  };

  const onStop = async () => {
    if (!runId) return;
    try {
      await stopRun(runId);
    } catch (e) {
      setErr(String(e instanceof Error ? e.message : e));
    } finally {
      closeStream();
      stopTimer();
      setPhase("done");
      // Fetch the final summary as one last Sample for the headline cards.
      try {
        const final = await getRun(runId);
        setSample({ timeMs: elapsedSec * 1000, perWorkload: final.summary.perWorkload });
      } catch {
        /* summary fetch is best-effort */
      }
    }
  };

  const profilingCapable = !!probe && probe.nodes.some((n) => n.profiling);
  const hasRun = phase !== "idle";

  return (
    <div className="ws-app">
      <header className="ws-appbar">
        <div className="ws-wordmark">
          <h1 className="ws-headline">
            Wave<span className="ws-wordmark__glyph">Span</span>
          </h1>
          <span className="ws-wordmark__sub">benchmarks</span>
        </div>
        <ThemeToggle />
      </header>

      <main style={{ display: "flex", flexDirection: "column", gap: "var(--ws-space-xl)" }}>
        <Target
          onProbed={(res, nodes) => {
            setProbe(res);
            setProbedNodes(nodes);
          }}
        />

        {err && (
          <InlineMessage tone="danger">
            <span className="ws-mono">{err}</span>
          </InlineMessage>
        )}

        <div
          style={{
            display: "grid",
            gridTemplateColumns: "minmax(0, 2fr) minmax(260px, 1fr)",
            gap: "var(--ws-space-xl)",
            alignItems: "start",
          }}
        >
          <Workloads value={config} onChange={setConfig} dataAddr={dataAddr} />
          <div style={{ display: "flex", flexDirection: "column", gap: "var(--ws-space-lg)" }}>
            <RunControls
              state={phase}
              onStart={canStart ? onStart : () => undefined}
              onPause={onPause}
              onResume={onResume}
              onStop={onStop}
              elapsedSec={elapsedSec}
            />
            {!canStart && (phase === "idle" || phase === "done") && (
              <span className="ws-caption ws-muted">
                {starting ? (
                  <>
                    <Spinner /> starting…
                  </>
                ) : !probe ? (
                  "Probe a target first."
                ) : config.workloads.length === 0 ? (
                  "Select at least one workload."
                ) : null}
              </span>
            )}
          </div>
        </div>

        <Panel title="Live metrics">
          {hasRun || sample ? (
            <Charts ref={chartsRef} sample={sample} />
          ) : (
            <EmptyState title="No run in progress" icon="▱">
              Probe a target, choose workloads, and press Start to stream live throughput and latency.
            </EmptyState>
          )}
        </Panel>

        {profilingCapable && (
          <Profiling runId={runId} profilingCapable={profilingCapable} nodes={probedNodes} />
        )}
      </main>
    </div>
  );
}
