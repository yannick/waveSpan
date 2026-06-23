// Bulk-Remove panel: the dedicated UI for the collections "wipe an element from every set" operation.
// Three stages, top to bottom:
//   1. Seed  — stream `{done,total}` progress while N sets (each containing `member`) are created.
//   2. Remove from all sets — one blocking fan-out call; we show a Spinner (no per-step progress is
//      available) and on success render headline StatCards. The single metric is whole-fan-out
//      wall-clock → sets/sec; there is NO per-collection latency to report.
//   3. Scaling sweep — run the same fan-out across a range of set counts and plot sets/sec vs N on a
//      LOG x-scale (a static uPlot built once the final `{points}` frame arrives).
//
// The full-namespace remove is destructive across the entire namespace — every set in it loses the
// member. The copy in the panel says so.
import { useEffect, useRef, useState } from "react";
import uPlot from "uplot";
import "uplot/dist/uPlot.min.css";
import {
  Button,
  Card,
  FieldLabel,
  InlineMessage,
  Input,
  Panel,
  Spinner,
  StatCard,
} from "../components";
import { color, readToken } from "../theme/tokens";
import {
  bulkRemove,
  seedCollections,
  sweepCollections,
  type BulkRemoveResult,
  type StreamHandle,
  type SweepPoint,
} from "./api";

interface BulkRemoveProps {
  /** Data port for the cluster (probed in the Target panel). */
  dataAddr: string;
}

export function BulkRemove({ dataAddr }: BulkRemoveProps) {
  const [namespace, setNamespace] = useState("bulk-bench");
  const [sets, setSets] = useState("50000");
  const [filler, setFiller] = useState("0");
  const [member, setMember] = useState("doomed");
  const [concurrency, setConcurrency] = useState("64");

  const ready = !!dataAddr.trim();
  const setsN = Number(sets) || 0;

  return (
    <Panel title="Bulk remove — wipe an element from every set">
      <div className="ws-body-sm ws-muted" style={{ marginBottom: "var(--ws-space-md)" }}>
        Seed a namespace of sets that all contain a target <span className="ws-mono">member</span>, then
        remove that member from <em>every</em> set in one fan-out call. The reported metric is the whole
        fan-out's wall-clock turned into <span className="ws-mono">sets/sec</span> — there is no
        per-collection latency. A full-namespace remove is destructive across the entire namespace.
      </div>

      <div
        style={{
          display: "flex",
          flexWrap: "wrap",
          gap: "var(--ws-space-md)",
          alignItems: "center",
        }}
      >
        <FieldLabel style={{ display: "inline-flex", alignItems: "center", gap: "var(--ws-space-xs)" }}>
          namespace
          <Input mono style={{ width: 140 }} value={namespace} onChange={(e) => setNamespace(e.target.value)} />
        </FieldLabel>
        <FieldLabel style={{ display: "inline-flex", alignItems: "center", gap: "var(--ws-space-xs)" }}>
          sets (N)
          <Input mono style={{ width: 110 }} type="number" min={0} value={sets} onChange={(e) => setSets(e.target.value)} />
        </FieldLabel>
        <FieldLabel style={{ display: "inline-flex", alignItems: "center", gap: "var(--ws-space-xs)" }}>
          filler
          <Input mono style={{ width: 88 }} type="number" min={0} value={filler} onChange={(e) => setFiller(e.target.value)} />
        </FieldLabel>
        <FieldLabel style={{ display: "inline-flex", alignItems: "center", gap: "var(--ws-space-xs)" }}>
          member
          <Input mono style={{ width: 120 }} value={member} onChange={(e) => setMember(e.target.value)} />
        </FieldLabel>
        <FieldLabel style={{ display: "inline-flex", alignItems: "center", gap: "var(--ws-space-xs)" }}>
          concurrency
          <Input mono style={{ width: 88 }} type="number" min={1} value={concurrency} onChange={(e) => setConcurrency(e.target.value)} />
        </FieldLabel>
      </div>

      <div style={{ marginTop: "var(--ws-space-lg)", display: "flex", flexDirection: "column", gap: "var(--ws-space-lg)" }}>
        <SeedStep
          dataAddr={dataAddr}
          namespace={namespace}
          sets={setsN}
          filler={Number(filler) || 0}
          member={member}
          concurrency={Number(concurrency) || 1}
          ready={ready}
        />
        <RemoveStep dataAddr={dataAddr} namespace={namespace} member={member} ready={ready} />
        <SweepStep
          dataAddr={dataAddr}
          namespace={namespace}
          member={member}
          filler={Number(filler) || 0}
          concurrency={Number(concurrency) || 1}
          ready={ready}
        />
      </div>
    </Panel>
  );
}

// ---------------------------------------------------------------------------------------------------
// 1. Seed
// ---------------------------------------------------------------------------------------------------

function SeedStep({
  dataAddr,
  namespace,
  sets,
  filler,
  member,
  concurrency,
  ready,
}: {
  dataAddr: string;
  namespace: string;
  sets: number;
  filler: number;
  member: string;
  concurrency: number;
  ready: boolean;
}) {
  const [running, setRunning] = useState(false);
  const [progress, setProgress] = useState<{ done: number; total: number } | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const handleRef = useRef<StreamHandle | null>(null);

  // Abort an in-flight seed on unmount.
  useEffect(() => () => handleRef.current?.close(), []);

  const run = () => {
    setErr(null);
    setProgress(null);
    setRunning(true);
    const h = seedCollections(
      { dataAddr, namespace, sets, filler, member, concurrency },
      (data) => {
        // Frames are JSON: progress `{done,total}` or `{error}`. Ignore anything non-JSON.
        let frame: unknown;
        try {
          frame = JSON.parse(data);
        } catch {
          return;
        }
        if (frame && typeof frame === "object") {
          const f = frame as { done?: number; total?: number; error?: string };
          if (typeof f.error === "string") {
            setErr(f.error);
          } else if (typeof f.done === "number" && typeof f.total === "number") {
            setProgress({ done: f.done, total: f.total });
          }
        }
      },
    );
    handleRef.current = h;
    h.done
      .catch((e) => setErr(String(e instanceof Error ? e.message : e)))
      .finally(() => {
        setRunning(false);
        handleRef.current = null;
      });
  };

  const pct =
    progress && progress.total > 0 ? Math.min(100, (progress.done / progress.total) * 100) : 0;
  const big = sets >= 100000;

  return (
    <Card flat>
      <div style={{ padding: "var(--ws-space-md)" }}>
        <div className="ws-title-sm" style={{ marginBottom: "var(--ws-space-sm)" }}>
          1 · Seed sets
        </div>
        <div style={{ display: "flex", flexWrap: "wrap", gap: "var(--ws-space-md)", alignItems: "center" }}>
          <Button variant="secondary" onClick={run} disabled={running || !ready}>
            {running ? <Spinner /> : null}
            {running ? "Seeding…" : "Seed"}
          </Button>
          {progress && (
            <span className="ws-mono ws-body-sm ws-muted">
              {fmtNum(progress.done)} / {fmtNum(progress.total)} ({pct.toFixed(0)}%)
            </span>
          )}
        </div>

        {big && (
          <div style={{ marginTop: "var(--ws-space-sm)" }}>
            <InlineMessage tone="warning">
              seeding {fmtNum(sets)} sets ≈ {fmtNum(sets)} writes, this can take minutes
            </InlineMessage>
          </div>
        )}

        {progress && (
          <div
            style={{
              marginTop: "var(--ws-space-md)",
              height: 8,
              borderRadius: "var(--ws-radius-sm)",
              background: "var(--ws-color-surface-alt)",
              border: "var(--ws-stroke-hairline) solid var(--ws-color-border)",
              overflow: "hidden",
            }}
          >
            <div
              style={{
                width: `${pct}%`,
                height: "100%",
                background: color.teal,
                transition: "width 120ms linear",
              }}
            />
          </div>
        )}

        {err && (
          <div style={{ marginTop: "var(--ws-space-md)" }}>
            <InlineMessage tone="danger">
              seed failed: <span className="ws-mono">{err}</span>
            </InlineMessage>
          </div>
        )}
      </div>
    </Card>
  );
}

// ---------------------------------------------------------------------------------------------------
// 2. Remove from all sets
// ---------------------------------------------------------------------------------------------------

function RemoveStep({
  dataAddr,
  namespace,
  member,
  ready,
}: {
  dataAddr: string;
  namespace: string;
  member: string;
  ready: boolean;
}) {
  const [running, setRunning] = useState(false);
  const [result, setResult] = useState<BulkRemoveResult | null>(null);
  const [err, setErr] = useState<string | null>(null);
  // The remove is a single blocking fan-out call with NO incremental progress, so we just gate a
  // Spinner on `running` rather than streaming anything; a `live` guard drops a late resolve after
  // unmount instead of setting state on a dead component.
  const liveRef = useRef(true);
  useEffect(() => {
    liveRef.current = true;
    return () => {
      liveRef.current = false;
    };
  }, []);

  const run = async () => {
    setErr(null);
    setResult(null);
    setRunning(true);
    try {
      const res = await bulkRemove({ dataAddr, namespace, member });
      if (liveRef.current) setResult(res);
    } catch (e) {
      if (liveRef.current) setErr(String(e instanceof Error ? e.message : e));
    } finally {
      if (liveRef.current) setRunning(false);
    }
  };

  return (
    <Card flat>
      <div style={{ padding: "var(--ws-space-md)" }}>
        <div className="ws-title-sm" style={{ marginBottom: "var(--ws-space-sm)" }}>
          2 · Remove from all sets
        </div>
        <div style={{ display: "flex", flexWrap: "wrap", gap: "var(--ws-space-md)", alignItems: "center" }}>
          <Button variant="secondary" onClick={run} disabled={running || !ready}>
            {running ? <Spinner /> : null}
            {running ? "Removing…" : "Remove from all sets"}
          </Button>
          {running && (
            <span className="ws-body-sm ws-muted">one blocking fan-out call — no incremental progress</span>
          )}
        </div>

        {result && (
          <div style={{ marginTop: "var(--ws-space-md)", display: "flex", gap: "var(--ws-space-lg)", flexWrap: "wrap" }}>
            <StatCard
              label="Throughput"
              value={fmtNum(result.setsPerSec)}
              hint="sets / sec · whole fan-out"
              accent={color.teal}
            />
            <StatCard
              label="Wall time"
              value={`${(result.wallMs / 1000).toFixed(2)} s`}
              hint={`${fmtNum(result.wallMs)} ms`}
              accent={color.orange}
            />
            <StatCard
              label="Removed"
              value={fmtNum(result.removed)}
              hint="members removed"
              accent={color.blue}
            />
            <StatCard
              label="Sets touched"
              value={fmtNum(result.sets)}
              hint="sets in namespace"
              accent={color.olive}
            />
          </div>
        )}

        {err && (
          <div style={{ marginTop: "var(--ws-space-md)" }}>
            <InlineMessage tone="danger">
              remove failed: <span className="ws-mono">{err}</span>
            </InlineMessage>
          </div>
        )}
      </div>
    </Card>
  );
}

// ---------------------------------------------------------------------------------------------------
// 3. Scaling sweep
// ---------------------------------------------------------------------------------------------------

function SweepStep({
  dataAddr,
  namespace,
  member,
  filler,
  concurrency,
  ready,
}: {
  dataAddr: string;
  namespace: string;
  member: string;
  filler: number;
  concurrency: number;
  ready: boolean;
}) {
  const [sizes, setSizes] = useState("1000,10000,100000,1000000");
  const [lines, setLines] = useState<string[]>([]);
  const [points, setPoints] = useState<SweepPoint[] | null>(null);
  const [running, setRunning] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const handleRef = useRef<StreamHandle | null>(null);

  // Abort an in-flight sweep on unmount.
  useEffect(() => () => handleRef.current?.close(), []);

  const run = () => {
    const parsed = sizes
      .split(",")
      .map((s) => Number(s.trim()))
      .filter((n) => Number.isFinite(n) && n > 0);
    if (parsed.length === 0) {
      setErr("enter at least one positive set count, e.g. 1000,10000,100000");
      return;
    }
    setErr(null);
    setLines([]);
    setPoints(null);
    setRunning(true);
    const h = sweepCollections(
      { dataAddr, namespace, member, sizes: parsed, filler, concurrency },
      (data) => {
        // Frames are textual progress lines, then a FINAL JSON frame `{points:[…]}`. Try JSON first;
        // a frame carrying `points` is the result, anything else is a progress line for the log.
        try {
          const frame = JSON.parse(data) as { points?: SweepPoint[] };
          if (frame && Array.isArray(frame.points)) {
            setPoints(frame.points);
            return;
          }
        } catch {
          // not JSON — fall through and treat as a progress line
        }
        setLines((ls) => [...ls.slice(-199), data]);
      },
    );
    handleRef.current = h;
    h.done
      .catch((e) => setErr(String(e instanceof Error ? e.message : e)))
      .finally(() => {
        setRunning(false);
        handleRef.current = null;
      });
  };

  return (
    <Card flat>
      <div style={{ padding: "var(--ws-space-md)" }}>
        <div className="ws-title-sm" style={{ marginBottom: "var(--ws-space-sm)" }}>
          3 · Scaling sweep
        </div>
        <div className="ws-body-sm ws-muted" style={{ marginBottom: "var(--ws-space-sm)" }}>
          Re-run the fan-out across a range of set counts and plot <span className="ws-mono">sets/sec</span>{" "}
          vs N (log scale).
        </div>
        <div style={{ display: "flex", flexWrap: "wrap", gap: "var(--ws-space-md)", alignItems: "center" }}>
          <FieldLabel style={{ display: "inline-flex", alignItems: "center", gap: "var(--ws-space-xs)" }}>
            sizes
            <Input
              mono
              style={{ width: 260 }}
              value={sizes}
              onChange={(e) => setSizes(e.target.value)}
              placeholder="1000,10000,100000,1000000"
            />
          </FieldLabel>
          <Button variant="secondary" onClick={run} disabled={running || !ready}>
            {running ? <Spinner /> : null}
            {running ? "Running sweep…" : "Run sweep"}
          </Button>
        </div>

        {err && (
          <div style={{ marginTop: "var(--ws-space-md)" }}>
            <InlineMessage tone="danger">
              sweep failed: <span className="ws-mono">{err}</span>
            </InlineMessage>
          </div>
        )}

        {points && points.length > 0 && <SweepChart points={points} />}

        {lines.length > 0 && (
          <pre
            className="ws-mono ws-body-sm"
            style={{
              marginTop: "var(--ws-space-md)",
              maxHeight: 160,
              overflow: "auto",
              background: "var(--ws-color-surface-alt)",
              border: "var(--ws-stroke-hairline) solid var(--ws-color-border)",
              borderRadius: "var(--ws-radius-sm)",
              padding: "var(--ws-space-sm) var(--ws-space-md)",
              whiteSpace: "pre-wrap",
            }}
          >
            {lines.join("\n")}
          </pre>
        )}
      </div>
    </Card>
  );
}

/**
 * A static uPlot of sets/sec (y) vs N (x). The x-scale is logarithmic (`distr: 3`) so a geometric set
 * of N values (1k/10k/100k/1M) spreads out evenly; ticks are formatted compactly. The plot is built
 * fresh whenever `points` changes and destroyed on unmount — mirroring TimeSeries' lifecycle but as a
 * one-shot static chart (no ring buffer, no imperative push).
 */
function SweepChart({ points }: { points: SweepPoint[] }) {
  const hostRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    const host = hostRef.current;
    if (!host) return;

    const axisStroke = readToken("--ws-color-ink-muted") || "#888";
    const gridStroke = readToken("--ws-color-border") || "#ddd";
    const teal = readToken("--ws-color-teal") || "#4a8";
    const fontFamily = `12px ${readToken("--ws-font-mono") || "monospace"}`;

    // Build uPlot's columnar data: data[0] = N (x), data[1] = sets/sec (y), sorted by N ascending.
    const sorted = [...points].sort((a, b) => a.n - b.n);
    const xs = sorted.map((p) => p.n);
    const ys = sorted.map((p) => p.setsPerSec);
    const data: uPlot.AlignedData = [xs, ys];

    const opts: uPlot.Options = {
      width: host.clientWidth || 600,
      height: 240,
      legend: { show: true },
      cursor: { drag: { x: false, y: false } },
      scales: {
        x: { distr: 3 }, // log10 x-scale for the geometric N sweep
        y: { distr: 1 }, // linear sets/sec
      },
      axes: [
        {
          stroke: axisStroke,
          grid: { stroke: gridStroke, width: 1 },
          ticks: { stroke: gridStroke, width: 1 },
          font: fontFamily,
          values: (_u, vals) => vals.map((v) => fmtN(v)),
        },
        {
          stroke: axisStroke,
          grid: { stroke: gridStroke, width: 1 },
          ticks: { stroke: gridStroke, width: 1 },
          font: fontFamily,
          size: 56,
          values: (_u, vals) => vals.map((v) => fmtNum(v)),
        },
      ],
      series: [
        { label: "N", value: (_u, v) => (v == null ? "—" : fmtN(v)) },
        {
          label: "sets/sec",
          stroke: teal,
          width: 2,
          points: { show: true, size: 6 },
          value: (_u, v) => (v == null ? "—" : `${fmtNum(v)} sets/s`),
        },
      ],
    };

    const u = new uPlot(opts, data, host);

    const ro = new ResizeObserver(() => {
      const w = host.clientWidth;
      if (w > 0) u.setSize({ width: w, height: 240 });
    });
    ro.observe(host);

    return () => {
      ro.disconnect();
      u.destroy();
    };
  }, [points]);

  return (
    <div style={{ marginTop: "var(--ws-space-md)" }}>
      <div className="ws-title-sm" style={{ marginBottom: "var(--ws-space-sm)" }}>
        sets/sec vs N (log scale)
      </div>
      <div ref={hostRef} style={{ width: "100%" }} />
    </div>
  );
}

/** Compact count formatting (k/M suffixes) for the N axis: 1k / 10k / 100k / 1M. */
function fmtN(v: number): string {
  if (!Number.isFinite(v)) return "—";
  const abs = Math.abs(v);
  if (abs >= 1_000_000) return `${trim(v / 1_000_000)}M`;
  if (abs >= 1_000) return `${trim(v / 1_000)}k`;
  return trim(v);
}

function trim(v: number): string {
  // Drop a trailing ".0" so 1.0k renders as 1k.
  return Number.isInteger(v) ? v.toString() : v.toFixed(1).replace(/\.0$/, "");
}

/** Compact numeric formatting for axis ticks / tooltips (k/M suffixes, ≤1 decimal). */
function fmtNum(v: number): string {
  if (!Number.isFinite(v)) return "—";
  const abs = Math.abs(v);
  if (abs >= 1_000_000) return `${(v / 1_000_000).toFixed(1)}M`;
  if (abs >= 1_000) return `${(v / 1_000).toFixed(1)}k`;
  if (abs >= 100 || Number.isInteger(v)) return Math.round(v).toString();
  return v.toFixed(1);
}
