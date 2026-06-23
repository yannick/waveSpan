// A thin React wrapper around uPlot for live-updating line charts. The chart owns a bounded ring
// buffer (last RING points) of [t, ...series] data; callers push new frames via the imperative
// `push(tSec, values)` handle. The uPlot instance is created once, resized to its container via a
// ResizeObserver, and disposed on unmount so repeated mounts don't leak canvases.
import {
  forwardRef,
  useEffect,
  useImperativeHandle,
  useRef,
} from "react";
import uPlot from "uplot";
import "uplot/dist/uPlot.min.css";
import { readToken } from "../theme/tokens";

/** One plotted series: a legend label and the CSS custom property name to colour its stroke. */
export interface TimeSeriesDef {
  label: string;
  /** A CSS custom property like "--ws-color-teal"; resolved to a concrete colour for the canvas. */
  strokeVar: string;
}

export interface TimeSeriesProps {
  title: string;
  series: TimeSeriesDef[];
  /** Unit suffix for the y-axis / tooltip values, e.g. "ops/s" or "ms". */
  yUnit: string;
  /** Rendered height in px (width fills the container). */
  height?: number;
}

/** Imperative API exposed via ref. */
export interface TimeSeriesHandle {
  /** Append a frame: `tSec` is the x value (seconds), `values` one number per configured series. */
  push(tSec: number, values: number[]): void;
  /** Drop all buffered points (e.g. when a new run starts). */
  clear(): void;
}

const RING = 120; // keep at most the last ~2 minutes of 1s samples

export const TimeSeries = forwardRef<TimeSeriesHandle, TimeSeriesProps>(function TimeSeries(
  { title, series, yUnit, height = 220 },
  ref,
) {
  const hostRef = useRef<HTMLDivElement | null>(null);
  const plotRef = useRef<uPlot | null>(null);
  // The ring buffer in uPlot's columnar layout: data[0] = x (time), data[1..] = each series' y.
  const dataRef = useRef<number[][]>([[], ...series.map(() => [])]);

  // (Re)build the plot whenever the series definitions change. Resolving CSS vars here means the
  // colours follow the active theme at mount; that is good enough for a live dashboard.
  useEffect(() => {
    const host = hostRef.current;
    if (!host) return;

    const axisStroke = readToken("--ws-color-ink-muted") || "#888";
    const gridStroke = readToken("--ws-color-border") || "#ddd";
    const font = `12px ${readToken("--ws-font-mono") || "monospace"}`;

    const opts: uPlot.Options = {
      width: host.clientWidth || 600,
      height,
      // No native legend / cursor crosshair clutter — keep it clean and readable.
      legend: { show: true },
      cursor: { drag: { x: false, y: false } },
      scales: { x: { time: false } },
      axes: [
        {
          stroke: axisStroke,
          grid: { stroke: gridStroke, width: 1 },
          ticks: { stroke: gridStroke, width: 1 },
          font,
          values: (_u, vals) => vals.map((v) => `${Math.round(v)}s`),
        },
        {
          stroke: axisStroke,
          grid: { stroke: gridStroke, width: 1 },
          ticks: { stroke: gridStroke, width: 1 },
          font,
          size: 56,
          values: (_u, vals) => vals.map((v) => fmtNum(v)),
        },
      ],
      series: [
        {},
        ...series.map((s) => ({
          label: s.label,
          stroke: readToken(s.strokeVar) || "#4a8",
          width: 2,
          points: { show: false },
          value: (_u: uPlot, v: number | null) =>
            v == null ? "—" : `${fmtNum(v)} ${yUnit}`,
        })),
      ],
    };

    const u = new uPlot(opts, dataRef.current as uPlot.AlignedData, host);
    plotRef.current = u;

    const ro = new ResizeObserver(() => {
      const w = host.clientWidth;
      if (w > 0) u.setSize({ width: w, height });
    });
    ro.observe(host);

    return () => {
      ro.disconnect();
      u.destroy();
      plotRef.current = null;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [series.map((s) => `${s.label}:${s.strokeVar}`).join("|"), yUnit, height]);

  useImperativeHandle(
    ref,
    (): TimeSeriesHandle => ({
      push(tSec, values) {
        const data = dataRef.current;
        data[0].push(tSec);
        for (let i = 0; i < series.length; i++) {
          data[i + 1].push(values[i] ?? 0);
        }
        // Bound every column to the ring length.
        if (data[0].length > RING) {
          for (const col of data) col.splice(0, col.length - RING);
        }
        plotRef.current?.setData(data as uPlot.AlignedData);
      },
      clear() {
        const fresh: number[][] = [[], ...series.map(() => [])];
        dataRef.current = fresh;
        plotRef.current?.setData(fresh as uPlot.AlignedData);
      },
    }),
    [series],
  );

  return (
    <div>
      <div className="ws-title-sm" style={{ marginBottom: "var(--ws-space-sm)" }}>
        {title}
      </div>
      <div ref={hostRef} style={{ width: "100%" }} />
    </div>
  );
});

/** Compact numeric formatting for axis ticks and tooltips (k/M suffixes, ≤1 decimal). */
function fmtNum(v: number): string {
  if (!Number.isFinite(v)) return "—";
  const abs = Math.abs(v);
  if (abs >= 1_000_000) return `${(v / 1_000_000).toFixed(1)}M`;
  if (abs >= 1_000) return `${(v / 1_000).toFixed(1)}k`;
  if (abs >= 100 || Number.isInteger(v)) return Math.round(v).toString();
  return v.toFixed(1);
}
