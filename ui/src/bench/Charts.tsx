// Live charts: two uPlot time-series (aggregate throughput and latency percentiles) plus headline
// StatCards. The parent (BenchApp) feeds samples in two ways:
//   - via the imperative `push(sample)` handle (ref) → appends a frame to both charts, and
//   - via the `sample` prop → drives the StatCards (current totals).
// We aggregate across workloads into a single readable series per chart: total throughput is the sum
// of per-workload tput; latency percentiles are the max across workloads (worst-case tail), which is
// the number an operator actually cares about. Keeping one series each keeps the charts legible.
import { forwardRef, useImperativeHandle, useRef } from "react";
import { StatCard } from "../components";
import { color } from "../theme/tokens";
import { TimeSeries, type TimeSeriesHandle } from "./TimeSeries";
import type { Sample, WindowStat } from "./api";

export interface ChartsHandle {
  push(sample: Sample): void;
  clear(): void;
}

interface ChartsProps {
  /** Latest sample, for the headline StatCards. Null before the first frame. */
  sample: Sample | null;
}

/** Fold a sample's per-workload window stats into one aggregate row. */
export function aggregate(sample: Sample): {
  tput: number;
  p50: number;
  p95: number;
  p99: number;
  errs: number;
} {
  let tput = 0;
  let p50 = 0;
  let p95 = 0;
  let p99 = 0;
  let errs = 0;
  for (const w of Object.values(sample.perWorkload) as WindowStat[]) {
    tput += w.tput;
    errs += w.errs;
    p50 = Math.max(p50, w.p50Ms);
    p95 = Math.max(p95, w.p95Ms);
    p99 = Math.max(p99, w.p99Ms);
  }
  return { tput, p50, p95, p99, errs };
}

export const Charts = forwardRef<ChartsHandle, ChartsProps>(function Charts({ sample }, ref) {
  const tputRef = useRef<TimeSeriesHandle | null>(null);
  const latRef = useRef<TimeSeriesHandle | null>(null);

  useImperativeHandle(
    ref,
    (): ChartsHandle => ({
      push(s) {
        const a = aggregate(s);
        const tSec = s.timeMs / 1000;
        tputRef.current?.push(tSec, [a.tput]);
        latRef.current?.push(tSec, [a.p50, a.p95, a.p99]);
      },
      clear() {
        tputRef.current?.clear();
        latRef.current?.clear();
      },
    }),
    [],
  );

  const agg = sample ? aggregate(sample) : null;

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "var(--ws-space-xl)" }}>
      <div style={{ display: "flex", gap: "var(--ws-space-lg)", flexWrap: "wrap" }}>
        <StatCard
          label="Throughput"
          value={agg ? fmt(agg.tput) : "—"}
          hint="ops / sec · all workloads"
          accent={color.teal}
        />
        <StatCard
          label="p99 latency"
          value={agg ? `${fmt(agg.p99)} ms` : "—"}
          hint="worst-case across workloads"
          accent={color.orange}
        />
        <StatCard
          label="Errors"
          value={agg ? fmt(agg.errs) : "—"}
          hint="in the latest window"
          accent={agg && agg.errs > 0 ? color.red : color.olive}
        />
      </div>

      <TimeSeries
        title="Throughput (ops/s)"
        yUnit="ops/s"
        series={[{ label: "total", strokeVar: "--ws-color-teal" }]}
      />
      <TimeSeries
        title="Latency (ms)"
        yUnit="ms"
        series={[
          { label: "p50", strokeVar: "--ws-color-blue" },
          { label: "p95", strokeVar: "--ws-color-mustard" },
          { label: "p99", strokeVar: "--ws-color-orange" },
        ]}
      />
    </div>
  );
});

function fmt(v: number): string {
  if (!Number.isFinite(v)) return "—";
  const abs = Math.abs(v);
  if (abs >= 1_000_000) return `${(v / 1_000_000).toFixed(1)}M`;
  if (abs >= 1_000) return `${(v / 1_000).toFixed(1)}k`;
  if (abs >= 100 || Number.isInteger(v)) return Math.round(v).toString();
  return v.toFixed(1);
}
