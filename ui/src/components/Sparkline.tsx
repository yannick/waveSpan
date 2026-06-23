import { useId } from "react";

interface SparklineProps {
  /** Time-ordered samples (oldest → newest). */
  data: number[];
  stroke?: string;
  /** Rendered height in px; width fills the container. */
  height?: number;
}

const VBW = 200; // viewBox width — the SVG scales to its container via width:100%

/** A tiny, dependency-free line chart that fills its container width. Scales to the data's own
 *  min/max with a faint area fill and a dot on the latest point. */
export function Sparkline({ data, stroke = "var(--ws-color-teal)", height = 40 }: SparklineProps) {
  const gid = useId();
  const pad = 3;
  const h = height;
  if (data.length < 2) {
    return <svg className="ws-spark" viewBox={`0 0 ${VBW} ${h}`} preserveAspectRatio="none" height={h} role="img" aria-label="awaiting data" />;
  }
  let max = -Infinity;
  let min = Infinity;
  for (const v of data) {
    if (v > max) max = v;
    if (v < min) min = v;
  }
  if (max === min) {
    max += 1;
    min -= 1;
  }
  const range = max - min;
  const innerW = VBW - pad * 2;
  const innerH = h - pad * 2;
  const x = (i: number) => pad + (i / (data.length - 1)) * innerW;
  const y = (v: number) => pad + innerH - ((v - min) / range) * innerH;
  const line = data.map((v, i) => `${x(i).toFixed(1)},${y(v).toFixed(1)}`).join(" ");
  const area = `${pad},${h - pad} ${line} ${(pad + innerW).toFixed(1)},${h - pad}`;
  const lastX = x(data.length - 1);
  const lastY = y(data[data.length - 1]);
  return (
    <svg className="ws-spark" viewBox={`0 0 ${VBW} ${h}`} preserveAspectRatio="none" height={h} role="img" aria-label="metric history">
      <defs>
        <linearGradient id={`spark-${gid}`} x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stopColor={stroke} stopOpacity="0.22" />
          <stop offset="100%" stopColor={stroke} stopOpacity="0" />
        </linearGradient>
      </defs>
      <polygon points={area} fill={`url(#spark-${gid})`} stroke="none" />
      <polyline points={line} fill="none" stroke={stroke} strokeWidth="1.5" strokeLinejoin="round" strokeLinecap="round" vectorEffect="non-scaling-stroke" />
      <circle cx={lastX} cy={lastY} r="2.5" fill={stroke} />
    </svg>
  );
}
