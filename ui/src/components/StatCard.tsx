import type { CSSProperties, ReactNode } from "react";
import { color } from "../theme/tokens";

interface StatCardProps {
  label: ReactNode;
  value: ReactNode;
  hint?: ReactNode;
  accent?: string;
  /** Optional chart (e.g. a Sparkline) rendered under the value; makes the card taller. */
  chart?: ReactNode;
}

/** Big-number metric card with a Linea accent bar, and an optional history chart. */
export function StatCard({ label, value, hint, accent = color.teal, chart }: StatCardProps) {
  const style = { ["--_accent" as string]: accent } as CSSProperties;
  return (
    <div className={chart ? "ws-stat ws-stat--chart" : "ws-stat"} style={style}>
      <div className="ws-stat__label">{label}</div>
      <div className="ws-stat__value">{value}</div>
      {chart && <div className="ws-stat__chart">{chart}</div>}
      {hint && <div className="ws-stat__hint">{hint}</div>}
    </div>
  );
}
