import type { CSSProperties, ReactNode } from "react";
import { color } from "../theme/tokens";

interface StatCardProps {
  label: ReactNode;
  value: ReactNode;
  hint?: ReactNode;
  accent?: string;
}

/** Big-number metric card with a Linea accent bar. */
export function StatCard({ label, value, hint, accent = color.teal }: StatCardProps) {
  const style = { ["--_accent" as string]: accent } as CSSProperties;
  return (
    <div className="ws-stat" style={style}>
      <div className="ws-stat__label">{label}</div>
      <div className="ws-stat__value">{value}</div>
      {hint && <div className="ws-stat__hint">{hint}</div>}
    </div>
  );
}
