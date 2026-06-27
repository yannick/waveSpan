import { InlineMessage, Sparkline, StatCard } from "../index";
import { color } from "../../theme/tokens";
import { TypedRows } from "./TypedRows";
import { valueToRow } from "./value";
import type { DrawerTarget } from "./Drawer";

type VectorTarget = Extract<DrawerTarget, { kind: "vector" }>;

// VectorBody is read-mostly for v1: it shows dims/dtype, a sparkline of the raw values, and the
// metadata as (read-only) typed rows. High-dim float editing by hand is deferred (design §8).
export function VectorBody({ target }: { target: VectorTarget }) {
  const rows = Object.entries(target.metadata).map(([k, v]) => valueToRow(k, v));
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "var(--ws-space-md)" }}>
      <div style={{ display: "flex", gap: "var(--ws-space-sm)", flexWrap: "wrap" }}>
        <StatCard label="Dimensions" value={String(target.dims)} />
        <StatCard label="Dtype" value={target.dtype || "—"} />
      </div>

      {target.values.length >= 2 ? (
        <div>
          <div className="ws-caption ws-muted" style={{ marginBottom: "var(--ws-space-xs)" }}>values</div>
          <Sparkline data={target.values} stroke={color.teal} height={56} />
        </div>
      ) : (
        <span className="ws-caption ws-muted">no vector values to plot</span>
      )}

      <div>
        <div className="ws-caption ws-muted" style={{ marginBottom: "var(--ws-space-xs)" }}>metadata</div>
        <TypedRows rows={rows} onChange={() => {}} readOnly />
      </div>

      <InlineMessage tone="info">Vector editing is deferred — this view is read-only for v1.</InlineMessage>
    </div>
  );
}
