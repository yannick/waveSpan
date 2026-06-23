import { useEffect, useRef, useState } from "react";
import { obs } from "../transport";
import { Badge, EmptyState, StatCard, Table } from "../components";
import { color } from "../theme/tokens";
import { parsePrometheus, type MetricFamily } from "../lib/promparse";

const REFRESH_MS = 2000;

// Subsystem grouping: WaveSpan-specific metrics lead; Go/process runtime trails. Order here is the
// render order; the first matching prefix wins, and anything unmatched falls into "Other".
const GROUPS: { name: string; blurb: string; prefixes: string[] }[] = [
  { name: "KV store", blurb: "Replication health and the TTL sweeper.", prefixes: ["kv_"] },
  { name: "Global replication", blurb: "Cross-cluster active-active shipping, lag, and conflicts.", prefixes: ["global_repl_"] },
  { name: "Transport", blurb: "TLS handshakes and open connections per listener.", prefixes: ["tls_", "node_"] },
  { name: "Runtime", blurb: "Go runtime and process counters.", prefixes: ["go_", "process_"] },
];
const OTHER = "Other";

function groupFor(name: string): string {
  for (const g of GROUPS) {
    if (g.prefixes.some((p) => name.startsWith(p))) return g.name;
  }
  return OTHER;
}

/** Compact, human-friendly value: integers verbatim, floats to 3 sig digits, ±Inf/NaN as text. */
function fmtValue(v: number): string {
  if (!Number.isFinite(v)) return v > 0 ? "+Inf" : v < 0 ? "-Inf" : "NaN";
  if (Number.isInteger(v)) return v.toLocaleString();
  if (Math.abs(v) >= 1000) return Math.round(v).toLocaleString();
  return v.toPrecision(4).replace(/\.?0+$/, "");
}

/** Render a label set as `key="value", …`, or "—" when there are none. */
function fmtLabels(labels: Record<string, string>): string {
  const parts = Object.entries(labels).map(([k, val]) => `${k}="${val}"`);
  return parts.length ? parts.join(", ") : "—";
}

function ago(ms: number): string {
  const s = Math.round(ms / 1000);
  if (s <= 0) return "just now";
  if (s < 60) return `${s}s ago`;
  return `${Math.floor(s / 60)}m ${s % 60}s ago`;
}

export function MetricsSummary() {
  const [underReplicated, setUnderReplicated] = useState<bigint | null>(null);
  const [members, setMembers] = useState<number | null>(null);
  const [families, setFamilies] = useState<MetricFamily[]>([]);
  const [updatedAt, setUpdatedAt] = useState<number | null>(null);
  const [error, setError] = useState("");
  const [, forceTick] = useState(0); // re-render so the "updated …s ago" label keeps ticking
  const updatedRef = useRef<number | null>(null);

  useEffect(() => {
    let live = true;
    const tick = async () => {
      try {
        // Cluster view (members + under-replicated) and the raw Prometheus surface, in parallel.
        const [view, metricsText] = await Promise.all([
          obs.getClusterView({}).catch(() => null),
          fetch("/metrics", { credentials: "same-origin" }).then((r) => {
            if (!r.ok) throw new Error(`/metrics → HTTP ${r.status}`);
            return r.text();
          }),
        ]);
        if (!live) return;
        if (view) {
          setMembers(view.members.length);
          setUnderReplicated(view.underReplicatedEstimate);
        }
        setFamilies(parsePrometheus(metricsText).filter((f) => f.samples.length > 0));
        updatedRef.current = Date.now();
        setUpdatedAt(updatedRef.current);
        setError("");
      } catch (e) {
        if (live) setError(String(e instanceof Error ? e.message : e));
      }
    };
    tick();
    const id = setInterval(tick, REFRESH_MS);
    // Keep the relative "updated …" label fresh between polls.
    const clock = setInterval(() => live && forceTick((n) => n + 1), 1000);
    return () => {
      live = false;
      clearInterval(id);
      clearInterval(clock);
    };
  }, []);

  const underRepCount = underReplicated === null ? null : Number(underReplicated);

  // Bucket families into ordered sections.
  const sectionNames = [...GROUPS.map((g) => g.name), OTHER];
  const sections = sectionNames
    .map((name) => ({
      name,
      blurb: GROUPS.find((g) => g.name === name)?.blurb ?? "Uncategorized metrics.",
      families: families.filter((f) => groupFor(f.name) === name),
    }))
    .filter((s) => s.families.length > 0);

  return (
    <div>
      <h2 className="ws-title ws-view__title">Cluster metrics</h2>
      <p className="ws-view__intro">
        Live counters sampled from this node's observability service and Prometheus registry, refreshed
        every {REFRESH_MS / 1000}s while this tab is open.
      </p>

      <div style={{ display: "flex", alignItems: "center", gap: "var(--ws-space-sm)", marginBottom: "var(--ws-space-lg)" }}>
        <Badge tone={error ? "danger" : "success"} dot>
          {error ? "stale" : "live"}
        </Badge>
        <span className="ws-caption ws-muted">
          {error
            ? error
            : updatedAt
              ? `updated ${ago(Date.now() - updatedAt)}`
              : "loading…"}
        </span>
      </div>

      <div style={{ display: "flex", gap: "var(--ws-space-lg)", flexWrap: "wrap", marginBottom: "var(--ws-space-xl)" }}>
        <StatCard
          label="Members"
          value={members === null ? "…" : String(members)}
          hint="known cluster members"
          accent={color.teal}
        />
        <StatCard
          label="Under-replicated keys"
          value={underReplicated === null ? "…" : String(underReplicated)}
          hint="estimate · target-N not yet met"
          accent={underRepCount && underRepCount > 0 ? color.mustard : color.olive}
        />
        <StatCard
          label="Metric series"
          value={families.length === 0 ? "…" : String(families.reduce((n, f) => n + f.samples.length, 0))}
          hint={`${families.length} families exposed`}
          accent={color.blue}
        />
      </div>

      {families.length === 0 && !error ? (
        <EmptyState title="Loading metrics…" icon="◴" />
      ) : (
        sections.map((s) => (
          <section key={s.name} style={{ marginBottom: "var(--ws-space-xl)" }}>
            <h3 className="ws-title-sm" style={{ marginBottom: "var(--ws-space-xxs)" }}>{s.name}</h3>
            <p className="ws-caption ws-muted" style={{ marginBottom: "var(--ws-space-sm)" }}>{s.blurb}</p>
            <Table mono>
              <thead>
                <tr>
                  <th>metric</th>
                  <th>labels</th>
                  <th style={{ textAlign: "right" }}>value</th>
                </tr>
              </thead>
              <tbody>
                {s.families.flatMap((f) =>
                  f.samples.map((sample, i) => (
                    <tr key={`${f.name}-${i}`}>
                      <td title={f.help || undefined}>
                        {i === 0 ? f.name : <span className="ws-muted">↳</span>}
                      </td>
                      <td className="ws-muted">{fmtLabels(sample.labels)}</td>
                      <td style={{ textAlign: "right" }}>{fmtValue(sample.value)}</td>
                    </tr>
                  )),
                )}
              </tbody>
            </Table>
          </section>
        ))
      )}

      <p className="ws-body-sm ws-muted">
        Raw exposition is available at <a href="/metrics">/metrics</a> in Prometheus format.
      </p>
    </div>
  );
}
