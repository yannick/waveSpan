import { useEffect, useRef, useState } from "react";
import { obs } from "../transport";
import { Badge, EmptyState, Sparkline, StatCard, Table } from "../components";
import { color } from "../theme/tokens";
import { parsePrometheus, type MetricFamily } from "../lib/promparse";

const REFRESH_MS = 2000;

// Subsystem grouping: WaveSpan-specific metrics lead; Go/process runtime trails. Order here is the
// render order; the first matching prefix wins, and anything unmatched falls into "Other".
const GROUPS: { name: string; blurb: string; prefixes: string[] }[] = [
  { name: "Throughput", blurb: "RPC, transaction, bandwidth and connection counters (per-second rates shown above).", prefixes: ["wavespan_rpc_", "wavespan_transactions_", "wavespan_network_", "wavespan_connections_", "wavespan_vector_search_scattered"] },
  { name: "KV store", blurb: "Replication health and the TTL sweeper.", prefixes: ["kv_"] },
  { name: "Vector", blurb: "Per-collection vector load, buckets, and quantizer version.", prefixes: ["wavespan_vector_"] },
  { name: "Global replication", blurb: "Cross-cluster active-active shipping, lag, and conflicts.", prefixes: ["global_repl_"] },
  { name: "Transport", blurb: "TLS handshakes and open connections per listener.", prefixes: ["tls_", "node_"] },
  { name: "Runtime", blurb: "Go runtime and process counters.", prefixes: ["go_", "process_"] },
];
const OTHER = "Other";

type Labels = Record<string, string>;

/** Sum a counter family's samples (optionally filtered by label), or 0 when absent. */
function sumFamily(families: MetricFamily[], name: string, filter?: (l: Labels) => boolean): number {
  const f = families.find((fam) => fam.name === name);
  if (!f) return 0;
  return f.samples.reduce((s, sm) => (!filter || filter(sm.labels) ? s + sm.value : s), 0);
}

/** Per-second rate from a monotonic counter delta, formatted with 1 decimal. */
function fmtRate(perSec: number | undefined): string {
  if (perSec === undefined) return "…";
  if (perSec >= 1000) return `${(perSec / 1000).toFixed(1)}k`;
  return perSec >= 100 || perSec === 0 ? Math.round(perSec).toString() : perSec.toFixed(1);
}

/** Bytes/sec to a human-readable bandwidth string. */
function fmtBandwidth(bytesPerSec: number | undefined): string {
  if (bytesPerSec === undefined) return "…";
  const u = ["B/s", "KB/s", "MB/s", "GB/s"];
  let v = bytesPerSec;
  let i = 0;
  while (v >= 1024 && i < u.length - 1) {
    v /= 1024;
    i++;
  }
  return `${v >= 100 || i === 0 ? Math.round(v) : v.toFixed(1)} ${u[i]}`;
}

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
  // Per-second rates are derived from monotonic counters by differencing successive scrapes; a
  // rolling history per metric (capped) feeds the live sparklines for as long as the tab is open.
  const [rates, setRates] = useState<Record<string, number> | null>(null);
  const prevRef = useRef<{ t: number; totals: Record<string, number> } | null>(null);
  const historyRef = useRef<Record<string, number[]>>({});

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
        const fams = parsePrometheus(metricsText).filter((f) => f.samples.length > 0);
        setFamilies(fams);

        // Difference the throughput counters against the previous scrape to get per-second rates.
        const now = Date.now();
        const totals: Record<string, number> = {
          reads: sumFamily(fams, "wavespan_rpc_requests_total", (l) => l.kind === "read"),
          writes: sumFamily(fams, "wavespan_rpc_requests_total", (l) => l.kind === "write"),
          qps: sumFamily(fams, "wavespan_rpc_requests_total", (l) => l.kind === "read" || l.kind === "write"),
          txns: sumFamily(fams, "wavespan_transactions_total"),
          bytes: sumFamily(fams, "wavespan_network_received_bytes_total") + sumFamily(fams, "wavespan_network_transmitted_bytes_total"),
          conns: sumFamily(fams, "wavespan_connections_accepted_total"),
        };
        if (prevRef.current) {
          const dt = (now - prevRef.current.t) / 1000;
          if (dt > 0) {
            const r: Record<string, number> = {};
            for (const k of Object.keys(totals)) r[k] = Math.max(0, (totals[k] - (prevRef.current.totals[k] ?? 0)) / dt);
            const MAX_POINTS = 240; // ~8 min at the 2s poll
            for (const k of Object.keys(r)) {
              const arr = historyRef.current[k] ?? (historyRef.current[k] = []);
              arr.push(r[k]);
              if (arr.length > MAX_POINTS) arr.shift();
            }
            setRates(r);
          }
        }
        prevRef.current = { t: now, totals };

        updatedRef.current = now;
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

      <h3 className="ws-title-sm" style={{ marginBottom: "var(--ws-space-sm)" }}>Throughput</h3>
      <div style={{ display: "flex", gap: "var(--ws-space-lg)", flexWrap: "wrap", marginBottom: "var(--ws-space-xl)" }}>
        <StatCard label="QPS" value={fmtRate(rates?.qps)} hint="read + write RPCs / sec" accent={color.teal} chart={<Sparkline data={historyRef.current.qps ?? []} stroke={color.teal} />} />
        <StatCard label="Reads/s" value={fmtRate(rates?.reads)} hint="Get · Scan · Search · Query" accent={color.blue} chart={<Sparkline data={historyRef.current.reads ?? []} stroke={color.blue} />} />
        <StatCard label="Writes/s" value={fmtRate(rates?.writes)} hint="Put · Delete · Store" accent={color.orange} chart={<Sparkline data={historyRef.current.writes ?? []} stroke={color.orange} />} />
        <StatCard label="TPS" value={fmtRate(rates?.txns)} hint="durable commits / sec" accent={color.olive} chart={<Sparkline data={historyRef.current.txns ?? []} stroke={color.olive} />} />
        <StatCard label="Bandwidth" value={fmtBandwidth(rates?.bytes)} hint="rx + tx, all listeners" accent={color.mustard} chart={<Sparkline data={historyRef.current.bytes ?? []} stroke={color.mustard} />} />
        <StatCard label="New conns/s" value={fmtRate(rates?.conns)} hint="accepted connections / sec" accent={color.purple} chart={<Sparkline data={historyRef.current.conns ?? []} stroke={color.purple} />} />
      </div>

      <h3 className="ws-title-sm" style={{ marginBottom: "var(--ws-space-sm)" }}>Cluster</h3>
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
