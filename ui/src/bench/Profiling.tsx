// Profiling panel: capture a CPU/block/mutex/alloc profile across the probed nodes, fetch the analysed
// report, and render each section's top application functions as a Linea table. Per-node raw `.pb.gz`
// artifacts are offered as download links. Only rendered by the parent when at least one node reports
// profiling support.
import { useState } from "react";
import {
  Badge,
  Button,
  CodeInline,
  EmptyState,
  FieldLabel,
  InlineMessage,
  Panel,
  Spinner,
  Table,
} from "../components";
import {
  getReport,
  rawProfileURL,
  startProfile,
  type NodeRef,
  type Report,
  type ReportSection,
} from "./api";

interface ProfilingProps {
  runId: string | null;
  profilingCapable: boolean;
  nodes: NodeRef[];
}

/** One analysed function row inside a section's `app` breakdown (server shape, parsed defensively). */
interface FuncRow {
  func: string;
  flat: number;
  cum: number;
  leaf?: boolean;
}

/** Coerce a section's `app` field (typed `unknown` in the API) into renderable rows + a hottest leaf. */
function appRows(section: ReportSection): { rows: FuncRow[]; hottest: string | null } {
  const app = section.app as unknown;
  let raw: unknown[] = [];
  if (Array.isArray(app)) {
    raw = app;
  } else if (app && typeof app === "object") {
    const obj = app as Record<string, unknown>;
    if (Array.isArray(obj.funcs)) raw = obj.funcs;
    else if (Array.isArray(obj.rows)) raw = obj.rows;
    else if (Array.isArray(obj.top)) raw = obj.top;
  }
  const rows: FuncRow[] = raw.map((r) => {
    const o = (r ?? {}) as Record<string, unknown>;
    return {
      func: String(o.func ?? o.name ?? o.function ?? "?"),
      flat: Number(o.flat ?? o.self ?? 0),
      cum: Number(o.cum ?? o.cumulative ?? o.total ?? 0),
      leaf: Boolean(o.leaf ?? o.hottest ?? false),
    };
  });
  // Hottest leaf: explicit flag if present, else the row with the largest flat value.
  let hottest: string | null = rows.find((r) => r.leaf)?.func ?? null;
  if (!hottest && rows.length > 0) {
    hottest = rows.reduce((a, b) => (b.flat > a.flat ? b : a)).func;
  }
  return { rows, hottest };
}

function fmtVal(v: number, unit: string): string {
  if (!Number.isFinite(v)) return "—";
  if (Math.abs(v) >= 1000) return `${(v / 1000).toFixed(1)}k ${unit}`.trim();
  return `${v % 1 === 0 ? v : v.toFixed(2)} ${unit}`.trim();
}

const KINDS = ["cpu", "block", "mutex", "alloc"] as const;

export function Profiling({ runId, profilingCapable, nodes }: ProfilingProps) {
  const [cpuSeconds, setCpuSeconds] = useState("10");
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [report, setReport] = useState<Report | null>(null);
  const [pid, setPid] = useState<string | null>(null);

  if (!profilingCapable) return null;

  const capture = async () => {
    if (!runId) return;
    setBusy(true);
    setErr(null);
    try {
      const { pid: newPid } = await startProfile(runId, {
        cpuSeconds: Math.max(1, Number(cpuSeconds) || 10),
        nodes,
      });
      setPid(newPid);
      const rep = await getReport(newPid);
      setReport(rep);
    } catch (e) {
      setErr(String(e instanceof Error ? e.message : e));
    } finally {
      setBusy(false);
    }
  };

  return (
    <Panel
      title="Profiling"
      actions={
        <div style={{ display: "flex", alignItems: "center", gap: "var(--ws-space-sm)" }}>
          <FieldLabel style={{ display: "inline-flex", alignItems: "center", gap: "var(--ws-space-xs)" }}>
            cpuSeconds
            <input
              className="ws-field ws-field--mono"
              style={{ width: 72 }}
              type="number"
              min={1}
              value={cpuSeconds}
              onChange={(e) => setCpuSeconds(e.target.value)}
            />
          </FieldLabel>
          <Button variant="primary" onClick={capture} disabled={busy || !runId}>
            {busy ? <Spinner /> : null}
            {busy ? "Capturing…" : "Capture"}
          </Button>
        </div>
      }
    >
      {!runId && (
        <InlineMessage tone="info">Start a run to capture a profile.</InlineMessage>
      )}

      {err && (
        <div style={{ marginBottom: "var(--ws-space-md)" }}>
          <InlineMessage tone="danger">
            profile failed: <span className="ws-mono">{err}</span>
          </InlineMessage>
        </div>
      )}

      {!report && !err && runId && (
        <EmptyState title="No profile captured yet" icon="◴">
          Capture a {cpuSeconds}s profile across {nodes.length} node{nodes.length === 1 ? "" : "s"}.
        </EmptyState>
      )}

      {report && (
        <div style={{ display: "flex", flexDirection: "column", gap: "var(--ws-space-xl)" }}>
          <div style={{ display: "flex", flexWrap: "wrap", gap: "var(--ws-space-sm)", alignItems: "center" }}>
            <Badge tone="info">{report.bench}</Badge>
            <span className="ws-body-sm ws-muted">
              {report.cpuSeconds}s · {report.nodes.length} node{report.nodes.length === 1 ? "" : "s"}
            </span>
          </div>

          {report.sections.map((s) => {
            const { rows, hottest } = appRows(s);
            return (
              <section key={s.kind}>
                <div className="ws-title-sm" style={{ marginBottom: "var(--ws-space-xxs)" }}>
                  {s.title}{" "}
                  <span className="ws-muted ws-mono" style={{ fontWeight: 400 }}>
                    · {fmtVal(s.total, s.unit)}
                  </span>
                </div>
                {s.explain && (
                  <p className="ws-caption ws-muted" style={{ marginBottom: "var(--ws-space-sm)", maxWidth: "70ch" }}>
                    {s.explain}
                  </p>
                )}
                {rows.length === 0 ? (
                  <span className="ws-body-sm ws-muted">No application frames in this section.</span>
                ) : (
                  <Table mono>
                    <thead>
                      <tr>
                        <th>function</th>
                        <th style={{ textAlign: "right" }}>flat</th>
                        <th style={{ textAlign: "right" }}>cum</th>
                      </tr>
                    </thead>
                    <tbody>
                      {rows.map((r, i) => {
                        const hot = r.func === hottest;
                        return (
                          <tr key={`${r.func}-${i}`}>
                            <td>
                              {hot ? (
                                <span style={{ display: "inline-flex", alignItems: "center", gap: "var(--ws-space-xs)" }}>
                                  <Badge tone="accent">hot</Badge>
                                  <CodeInline>{r.func}</CodeInline>
                                </span>
                              ) : (
                                <CodeInline>{r.func}</CodeInline>
                              )}
                            </td>
                            <td style={{ textAlign: "right" }}>{fmtVal(r.flat, s.unit)}</td>
                            <td style={{ textAlign: "right" }}>{fmtVal(r.cum, s.unit)}</td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </Table>
                )}
                {s.notes && s.notes.length > 0 && (
                  <ul className="ws-caption ws-muted" style={{ margin: "var(--ws-space-sm) 0 0", paddingLeft: "var(--ws-space-lg)" }}>
                    {s.notes.map((n, i) => (
                      <li key={i}>{n}</li>
                    ))}
                  </ul>
                )}
              </section>
            );
          })}

          {pid && report.nodes.length > 0 && (
            <section>
              <div className="ws-field-label" style={{ marginBottom: "var(--ws-space-sm)" }}>
                Raw profiles
              </div>
              <div style={{ display: "flex", flexDirection: "column", gap: "var(--ws-space-xs)" }}>
                {report.nodes.map((node) => (
                  <div key={node} style={{ display: "flex", flexWrap: "wrap", alignItems: "center", gap: "var(--ws-space-sm)" }}>
                    <span className="ws-mono ws-body-sm" style={{ minWidth: 120 }}>{node}</span>
                    {KINDS.map((kind) => (
                      <a
                        key={kind}
                        href={rawProfileURL(pid, node, kind)}
                        download
                        className="ws-chip"
                        style={{ textDecoration: "none" }}
                      >
                        {kind}.pb.gz
                      </a>
                    ))}
                  </div>
                ))}
              </div>
            </section>
          )}
        </div>
      )}
    </Panel>
  );
}
