// Workloads panel: select which workloads to run and tune their parameters, plus the global run knobs
// (concurrency, duration) and a dataset-prepare sub-panel that streams load progress. The composed
// selection is lifted up via `onChange` as a `WorkloadConfig`, which the parent turns into a
// CreateRunBody when the run starts.
import { useEffect, useRef, useState } from "react";
import {
  Button,
  Card,
  Checkbox,
  FieldLabel,
  InlineMessage,
  Input,
  Panel,
  Spinner,
} from "../components";
import {
  listWorkloads,
  openLoadStream,
  type StreamHandle,
  type WorkloadInfo,
  type WorkloadSelection,
} from "./api";

/** The full workload configuration this panel owns and emits via `onChange`. */
export interface WorkloadConfig {
  graph: string;
  workloads: WorkloadSelection[];
  concurrency: number;
  durationMs: number;
}

interface WorkloadsProps {
  value: WorkloadConfig;
  onChange(next: WorkloadConfig): void;
  /** Data port for the dataset-prepare stream (probed in the Target panel). */
  dataAddr: string;
}

// The parameters we surface per workload kind. We intentionally drive these from a static map (rather
// than the server's generic param list) so each row gets a sensible label + numeric input; unknown
// kinds fall back to whatever params the server advertises.
const PARAM_HINTS: Record<string, string[]> = {
  kv: ["concurrency", "keys", "readRatio", "valueSize"],
  multiget: ["concurrency", "batch", "keys"],
  cypher: ["concurrency", "graph"],
};

interface SelState {
  enabled: boolean;
  params: Record<string, string>; // raw input strings, coerced on emit
}

export function Workloads({ value, onChange, dataAddr }: WorkloadsProps) {
  const [infos, setInfos] = useState<WorkloadInfo[]>([]);
  const [loadErr, setLoadErr] = useState<string | null>(null);
  const [sel, setSel] = useState<Record<string, SelState>>({});

  // Discover available workloads once and seed per-kind state from their defaults.
  useEffect(() => {
    let live = true;
    listWorkloads()
      .then((ws) => {
        if (!live) return;
        setInfos(ws);
        setSel((prev) => {
          const next = { ...prev };
          for (const w of ws) {
            if (next[w.kind]) continue;
            const params: Record<string, string> = {};
            const names = PARAM_HINTS[w.kind] ?? w.params.map((p) => p.name);
            for (const name of names) {
              const def = w.params.find((p) => p.name === name)?.default;
              params[name] = def === undefined || def === null ? "" : String(def);
            }
            next[w.kind] = { enabled: false, params };
          }
          return next;
        });
      })
      .catch((e) => live && setLoadErr(String(e instanceof Error ? e.message : e)));
    return () => {
      live = false;
    };
  }, []);

  // Recompute and emit the WorkloadConfig whenever selection / params / globals change.
  const emit = (
    nextSel: Record<string, SelState>,
    globals: Pick<WorkloadConfig, "graph" | "concurrency" | "durationMs">,
  ) => {
    const workloads: WorkloadSelection[] = [];
    for (const [kind, st] of Object.entries(nextSel)) {
      if (!st.enabled) continue;
      const params: Record<string, unknown> = {};
      for (const [k, raw] of Object.entries(st.params)) {
        const num = Number(raw);
        params[k] = raw.trim() !== "" && Number.isFinite(num) ? num : raw;
      }
      workloads.push({ kind, params });
    }
    onChange({ ...globals, workloads });
  };

  const toggle = (kind: string, enabled: boolean) => {
    setSel((prev) => {
      const next = { ...prev, [kind]: { ...prev[kind], enabled } };
      emit(next, value);
      return next;
    });
  };
  const setParam = (kind: string, name: string, raw: string) => {
    setSel((prev) => {
      const next = {
        ...prev,
        [kind]: { ...prev[kind], params: { ...prev[kind].params, [name]: raw } },
      };
      emit(next, value);
      return next;
    });
  };
  const setGlobal = (g: Partial<Pick<WorkloadConfig, "graph" | "concurrency" | "durationMs">>) => {
    const merged = { ...value, ...g };
    emit(sel, merged);
  };

  return (
    <Panel title="Workloads">
      {loadErr && (
        <div style={{ marginBottom: "var(--ws-space-md)" }}>
          <InlineMessage tone="danger">
            couldn't load workloads: <span className="ws-mono">{loadErr}</span>
          </InlineMessage>
        </div>
      )}

      <div style={{ display: "flex", flexDirection: "column", gap: "var(--ws-space-md)" }}>
        {infos.length === 0 && !loadErr && (
          <span className="ws-body-sm ws-muted">
            <Spinner /> loading workloads…
          </span>
        )}
        {infos.map((w) => {
          const st = sel[w.kind];
          if (!st) return null;
          const names = PARAM_HINTS[w.kind] ?? w.params.map((p) => p.name);
          return (
            <Card key={w.kind} flat accent={st.enabled ? "var(--ws-color-teal)" : undefined}>
              <div style={{ padding: "var(--ws-space-md)" }}>
                <Checkbox
                  label={<span className="ws-mono" style={{ fontWeight: 700 }}>{w.kind}</span>}
                  checked={st.enabled}
                  onChange={(e) => toggle(w.kind, e.target.checked)}
                />
                <div
                  style={{
                    display: "flex",
                    flexWrap: "wrap",
                    gap: "var(--ws-space-md)",
                    marginTop: "var(--ws-space-sm)",
                    opacity: st.enabled ? 1 : 0.55,
                  }}
                >
                  {names.map((name) => (
                    <FieldLabel key={name} style={{ display: "inline-flex", alignItems: "center", gap: "var(--ws-space-xs)" }}>
                      {name}
                      <Input
                        mono
                        style={{ width: 96 }}
                        value={st.params[name] ?? ""}
                        disabled={!st.enabled}
                        onChange={(e) => setParam(w.kind, name, e.target.value)}
                      />
                    </FieldLabel>
                  ))}
                </div>
              </div>
            </Card>
          );
        })}
      </div>

      <div
        style={{
          display: "flex",
          flexWrap: "wrap",
          gap: "var(--ws-space-lg)",
          marginTop: "var(--ws-space-lg)",
        }}
      >
        <FieldLabel style={{ display: "inline-flex", alignItems: "center", gap: "var(--ws-space-xs)" }}>
          graph
          <Input
            mono
            style={{ width: 120 }}
            value={value.graph}
            onChange={(e) => setGlobal({ graph: e.target.value })}
          />
        </FieldLabel>
        <FieldLabel style={{ display: "inline-flex", alignItems: "center", gap: "var(--ws-space-xs)" }}>
          concurrency
          <Input
            mono
            style={{ width: 88 }}
            type="number"
            min={1}
            value={value.concurrency}
            onChange={(e) => setGlobal({ concurrency: Math.max(1, Number(e.target.value) || 1) })}
          />
        </FieldLabel>
        <FieldLabel style={{ display: "inline-flex", alignItems: "center", gap: "var(--ws-space-xs)" }}>
          duration (ms, 0 = unbounded)
          <Input
            mono
            style={{ width: 120 }}
            type="number"
            min={0}
            value={value.durationMs}
            onChange={(e) => setGlobal({ durationMs: Math.max(0, Number(e.target.value) || 0) })}
          />
        </FieldLabel>
      </div>

      <div style={{ marginTop: "var(--ws-space-lg)" }}>
        <PrepareDataset dataAddr={dataAddr} graph={value.graph} />
      </div>
    </Panel>
  );
}

// ---------------------------------------------------------------------------------------------------
// Dataset prepare sub-panel
// ---------------------------------------------------------------------------------------------------

function PrepareDataset({ dataAddr, graph }: { dataAddr: string; graph: string }) {
  const [users, setUsers] = useState("1000");
  const [follows, setFollows] = useState("5000");
  const [kv, setKv] = useState("10000");
  const [lines, setLines] = useState<string[]>([]);
  const [running, setRunning] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const handleRef = useRef<StreamHandle | null>(null);

  // Abort an in-flight load on unmount.
  useEffect(() => () => handleRef.current?.close(), []);

  const run = () => {
    setErr(null);
    setLines([]);
    setRunning(true);
    const h = openLoadStream(
      {
        dataAddr,
        graph,
        users: Number(users) || 0,
        follows: Number(follows) || 0,
        kv: Number(kv) || 0,
      },
      (data) => setLines((ls) => [...ls.slice(-199), data]),
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
          Prepare dataset
        </div>
        <div style={{ display: "flex", flexWrap: "wrap", gap: "var(--ws-space-md)", alignItems: "center" }}>
          <FieldLabel style={{ display: "inline-flex", alignItems: "center", gap: "var(--ws-space-xs)" }}>
            users
            <Input mono style={{ width: 96 }} type="number" min={0} value={users} onChange={(e) => setUsers(e.target.value)} />
          </FieldLabel>
          <FieldLabel style={{ display: "inline-flex", alignItems: "center", gap: "var(--ws-space-xs)" }}>
            follows
            <Input mono style={{ width: 96 }} type="number" min={0} value={follows} onChange={(e) => setFollows(e.target.value)} />
          </FieldLabel>
          <FieldLabel style={{ display: "inline-flex", alignItems: "center", gap: "var(--ws-space-xs)" }}>
            kv
            <Input mono style={{ width: 96 }} type="number" min={0} value={kv} onChange={(e) => setKv(e.target.value)} />
          </FieldLabel>
          <Button variant="secondary" onClick={run} disabled={running || !dataAddr.trim()}>
            {running ? <Spinner /> : null}
            {running ? "Loading…" : "Load dataset"}
          </Button>
        </div>

        {err && (
          <div style={{ marginTop: "var(--ws-space-md)" }}>
            <InlineMessage tone="danger">
              load failed: <span className="ws-mono">{err}</span>
            </InlineMessage>
          </div>
        )}

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
