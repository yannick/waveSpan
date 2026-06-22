import { useState } from "react";
import { cypher } from "../transport";
import { Completeness } from "../gen/wavespan/v1/common_pb";
import { type QueryMeta, type Value } from "../gen/wavespan/v1/cypher_pb";

function valueToString(v: Value | undefined): string {
  if (!v || !v.value) return "";
  switch (v.value.case) {
    case "stringValue":
      return v.value.value;
    case "intValue":
      return String(v.value.value);
    case "doubleValue":
      return String(v.value.value);
    case "boolValue":
      return String(v.value.value);
    case "null":
      return "null";
    default:
      return "·";
  }
}

const EXAMPLES = [
  "MATCH (n:User) RETURN n.name LIMIT 25",
  "MATCH (n:User)-[:FOLLOWS]->(m) RETURN n.name, m.name",
  "MATCH (n:User) WHERE n.age >= 30 RETURN n.name, n.age",
];

export function CypherConsole() {
  const [graphId, setGraphId] = useState("g");
  const [query, setQuery] = useState(EXAMPLES[0]);
  const [columns, setColumns] = useState<string[]>([]);
  const [rows, setRows] = useState<Record<string, string>[]>([]);
  const [meta, setMeta] = useState<QueryMeta | null>(null);
  const [error, setError] = useState("");
  const [running, setRunning] = useState(false);

  const run = async () => {
    setError("");
    setMeta(null);
    setRows([]);
    setColumns([]);
    setRunning(true);
    const collected: Record<string, string>[] = [];
    const cols = new Set<string>();
    try {
      for await (const res of cypher.query({ graphId, query, parameters: {} })) {
        if (res.msg.case === "row") {
          const row: Record<string, string> = {};
          for (const [k, v] of Object.entries(res.msg.value.columns)) {
            cols.add(k);
            row[k] = valueToString(v as Value);
          }
          collected.push(row);
        } else if (res.msg.case === "meta") {
          setMeta(res.msg.value);
        }
      }
      setColumns([...cols]);
      setRows(collected);
    } catch (e) {
      setError(String(e));
    } finally {
      setRunning(false);
    }
  };

  const onKeyDown = (e: React.KeyboardEvent) => {
    if ((e.metaKey || e.ctrlKey) && e.key === "Enter") run();
  };

  return (
    <div>
      <div style={{ display: "flex", gap: 8, marginBottom: 8, alignItems: "center" }}>
        <label style={{ fontSize: 12 }}>
          graph <input value={graphId} onChange={(e) => setGraphId(e.target.value)} style={{ width: 80 }} />
        </label>
        <select value="" onChange={(e) => e.target.value && setQuery(e.target.value)} style={{ fontSize: 12 }}>
          <option value="">examples…</option>
          {EXAMPLES.map((q) => (
            <option key={q} value={q}>
              {q.length > 50 ? q.slice(0, 50) + "…" : q}
            </option>
          ))}
        </select>
        <button onClick={run} disabled={running}>
          {running ? "Running…" : "Run (⌘↵)"}
        </button>
      </div>
      <textarea
        value={query}
        onChange={(e) => setQuery(e.target.value)}
        onKeyDown={onKeyDown}
        spellCheck={false}
        style={{ width: "100%", height: 100, fontFamily: "ui-monospace, monospace", fontSize: 13, padding: 8, boxSizing: "border-box" }}
      />
      {error && <div style={{ color: "#c62828", fontFamily: "monospace", fontSize: 12, padding: 8, background: "#ffebee", marginTop: 8 }}>{error}</div>}
      {columns.length > 0 && (
        <table style={{ width: "100%", fontSize: 12, borderCollapse: "collapse", marginTop: 8 }}>
          <thead>
            <tr style={{ textAlign: "left", borderBottom: "1px solid #ccc" }}>
              {columns.map((c) => (
                <th key={c}>{c}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((r, i) => (
              <tr key={i} style={{ borderBottom: "1px solid #eee" }}>
                {columns.map((c) => (
                  <td key={c}>{r[c] ?? ""}</td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      )}
      {meta && (
        <div style={{ fontSize: 12, color: "#666", marginTop: 8 }}>
          {rows.length} rows · consistency=eventual · completeness={Completeness[meta.completeness]}
          {meta.partialGraphPossible && " · partial graph possible"}
          {meta.warnings.length > 0 && ` · warnings: ${meta.warnings.join("; ")}`}
        </div>
      )}
    </div>
  );
}
