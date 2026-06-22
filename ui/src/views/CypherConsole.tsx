import { useState } from "react";
import { cypher } from "../transport";
import { Completeness } from "../gen/wavespan/v1/common_pb";
import { type QueryMeta, type Value } from "../gen/wavespan/v1/cypher_pb";
import {
  Badge,
  Button,
  Chip,
  FieldLabel,
  InlineMessage,
  Input,
  Spinner,
  Table,
  Textarea,
  Toolbar,
  Kbd,
} from "../components";

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
      <h2 className="ws-title ws-view__title">Cypher Console</h2>
      <p className="ws-view__intro">
        Run graph &amp; vector queries against this node. Results stream back eventually-consistent —
        the trailer declares completeness. Press <Kbd>⌘</Kbd> <Kbd>↵</Kbd> to run.
      </p>

      <Toolbar style={{ marginBottom: "var(--ws-space-sm)" }}>
        <FieldLabel>
          graph
          <Input value={graphId} onChange={(e) => setGraphId(e.target.value)} style={{ width: 80 }} mono />
        </FieldLabel>
        <Button variant="primary" onClick={run} disabled={running}>
          {running ? <Spinner /> : null}
          {running ? "Running…" : "Run (⌘↵)"}
        </Button>
      </Toolbar>

      <div style={{ display: "flex", flexWrap: "wrap", gap: "var(--ws-space-xs)", marginBottom: "var(--ws-space-sm)" }}>
        {EXAMPLES.map((q) => (
          <Chip key={q} onClick={() => setQuery(q)} title={q}>
            {q.length > 46 ? q.slice(0, 46) + "…" : q}
          </Chip>
        ))}
      </div>

      <Textarea
        value={query}
        onChange={(e) => setQuery(e.target.value)}
        onKeyDown={onKeyDown}
        mono
        style={{ width: "100%", height: 110, boxSizing: "border-box" }}
      />

      {error && (
        <div style={{ marginTop: "var(--ws-space-md)" }}>
          <InlineMessage tone="danger">
            <span className="ws-mono">{error}</span>
          </InlineMessage>
        </div>
      )}

      {columns.length > 0 && (
        <div style={{ marginTop: "var(--ws-space-md)" }}>
          <Table mono>
            <thead>
              <tr>
                {columns.map((c) => (
                  <th key={c}>{c}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {rows.map((r, i) => (
                <tr key={i}>
                  {columns.map((c) => (
                    <td key={c}>{r[c] ?? ""}</td>
                  ))}
                </tr>
              ))}
            </tbody>
          </Table>
        </div>
      )}

      {meta && (
        <div style={{ display: "flex", flexWrap: "wrap", gap: "var(--ws-space-sm)", alignItems: "center", marginTop: "var(--ws-space-md)" }}>
          <Badge tone="neutral">{rows.length} rows</Badge>
          <Badge tone="info">consistency: eventual</Badge>
          <Badge tone={meta.completeness === Completeness.COMPLETE ? "success" : "warning"}>
            completeness: {Completeness[meta.completeness]}
          </Badge>
          {meta.partialGraphPossible && <Badge tone="warning">partial graph possible</Badge>}
          {meta.warnings.length > 0 && (
            <span className="ws-caption">warnings: {meta.warnings.join("; ")}</span>
          )}
        </div>
      )}
    </div>
  );
}
