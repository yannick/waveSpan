import { useState } from "react";
import { useUrlState } from "../router";
import { cypher } from "../transport";
import { Completeness } from "../gen/wavespan/v1/common_pb";
import { create } from "@bufbuild/protobuf";
import { NodeRecordSchema, type QueryMeta, type Value } from "../gen/wavespan/v1/cypher_pb";
import { InspectorDrawer, type DrawerTarget } from "../components/inspector/Drawer";
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
  "MATCH (u:User) RETURN u.name, kv.get('profile', u.id) AS profile",
  "CALL kv.put('profile', 'u1', '{\"v\":2}') YIELD version RETURN version",
  "RETURN kv.get('profile', 'u1') AS profile",
];

export function CypherConsole() {
  const [graphId, setGraphId] = useUrlState("graph", "g");
  const [query, setQuery] = useUrlState("q", EXAMPLES[0]);
  const [columns, setColumns] = useState<string[]>([]);
  const [rows, setRows] = useState<Record<string, string>[]>([]);
  // Raw Value rows kept in parallel so the "inspect" affordance can recover a node id (a string cell).
  const [rawRows, setRawRows] = useState<Record<string, Value>[]>([]);
  const [meta, setMeta] = useState<QueryMeta | null>(null);
  const [error, setError] = useState("");
  const [running, setRunning] = useState(false);
  const [drawer, setDrawer] = useState<DrawerTarget | null>(null);

  const run = async () => {
    setError("");
    setMeta(null);
    setRows([]);
    setRawRows([]);
    setColumns([]);
    setRunning(true);
    const collected: Record<string, string>[] = [];
    const collectedRaw: Record<string, Value>[] = [];
    const cols = new Set<string>();
    try {
      for await (const res of cypher.query({ graphId, query, parameters: {} })) {
        if (res.msg.case === "row") {
          const row: Record<string, string> = {};
          const raw: Record<string, Value> = {};
          for (const [k, v] of Object.entries(res.msg.value.columns)) {
            cols.add(k);
            row[k] = valueToString(v as Value);
            raw[k] = v as Value;
          }
          collected.push(row);
          collectedRaw.push(raw);
        } else if (res.msg.case === "meta") {
          setMeta(res.msg.value);
        }
      }
      setColumns([...cols]);
      setRows(collected);
      setRawRows(collectedRaw);
    } catch (e) {
      setError(String(e));
    } finally {
      setRunning(false);
    }
  };

  const onKeyDown = (e: React.KeyboardEvent) => {
    if ((e.metaKey || e.ctrlKey) && e.key === "Enter") run();
  };

  // Recover a candidate node id from a result row: the first string-valued cell. Cypher rows are
  // flattened projections, so this is best-effort — it opens the graph-node drawer for that id.
  const nodeIdOf = (raw: Record<string, Value> | undefined): string | null => {
    if (!raw) return null;
    for (const v of Object.values(raw)) {
      if (v?.value?.case === "stringValue" && v.value.value) return v.value.value;
    }
    return null;
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
        <div style={{ marginTop: "var(--ws-space-md)", display: "flex", gap: "var(--ws-space-md)", alignItems: "flex-start" }}>
          <div style={{ flex: 1, minWidth: 0, overflowX: "auto" }}>
            <Table mono>
              <thead>
                <tr>
                  {columns.map((c) => (
                    <th key={c}>{c}</th>
                  ))}
                  <th aria-label="actions" />
                </tr>
              </thead>
              <tbody>
                {rows.map((r, i) => {
                  const nid = nodeIdOf(rawRows[i]);
                  return (
                    <tr key={i}>
                      {columns.map((c) => (
                        <td key={c}>{r[c] ?? ""}</td>
                      ))}
                      <td style={{ textAlign: "right", whiteSpace: "nowrap" }}>
                        {nid && (
                          <Button
                            variant="ghost"
                            size="sm"
                            title={`inspect node ${nid}`}
                            onClick={() =>
                              setDrawer({
                                kind: "graph-node",
                                graphId,
                                record: create(NodeRecordSchema, { graphId, nodeId: nid, labels: [], properties: {}, tombstone: false }),
                              })
                            }
                          >
                            inspect
                          </Button>
                        )}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </Table>
          </div>
          {drawer && <InspectorDrawer target={drawer} onClose={() => setDrawer(null)} onSaved={() => setDrawer(null)} />}
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
