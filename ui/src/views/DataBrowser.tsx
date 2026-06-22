import { useState } from "react";
import { obs } from "../transport";
import { Completeness } from "../gen/wavespan/v1/common_pb";
import { type InspectKey, Keyspace } from "../gen/wavespan/v1/observability_pb";
import {
  Badge,
  Button,
  Checkbox,
  EmptyState,
  Input,
  Select,
  Table,
  Toolbar,
} from "../components";

// cluster = all nodes in this cluster (default), node = this node only, global = cross-cluster key.
type Scope = "cluster" | "node" | "global";

function completenessTone(c: Completeness): "success" | "warning" | "neutral" {
  if (c === Completeness.COMPLETE) return "success";
  if (c === Completeness.PARTIAL) return "warning";
  return "neutral";
}

export function DataBrowser() {
  const [scope, setScope] = useState<Scope>("cluster");
  const [namespace, setNamespace] = useState("default");
  const [query, setQuery] = useState("");
  const [includeValue, setIncludeValue] = useState(true);
  const [keys, setKeys] = useState<InspectKey[]>([]);
  const [completeness, setCompleteness] = useState<Completeness | null>(null);
  const [warnings, setWarnings] = useState<string[]>([]);
  const [searched, setSearched] = useState(false);

  const run = async () => {
    setKeys([]);
    setWarnings([]);
    setCompleteness(null);
    setSearched(true);
    const collected: InspectKey[] = [];
    const stream =
      scope === "global"
        ? obs.inspectGlobal({ keyspace: Keyspace.KV, namespace, key: new TextEncoder().encode(query), includeValue })
        : obs.inspectLocal({
            keyspace: Keyspace.KV,
            namespace,
            prefix: new TextEncoder().encode(query),
            includeValue,
            clusterWide: scope === "cluster",
          });
    for await (const row of stream) {
      if (row.row.case === "key") collected.push(row.row.value);
      else if (row.row.case === "trailer") {
        setCompleteness(row.row.value.finalCompleteness);
        setWarnings(row.row.value.warnings);
      }
    }
    setKeys(collected);
  };

  const dec = new TextDecoder();
  return (
    <div>
      <h2 className="ws-title ws-view__title">Data Browser</h2>
      <p className="ws-view__intro">
        Inspect KV records by prefix across this node, the whole cluster, or globally across clusters.
        Each scan declares its completeness so you know whether gaps are possible.
      </p>

      <Toolbar style={{ marginBottom: "var(--ws-space-md)" }}>
        <Select value={scope} onChange={(e) => setScope(e.target.value as Scope)}>
          <option value="cluster">Cluster (all nodes)</option>
          <option value="node">This node</option>
          <option value="global">Global (cross-cluster)</option>
        </Select>
        <Input value={namespace} onChange={(e) => setNamespace(e.target.value)} placeholder="namespace" style={{ width: 130 }} mono />
        <Input
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          placeholder={scope === "global" ? "key" : "prefix"}
          style={{ width: 220 }}
          mono
        />
        <Checkbox
          checked={includeValue}
          onChange={(e) => setIncludeValue(e.target.checked)}
          label="include value (admin)"
        />
        <Button variant="primary" onClick={run}>
          Search
        </Button>
      </Toolbar>

      {scope !== "node" && completeness !== null && (
        <div style={{ display: "flex", alignItems: "center", gap: "var(--ws-space-sm)", marginBottom: "var(--ws-space-md)" }}>
          <Badge tone={completenessTone(completeness)} dot>
            completeness: {Completeness[completeness]}
          </Badge>
          {warnings.length > 0 && <span className="ws-caption">warnings: {warnings.join("; ")}</span>}
        </div>
      )}

      {keys.length === 0 && searched ? (
        <EmptyState title="No keys" icon="◌">
          Nothing matched this prefix in the selected scope.
        </EmptyState>
      ) : keys.length > 0 ? (
        <Table>
          <thead>
            <tr>
              <th>path</th>
              <th>version</th>
              <th>holders</th>
              <th>tombstone</th>
              <th>value</th>
            </tr>
          </thead>
          <tbody>
            {keys.map((k, i) => (
              <tr key={i}>
                <td title={k.keyHash} className="ws-mono">{k.logicalPath}</td>
                <td className="ws-mono">
                  {k.version ? `${k.version.hlcPhysicalMs}.${k.version.hlcLogical}@${k.version.writerMemberId}` : ""}
                </td>
                <td className="ws-mono">{k.holders.map((h) => h.memberId).join(", ")}</td>
                <td>{k.tombstone ? <Badge tone="danger">tombstone</Badge> : ""}</td>
                <td className="ws-mono">{k.value.length > 0 ? dec.decode(k.value) : <span className="ws-muted">&lt;redacted&gt;</span>}</td>
              </tr>
            ))}
          </tbody>
        </Table>
      ) : (
        <EmptyState title="Browse records" icon="⌕">
          Choose a scope and namespace, then search by key prefix.
        </EmptyState>
      )}
    </div>
  );
}
