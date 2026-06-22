import { useState } from "react";
import { obs } from "../transport";
import { Completeness } from "../gen/wavespan/v1/common_pb";
import { type InspectKey, Keyspace } from "../gen/wavespan/v1/observability_pb";

// cluster = all nodes in this cluster (default), node = this node only, global = cross-cluster key.
type Scope = "cluster" | "node" | "global";

export function DataBrowser() {
  const [scope, setScope] = useState<Scope>("cluster");
  const [namespace, setNamespace] = useState("default");
  const [query, setQuery] = useState("");
  const [includeValue, setIncludeValue] = useState(true);
  const [keys, setKeys] = useState<InspectKey[]>([]);
  const [completeness, setCompleteness] = useState<Completeness | null>(null);
  const [warnings, setWarnings] = useState<string[]>([]);

  const run = async () => {
    setKeys([]);
    setWarnings([]);
    setCompleteness(null);
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
      <div style={{ display: "flex", gap: 8, marginBottom: 8, alignItems: "center" }}>
        <select value={scope} onChange={(e) => setScope(e.target.value as Scope)}>
          <option value="cluster">Cluster (all nodes)</option>
          <option value="node">This node</option>
          <option value="global">Global (cross-cluster)</option>
        </select>
        <input value={namespace} onChange={(e) => setNamespace(e.target.value)} placeholder="namespace" style={{ width: 120 }} />
        <input value={query} onChange={(e) => setQuery(e.target.value)} placeholder={scope === "global" ? "key" : "prefix"} style={{ width: 200 }} />
        <label style={{ fontSize: 12 }}>
          <input type="checkbox" checked={includeValue} onChange={(e) => setIncludeValue(e.target.checked)} /> include value (admin)
        </label>
        <button onClick={run}>Search</button>
      </div>
      {scope !== "node" && completeness !== null && (
        <div style={{ padding: 8, marginBottom: 8, background: completeness === Completeness.COMPLETE ? "#e9ffe9" : "#fff3cd" }}>
          completeness: {Completeness[completeness]}
          {warnings.length > 0 && <div style={{ fontSize: 12 }}>warnings: {warnings.join("; ")}</div>}
        </div>
      )}
      <table style={{ width: "100%", fontSize: 12, borderCollapse: "collapse" }}>
        <thead>
          <tr style={{ textAlign: "left", borderBottom: "1px solid #ccc" }}>
            <th>path</th><th>version</th><th>holders</th><th>tombstone</th><th>value</th>
          </tr>
        </thead>
        <tbody>
          {keys.map((k, i) => (
            <tr key={i}>
              <td title={k.keyHash}>{k.logicalPath}</td>
              <td>{k.version ? `${k.version.hlcPhysicalMs}.${k.version.hlcLogical}@${k.version.writerMemberId}` : ""}</td>
              <td>{k.holders.map((h) => h.memberId).join(", ")}</td>
              <td>{k.tombstone ? "yes" : ""}</td>
              <td>{k.value.length > 0 ? dec.decode(k.value) : "<redacted>"}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
