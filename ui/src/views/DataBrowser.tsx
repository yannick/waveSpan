import { useState } from "react";
import { obs } from "../transport";
import { Completeness } from "../gen/wavespan/v1/common_pb";
import { type InspectKey, Keyspace } from "../gen/wavespan/v1/observability_pb";

type Scope = "local" | "global";

export function DataBrowser() {
  const [scope, setScope] = useState<Scope>("local");
  const [namespace, setNamespace] = useState("default");
  const [query, setQuery] = useState("");
  const [includeValue, setIncludeValue] = useState(false);
  const [keys, setKeys] = useState<InspectKey[]>([]);
  const [completeness, setCompleteness] = useState<Completeness | null>(null);
  const [warnings, setWarnings] = useState<string[]>([]);

  const run = async () => {
    setKeys([]);
    setWarnings([]);
    setCompleteness(null);
    const collected: InspectKey[] = [];
    const stream =
      scope === "local"
        ? obs.inspectLocal({ keyspace: Keyspace.KV, namespace, prefix: new TextEncoder().encode(query), includeValue })
        : obs.inspectGlobal({ keyspace: Keyspace.KV, namespace, key: new TextEncoder().encode(query), includeValue });
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
          <option value="local">Local</option>
          <option value="global">Global</option>
        </select>
        <input value={namespace} onChange={(e) => setNamespace(e.target.value)} placeholder="namespace" style={{ width: 120 }} />
        <input value={query} onChange={(e) => setQuery(e.target.value)} placeholder={scope === "local" ? "prefix" : "key"} style={{ width: 200 }} />
        <label style={{ fontSize: 12 }}>
          <input type="checkbox" checked={includeValue} onChange={(e) => setIncludeValue(e.target.checked)} /> include value (admin)
        </label>
        <button onClick={run}>Search</button>
      </div>
      {scope === "global" && completeness !== null && (
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
