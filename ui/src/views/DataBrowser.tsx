import { useEffect, useState } from "react";
import { obs } from "../transport";
import { Completeness } from "../gen/wavespan/v1/common_pb";
import { type InspectKey, Keyspace } from "../gen/wavespan/v1/observability_pb";
import {
  Badge,
  Button,
  Checkbox,
  EmptyState,
  InlineMessage,
  Input,
  Select,
  Table,
  Toolbar,
  type Tone,
} from "../components";

// cluster = all nodes in this cluster (default), node = this node only, global = cross-cluster key.
type Scope = "cluster" | "node" | "global";

function completenessTone(c: Completeness): "success" | "warning" | "neutral" {
  if (c === Completeness.COMPLETE) return "success";
  if (c === Completeness.PARTIAL) return "warning";
  return "neutral";
}

// humanize a positive millisecond span into a compact "1h 2m" / "3m 4s" / "5s".
function humanize(ms: number): string {
  const s = Math.ceil(ms / 1000);
  if (s < 60) return `${s}s`;
  if (s < 3600) return `${Math.floor(s / 60)}m ${s % 60}s`;
  return `${Math.floor(s / 3600)}h ${Math.floor((s % 3600) / 60)}m`;
}

// Render the TTL state of a row from its absolute expiry and the current clock.
function expiry(expiresAtUnixMs: bigint | undefined, now: number): { label: string; tone: Tone } | null {
  if (expiresAtUnixMs == null) return null;
  const remaining = Number(expiresAtUnixMs) - now;
  if (remaining <= 0) return { label: "expired", tone: "danger" };
  return { label: `in ${humanize(remaining)}`, tone: remaining < 10_000 ? "warning" : "neutral" };
}

export function DataBrowser() {
  const [scope, setScope] = useState<Scope>("cluster");
  const [namespace, setNamespace] = useState("default");
  const [query, setQuery] = useState("");
  const [includeValue, setIncludeValue] = useState(true);
  const [hideExpired, setHideExpired] = useState(true);
  const [keys, setKeys] = useState<InspectKey[]>([]);
  const [completeness, setCompleteness] = useState<Completeness | null>(null);
  const [warnings, setWarnings] = useState<string[]>([]);
  const [searched, setSearched] = useState(false);
  const [deleting, setDeleting] = useState<string | null>(null);
  const [actionError, setActionError] = useState<string | null>(null);

  // Live clock so TTL countdowns tick and expired rows drop without re-querying.
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    const id = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(id);
  }, []);

  const run = async () => {
    setKeys([]);
    setWarnings([]);
    setCompleteness(null);
    setSearched(true);
    setActionError(null);
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

  const onDelete = async (k: InspectKey) => {
    setActionError(null);
    setDeleting(k.keyHash);
    try {
      const res = await obs.adminDelete({ namespace, key: k.logicalKey, targetMemberId: "" });
      if (!res.ok) {
        setActionError(res.error || "delete failed");
      }
      await run();
    } catch (e) {
      setActionError(String(e));
    } finally {
      setDeleting(null);
    }
  };

  const dec = new TextDecoder();
  const isExpired = (k: InspectKey) => k.expiresAtUnixMs != null && Number(k.expiresAtUnixMs) <= now;
  const visible = hideExpired ? keys.filter((k) => !isExpired(k)) : keys;
  const hiddenCount = keys.length - visible.length;

  return (
    <div>
      <h2 className="ws-title ws-view__title">Data Browser</h2>
      <p className="ws-view__intro">
        Inspect KV records by prefix across this node, the whole cluster, or globally across clusters.
        Each scan declares its completeness; TTL'd records show their remaining lifetime and can be
        deleted (a tombstone write).
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
        <Checkbox
          checked={hideExpired}
          onChange={(e) => setHideExpired(e.target.checked)}
          label="hide expired"
        />
        <Button variant="primary" onClick={run}>
          Search
        </Button>
      </Toolbar>

      {(scope !== "node" && completeness !== null) || hiddenCount > 0 ? (
        <div style={{ display: "flex", alignItems: "center", gap: "var(--ws-space-sm)", marginBottom: "var(--ws-space-md)", flexWrap: "wrap" }}>
          {scope !== "node" && completeness !== null && (
            <Badge tone={completenessTone(completeness)} dot>
              completeness: {Completeness[completeness]}
            </Badge>
          )}
          {hiddenCount > 0 && <Badge tone="neutral">{hiddenCount} expired hidden</Badge>}
          {warnings.length > 0 && <span className="ws-caption">warnings: {warnings.join("; ")}</span>}
        </div>
      ) : null}

      {actionError && (
        <div style={{ marginBottom: "var(--ws-space-md)" }}>
          <InlineMessage tone="danger"><span className="ws-mono">{actionError}</span></InlineMessage>
        </div>
      )}

      {visible.length === 0 && searched ? (
        <EmptyState title="No keys" icon="◌">
          {keys.length > 0 && hideExpired
            ? "All matching records have expired — untick “hide expired” to see them."
            : "Nothing matched this prefix in the selected scope."}
        </EmptyState>
      ) : visible.length > 0 ? (
        <Table>
          <thead>
            <tr>
              <th>path</th>
              <th>version</th>
              <th>holders</th>
              <th>expires</th>
              <th>state</th>
              <th>value</th>
              <th aria-label="actions" />
            </tr>
          </thead>
          <tbody>
            {visible.map((k, i) => {
              const exp = expiry(k.expiresAtUnixMs, now);
              return (
                <tr key={i} style={exp?.tone === "danger" ? { opacity: 0.6 } : undefined}>
                  <td title={k.keyHash} className="ws-mono">{k.logicalPath}</td>
                  <td className="ws-mono">
                    {k.version ? `${k.version.hlcPhysicalMs}.${k.version.hlcLogical}@${k.version.writerMemberId}` : ""}
                  </td>
                  <td className="ws-mono">{k.holders.map((h) => h.memberId).join(", ")}</td>
                  <td>{exp ? <Badge tone={exp.tone}>{exp.label}</Badge> : <span className="ws-muted">—</span>}</td>
                  <td>{k.tombstone ? <Badge tone="danger">tombstone</Badge> : <span className="ws-muted">live</span>}</td>
                  <td className="ws-mono">{k.value.length > 0 ? dec.decode(k.value) : <span className="ws-muted">&lt;redacted&gt;</span>}</td>
                  <td style={{ textAlign: "right", whiteSpace: "nowrap" }}>
                    {!k.tombstone && (
                      <Button
                        variant="danger"
                        size="sm"
                        disabled={deleting === k.keyHash}
                        onClick={() => onDelete(k)}
                        title="Write a tombstone for this key"
                      >
                        {deleting === k.keyHash ? "Deleting…" : "Delete"}
                      </Button>
                    )}
                  </td>
                </tr>
              );
            })}
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
