import { useEffect, useState } from "react";
import { useUrlState } from "../router";
import { obs, collections } from "../transport";
import { Keyspace, type GraphNode, type InspectKey } from "../gen/wavespan/v1/observability_pb";
import { create } from "@bufbuild/protobuf";
import { NodeRecordSchema } from "../gen/wavespan/v1/cypher_pb";
import {
  Badge,
  Button,
  Card,
  EmptyState,
  InlineMessage,
  Input,
  Select,
  Spinner,
  StatCard,
  Table,
} from "../components";
import { formatBytes, preview, sniff } from "../lib/valuecodec";
import { InspectorDrawer, type DrawerTarget } from "../components/inspector/Drawer";
import { CollectionBody } from "../components/inspector/CollectionBody";
import { color } from "../theme/tokens";

type Scope = "cluster" | "node";
type CType = "set" | "hash" | "zset";

// The selected source is encoded in the URL so a reload / shared link restores it. Forms:
//   kv:<namespace>
//   coll:<namespace>:<name>:<ctype>
//   graph:<graphId>
type Selection =
  | { kind: "kv"; namespace: string }
  | { kind: "coll"; namespace: string; name: string; ctype: CType }
  | { kind: "graph"; graphId: string }
  | null;

function parseSel(s: string): Selection {
  if (!s) return null;
  const [tag, ...rest] = s.split(":");
  if (tag === "kv") return { kind: "kv", namespace: rest.join(":") };
  if (tag === "coll") {
    const [namespace, name, ctype] = [rest[0], rest[1], rest[2] as CType];
    return { kind: "coll", namespace, name, ctype: ctype || "set" };
  }
  if (tag === "graph") return { kind: "graph", graphId: rest.join(":") };
  return null;
}
function encodeSel(sel: Selection): string {
  if (!sel) return "";
  if (sel.kind === "kv") return `kv:${sel.namespace}`;
  if (sel.kind === "coll") return `coll:${sel.namespace}:${sel.name}:${sel.ctype}`;
  return `graph:${sel.graphId}`;
}

const labelStyle = { fontWeight: 700 as const, fontSize: "var(--ws-text-body-sm-size)" };

export function DataWorkbench() {
  const [selStr, setSelStr] = useUrlState("src", "");
  const sel = parseSel(selStr);
  const [scopeStr, setScope] = useUrlState("scope", "cluster");
  const scope = scopeStr as Scope;
  const [prefix, setPrefix] = useUrlState("q", "");

  const [namespaces, setNamespaces] = useState<string[]>([]);
  const [stats, setStats] = useState<{ local: bigint; distinct: bigint } | null>(null);

  // KV results
  const [keys, setKeys] = useState<InspectKey[]>([]);
  // graph results
  const [graphNodes, setGraphNodes] = useState<GraphNode[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Collections: a namespace is browsable (ListCollections returns name + type); graph/vector have no
  // list RPC, so they stay "open by name".
  const [collNs, setCollNs] = useUrlState("collns", "default");
  const [collList, setCollList] = useState<{ name: string; type: CType }[]>([]);
  const [collListLoading, setCollListLoading] = useState(false);
  const [creatingColl, setCreatingColl] = useState(false);
  const [graphId, setGraphId] = useState("g");
  const [vectorName, setVectorName] = useState("");
  const [openColl, setOpenColl] = useState(true);
  const [openGraph, setOpenGraph] = useState(true);
  const [openVector, setOpenVector] = useState(false);

  const [drawer, setDrawer] = useState<DrawerTarget | null>(null);

  const loadCluster = async () => {
    try {
      const v = await obs.getClusterView({});
      setNamespaces(v.namespaces);
      setStats({ local: v.localKeys, distinct: v.distinctKeysEstimate });
    } catch {
      /* best-effort */
    }
  };
  useEffect(() => {
    void loadCluster();
  }, []);

  // List the collections (with their datatype) in the chosen namespace. A linearizable read is used
  // right after a create so the new collection shows immediately (a stale read can lag the write).
  const loadCollList = async (namespace: string, linearizable = false) => {
    setCollListLoading(true);
    try {
      const res = await collections.listCollections({ namespace, linearizable });
      const dec = new TextDecoder();
      setCollList(
        res.collections
          .map((c) => ({ name: dec.decode(c.name), type: (c.type || "set") as CType }))
          .sort((a, b) => a.name.localeCompare(b.name)),
      );
    } catch {
      setCollList([]); // best-effort (tier may be disabled)
    } finally {
      setCollListLoading(false);
    }
  };
  useEffect(() => {
    void loadCollList(collNs);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [collNs]);

  // Stream KV records for the selected namespace.
  const loadKv = async (namespace: string) => {
    setLoading(true);
    setError(null);
    setKeys([]);
    const collected: InspectKey[] = [];
    try {
      const stream = obs.inspectLocal({
        keyspace: Keyspace.KV,
        namespace,
        prefix: new TextEncoder().encode(prefix),
        includeValue: true,
        clusterWide: scope === "cluster",
      });
      for await (const row of stream) {
        if (row.row.case === "key") collected.push(row.row.value);
      }
      setKeys(collected);
    } catch (e) {
      setError(String(e));
    } finally {
      setLoading(false);
    }
  };

  const loadGraph = async (gid: string) => {
    setLoading(true);
    setError(null);
    setGraphNodes([]);
    try {
      const resp = await obs.graphExplore({ graphId: gid, seedNodeId: "", depth: 1, limit: 250, includeValue: true });
      setGraphNodes(resp.nodes);
    } catch (e) {
      setError(String(e));
    } finally {
      setLoading(false);
    }
  };

  // Load whenever the selection (or scope/prefix for KV) changes.
  useEffect(() => {
    if (!sel) return;
    if (sel.kind === "kv") void loadKv(sel.namespace);
    else if (sel.kind === "graph") void loadGraph(sel.graphId);
    // collection contents are loaded inside CollectionBody (its own browse).
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selStr, scope]);

  const select = (next: Selection) => {
    setSelStr(encodeSel(next));
    setCreatingColl(false);
    setDrawer(null);
  };

  const refresh = () => {
    if (sel?.kind === "kv") void loadKv(sel.namespace);
    else if (sel?.kind === "graph") void loadGraph(sel.graphId);
    void loadCollList(collNs); // a collection add/remove may have created/emptied one
    void loadCluster();
  };

  const onSaved = () => {
    setDrawer(null);
    refresh();
  };

  // Create a collection by writing its first entry (a collection is created on first write, which also
  // fixes its datatype). On success, refresh the list and open the new collection for editing.
  const createCollection = async (ns: string, name: string, type: CType, entry: { member?: string; field?: string; value?: string; score?: string }) => {
    const enc = (s: string) => new TextEncoder().encode(s);
    const coll = enc(name);
    if (type === "set") {
      await collections.sAdd({ namespace: ns, collection: coll, members: [enc(entry.member ?? "")] });
    } else if (type === "hash") {
      await collections.hSet({ namespace: ns, collection: coll, fields: [{ field: enc(entry.field ?? ""), value: enc(entry.value ?? "") }] });
    } else {
      await collections.zAdd({ namespace: ns, collection: coll, members: [{ member: enc(entry.member ?? ""), score: Number(entry.score) || 0 }] });
    }
    setCollNs(ns); // switch the browse namespace to where we just created
    await loadCollList(ns, true); // linearizable so the new collection appears immediately
    setCreatingColl(false);
    select({ kind: "coll", namespace: ns, name, ctype: type });
  };

  // Namespaces offered in the Collections picker: the gossiped set plus "default" and whatever is
  // currently selected (so a freshly-created namespace stays selectable). Typing a brand-new one is
  // done in the New-collection form.
  const knownNamespaces = Array.from(new Set(["default", collNs, ...namespaces])).sort();

  const sectionHead = (label: string, open: boolean, toggle: () => void) => (
    <button
      onClick={toggle}
      className="ws-mono"
      style={{
        display: "flex",
        alignItems: "center",
        gap: "var(--ws-space-xs)",
        width: "100%",
        background: "none",
        border: "none",
        cursor: "pointer",
        padding: "var(--ws-space-xs) 0",
        font: "inherit",
        color: color.ink,
        ...labelStyle,
      }}
    >
      <span style={{ width: 12 }}>{open ? "▾" : "▸"}</span> {label}
    </button>
  );

  return (
    <div>
      <h2 className="ws-title ws-view__title">Data</h2>
      <p className="ws-view__intro">
        A unified workspace over every data model. Pick a source on the left; rows preview their value
        type-aware. Click a row to inspect &amp; edit it in the drawer. Selection &amp; scope persist in the URL.
      </p>

      {stats && (
        <div style={{ display: "flex", gap: "var(--ws-space-sm)", flexWrap: "wrap", marginBottom: "var(--ws-space-md)" }}>
          <StatCard label="Local KV keys" value={String(stats.local)} />
          <StatCard label="Distinct (est.)" value={String(stats.distinct)} accent={color.olive} />
        </div>
      )}

      <div style={{ display: "flex", gap: "var(--ws-space-md)", alignItems: "flex-start" }}>
        {/* ---- left source tree ---- */}
        <Card flat style={{ width: 240, flex: "0 0 240px", alignSelf: "flex-start" }}>
          <div style={labelStyle}>KV namespaces</div>
          <div style={{ display: "flex", flexDirection: "column", gap: 2, margin: "var(--ws-space-xs) 0 var(--ws-space-md)" }}>
            {namespaces.length === 0 && <span className="ws-caption ws-muted">none gossiped yet</span>}
            {namespaces.map((ns) => {
              const active = sel?.kind === "kv" && sel.namespace === ns;
              return (
                <button
                  key={ns}
                  onClick={() => select({ kind: "kv", namespace: ns })}
                  className="ws-mono"
                  style={{
                    textAlign: "left",
                    background: active ? "var(--ws-color-surface-alt)" : "none",
                    border: "none",
                    borderRadius: "var(--ws-radius-sm)",
                    padding: "2px var(--ws-space-xs)",
                    cursor: "pointer",
                    color: color.ink,
                    fontWeight: active ? 700 : 400,
                  }}
                >
                  {ns === "" ? "(default)" : ns}
                </button>
              );
            })}
          </div>

          {sectionHead("Collections", openColl, () => setOpenColl(!openColl))}
          {openColl && (
            <div style={{ display: "flex", flexDirection: "column", gap: "var(--ws-space-xs)", margin: "var(--ws-space-xs) 0 var(--ws-space-md)" }}>
              <Select value={collNs} onChange={(e) => setCollNs(e.target.value)} title="namespace">
                {knownNamespaces.map((ns) => (
                  <option key={ns} value={ns}>{ns === "" ? "(default)" : ns}</option>
                ))}
              </Select>
              {collListLoading && <Spinner />}
              {!collListLoading && collList.length === 0 && (
                <span className="ws-caption ws-muted">no collections in this namespace</span>
              )}
              {collList.map((c) => {
                const active = sel?.kind === "coll" && sel.namespace === collNs && sel.name === c.name;
                return (
                  <button
                    key={`${c.type}:${c.name}`}
                    onClick={() => select({ kind: "coll", namespace: collNs, name: c.name, ctype: c.type })}
                    title={c.name}
                    style={{
                      display: "flex", alignItems: "center", justifyContent: "space-between", gap: "var(--ws-space-xs)",
                      width: "100%", textAlign: "left", border: "none", borderRadius: "var(--ws-radius-sm)",
                      padding: "2px var(--ws-space-xs)", cursor: "pointer", color: color.ink,
                      background: active ? "var(--ws-color-surface-alt)" : "none",
                    }}
                  >
                    <span className="ws-mono" style={{ overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", fontWeight: active ? 700 : 400 }}>{c.name}</span>
                    <Badge tone="olive">{c.type}</Badge>
                  </button>
                );
              })}
              <Button size="sm" variant="ghost" onClick={() => { setCreatingColl(true); setSelStr(""); }}>
                + New collection
              </Button>
            </div>
          )}

          {sectionHead("Graphs", openGraph, () => setOpenGraph(!openGraph))}
          {openGraph && (
            <div style={{ display: "flex", flexDirection: "column", gap: "var(--ws-space-xs)", margin: "var(--ws-space-xs) 0 var(--ws-space-md)" }}>
              <Input value={graphId} onChange={(e) => setGraphId(e.target.value)} placeholder="graph id" mono />
              <Button size="sm" disabled={!graphId.trim()} onClick={() => select({ kind: "graph", graphId })}>
                Open
              </Button>
            </div>
          )}

          {sectionHead("Vector", openVector, () => setOpenVector(!openVector))}
          {openVector && (
            <div style={{ display: "flex", flexDirection: "column", gap: "var(--ws-space-xs)", margin: "var(--ws-space-xs) 0" }}>
              <Input value={vectorName} onChange={(e) => setVectorName(e.target.value)} placeholder="collection name" mono />
              <span className="ws-caption ws-muted">vector browsing is deferred (read-only drawer)</span>
            </div>
          )}
        </Card>

        {/* ---- center results pane ---- */}
        <div style={{ flex: 1, minWidth: 0 }}>
          {sel?.kind === "kv" && (
            <>
              <div style={{ display: "flex", gap: "var(--ws-space-sm)", alignItems: "center", marginBottom: "var(--ws-space-sm)", flexWrap: "wrap" }}>
                <Select value={scope} onChange={(e) => setScope(e.target.value as Scope)}>
                  <option value="cluster">Cluster (all nodes)</option>
                  <option value="node">This node</option>
                </Select>
                <Input value={prefix} onChange={(e) => setPrefix(e.target.value)} placeholder="key prefix" mono style={{ width: 200 }} />
                <Button variant="primary" size="sm" onClick={() => sel && loadKv(sel.namespace)}>
                  Search
                </Button>
                <Button size="sm" onClick={() => setDrawer({ kind: "kv-new", namespace: sel.namespace })}>
                  + New
                </Button>
                {loading && <Spinner />}
              </div>
              <KvResults keys={keys} loading={loading} onOpen={(k) => setDrawer({
                kind: "kv",
                namespace: sel.namespace,
                key: k.logicalKey,
                value: k.value,
                version: k.version,
                tombstone: k.tombstone,
                expiresAtUnixMs: k.expiresAtUnixMs,
                holders: k.holders,
                keyLabel: k.logicalPath,
              })} />
            </>
          )}

          {sel?.kind === "coll" && (
            <Card flat>
              <div className="ws-title-sm" style={{ marginBottom: "var(--ws-space-sm)" }}>
                {sel.namespace} / {sel.name} <Badge tone="olive">{sel.ctype}</Badge>
              </div>
              {/* The collection's contents ARE the thing being inspected — edit them directly here, no
                  intermediate "inspect & edit" hop or drawer. */}
              <CollectionBody key={`${sel.namespace}:${sel.name}:${sel.ctype}`} target={{ kind: "collection", namespace: sel.namespace, collection: sel.name, ctype: sel.ctype }} />
            </Card>
          )}

          {sel?.kind === "graph" && (
            <GraphResults
              nodes={graphNodes}
              loading={loading}
              onNew={() =>
                setDrawer({
                  kind: "graph-node",
                  graphId: sel.graphId,
                  record: create(NodeRecordSchema, { graphId: sel.graphId, nodeId: "", labels: [], properties: {}, tombstone: false }),
                })
              }
              onOpen={(n) =>
                setDrawer({
                  kind: "graph-node",
                  graphId: sel.graphId,
                  record: create(NodeRecordSchema, { graphId: sel.graphId, nodeId: n.nodeId, labels: n.labels, properties: n.properties, tombstone: false }),
                })
              }
            />
          )}

          {creatingColl && (
            <NewCollectionForm
              namespace={collNs}
              onCancel={() => setCreatingColl(false)}
              onCreate={createCollection}
            />
          )}

          {!sel && !creatingColl && (
            <EmptyState title="Pick a source" icon="◧">
              Choose a KV namespace or a collection on the left (graphs open by id).
            </EmptyState>
          )}

          {error && (
            <div style={{ marginTop: "var(--ws-space-md)" }}>
              <InlineMessage tone="danger"><span className="ws-mono">{error}</span></InlineMessage>
            </div>
          )}
        </div>

        {/* ---- right inspector drawer ---- */}
        {drawer && <InspectorDrawer target={drawer} onClose={() => setDrawer(null)} onSaved={onSaved} />}
      </div>
    </div>
  );
}

function NewCollectionForm({
  namespace,
  onCancel,
  onCreate,
}: {
  namespace: string;
  onCancel: () => void;
  onCreate: (ns: string, name: string, type: CType, entry: { member?: string; field?: string; value?: string; score?: string }) => Promise<void>;
}) {
  const [ns, setNs] = useState(namespace);
  const [name, setName] = useState("");
  const [type, setType] = useState<CType>("set");
  const [member, setMember] = useState("");
  const [field, setField] = useState("");
  const [value, setValue] = useState("");
  const [score, setScore] = useState("");
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  // A collection is created by its first write, which also fixes its type — so creation needs one entry.
  const entryReady = type === "hash" ? field.trim() !== "" : member.trim() !== "";
  const canCreate = ns.trim() !== "" && name.trim() !== "" && entryReady && !busy;

  const submit = async () => {
    setBusy(true);
    setErr(null);
    try {
      await onCreate(ns.trim(), name.trim(), type, { member, field, value, score });
    } catch (e) {
      setErr(String(e instanceof Error ? e.message : e));
    } finally {
      setBusy(false);
    }
  };

  const row = { display: "grid", gridTemplateColumns: "90px 1fr", gap: "var(--ws-space-sm)", alignItems: "center" as const };
  return (
    <Card flat>
      <div className="ws-title-sm" style={{ marginBottom: "var(--ws-space-sm)" }}>New collection</div>
      <div style={{ display: "flex", flexDirection: "column", gap: "var(--ws-space-sm)", maxWidth: 460 }}>
        <div style={row}><label>Namespace</label><Input value={ns} onChange={(e) => setNs(e.target.value)} placeholder="namespace (type a new one to create it)" mono /></div>
        <div style={row}><label>Name</label><Input value={name} onChange={(e) => setName(e.target.value)} placeholder="collection name" mono /></div>
        <div style={row}>
          <label>Type</label>
          <Select value={type} onChange={(e) => setType(e.target.value as CType)}>
            <option value="set">set</option>
            <option value="hash">hash</option>
            <option value="zset">zset</option>
          </Select>
        </div>
        {type === "hash" ? (
          <>
            <div style={row}><label>First field</label><Input value={field} onChange={(e) => setField(e.target.value)} placeholder="field" mono /></div>
            <div style={row}><label>Value</label><Input value={value} onChange={(e) => setValue(e.target.value)} placeholder="value" mono /></div>
          </>
        ) : (
          <>
            <div style={row}><label>First member</label><Input value={member} onChange={(e) => setMember(e.target.value)} placeholder="member" mono /></div>
            {type === "zset" && <div style={row}><label>Score</label><Input value={score} onChange={(e) => setScore(e.target.value)} placeholder="1.0" mono /></div>}
          </>
        )}
        <p className="ws-caption ws-muted">A collection is created by its first entry, which also sets its datatype.</p>
        <div style={{ display: "flex", gap: "var(--ws-space-sm)" }}>
          <Button variant="primary" size="sm" onClick={submit} disabled={!canCreate}>{busy ? "Creating…" : "Create"}</Button>
          <Button variant="ghost" size="sm" onClick={onCancel} disabled={busy}>Cancel</Button>
          {busy && <Spinner />}
        </div>
        {err && <InlineMessage tone="danger"><span className="ws-mono">{err}</span></InlineMessage>}
      </div>
    </Card>
  );
}

function KvResults({ keys, loading, onOpen }: { keys: InspectKey[]; loading: boolean; onOpen: (k: InspectKey) => void }) {
  if (loading && keys.length === 0) return <Spinner />;
  if (keys.length === 0) return <EmptyState title="No keys" icon="◌">Nothing matched this prefix in the selected scope.</EmptyState>;
  return (
    <Table mono>
      <thead>
        <tr>
          <th>key</th>
          <th>type</th>
          <th>value</th>
          <th>version · ttl</th>
        </tr>
      </thead>
      <tbody>
        {keys.map((k, i) => {
          const codec = k.value.length > 0 ? sniff(k.value) : "text";
          const ver = k.version ? `${k.version.hlcPhysicalMs}.${k.version.hlcLogical}` : "—";
          return (
            <tr key={i} style={{ cursor: "pointer" }} onClick={() => onOpen(k)}>
              <td>{k.logicalPath}</td>
              <td>
                <Badge tone="neutral">{k.value.length > 0 ? codec : "—"}</Badge>
              </td>
              <td>{k.value.length > 0 ? preview(k.value, codec) : <span className="ws-muted">&lt;redacted&gt;</span>}</td>
              <td>
                {ver}
                {k.expiresAtUnixMs != null ? ` · ttl` : ""} {k.value.length > 0 ? `· ${formatBytes(k.value.length)}` : ""}
              </td>
            </tr>
          );
        })}
      </tbody>
    </Table>
  );
}

function GraphResults({
  nodes,
  loading,
  onOpen,
  onNew,
}: {
  nodes: GraphNode[];
  loading: boolean;
  onOpen: (n: GraphNode) => void;
  onNew: () => void;
}) {
  return (
    <>
      <div style={{ marginBottom: "var(--ws-space-sm)", display: "flex", gap: "var(--ws-space-sm)", alignItems: "center" }}>
        <Button size="sm" onClick={onNew}>
          + New node
        </Button>
        <Badge tone="neutral">{nodes.length} nodes</Badge>
        {loading && <Spinner />}
      </div>
      {nodes.length === 0 ? (
        <EmptyState title="No nodes" icon="◌">This graph returned no nodes (try the Node Explorer to load a sample dataset).</EmptyState>
      ) : (
        <Table mono>
          <thead>
            <tr>
              <th>node id</th>
              <th>labels</th>
              <th>props</th>
            </tr>
          </thead>
          <tbody>
            {nodes.map((n) => (
              <tr key={n.nodeId} style={{ cursor: "pointer" }} onClick={() => onOpen(n)}>
                <td>{n.nodeId}</td>
                <td>{n.labels.join(", ")}</td>
                <td>{Object.keys(n.properties).length}</td>
              </tr>
            ))}
          </tbody>
        </Table>
      )}
    </>
  );
}
