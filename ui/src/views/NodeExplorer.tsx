import { useEffect, useRef, useState } from "react";
import ForceGraph3D, { type ForceGraph3DInstance, type NodeObject, type LinkObject } from "3d-force-graph";
import { Color, Fog } from "three";
import SpriteText from "three-spritetext";
import { useUrlNumber, useUrlState } from "../router";
import { obs, cypher } from "../transport";
import { Completeness } from "../gen/wavespan/v1/common_pb";
import { type GraphEdge, type GraphNode } from "../gen/wavespan/v1/observability_pb";
import { type QueryMeta, type Value } from "../gen/wavespan/v1/cypher_pb";
import {
  Badge,
  Button,
  Card,
  Checkbox,
  FieldLabel,
  InlineMessage,
  Input,
  Kbd,
  Spinner,
  Table,
  Textarea,
  Toolbar,
} from "../components";
import { categoricalColor } from "../theme/tokens";

const CANVAS_H = 560;

// --- value helpers (GraphNode.properties + Cypher columns are wavespan.v1.Value) ---

function valToStr(v: Value | undefined): string {
  const c = v?.value;
  if (!c) return "";
  switch (c.case) {
    case "stringValue":
      return c.value;
    case "intValue":
    case "doubleValue":
      return String(c.value);
    case "boolValue":
      return String(c.value);
    case "listValue":
      return c.value.values.map(valToStr).join(", ");
    case "null":
      return "null";
    default:
      return "·";
  }
}

// collectIds harvests every string value in a Cypher result cell — these are candidate node ids that
// GraphSubgraph resolves into the actual graph (non-ids are simply ignored server-side).
function collectIds(v: Value | undefined, into: Set<string>) {
  const c = v?.value;
  if (!c) return;
  if (c.case === "stringValue") {
    if (c.value) into.add(c.value);
  } else if (c.case === "listValue") {
    c.value.values.forEach((x) => collectIds(x, into));
  }
}

// resolveVar turns a `var(--ws-color-x)` token expression into its resolved hex/rgb at call time, so
// WebGL materials stay in sync with the active Linea light/dark theme.
function resolveVar(expr: string): string {
  const m = /var\((--[a-z0-9-]+)\)/i.exec(expr);
  if (!m) return expr;
  const v = getComputedStyle(document.documentElement).getPropertyValue(m[1]).trim();
  return v || expr;
}

function labelHex(labels: string[]): string {
  return resolveVar(categoricalColor(labels[0] ?? ""));
}

function nodeDisplay(n: GraphNode): string {
  return valToStr(n.properties["title"]) || valToStr(n.properties["name"]) || n.nodeId;
}

// The force-graph node/link objects carry our domain fields alongside the simulation's x/y/z.
type GNode = NodeObject & {
  id: string;
  labels: string[];
  properties: Record<string, Value>;
  display: string;
  color: string;
  degree: number;
};
type GLink = LinkObject<GNode> & { type: string };

export function NodeExplorer() {
  const [graphId, setGraphId] = useUrlState("graph", "g");
  const [seed, setSeed] = useUrlState("seed", "");
  const [depth, setDepth] = useUrlNumber("depth", 2);
  const [query, setQuery] = useUrlState("q", "");
  const [expandNeighbors, setExpandNeighbors] = useState(true);

  const [nodes, setNodes] = useState<GraphNode[]>([]);
  const [edges, setEdges] = useState<GraphEdge[]>([]);
  const [selected, setSelected] = useState<GNode | null>(null);
  const [truncated, setTruncated] = useState(false);
  const [meta, setMeta] = useState<QueryMeta | null>(null);
  const [error, setError] = useState("");
  const [running, setRunning] = useState(false);
  const [loadMsg, setLoadMsg] = useState<{ tone: "success" | "warning" | "danger"; text: string } | null>(null);

  const mountRef = useRef<HTMLDivElement | null>(null);
  const graphRef = useRef<ForceGraph3DInstance | null>(null);

  // Centre the camera on a node (the "classic" click-to-focus orbit).
  const focusNode = (n: GNode) => {
    const g = graphRef.current;
    if (!g) return;
    const x = n.x ?? 0;
    const y = n.y ?? 0;
    const z = n.z ?? 0;
    const dist = 90;
    const ratio = 1 + dist / Math.hypot(x, y, z || 1);
    g.cameraPosition({ x: x * ratio, y: y * ratio, z: z * ratio }, { x, y, z }, 1200);
  };

  // --- build the WebGL graph once ---
  useEffect(() => {
    const el = mountRef.current;
    if (!el) return;
    const ink = resolveVar("var(--ws-color-ink)");
    const muted = resolveVar("var(--ws-color-ink-muted)");
    const bg = resolveVar("var(--ws-color-paper)");

    const g = new ForceGraph3D(el)
      .backgroundColor(bg)
      .showNavInfo(false)
      .nodeRelSize(5)
      .nodeColor((n) => (n as GNode).color)
      .nodeVal((n) => (n as GNode).degree)
      .nodeOpacity(0.92)
      .nodeResolution(16)
      .nodeLabel((n) => (n as GNode).display)
      .nodeThreeObjectExtend(true)
      .nodeThreeObject((n) => {
        const gn = n as GNode;
        const s = new SpriteText(gn.display);
        s.color = ink;
        s.textHeight = 4;
        s.fontWeight = "600";
        s.padding = 1;
        s.position.set(0, 7 + Math.cbrt(gn.degree) * 2, 0);
        return s;
      })
      .linkColor(() => muted)
      .linkOpacity(0.35)
      .linkWidth(0.8)
      .linkLabel((l) => (l as GLink).type)
      .linkDirectionalArrowLength(3.5)
      .linkDirectionalArrowRelPos(1)
      .linkDirectionalParticles(2)
      .linkDirectionalParticleWidth(1.1)
      .onNodeClick((n) => {
        const gn = n as GNode;
        focusNode(gn);
        setSelected(gn);
      });

    // Fog matched to the background gives real depth — far nodes recede instead of cluttering.
    g.scene().fog = new Fog(new Color(bg).getHex(), 90, 360);
    g.width(el.clientWidth).height(CANVAS_H);
    graphRef.current = g;

    const ro = new ResizeObserver(() => g.width(el.clientWidth).height(CANVAS_H));
    ro.observe(el);

    return () => {
      ro.disconnect();
      g._destructor();
      el.replaceChildren();
      graphRef.current = null;
    };
  }, []);

  // --- push data into the graph whenever the result set changes ---
  useEffect(() => {
    const g = graphRef.current;
    if (!g) return;
    const deg = new Map<string, number>();
    for (const e of edges) {
      deg.set(e.source, (deg.get(e.source) ?? 0) + 1);
      deg.set(e.target, (deg.get(e.target) ?? 0) + 1);
    }
    const ids = new Set(nodes.map((n) => n.nodeId));
    const gNodes: GNode[] = nodes.map((n) => ({
      id: n.nodeId,
      labels: n.labels,
      properties: n.properties,
      display: nodeDisplay(n),
      color: labelHex(n.labels),
      degree: 1 + (deg.get(n.nodeId) ?? 0),
    }));
    const gLinks: GLink[] = edges
      .filter((e) => ids.has(e.source) && ids.has(e.target))
      .map((e) => ({ source: e.source, target: e.target, type: e.type }));
    g.graphData({ nodes: gNodes, links: gLinks });
  }, [nodes, edges]);

  // --- data sources: BFS explore, Cypher → subgraph, sample loader ---

  const explore = async (seedNodeId: string) => {
    setError("");
    setMeta(null);
    try {
      const resp = await obs.graphExplore({ graphId, seedNodeId, depth, limit: 250, includeValue: true });
      setNodes(resp.nodes);
      setEdges(resp.edges);
      setTruncated(resp.truncated);
      setSelected(null);
    } catch (e) {
      setError(String(e));
    }
  };

  const runCypher = async () => {
    if (!query.trim()) return;
    setError("");
    setRunning(true);
    setMeta(null);
    const idSet = new Set<string>();
    try {
      for await (const res of cypher.query({ graphId, query, parameters: {} })) {
        if (res.msg.case === "row") {
          for (const v of Object.values(res.msg.value.columns)) collectIds(v as Value, idSet);
        } else if (res.msg.case === "meta") {
          setMeta(res.msg.value);
        }
      }
      const resp = await obs.graphSubgraph({
        graphId,
        nodeIds: [...idSet],
        neighborDepth: expandNeighbors ? 1 : 0,
        includeValue: true,
      });
      setNodes(resp.nodes);
      setEdges(resp.edges);
      setTruncated(resp.truncated);
      setSelected(null);
    } catch (e) {
      setError(String(e));
    } finally {
      setRunning(false);
    }
  };

  const loadSample = async () => {
    setLoadMsg(null);
    setError("");
    try {
      const r = await obs.loadSampleDataset({ graphId });
      if (!r.ok) {
        setLoadMsg({ tone: "warning", text: r.error || "sample dataset loading is disabled" });
        return;
      }
      setLoadMsg({
        tone: "success",
        text: `Loaded ${r.datasetName}: ${r.nodesCreated} nodes, ${r.edgesCreated} edges · ${r.attribution}`,
      });
      setSeed("");
      await explore("");
    } catch (e) {
      setLoadMsg({ tone: "danger", text: String(e) });
    }
  };

  // Restore exactly what the URL describes on first mount.
  useEffect(() => {
    explore(seed);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const onQueryKeyDown = (e: React.KeyboardEvent) => {
    if ((e.metaKey || e.ctrlKey) && e.key === "Enter") runCypher();
  };

  return (
    <div>
      <h2 className="ws-title ws-view__title">Node Explorer</h2>
      <p className="ws-view__intro">
        A 3D, WebGL view of the property graph on this node. Click a node to centre the camera and see
        its properties; drag to orbit, scroll to zoom. Run a Cypher query to project its results into
        the scene. Press <Kbd>⌘</Kbd> <Kbd>↵</Kbd> in the query box to run.
      </p>

      <Toolbar style={{ marginBottom: "var(--ws-space-sm)" }}>
        <FieldLabel>
          graph
          <Input value={graphId} onChange={(e) => setGraphId(e.target.value)} style={{ width: 64 }} mono />
        </FieldLabel>
        <FieldLabel>
          seed
          <Input value={seed} onChange={(e) => setSeed(e.target.value)} placeholder="(whole graph)" style={{ width: 130 }} mono />
        </FieldLabel>
        <FieldLabel>
          depth
          <Input type="number" value={depth} min={0} max={6} onChange={(e) => setDepth(Number(e.target.value))} style={{ width: 54 }} mono />
        </FieldLabel>
        <Button variant="primary" onClick={() => explore(seed)}>Explore</Button>
        <Button onClick={loadSample}>Load sample dataset</Button>
        <Badge tone="neutral">{nodes.length} nodes</Badge>
        <Badge tone="neutral">{edges.length} edges</Badge>
        {truncated && <Badge tone="warning">truncated</Badge>}
      </Toolbar>

      <div style={{ display: "flex", gap: "var(--ws-space-sm)", alignItems: "flex-start", marginBottom: "var(--ws-space-sm)", flexWrap: "wrap" }}>
        <Textarea
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          onKeyDown={onQueryKeyDown}
          placeholder="Cypher → graph, e.g.  MATCH (p:Person)-[:ACTED_IN]->(m:Movie) WHERE m.title = 'The Matrix' RETURN p, m"
          mono
          style={{ flex: "1 1 420px", height: 58, boxSizing: "border-box" }}
        />
        <div style={{ display: "flex", flexDirection: "column", gap: "var(--ws-space-xs)" }}>
          <Button variant="primary" onClick={runCypher} disabled={running}>
            {running ? <Spinner /> : null}
            {running ? "Running…" : "Run Cypher (⌘↵)"}
          </Button>
          <Checkbox
            label="+1 hop of neighbours"
            checked={expandNeighbors}
            onChange={(e) => setExpandNeighbors(e.target.checked)}
          />
        </div>
      </div>

      {loadMsg && (
        <div style={{ marginBottom: "var(--ws-space-sm)" }}>
          <InlineMessage tone={loadMsg.tone}>{loadMsg.text}</InlineMessage>
        </div>
      )}
      {error && (
        <div style={{ marginBottom: "var(--ws-space-sm)" }}>
          <InlineMessage tone="danger"><span className="ws-mono">{error}</span></InlineMessage>
        </div>
      )}
      {meta && (
        <div style={{ display: "flex", gap: "var(--ws-space-sm)", alignItems: "center", marginBottom: "var(--ws-space-sm)", flexWrap: "wrap" }}>
          <Badge tone="info">consistency: eventual</Badge>
          <Badge tone={meta.completeness === Completeness.COMPLETE ? "success" : "warning"}>
            completeness: {Completeness[meta.completeness]}
          </Badge>
          {meta.partialGraphPossible && <Badge tone="warning">partial graph possible</Badge>}
          {meta.warnings.length > 0 && <span className="ws-caption">warnings: {meta.warnings.join("; ")}</span>}
        </div>
      )}

      <div style={{ display: "flex", gap: "var(--ws-space-md)", alignItems: "flex-start", flexWrap: "wrap" }}>
        <div style={{ position: "relative", flex: "1 1 560px", minWidth: 320 }}>
          <div
            ref={mountRef}
            style={{
              height: CANVAS_H,
              border: "var(--ws-stroke-regular) solid var(--ws-color-border-strong)",
              borderRadius: "var(--ws-radius-md)",
              overflow: "hidden",
              background: "var(--ws-color-paper)",
            }}
          />
          {nodes.length === 0 && (
            <div
              style={{
                position: "absolute",
                inset: 0,
                display: "flex",
                flexDirection: "column",
                gap: "var(--ws-space-sm)",
                alignItems: "center",
                justifyContent: "center",
                pointerEvents: "none",
                textAlign: "center",
                padding: "var(--ws-space-lg)",
              }}
            >
              <div className="ws-title-sm">No graph data yet</div>
              <div className="ws-caption">
                Load the sample dataset to explore a small movie graph, or run a Cypher query.
              </div>
            </div>
          )}
        </div>

        <Card flat style={{ width: 250, alignSelf: "flex-start" }}>
          {selected ? (
            <div>
              <div className="ws-title-sm">{selected.display}</div>
              <div className="ws-caption ws-mono" style={{ marginTop: 2 }}>{selected.id}</div>
              <div style={{ margin: "var(--ws-space-sm) 0 var(--ws-space-md)", display: "flex", gap: "var(--ws-space-xs)", flexWrap: "wrap" }}>
                {selected.labels.length > 0 ? (
                  selected.labels.map((l) => (
                    <span
                      key={l}
                      className="ws-badge"
                      style={{ ["--_dot" as string]: labelHex([l]), borderColor: labelHex([l]), color: labelHex([l]) }}
                    >
                      {l}
                    </span>
                  ))
                ) : (
                  <span className="ws-caption">(no labels)</span>
                )}
              </div>
              {Object.keys(selected.properties).length > 0 ? (
                <Table mono>
                  <tbody>
                    {Object.entries(selected.properties).map(([k, v]) => (
                      <tr key={k}>
                        <td className="ws-muted">{k}</td>
                        <td>{valToStr(v)}</td>
                      </tr>
                    ))}
                  </tbody>
                </Table>
              ) : (
                <div className="ws-caption">properties require admin</div>
              )}
              <div style={{ marginTop: "var(--ws-space-md)" }}>
                <Button size="sm" onClick={() => { setSeed(selected.id); explore(selected.id); }}>
                  expand from here
                </Button>
              </div>
            </div>
          ) : (
            <div className="ws-caption">click a node to centre on it and see its properties</div>
          )}
        </Card>
      </div>
    </div>
  );
}
