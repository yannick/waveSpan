import { useEffect, useMemo, useState } from "react";
import { obs } from "../transport";
import { type GraphEdge, type GraphNode } from "../gen/wavespan/v1/observability_pb";
import { Button, Card, FieldLabel, InlineMessage, Input, Table, Toolbar, Badge } from "../components";
import { categoricalColor } from "../theme/tokens";

const W = 820;
const H = 560;
const R = 18;

interface Pt {
  x: number;
  y: number;
}

// labelColor maps a node's first label to a stable Linea accent hue.
function labelColor(labels: string[]): string {
  return categoricalColor(labels[0] ?? "");
}

// forceLayout runs a small deterministic force simulation (circle seed -> repulsion + edge springs).
function forceLayout(nodes: GraphNode[], edges: GraphEdge[]): Map<string, Pt> {
  const idx = new Map(nodes.map((n, i) => [n.nodeId, i]));
  const pos: Pt[] = nodes.map((_, i) => {
    const a = (2 * Math.PI * i) / Math.max(nodes.length, 1);
    return { x: W / 2 + Math.cos(a) * 200, y: H / 2 + Math.sin(a) * 180 };
  });
  const links = edges
    .map((e) => [idx.get(e.source), idx.get(e.target)] as [number | undefined, number | undefined])
    .filter(([a, b]) => a !== undefined && b !== undefined) as [number, number][];

  for (let iter = 0; iter < 300; iter++) {
    // repulsion
    for (let i = 0; i < pos.length; i++) {
      for (let j = i + 1; j < pos.length; j++) {
        let dx = pos[i].x - pos[j].x;
        let dy = pos[i].y - pos[j].y;
        let d2 = dx * dx + dy * dy || 0.01;
        const f = 4000 / d2;
        dx *= f;
        dy *= f;
        pos[i].x += dx;
        pos[i].y += dy;
        pos[j].x -= dx;
        pos[j].y -= dy;
      }
    }
    // edge springs
    for (const [a, b] of links) {
      const dx = pos[b].x - pos[a].x;
      const dy = pos[b].y - pos[a].y;
      const d = Math.sqrt(dx * dx + dy * dy) || 0.01;
      const f = (d - 120) * 0.02;
      pos[a].x += (dx / d) * f;
      pos[a].y += (dy / d) * f;
      pos[b].x -= (dx / d) * f;
      pos[b].y -= (dy / d) * f;
    }
    // gentle centering + bounds
    for (const p of pos) {
      p.x += (W / 2 - p.x) * 0.01;
      p.y += (H / 2 - p.y) * 0.01;
      p.x = Math.max(R, Math.min(W - R, p.x));
      p.y = Math.max(R, Math.min(H - R, p.y));
    }
  }
  return new Map(nodes.map((n, i) => [n.nodeId, pos[i]]));
}

export function NodeExplorer() {
  const [graphId, setGraphId] = useState("g");
  const [seed, setSeed] = useState("");
  const [depth, setDepth] = useState(2);
  const [nodes, setNodes] = useState<GraphNode[]>([]);
  const [edges, setEdges] = useState<GraphEdge[]>([]);
  const [selected, setSelected] = useState<GraphNode | null>(null);
  const [truncated, setTruncated] = useState(false);
  const [error, setError] = useState("");

  const explore = async (seedNodeId: string) => {
    setError("");
    try {
      const resp = await obs.graphExplore({ graphId, seedNodeId, depth, limit: 200, includeValue: true });
      setNodes(resp.nodes);
      setEdges(resp.edges);
      setTruncated(resp.truncated);
      setSelected(null);
    } catch (e) {
      setError(String(e));
    }
  };

  useEffect(() => {
    explore("");
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const layout = useMemo(() => forceLayout(nodes, edges), [nodes, edges]);

  return (
    <div>
      <h2 className="ws-title ws-view__title">Node Explorer</h2>
      <p className="ws-view__intro">
        Force-directed view of the property graph stored on this node. Click a node for its
        properties; double-click to re-root the traversal from it.
      </p>

      <Toolbar style={{ marginBottom: "var(--ws-space-md)" }}>
        <FieldLabel>
          graph
          <Input value={graphId} onChange={(e) => setGraphId(e.target.value)} style={{ width: 70 }} mono />
        </FieldLabel>
        <FieldLabel>
          seed
          <Input value={seed} onChange={(e) => setSeed(e.target.value)} placeholder="(whole graph)" style={{ width: 130 }} mono />
        </FieldLabel>
        <FieldLabel>
          depth
          <Input type="number" value={depth} min={0} max={6} onChange={(e) => setDepth(Number(e.target.value))} style={{ width: 56 }} mono />
        </FieldLabel>
        <Button variant="primary" onClick={() => explore(seed)}>Explore</Button>
        <Badge tone="neutral">{nodes.length} nodes</Badge>
        <Badge tone="neutral">{edges.length} edges</Badge>
        {truncated && <Badge tone="warning">truncated</Badge>}
      </Toolbar>

      {error && (
        <div style={{ marginBottom: "var(--ws-space-md)" }}>
          <InlineMessage tone="danger"><span className="ws-mono">{error}</span></InlineMessage>
        </div>
      )}

      <div style={{ display: "flex", gap: "var(--ws-space-md)", flexWrap: "wrap" }}>
        <svg
          width={W}
          height={H}
          style={{
            border: "var(--ws-stroke-regular) solid var(--ws-color-border-strong)",
            borderRadius: "var(--ws-radius-md)",
            background: "var(--ws-color-surface)",
            maxWidth: "100%",
          }}
        >
          <defs>
            <marker id="arrow" markerWidth="8" markerHeight="8" refX="20" refY="3" orient="auto">
              <path d="M0,0 L6,3 L0,6 Z" fill="var(--ws-color-ink-muted)" />
            </marker>
          </defs>
          {edges.map((e, i) => {
            const a = layout.get(e.source);
            const b = layout.get(e.target);
            if (!a || !b) return null;
            return (
              <g key={i}>
                <line x1={a.x} y1={a.y} x2={b.x} y2={b.y} stroke="var(--ws-color-border-strong)" strokeOpacity={0.4} strokeWidth={1.5} markerEnd="url(#arrow)" />
                <text x={(a.x + b.x) / 2} y={(a.y + b.y) / 2 - 3} fontSize={9} fill="var(--ws-color-ink-muted)" textAnchor="middle">
                  {e.type}
                </text>
              </g>
            );
          })}
          {nodes.map((n) => {
            const p = layout.get(n.nodeId);
            if (!p) return null;
            const sel = selected?.nodeId === n.nodeId;
            return (
              <g
                key={n.nodeId}
                transform={`translate(${p.x}, ${p.y})`}
                style={{ cursor: "pointer" }}
                onClick={() => setSelected(n)}
                onDoubleClick={() => {
                  setSeed(n.nodeId);
                  explore(n.nodeId);
                }}
              >
                <circle
                  r={R}
                  fill={labelColor(n.labels)}
                  stroke={sel ? "var(--ws-color-ink)" : "var(--ws-color-paper)"}
                  strokeWidth={sel ? 3 : 1.6}
                />
                <text y={3} fontSize={9} fill="var(--ws-color-on-accent)" textAnchor="middle" fontWeight={700}>
                  {n.nodeId.length > 6 ? n.nodeId.slice(0, 6) : n.nodeId}
                </text>
              </g>
            );
          })}
        </svg>

        <Card flat style={{ width: 240, alignSelf: "flex-start" }}>
          {selected ? (
            <div>
              <div className="ws-title-sm ws-mono">{selected.nodeId}</div>
              <div style={{ margin: "var(--ws-space-xs) 0 var(--ws-space-md)", display: "flex", gap: "var(--ws-space-xs)", flexWrap: "wrap" }}>
                {selected.labels.length > 0 ? (
                  selected.labels.map((l) => <Badge key={l} tone={categoricalColor(l)}>{l}</Badge>)
                ) : (
                  <span className="ws-caption">(no labels)</span>
                )}
              </div>
              <Table mono>
                <tbody>
                  {Object.entries(selected.properties).map(([k, v]) => (
                    <tr key={k}>
                      <td className="ws-muted">{k}</td>
                      <td>{propStr(v)}</td>
                    </tr>
                  ))}
                </tbody>
              </Table>
              {Object.keys(selected.properties).length === 0 && (
                <div className="ws-caption" style={{ marginTop: "var(--ws-space-sm)" }}>properties require admin</div>
              )}
              <div style={{ marginTop: "var(--ws-space-md)" }}>
                <Button size="sm" onClick={() => { setSeed(selected.nodeId); explore(selected.nodeId); }}>
                  expand from here
                </Button>
              </div>
            </div>
          ) : (
            <div className="ws-caption">click a node for details · double-click to expand from it</div>
          )}
        </Card>
      </div>
    </div>
  );
}

function propStr(v: unknown): string {
  const val = v as { value?: { case?: string; value?: unknown } };
  if (!val?.value) return "";
  return String(val.value.value ?? "");
}
