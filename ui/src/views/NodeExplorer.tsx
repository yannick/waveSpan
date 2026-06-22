import { useEffect, useMemo, useState } from "react";
import { obs } from "../transport";
import { type GraphEdge, type GraphNode } from "../gen/wavespan/v1/observability_pb";

const W = 820;
const H = 560;
const R = 18;

interface Pt {
  x: number;
  y: number;
}

// labelColor maps a node's first label to a stable hue.
function labelColor(labels: string[]): string {
  const s = labels[0] ?? "";
  let h = 0;
  for (let i = 0; i < s.length; i++) h = (h * 31 + s.charCodeAt(i)) % 360;
  return `hsl(${h}, 60%, 55%)`;
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
      <div style={{ display: "flex", gap: 8, marginBottom: 8, alignItems: "center" }}>
        <label style={{ fontSize: 12 }}>
          graph <input value={graphId} onChange={(e) => setGraphId(e.target.value)} style={{ width: 70 }} />
        </label>
        <label style={{ fontSize: 12 }}>
          seed <input value={seed} onChange={(e) => setSeed(e.target.value)} placeholder="(whole graph)" style={{ width: 120 }} />
        </label>
        <label style={{ fontSize: 12 }}>
          depth <input type="number" value={depth} min={0} max={6} onChange={(e) => setDepth(Number(e.target.value))} style={{ width: 50 }} />
        </label>
        <button onClick={() => explore(seed)}>Explore</button>
        <span style={{ fontSize: 12, color: "#888" }}>
          {nodes.length} nodes · {edges.length} edges {truncated && "· truncated"}
        </span>
      </div>
      {error && <div style={{ color: "#c62828", fontSize: 12, marginBottom: 8 }}>{error}</div>}
      <div style={{ display: "flex", gap: 12 }}>
        <svg width={W} height={H} style={{ border: "1px solid #ddd", borderRadius: 6, background: "#fafafa" }}>
          <defs>
            <marker id="arrow" markerWidth="8" markerHeight="8" refX="20" refY="3" orient="auto">
              <path d="M0,0 L6,3 L0,6 Z" fill="#999" />
            </marker>
          </defs>
          {edges.map((e, i) => {
            const a = layout.get(e.source);
            const b = layout.get(e.target);
            if (!a || !b) return null;
            return (
              <g key={i}>
                <line x1={a.x} y1={a.y} x2={b.x} y2={b.y} stroke="#bbb" strokeWidth={1.5} markerEnd="url(#arrow)" />
                <text x={(a.x + b.x) / 2} y={(a.y + b.y) / 2 - 3} fontSize={9} fill="#999" textAnchor="middle">
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
                <circle r={R} fill={labelColor(n.labels)} stroke={sel ? "#111" : "#fff"} strokeWidth={sel ? 3 : 1.5} />
                <text y={3} fontSize={9} fill="#fff" textAnchor="middle">
                  {n.nodeId.length > 6 ? n.nodeId.slice(0, 6) : n.nodeId}
                </text>
              </g>
            );
          })}
        </svg>
        <div style={{ width: 220, fontSize: 12 }}>
          {selected ? (
            <div>
              <div style={{ fontWeight: 700 }}>{selected.nodeId}</div>
              <div style={{ color: "#888", marginBottom: 8 }}>{selected.labels.join(", ") || "(no labels)"}</div>
              <table style={{ width: "100%", borderCollapse: "collapse" }}>
                <tbody>
                  {Object.entries(selected.properties).map(([k, v]) => (
                    <tr key={k}>
                      <td style={{ color: "#666", paddingRight: 8 }}>{k}</td>
                      <td>{propStr(v)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
              {Object.keys(selected.properties).length === 0 && <div style={{ color: "#aaa" }}>properties require admin</div>}
              <button style={{ marginTop: 8 }} onClick={() => { setSeed(selected.nodeId); explore(selected.nodeId); }}>
                expand from here
              </button>
            </div>
          ) : (
            <div style={{ color: "#888" }}>click a node for details · double-click to expand from it</div>
          )}
        </div>
      </div>
    </div>
  );
}

function propStr(v: unknown): string {
  const val = v as { value?: { case?: string; value?: unknown } };
  if (!val?.value) return "";
  return String(val.value.value ?? "");
}
