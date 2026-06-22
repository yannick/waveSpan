import { useState } from "react";
import { GossipInspector } from "./views/GossipInspector";
import { DataBrowser } from "./views/DataBrowser";
import { ClusterTopology } from "./views/ClusterTopology";
import { MetricsSummary } from "./views/MetricsSummary";
import { CypherConsole } from "./views/CypherConsole";
import { NodeExplorer } from "./views/NodeExplorer";
import { KvWriter } from "./views/KvWriter";

type Tab = "cypher" | "explorer" | "gossip" | "data" | "write" | "topology" | "metrics";

const tabs: { id: Tab; label: string }[] = [
  { id: "cypher", label: "Cypher Console" },
  { id: "explorer", label: "Node Explorer" },
  { id: "gossip", label: "Gossip Inspector" },
  { id: "data", label: "Data Browser" },
  { id: "write", label: "Write KV" },
  { id: "topology", label: "Cluster Topology" },
  { id: "metrics", label: "Metrics" },
];

export function App() {
  const [tab, setTab] = useState<Tab>("cypher");
  return (
    <div style={{ fontFamily: "system-ui, sans-serif", padding: 16 }}>
      <h1 style={{ fontSize: 20 }}>WaveSpan node</h1>
      <nav style={{ display: "flex", gap: 8, marginBottom: 16 }}>
        {tabs.map((t) => (
          <button
            key={t.id}
            onClick={() => setTab(t.id)}
            style={{
              padding: "6px 12px",
              border: "1px solid #ccc",
              background: tab === t.id ? "#222" : "#fff",
              color: tab === t.id ? "#fff" : "#222",
              cursor: "pointer",
            }}
          >
            {t.label}
          </button>
        ))}
      </nav>
      {tab === "cypher" && <CypherConsole />}
      {tab === "explorer" && <NodeExplorer />}
      {tab === "gossip" && <GossipInspector />}
      {tab === "data" && <DataBrowser />}
      {tab === "write" && <KvWriter />}
      {tab === "topology" && <ClusterTopology />}
      {tab === "metrics" && <MetricsSummary />}
    </div>
  );
}
