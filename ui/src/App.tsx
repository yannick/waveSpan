import { useState } from "react";
import { GossipInspector } from "./views/GossipInspector";
import { DataBrowser } from "./views/DataBrowser";
import { ClusterTopology } from "./views/ClusterTopology";
import { MetricsSummary } from "./views/MetricsSummary";

type Tab = "gossip" | "data" | "topology" | "metrics";

const tabs: { id: Tab; label: string }[] = [
  { id: "gossip", label: "Gossip Inspector" },
  { id: "data", label: "Data Browser" },
  { id: "topology", label: "Cluster Topology" },
  { id: "metrics", label: "Metrics" },
];

export function App() {
  const [tab, setTab] = useState<Tab>("gossip");
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
      {tab === "gossip" && <GossipInspector />}
      {tab === "data" && <DataBrowser />}
      {tab === "topology" && <ClusterTopology />}
      {tab === "metrics" && <MetricsSummary />}
    </div>
  );
}
