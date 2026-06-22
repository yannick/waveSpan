import { GossipInspector } from "./views/GossipInspector";
import { DataBrowser } from "./views/DataBrowser";
import { ClusterTopology } from "./views/ClusterTopology";
import { MetricsSummary } from "./views/MetricsSummary";
import { CypherConsole } from "./views/CypherConsole";
import { NodeExplorer } from "./views/NodeExplorer";
import { KvWriter } from "./views/KvWriter";
import { Config } from "./views/Config";
import { Documentation } from "./views/Documentation";
import { Tabs, type TabItem, ThemeToggle } from "./components";
import { DEFAULT_SCREEN, navigate, useEnsureScreen, useRoute } from "./router";

type Tab =
  | "cypher"
  | "explorer"
  | "gossip"
  | "data"
  | "write"
  | "topology"
  | "metrics"
  | "config"
  | "docs";

const tabs: TabItem<Tab>[] = [
  { id: "cypher", label: "Cypher Console" },
  { id: "explorer", label: "Node Explorer" },
  { id: "gossip", label: "Gossip Inspector" },
  { id: "data", label: "Data Browser" },
  { id: "write", label: "KV Writer" },
  { id: "topology", label: "Cluster Topology" },
  { id: "metrics", label: "Metrics" },
  { id: "config", label: "Configuration" },
  { id: "docs", label: "Documentation" },
];

const screens = new Set<string>(tabs.map((t) => t.id));

export function App() {
  useEnsureScreen();
  const { screen } = useRoute();
  // The active screen comes from the URL hash (#/<screen>?…) so a reload or pasted URL restores it.
  const tab = (screens.has(screen) ? screen : DEFAULT_SCREEN) as Tab;
  return (
    <div className="ws-app">
      <header className="ws-appbar">
        <div className="ws-wordmark">
          <h1 className="ws-headline">
            wave<span className="ws-wordmark__glyph">·</span>span
          </h1>
          <span className="ws-wordmark__sub">node console</span>
        </div>
        <ThemeToggle />
      </header>

      <Tabs items={tabs} value={tab} onChange={(t) => navigate(t)} />

      <main className="ws-view">
        {tab === "cypher" && <CypherConsole />}
        {tab === "explorer" && <NodeExplorer />}
        {tab === "gossip" && <GossipInspector />}
        {tab === "data" && <DataBrowser />}
        {tab === "write" && <KvWriter />}
        {tab === "topology" && <ClusterTopology />}
        {tab === "metrics" && <MetricsSummary />}
        {tab === "config" && <Config />}
        {tab === "docs" && <Documentation />}
      </main>
    </div>
  );
}
