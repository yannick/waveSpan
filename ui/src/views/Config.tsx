import { useEffect, useMemo, useState } from "react";
import { obs } from "../transport";
import { type TunableState } from "../gen/wavespan/v1/observability_pb";
import { type MemberState, MemberLiveness } from "../gen/wavespan/v1/admin_pb";
import {
  Badge,
  Button,
  EmptyState,
  InlineMessage,
  Input,
  Select,
  Spinner,
  Table,
  Toolbar,
  type Tone,
} from "../components";

const SOURCE_TONE: Record<string, Tone> = {
  default: "neutral",
  file: "info",
  env: "olive",
  runtime: "accent",
};

export function Config() {
  const [members, setMembers] = useState<MemberState[]>([]);
  const [target, setTarget] = useState(""); // "" = this node
  const [tunables, setTunables] = useState<TunableState[]>([]);
  const [memberId, setMemberId] = useState("");
  const [filter, setFilter] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [drafts, setDrafts] = useState<Record<string, string>>({});
  const [setting, setSetting] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);

  // Discover members so the operator can inspect any node's effective config.
  useEffect(() => {
    let live = true;
    obs.getClusterView({}).then((v) => live && setMembers(v.members)).catch(() => {});
    return () => {
      live = false;
    };
  }, []);

  const load = async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await obs.getNodeConfig({ targetMemberId: target });
      setTunables(res.tunables);
      setMemberId(res.memberId);
      setDrafts({});
    } catch (e) {
      setError(String(e));
      setTunables([]);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    void load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [target]);

  const set = async (t: TunableState, clusterWide: boolean) => {
    const value = drafts[t.key] ?? t.value;
    setSetting(t.key);
    setNotice(null);
    setError(null);
    try {
      // Node-local set targets the node currently being viewed; cluster set gossips everywhere.
      const res = await obs.adminSetTunable({
        key: t.key,
        value,
        targetMemberId: clusterWide ? "" : target,
        clusterWide,
      });
      if (!res.ok) {
        setError(res.error || "set failed");
      } else {
        const where = clusterWide ? "whole cluster (gossiping)" : `node ${memberId || "this node"} only`;
        setNotice(
          res.requiresRestart
            ? `${t.key} staged on ${where} — static, applies on restart`
            : `${t.key} applied live on ${where}`,
        );
        await load();
      }
    } catch (e) {
      setError(String(e));
    } finally {
      setSetting(null);
    }
  };

  const groups = useMemo(() => {
    const q = filter.trim().toLowerCase();
    const match = (t: TunableState) =>
      !q || t.key.toLowerCase().includes(q) || t.doc.toLowerCase().includes(q);
    const byGroup = new Map<string, TunableState[]>();
    for (const t of tunables) {
      if (!match(t)) continue;
      if (!byGroup.has(t.group)) byGroup.set(t.group, []);
      byGroup.get(t.group)!.push(t);
    }
    return [...byGroup.entries()];
  }, [tunables, filter]);

  const overrideCount = tunables.filter((t) => t.source === "runtime").length;

  return (
    <div>
      <h2 className="ws-title ws-view__title">Configuration</h2>
      <p className="ws-view__intro">
        Effective tunables on each node — value, where it came from (default / file / env / runtime),
        and what it does. <strong>Set node</strong> pins a value on the selected node only (no gossip);
        <strong>Set cluster</strong> gossips it to every node (LWW). Hot tunables apply live; static
        ones are staged for restart. Both are persisted.
      </p>

      <Toolbar style={{ marginBottom: "var(--ws-space-md)" }}>
        <Select value={target} onChange={(e) => setTarget(e.target.value)}>
          <option value="">This node{memberId && !target ? ` (${memberId})` : ""}</option>
          {members.map((m, i) => {
            const id = m.member?.memberId ?? "?";
            const alive = m.state === MemberLiveness.MEMBER_ALIVE;
            return (
              <option key={i} value={id} disabled={!alive}>
                {id}
                {alive ? "" : " (not alive)"}
              </option>
            );
          })}
        </Select>
        <Input
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
          placeholder="filter by key or doc"
          style={{ width: 240 }}
          mono
        />
        <Button onClick={load} disabled={loading}>
          {loading ? <Spinner /> : null}
          {loading ? "Loading…" : "Refresh"}
        </Button>
        {overrideCount > 0 && <Badge tone="accent">{overrideCount} runtime override{overrideCount > 1 ? "s" : ""}</Badge>}
      </Toolbar>

      {notice && (
        <div style={{ marginBottom: "var(--ws-space-md)" }}>
          <InlineMessage tone="success">{notice}</InlineMessage>
        </div>
      )}
      {error && (
        <div style={{ marginBottom: "var(--ws-space-md)" }}>
          <InlineMessage tone="danger"><span className="ws-mono">{error}</span></InlineMessage>
        </div>
      )}

      {groups.length === 0 ? (
        <EmptyState title={loading ? "Loading config…" : "No tunables"} icon="⚙" />
      ) : (
        groups.map(([group, items]) => (
          <div key={group} style={{ marginBottom: "var(--ws-space-xl)" }}>
            <h3 className="ws-title-sm" style={{ marginBottom: "var(--ws-space-sm)" }}>{group}</h3>
            <Table>
              <thead>
                <tr>
                  <th>tunable</th>
                  <th>value</th>
                  <th>source</th>
                  <th>type</th>
                  <th aria-label="set" />
                </tr>
              </thead>
              <tbody>
                {items.map((t) => {
                  const draft = drafts[t.key] ?? t.value;
                  const dirty = draft !== t.value;
                  return (
                    <tr key={t.key}>
                      <td>
                        <div className="ws-mono" title={t.envVar}>{t.key.split(".").pop()}</div>
                        <div className="ws-caption" style={{ maxWidth: 360 }}>{t.doc}</div>
                      </td>
                      <td style={{ minWidth: 150 }}>
                        <Input
                          value={draft}
                          mono
                          onChange={(e) => setDrafts((d) => ({ ...d, [t.key]: e.target.value }))}
                          style={{ width: 130 }}
                        />
                        {t.value !== t.defaultValue && (
                          <div className="ws-caption">default: {t.defaultValue}</div>
                        )}
                      </td>
                      <td>
                        <div style={{ display: "flex", flexDirection: "column", gap: "var(--ws-space-xxs)", alignItems: "flex-start" }}>
                          <Badge tone={SOURCE_TONE[t.source] ?? "neutral"} dot>{t.source}</Badge>
                          {t.overrideScope && (
                            <Badge tone={t.overrideScope === "node" ? "warning" : "info"}>{t.overrideScope}</Badge>
                          )}
                        </div>
                      </td>
                      <td>
                        <Badge tone={t.category === "hot" ? "success" : "neutral"}>{t.category}</Badge>
                        <div className="ws-caption">{t.kind}</div>
                      </td>
                      <td style={{ textAlign: "right", whiteSpace: "nowrap" }}>
                        <div style={{ display: "inline-flex", gap: "var(--ws-space-xs)" }}>
                          <Button
                            size="sm"
                            variant={dirty ? "secondary" : "ghost"}
                            disabled={!dirty || setting === t.key}
                            onClick={() => set(t, false)}
                            title={`Pin on ${memberId || "this node"} only (node-local, no gossip)`}
                          >
                            Set node
                          </Button>
                          <Button
                            size="sm"
                            variant={dirty ? "primary" : "ghost"}
                            disabled={!dirty || setting === t.key}
                            onClick={() => set(t, true)}
                            title="Set on the whole cluster (gossips to every node)"
                          >
                            Set cluster
                          </Button>
                        </div>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </Table>
          </div>
        ))
      )}
    </div>
  );
}
