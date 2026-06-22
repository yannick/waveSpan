import { useEffect, useState } from "react";
import { obs } from "../transport";
import { MemberLiveness } from "../gen/wavespan/v1/admin_pb";
import { type GetClusterViewResponse } from "../gen/wavespan/v1/observability_pb";
import { Badge, Card, EmptyState, Table, type Tone } from "../components";
import { color } from "../theme/tokens";

const LIVENESS: Record<number, { label: string; tone: Tone; accent: string }> = {
  [MemberLiveness.MEMBER_ALIVE]: { label: "alive", tone: "success", accent: color.teal },
  [MemberLiveness.MEMBER_SUSPECT]: { label: "suspect", tone: "warning", accent: color.mustard },
  [MemberLiveness.MEMBER_UNREACHABLE]: { label: "unreachable", tone: "danger", accent: color.red },
  [MemberLiveness.MEMBER_DEAD]: { label: "dead", tone: "neutral", accent: color.inkMuted },
  [MemberLiveness.MEMBER_FORGOTTEN]: { label: "forgotten", tone: "neutral", accent: color.inkMuted },
};

export function ClusterTopology() {
  const [view, setView] = useState<GetClusterViewResponse | null>(null);

  useEffect(() => {
    let live = true;
    const tick = async () => {
      try {
        const v = await obs.getClusterView({});
        if (live) setView(v);
      } catch {
        /* admin endpoint unreachable */
      }
    };
    tick();
    const id = setInterval(tick, 2000);
    return () => {
      live = false;
      clearInterval(id);
    };
  }, []);

  if (!view) {
    return (
      <div>
        <h2 className="ws-title ws-view__title">Cluster Topology</h2>
        <EmptyState title="Loading cluster view…" icon="◴" />
      </div>
    );
  }

  return (
    <div>
      <h2 className="ws-title ws-view__title">Cluster Topology</h2>
      <p className="ws-view__intro">
        Membership and the measured latency graph that drives replica placement. Member liveness
        flows ALIVE → SUSPECT → UNREACHABLE → DEAD as gossip detects failures.
      </p>

      <div style={{ display: "flex", alignItems: "center", gap: "var(--ws-space-sm)", marginBottom: "var(--ws-space-lg)" }}>
        <span className="ws-label ws-muted">under-replicated estimate</span>
        <Badge tone={Number(view.underReplicatedEstimate) > 0 ? "warning" : "olive"}>
          {String(view.underReplicatedEstimate)}
        </Badge>
      </div>

      <h3 className="ws-title-sm" style={{ marginBottom: "var(--ws-space-sm)" }}>Members</h3>
      <div style={{ display: "flex", flexWrap: "wrap", gap: "var(--ws-space-md)", marginBottom: "var(--ws-space-xl)" }}>
        {view.members.map((m, i) => {
          const l = LIVENESS[m.state] ?? { label: "?", tone: "neutral" as Tone, accent: color.inkMuted };
          return (
            <Card key={i} accent={l.accent} style={{ minWidth: 150, padding: "var(--ws-space-md)" }}>
              <div className="ws-title-sm" style={{ marginBottom: "var(--ws-space-xs)" }}>{m.member?.memberId}</div>
              <Badge tone={l.tone} dot>{l.label}</Badge>
              {m.member?.zone && <div className="ws-caption" style={{ marginTop: "var(--ws-space-xs)" }}>{m.member.zone}</div>}
            </Card>
          );
        })}
      </div>

      <h3 className="ws-title-sm" style={{ marginBottom: "var(--ws-space-sm)" }}>Latency edges</h3>
      {view.edges.length === 0 ? (
        <EmptyState title="No edges yet" icon="↔">
          Latency probes populate as gossip exchanges RTT samples between members.
        </EmptyState>
      ) : (
        <Table mono>
          <thead>
            <tr>
              <th>from → to</th>
              <th>ewma (ms)</th>
              <th>p95 (ms)</th>
              <th>loss</th>
            </tr>
          </thead>
          <tbody>
            {view.edges.map((e, i) => (
              <tr key={i}>
                <td>{e.fromMemberId} → {e.toMemberId}</td>
                <td>{e.ewmaRttMs.toFixed(2)}</td>
                <td>{e.p95RttMs.toFixed(2)}</td>
                <td>{(e.packetLoss * 100).toFixed(1)}%</td>
              </tr>
            ))}
          </tbody>
        </Table>
      )}
    </div>
  );
}
