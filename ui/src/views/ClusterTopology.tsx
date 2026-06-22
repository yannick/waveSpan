import { useEffect, useState } from "react";
import { obs } from "../transport";
import { MemberLiveness } from "../gen/wavespan/v1/admin_pb";
import { type GetClusterViewResponse } from "../gen/wavespan/v1/observability_pb";

const LIVENESS: Record<number, { label: string; color: string }> = {
  [MemberLiveness.MEMBER_ALIVE]: { label: "alive", color: "#2e7d32" },
  [MemberLiveness.MEMBER_SUSPECT]: { label: "suspect", color: "#f9a825" },
  [MemberLiveness.MEMBER_UNREACHABLE]: { label: "unreachable", color: "#e53935" },
  [MemberLiveness.MEMBER_DEAD]: { label: "dead", color: "#757575" },
  [MemberLiveness.MEMBER_FORGOTTEN]: { label: "forgotten", color: "#bdbdbd" },
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

  if (!view) return <div>loading cluster view…</div>;
  return (
    <div>
      <div style={{ marginBottom: 8 }}>
        under-replicated estimate: <b>{String(view.underReplicatedEstimate)}</b>
      </div>
      <h3 style={{ fontSize: 14 }}>Members</h3>
      <div style={{ display: "flex", flexWrap: "wrap", gap: 8, marginBottom: 16 }}>
        {view.members.map((m, i) => {
          const l = LIVENESS[m.state] ?? { label: "?", color: "#999" };
          return (
            <div key={i} style={{ border: `2px solid ${l.color}`, borderRadius: 6, padding: 8, minWidth: 120 }}>
              <div style={{ fontWeight: 600 }}>{m.member?.memberId}</div>
              <div style={{ fontSize: 12, color: l.color }}>{l.label}</div>
              <div style={{ fontSize: 11, color: "#888" }}>{m.member?.zone}</div>
            </div>
          );
        })}
      </div>
      <h3 style={{ fontSize: 14 }}>Latency edges</h3>
      <table style={{ fontSize: 12, borderCollapse: "collapse" }}>
        <thead>
          <tr style={{ textAlign: "left", borderBottom: "1px solid #ccc" }}>
            <th>from → to</th><th>ewma (ms)</th><th>p95 (ms)</th><th>loss</th>
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
      </table>
    </div>
  );
}
