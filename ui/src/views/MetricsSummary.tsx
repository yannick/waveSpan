import { useEffect, useState } from "react";
import { obs } from "../transport";

export function MetricsSummary() {
  const [underReplicated, setUnderReplicated] = useState<bigint | null>(null);
  const [members, setMembers] = useState<number | null>(null);

  useEffect(() => {
    let live = true;
    (async () => {
      try {
        const v = await obs.getClusterView({});
        if (live) {
          setUnderReplicated(v.underReplicatedEstimate);
          setMembers(v.members.length);
        }
      } catch {
        /* unreachable */
      }
    })();
    return () => {
      live = false;
    };
  }, []);

  const card = (label: string, value: string) => (
    <div style={{ border: "1px solid #ccc", borderRadius: 6, padding: 16, minWidth: 160 }}>
      <div style={{ fontSize: 12, color: "#888" }}>{label}</div>
      <div style={{ fontSize: 28, fontWeight: 700 }}>{value}</div>
    </div>
  );

  return (
    <div>
      <div style={{ display: "flex", gap: 12, marginBottom: 16 }}>
        {card("Members", members === null ? "…" : String(members))}
        {card("Under-replicated keys", underReplicated === null ? "…" : String(underReplicated))}
      </div>
      <p>
        Full metrics: <a href="/metrics">/metrics</a> (Prometheus exposition).
      </p>
    </div>
  );
}
