import { useEffect, useState } from "react";
import { obs } from "../transport";
import { StatCard } from "../components";
import { color } from "../theme/tokens";

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

  const underRepCount = underReplicated === null ? null : Number(underReplicated);

  return (
    <div>
      <h2 className="ws-title ws-view__title">Cluster metrics</h2>
      <p className="ws-view__intro">
        Live cluster-wide counters sampled from this node's observability service.
      </p>
      <div style={{ display: "flex", gap: "var(--ws-space-lg)", flexWrap: "wrap", marginBottom: "var(--ws-space-xl)" }}>
        <StatCard
          label="Members"
          value={members === null ? "…" : String(members)}
          hint="known cluster members"
          accent={color.teal}
        />
        <StatCard
          label="Under-replicated keys"
          value={underReplicated === null ? "…" : String(underReplicated)}
          hint="estimate · target-N not yet met"
          accent={underRepCount && underRepCount > 0 ? color.mustard : color.olive}
        />
      </div>
      <p className="ws-body-sm ws-muted">
        Full metrics are exposed at <a href="/metrics">/metrics</a> in Prometheus exposition format.
      </p>
    </div>
  );
}
