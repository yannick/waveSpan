import { useEffect, useRef, useState } from "react";
import { obs } from "../transport";
import { GossipDirection, GossipKind } from "../gen/wavespan/v1/observability_pb";

interface Row {
  seq: bigint;
  kind: string;
  direction: string;
  peer: string;
  detail: string;
  gap?: boolean;
}

const KIND_NAMES: Record<number, string> = {
  [GossipKind.GOSSIP_PING]: "ping",
  [GossipKind.GOSSIP_ACK]: "ack",
  [GossipKind.GOSSIP_INDIRECT]: "indirect",
  [GossipKind.GOSSIP_SUSPECT]: "suspect",
  [GossipKind.GOSSIP_ALIVE]: "alive",
  [GossipKind.GOSSIP_UNREACHABLE]: "unreachable",
  [GossipKind.GOSSIP_HOLDER_SUMMARY]: "holder-summary",
  [GossipKind.GOSSIP_LATENCY_EDGE]: "latency-edge",
  [GossipKind.GOSSIP_MEMBERSHIP_DELTA]: "membership-delta",
};

const FILTERABLE: { kind: GossipKind; label: string }[] = Object.entries(KIND_NAMES).map(
  ([k, label]) => ({ kind: Number(k) as GossipKind, label }),
);

const DIR_NAMES: Record<number, string> = {
  [GossipDirection.GOSSIP_SEND]: "send",
  [GossipDirection.GOSSIP_RECV]: "recv",
  [GossipDirection.GOSSIP_INTERNAL]: "internal",
};

export function GossipInspector() {
  const [rows, setRows] = useState<Row[]>([]);
  const [paused, setPaused] = useState(false);
  const [kinds, setKinds] = useState<Set<GossipKind>>(new Set());
  const abortRef = useRef<AbortController | null>(null);

  useEffect(() => {
    if (paused) return;
    const ac = new AbortController();
    abortRef.current = ac;
    const selected = [...kinds];
    (async () => {
      try {
        for await (const ev of obs.streamGossip(
          { filter: { kinds: selected, peers: [], direction: GossipDirection.GOSSIP_DIRECTION_UNSPECIFIED, namespace: "" }, backfill: true },
          { signal: ac.signal },
        )) {
          if (ev.event.case === "gap") {
            const g = ev.event.value;
            setRows((r) => [{ seq: 0n, kind: "GAP", direction: "", peer: "", detail: `dropped ${g.droppedCount}`, gap: true }, ...r].slice(0, 500));
          } else if (ev.event.case === "record") {
            const rec = ev.event.value;
            const s = rec.summary;
            const detail = s
              ? [s.rttMs && `rtt=${s.rttMs.toFixed(1)}ms`, s.newState, s.watermark && `wm=${s.watermark}`, s.ewmaMs && `ewma=${s.ewmaMs.toFixed(1)}`]
                  .filter(Boolean)
                  .join(" ")
              : "";
            setRows((r) => [{ seq: rec.seq, kind: KIND_NAMES[rec.kind] ?? String(rec.kind), direction: DIR_NAMES[rec.direction] ?? "", peer: rec.peer, detail }, ...r].slice(0, 500));
          }
        }
      } catch {
        /* stream aborted or closed */
      }
    })();
    return () => ac.abort();
  }, [paused, kinds]);

  const toggle = (k: GossipKind) =>
    setKinds((prev) => {
      const next = new Set(prev);
      next.has(k) ? next.delete(k) : next.add(k);
      return next;
    });

  return (
    <div>
      <div style={{ marginBottom: 8, display: "flex", flexWrap: "wrap", gap: 8, alignItems: "center" }}>
        <button onClick={() => setPaused((p) => !p)}>{paused ? "Resume" : "Pause"}</button>
        {FILTERABLE.map((f) => (
          <label key={f.kind} style={{ fontSize: 12 }}>
            <input type="checkbox" checked={kinds.has(f.kind)} onChange={() => toggle(f.kind)} /> {f.label}
          </label>
        ))}
        <span style={{ fontSize: 12, color: "#888" }}>{rows.length} events</span>
      </div>
      <table style={{ width: "100%", fontSize: 12, borderCollapse: "collapse" }}>
        <thead>
          <tr style={{ textAlign: "left", borderBottom: "1px solid #ccc" }}>
            <th>seq</th><th>kind</th><th>dir</th><th>peer</th><th>summary</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r, i) => (
            <tr key={i} style={{ background: r.gap ? "#ffe9e9" : undefined }}>
              <td>{r.gap ? "" : String(r.seq)}</td><td>{r.kind}</td><td>{r.direction}</td><td>{r.peer}</td><td>{r.detail}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
