import { useEffect, useMemo, useRef, useState } from "react";
import { useUrlBool, useUrlState } from "../router";
import { obs } from "../transport";
import { GossipDirection, GossipKind } from "../gen/wavespan/v1/observability_pb";
import { Badge, Button, Checkbox, Table, Toolbar, type Tone } from "../components";

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

// Map each gossip kind to a semantic badge tone.
const KIND_TONE: Record<string, Tone> = {
  ping: "info",
  ack: "neutral",
  indirect: "info",
  suspect: "warning",
  alive: "success",
  unreachable: "danger",
  "holder-summary": "olive",
  "latency-edge": "purple",
  "membership-delta": "accent",
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
  // Pause + kind filter live in the URL so a reload restores the exact filtered live view.
  const [paused, setPaused] = useUrlBool("paused", false);
  const [kindsStr, setKindsStr] = useUrlState("kinds", "");
  // Stable Set identity per filter string so the stream effect does not restart every render.
  const kinds = useMemo(
    () => new Set<GossipKind>(kindsStr ? kindsStr.split(",").filter(Boolean).map(Number) : []),
    [kindsStr],
  );
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
  }, [paused, kindsStr]);

  const toggle = (k: GossipKind) => {
    const next = new Set(kinds);
    next.has(k) ? next.delete(k) : next.add(k);
    setKindsStr([...next].map(String).sort().join(","));
  };

  return (
    <div>
      <h2 className="ws-title ws-view__title">Gossip Inspector</h2>
      <p className="ws-view__intro">
        Live SWIM gossip traffic on this node — pings, acks, suspicions and the piggybacked latency
        edges &amp; holder summaries. Filter by kind; dropped events surface as gaps.
      </p>

      <Toolbar style={{ marginBottom: "var(--ws-space-md)" }}>
        <Button variant={paused ? "primary" : "secondary"} onClick={() => setPaused(!paused)}>
          {paused ? "Resume" : "Pause"}
        </Button>
        {FILTERABLE.map((f) => (
          <Checkbox key={f.kind} checked={kinds.has(f.kind)} onChange={() => toggle(f.kind)} label={f.label} />
        ))}
        <Badge tone="neutral">{rows.length} events</Badge>
      </Toolbar>

      <Table>
        <thead>
          <tr>
            <th>seq</th>
            <th>kind</th>
            <th>dir</th>
            <th>peer</th>
            <th>summary</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r, i) => (
            <tr key={i} style={r.gap ? { background: "color-mix(in srgb, var(--ws-color-red) 12%, transparent)" } : undefined}>
              <td className="ws-mono">{r.gap ? "" : String(r.seq)}</td>
              <td>
                {r.gap ? (
                  <Badge tone="danger">GAP</Badge>
                ) : (
                  <Badge tone={KIND_TONE[r.kind] ?? "neutral"}>{r.kind}</Badge>
                )}
              </td>
              <td className="ws-mono ws-muted">{r.direction}</td>
              <td className="ws-mono">{r.peer}</td>
              <td className="ws-mono ws-muted">{r.detail}</td>
            </tr>
          ))}
        </tbody>
      </Table>
    </div>
  );
}
