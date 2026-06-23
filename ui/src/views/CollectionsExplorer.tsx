import { useEffect, useState } from "react";
import { useUrlState } from "../router";
import { collections } from "../transport";
import { Badge, Button, Card, InlineMessage, Input, Select, Spinner, StatCard, Table } from "../components";
import { color } from "../theme/tokens";

const enc = (s: string) => new TextEncoder().encode(s);
const dec = (b: Uint8Array) => new TextDecoder().decode(b);

type CollType = "set" | "hash" | "zset";
type Row = { key: string; extra?: string };

type ShardSt = {
  shardId: bigint;
  leaderReplicaId: bigint;
  hasLeader: boolean;
  isLeader: boolean;
  isData: boolean;
};
type TierInfo = {
  voter: boolean;
  raftAddress: string;
  selfReplicaId: bigint;
  rttMs: bigint;
  electionRtt: bigint;
  heartbeatRtt: bigint;
  snapshotEntries: bigint;
  compactionOverhead: bigint;
  sweepMs: bigint;
  shards: ShardSt[];
};

// TierPanel is a read-only operator view of this node's consensus-tier placement, active tunables, and
// per-shard leader status. It polls TierInfo every 3s so leadership changes show up live.
function TierPanel() {
  const [info, setInfo] = useState<TierInfo | null>(null);
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    let live = true;
    const load = () => {
      collections
        .tierInfo({})
        .then((r) => {
          if (live) {
            setInfo(r as unknown as TierInfo);
            setErr(null);
          }
        })
        .catch((e: unknown) => {
          if (live) setErr(e instanceof Error ? e.message : String(e));
        });
    };
    load();
    const id = setInterval(load, 3000);
    return () => {
      live = false;
      clearInterval(id);
    };
  }, []);

  if (err) return <InlineMessage tone="warning">Consensus tier status unavailable: {err}</InlineMessage>;
  if (!info) return <Spinner />;

  const tunables: [string, string][] = [
    ["RTT unit", `${info.rttMs} ms`],
    ["Election", `${info.electionRtt} × RTT`],
    ["Heartbeat", `${info.heartbeatRtt} × RTT`],
    ["Snapshot every", `${info.snapshotEntries} entries`],
    ["Compaction overhead", `${info.compactionOverhead} entries`],
    ["TTL sweep", `${info.sweepMs} ms`],
  ];

  return (
    <Card style={{ marginTop: 28 }}>
      <h3 className="ws-title" style={{ marginTop: 0, marginBottom: 12 }}>Consensus tier</h3>

      <div style={{ display: "flex", gap: 12, flexWrap: "wrap", marginBottom: 18 }}>
        <StatCard
          label="Role"
          value={info.voter ? "Voter" : "Spot"}
          hint={`replica #${info.selfReplicaId}`}
          accent={info.voter ? color.teal : color.orange}
        />
        <StatCard label="Raft address" value={info.raftAddress || "—"} />
        <StatCard label="Shards hosted" value={String(info.shards.length)} />
      </div>

      <div className="ws-label" style={{ marginBottom: 8 }}>Tunables</div>
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(auto-fit, minmax(160px, 1fr))",
          gap: 8,
          marginBottom: 18,
        }}
      >
        {tunables.map(([k, v]) => (
          <div key={k} style={{ fontSize: 13 }}>
            <span style={{ color: color.inkMuted }}>{k}: </span>
            <strong>{v}</strong>
          </div>
        ))}
      </div>

      <div className="ws-label" style={{ marginBottom: 8 }}>Shards</div>
      {info.shards.length === 0 ? (
        <InlineMessage tone="info">No shards hosted on this node yet.</InlineMessage>
      ) : (
        <Table mono>
          <thead>
            <tr>
              <th>shard</th>
              <th>kind</th>
              <th>leader</th>
              <th>this node</th>
            </tr>
          </thead>
          <tbody>
            {info.shards.map((s) => (
              <tr key={s.shardId.toString()}>
                <td>{s.shardId.toString()}</td>
                <td>{s.isData ? "data" : "meta"}</td>
                <td>{s.hasLeader ? `replica #${s.leaderReplicaId}` : "—"}</td>
                <td>
                  {s.isLeader ? (
                    <Badge tone="success" dot>leader</Badge>
                  ) : (
                    <Badge tone="neutral">follower</Badge>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </Table>
      )}
    </Card>
  );
}

// CollectionsExplorer is an operator console for the replicated-collections tier (design/30): pick a
// namespace, collection, and datatype, then add/remove elements and list the contents. Writes are
// linearizable through the owning shard's leader; the list reads are bounded-stale. The tier is on by
// default (set WAVESPAN_COLLECTIONS_ENABLED=0 to disable; the calls then return "not configured").
export function CollectionsExplorer() {
  const [ns, setNs] = useUrlState("ns", "default");
  const [coll, setColl] = useUrlState("coll", "");
  const [typeStr, setTypeStr] = useUrlState("ctype", "set");
  const type = typeStr as CollType;

  const [member, setMember] = useState("");
  const [field, setField] = useState("");
  const [value, setValue] = useState("");
  const [score, setScore] = useState("");

  const [rows, setRows] = useState<Row[]>([]);
  const [card, setCard] = useState<bigint | null>(null);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [msg, setMsg] = useState<string | null>(null);

  const collB = () => enc(coll);
  const ready = coll.trim().length > 0;

  // load refreshes the contents + cardinality for the selected collection.
  const load = async () => {
    if (type === "set") {
      const m = await collections.sMembers({ namespace: ns, collection: collB(), limit: 1000, linearizable: false });
      const c = await collections.sCard({ namespace: ns, collection: collB(), linearizable: false });
      setRows(m.members.map((x) => ({ key: dec(x) })));
      setCard(c.count);
    } else if (type === "hash") {
      const m = await collections.hGetAll({ namespace: ns, collection: collB(), limit: 1000, linearizable: false });
      const c = await collections.hLen({ namespace: ns, collection: collB(), linearizable: false });
      setRows(m.fields.map((f) => ({ key: dec(f.field), extra: dec(f.value) })));
      setCard(c.count);
    } else {
      const m = await collections.zRange({ namespace: ns, collection: collB(), limit: 1000, linearizable: false });
      const c = await collections.zCard({ namespace: ns, collection: collB(), linearizable: false });
      setRows(m.members.map((sm) => ({ key: dec(sm.member), extra: String(sm.score) })));
      setCard(c.count);
    }
  };

  const run = async (fn: () => Promise<void>) => {
    setBusy(true);
    setErr(null);
    setMsg(null);
    try {
      await fn();
    } catch (e) {
      setErr(String(e));
    } finally {
      setBusy(false);
    }
  };

  const refresh = () => run(load);

  const add = () =>
    run(async () => {
      if (type === "set") {
        const r = await collections.sAdd({ namespace: ns, collection: collB(), members: [enc(member)] });
        setMsg(`added ${r.count} member(s)`);
      } else if (type === "hash") {
        const r = await collections.hSet({ namespace: ns, collection: collB(), fields: [{ field: enc(field), value: enc(value) }] });
        setMsg(`set ${r.count} new field(s)`);
      } else {
        const r = await collections.zAdd({ namespace: ns, collection: collB(), members: [{ member: enc(member), score: Number(score) || 0 }] });
        setMsg(`added ${r.count} member(s)`);
      }
      await load();
    });

  const removeRow = (key: string) =>
    run(async () => {
      if (type === "set") {
        await collections.sRem({ namespace: ns, collection: collB(), keys: [enc(key)] });
      } else if (type === "hash") {
        await collections.hDel({ namespace: ns, collection: collB(), keys: [enc(key)] });
      } else {
        await collections.zRem({ namespace: ns, collection: collB(), keys: [enc(key)] });
      }
      await load();
    });

  const labelStyle = { fontWeight: 700 as const, fontSize: "var(--ws-text-body-sm-size)" };
  const gridStyle = { display: "grid", gridTemplateColumns: "120px 1fr", gap: "var(--ws-space-md)", alignItems: "center" as const };

  return (
    <div style={{ maxWidth: 760 }}>
      <h2 className="ws-title ws-view__title">Collections</h2>
      <p className="ws-view__intro">
        Browse and edit replicated collections (sets, hash tables, sorted sets) on the consensus tier.
        Writes are linearizable; listings are bounded-stale. The tier is enabled by default (disable
        with <code>WAVESPAN_COLLECTIONS_ENABLED=0</code>).
      </p>

      <div style={gridStyle}>
        <label style={labelStyle}>Namespace</label>
        <Input value={ns} onChange={(e) => setNs(e.target.value)} placeholder="default" />
        <label style={labelStyle}>Collection</label>
        <Input value={coll} onChange={(e) => setColl(e.target.value)} placeholder="my-set" />
        <label style={labelStyle}>Type</label>
        <Select value={typeStr} onChange={(e) => setTypeStr(e.target.value)}>
          <option value="set">Set</option>
          <option value="hash">Hash</option>
          <option value="zset">Sorted set</option>
        </Select>
      </div>

      <div style={{ marginTop: "var(--ws-space-lg)", ...gridStyle }}>
        {type === "hash" ? (
          <>
            <label style={labelStyle}>Field</label>
            <Input value={field} onChange={(e) => setField(e.target.value)} placeholder="field" />
            <label style={labelStyle}>Value</label>
            <Input value={value} onChange={(e) => setValue(e.target.value)} placeholder="value" />
          </>
        ) : (
          <>
            <label style={labelStyle}>Member</label>
            <Input value={member} onChange={(e) => setMember(e.target.value)} placeholder="member" />
            {type === "zset" && (
              <>
                <label style={labelStyle}>Score</label>
                <Input value={score} onChange={(e) => setScore(e.target.value)} placeholder="1.0" />
              </>
            )}
          </>
        )}
      </div>

      <div style={{ display: "flex", gap: "var(--ws-space-md)", marginTop: "var(--ws-space-lg)" }}>
        <Button onClick={add} disabled={!ready || busy}>
          Add
        </Button>
        <Button variant="ghost" onClick={refresh} disabled={!ready || busy}>
          Refresh
        </Button>
        {busy && <Spinner />}
      </div>

      {err && (
        <div style={{ marginTop: "var(--ws-space-md)" }}>
          <InlineMessage tone="danger">{err}</InlineMessage>
        </div>
      )}
      {msg && !err && (
        <div style={{ marginTop: "var(--ws-space-md)" }}>
          <InlineMessage tone="success">{msg}</InlineMessage>
        </div>
      )}

      {card !== null && (
        <div style={{ marginTop: "var(--ws-space-lg)", maxWidth: 220 }}>
          <StatCard label="Cardinality" value={String(card)} hint={`${ns} / ${coll}`} />
        </div>
      )}

      {rows.length > 0 && (
        <Table style={{ marginTop: "var(--ws-space-lg)" }}>
          <thead>
            <tr>
              <th>{type === "hash" ? "Field" : "Member"}</th>
              {type !== "set" && <th>{type === "hash" ? "Value" : "Score"}</th>}
              <th style={{ width: 80 }} />
            </tr>
          </thead>
          <tbody>
            {rows.map((r) => (
              <tr key={r.key}>
                <td>{r.key}</td>
                {type !== "set" && <td>{r.extra}</td>}
                <td>
                  <Button variant="ghost" onClick={() => removeRow(r.key)} disabled={busy}>
                    Remove
                  </Button>
                </td>
              </tr>
            ))}
          </tbody>
        </Table>
      )}

      {ready && rows.length === 0 && card !== null && (
        <div style={{ marginTop: "var(--ws-space-lg)" }}>
          <Badge tone="neutral">empty collection</Badge>
        </div>
      )}

      <TierPanel />
    </div>
  );
}
