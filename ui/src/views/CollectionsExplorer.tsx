import { useState } from "react";
import { useUrlState } from "../router";
import { collections } from "../transport";
import { Badge, Button, InlineMessage, Input, Select, Spinner, StatCard, Table } from "../components";

const enc = (s: string) => new TextEncoder().encode(s);
const dec = (b: Uint8Array) => new TextDecoder().decode(b);

type CollType = "set" | "hash" | "zset";
type Row = { key: string; extra?: string };

// CollectionsExplorer is an operator console for the replicated-collections tier (design/30): pick a
// namespace, collection, and datatype, then add/remove elements and list the contents. Writes are
// linearizable through the owning shard's leader; the list reads are bounded-stale. Requires the node
// to be started with WAVESPAN_COLLECTIONS_ENABLED=1 (otherwise the calls return "not configured").
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
        Writes are linearizable; listings are bounded-stale. The node must run with{" "}
        <code>WAVESPAN_COLLECTIONS_ENABLED=1</code>.
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
    </div>
  );
}
