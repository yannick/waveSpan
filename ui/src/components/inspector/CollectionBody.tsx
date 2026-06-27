import { useEffect, useState } from "react";
import { collections } from "../../transport";
import { Badge, Button, InlineMessage, Input, Spinner, Table } from "../index";
import type { DrawerTarget } from "./Drawer";

type CollTarget = Extract<DrawerTarget, { kind: "collection" }>;

const enc = (s: string) => new TextEncoder().encode(s);
const dec = (b: Uint8Array) => new TextDecoder().decode(b);
const labelStyle = { fontWeight: 700 as const, fontSize: "var(--ws-text-body-sm-size)" };

type Row = { key: string; extra?: string };

// CollectionBody browses & edits one replicated collection (set / hash / zset) via the collections
// client, mirroring CollectionsExplorer's ops. Listings are bounded-stale; writes are linearizable.
export function CollectionBody({ target }: { target: CollTarget }) {
  const { namespace: ns, collection: coll, ctype } = target;
  const collB = () => enc(coll);

  const [rows, setRows] = useState<Row[]>([]);
  const [card, setCard] = useState<bigint | null>(null);
  const [member, setMember] = useState("");
  const [field, setField] = useState("");
  const [value, setValue] = useState("");
  const [score, setScore] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const load = async () => {
    if (ctype === "set") {
      const m = await collections.sMembers({ namespace: ns, collection: collB(), limit: 1000, linearizable: false });
      const c = await collections.sCard({ namespace: ns, collection: collB(), linearizable: false });
      setRows(m.members.map((x) => ({ key: dec(x) })));
      setCard(c.count);
    } else if (ctype === "hash") {
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
    setError(null);
    try {
      await fn();
    } catch (e) {
      setError(String(e));
    } finally {
      setBusy(false);
    }
  };

  useEffect(() => {
    void run(load);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [ns, coll, ctype]);

  const add = () =>
    run(async () => {
      if (ctype === "set") {
        await collections.sAdd({ namespace: ns, collection: collB(), members: [enc(member)] });
      } else if (ctype === "hash") {
        await collections.hSet({ namespace: ns, collection: collB(), fields: [{ field: enc(field), value: enc(value) }] });
      } else {
        await collections.zAdd({ namespace: ns, collection: collB(), members: [{ member: enc(member), score: Number(score) || 0 }] });
      }
      setMember("");
      setField("");
      setValue("");
      setScore("");
      await load();
    });

  const removeRow = (key: string) =>
    run(async () => {
      if (ctype === "set") await collections.sRem({ namespace: ns, collection: collB(), keys: [enc(key)] });
      else if (ctype === "hash") await collections.hDel({ namespace: ns, collection: collB(), keys: [enc(key)] });
      else await collections.zRem({ namespace: ns, collection: collB(), keys: [enc(key)] });
      await load();
    });

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "var(--ws-space-sm)" }}>
      <div style={{ display: "flex", alignItems: "center", gap: "var(--ws-space-sm)" }}>
        {card !== null && <Badge tone="neutral">{String(card)} items</Badge>}
        {busy && <Spinner />}
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "70px 1fr", gap: "var(--ws-space-xs)", alignItems: "center" }}>
        {ctype === "hash" ? (
          <>
            <span style={labelStyle}>Field</span>
            <Input value={field} onChange={(e) => setField(e.target.value)} placeholder="field" mono />
            <span style={labelStyle}>Value</span>
            <Input value={value} onChange={(e) => setValue(e.target.value)} placeholder="value" mono />
          </>
        ) : (
          <>
            <span style={labelStyle}>Member</span>
            <Input value={member} onChange={(e) => setMember(e.target.value)} placeholder="member" mono />
            {ctype === "zset" && (
              <>
                <span style={labelStyle}>Score</span>
                <Input value={score} onChange={(e) => setScore(e.target.value)} placeholder="1.0" mono />
              </>
            )}
          </>
        )}
      </div>
      <div>
        <Button variant="primary" size="sm" onClick={add} disabled={busy}>
          Add
        </Button>
      </div>

      {error && <InlineMessage tone="danger"><span className="ws-mono">{error}</span></InlineMessage>}

      {rows.length > 0 && (
        <Table mono>
          <thead>
            <tr>
              <th>{ctype === "hash" ? "field" : "member"}</th>
              {ctype !== "set" && <th>{ctype === "hash" ? "value" : "score"}</th>}
              <th aria-label="actions" />
            </tr>
          </thead>
          <tbody>
            {rows.map((r) => (
              <tr key={r.key}>
                <td>{r.key}</td>
                {ctype !== "set" && <td>{r.extra}</td>}
                <td style={{ textAlign: "right" }}>
                  <Button variant="ghost" size="sm" onClick={() => removeRow(r.key)} disabled={busy}>
                    Remove
                  </Button>
                </td>
              </tr>
            ))}
          </tbody>
        </Table>
      )}
      {rows.length === 0 && card !== null && <Badge tone="neutral">empty collection</Badge>}
    </div>
  );
}
