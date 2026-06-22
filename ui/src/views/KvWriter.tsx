import { useEffect, useState } from "react";
import { obs } from "../transport";
import { MemberLiveness, type MemberState } from "../gen/wavespan/v1/admin_pb";
import { type AdminPutResponse } from "../gen/wavespan/v1/observability_pb";

// KvWriter is a small KV write client for testing: pick which cluster node coordinates the write
// (origin), enter a record, and submit. The write is forwarded to the chosen node's data port by the
// admin endpoint (AdminPut), so the selected node becomes the origin and replicates from there.
export function KvWriter() {
  const [members, setMembers] = useState<MemberState[]>([]);
  const [target, setTarget] = useState(""); // "" = the node serving this UI
  const [namespace, setNamespace] = useState("default");
  const [key, setKey] = useState("");
  const [value, setValue] = useState("");
  const [ttl, setTtl] = useState("");
  const [busy, setBusy] = useState(false);
  const [result, setResult] = useState<AdminPutResponse | null>(null);
  const [err, setErr] = useState<string | null>(null);

  // Discover the local cluster so the operator can choose a coordinator node.
  useEffect(() => {
    let live = true;
    const tick = async () => {
      try {
        const v = await obs.getClusterView({});
        if (live) setMembers(v.members);
      } catch {
        /* admin endpoint unreachable */
      }
    };
    tick();
    const id = setInterval(tick, 3000);
    return () => {
      live = false;
      clearInterval(id);
    };
  }, []);

  const submit = async () => {
    setBusy(true);
    setErr(null);
    setResult(null);
    try {
      const res = await obs.adminPut({
        namespace,
        key: new TextEncoder().encode(key),
        value: new TextEncoder().encode(value),
        ttlMs: ttl.trim() ? BigInt(ttl.trim()) : undefined,
        targetMemberId: target,
      });
      setResult(res);
    } catch (e) {
      setErr(String(e));
    } finally {
      setBusy(false);
    }
  };

  const canSubmit = key.length > 0 && !busy;
  const fmtVer = (v: AdminPutResponse["version"]) =>
    v ? `${v.hlcPhysicalMs}.${v.hlcLogical}@${v.writerMemberId}` : "";

  return (
    <div style={{ maxWidth: 640 }}>
      <p style={{ fontSize: 12, color: "#666" }}>
        Write a KV record for testing. Choose which node coordinates the write (it becomes the origin
        and replicates via origin+1). Reads in the Data Browser will then show it across the cluster.
      </p>
      <div style={{ display: "grid", gridTemplateColumns: "120px 1fr", gap: 8, alignItems: "center" }}>
        <label>Coordinator</label>
        <select value={target} onChange={(e) => setTarget(e.target.value)}>
          <option value="">This node (auto)</option>
          {members.map((m, i) => {
            const id = m.member?.memberId ?? "?";
            const alive = m.state === MemberLiveness.MEMBER_ALIVE;
            return (
              <option key={i} value={id} disabled={!alive}>
                {id} {m.member?.zone ? `· ${m.member.zone}` : ""} {alive ? "" : "(not alive)"}
              </option>
            );
          })}
        </select>

        <label>Namespace</label>
        <input value={namespace} onChange={(e) => setNamespace(e.target.value)} />

        <label>Key</label>
        <input value={key} onChange={(e) => setKey(e.target.value)} placeholder="e.g. user/42" />

        <label>Value</label>
        <textarea value={value} onChange={(e) => setValue(e.target.value)} rows={3} placeholder="some text" />

        <label>TTL (ms)</label>
        <input value={ttl} onChange={(e) => setTtl(e.target.value)} placeholder="optional, blank = none" />
      </div>

      <div style={{ marginTop: 12 }}>
        <button onClick={submit} disabled={!canSubmit}>{busy ? "Writing…" : "Write record"}</button>
      </div>

      {err && (
        <div style={{ marginTop: 12, padding: 8, background: "#ffe9e9", fontSize: 13 }}>request failed: {err}</div>
      )}
      {result && (
        <div style={{ marginTop: 12, padding: 8, background: result.ok ? "#e9ffe9" : "#fff3cd", fontSize: 13 }}>
          {result.ok ? (
            <>
              <div><b>written</b> via coordinator <b>{result.coordinatorMemberId}</b></div>
              <div>version: {fmtVer(result.version)}</div>
              <div>acked nearby replicas: {result.ackedNearbyReplicas}</div>
            </>
          ) : (
            <div>write failed{result.coordinatorMemberId ? ` (coordinator ${result.coordinatorMemberId})` : ""}: {result.error}</div>
          )}
        </div>
      )}
    </div>
  );
}
