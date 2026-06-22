import { useEffect, useState } from "react";
import { useUrlState } from "../router";
import { obs } from "../transport";
import { MemberLiveness, type MemberState } from "../gen/wavespan/v1/admin_pb";
import { type AdminPutResponse } from "../gen/wavespan/v1/observability_pb";
import { Badge, Button, InlineMessage, Input, Select, Spinner, Textarea } from "../components";

// KvWriter is a small KV write client for testing: pick which cluster node coordinates the write
// (origin), enter a record, and submit. The write is forwarded to the chosen node's data port by the
// admin endpoint (AdminPut), so the selected node becomes the origin and replicates from there.
export function KvWriter() {
  const [members, setMembers] = useState<MemberState[]>([]);
  // Addressing (which node + namespace + key) lives in the URL so a reload restores the same target;
  // the value/ttl draft is intentionally local (not echoed into a shareable URL).
  const [target, setTarget] = useUrlState("target", ""); // "" = the node serving this UI
  const [namespace, setNamespace] = useUrlState("ns", "default");
  const [key, setKey] = useUrlState("key", "");
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

  const labelStyle = { fontWeight: 700 as const, fontSize: "var(--ws-text-body-sm-size)" };

  return (
    <div style={{ maxWidth: 680 }}>
      <h2 className="ws-title ws-view__title">KV Writer</h2>
      <p className="ws-view__intro">
        Write a KV record for testing. Choose which node coordinates the write — it becomes the origin
        and replicates via origin+1. Reads in the Data Browser will then show it across the cluster.
      </p>

      <div style={{ display: "grid", gridTemplateColumns: "120px 1fr", gap: "var(--ws-space-md)", alignItems: "center" }}>
        <label style={labelStyle}>Coordinator</label>
        <Select value={target} onChange={(e) => setTarget(e.target.value)}>
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
        </Select>

        <label style={labelStyle}>Namespace</label>
        <Input value={namespace} onChange={(e) => setNamespace(e.target.value)} mono />

        <label style={labelStyle}>Key</label>
        <Input value={key} onChange={(e) => setKey(e.target.value)} placeholder="e.g. user/42" mono />

        <label style={labelStyle}>Value</label>
        <Textarea value={value} onChange={(e) => setValue(e.target.value)} rows={3} placeholder="some text" />

        <label style={labelStyle}>TTL (ms)</label>
        <Input value={ttl} onChange={(e) => setTtl(e.target.value)} placeholder="optional, blank = none" mono />
      </div>

      <div style={{ marginTop: "var(--ws-space-lg)" }}>
        <Button variant="primary" onClick={submit} disabled={!canSubmit}>
          {busy ? <Spinner /> : null}
          {busy ? "Writing…" : "Write record"}
        </Button>
      </div>

      {err && (
        <div style={{ marginTop: "var(--ws-space-lg)" }}>
          <InlineMessage tone="danger">request failed: <span className="ws-mono">{err}</span></InlineMessage>
        </div>
      )}
      {result && (
        <div style={{ marginTop: "var(--ws-space-lg)" }}>
          <InlineMessage tone={result.ok ? "success" : "warning"}>
            {result.ok ? (
              <div style={{ display: "flex", flexDirection: "column", gap: "var(--ws-space-xs)" }}>
                <div style={{ display: "flex", alignItems: "center", gap: "var(--ws-space-sm)" }}>
                  <Badge tone="success" dot>written</Badge>
                  via coordinator <span className="ws-mono">{result.coordinatorMemberId}</span>
                </div>
                <div>version: <span className="ws-mono">{fmtVer(result.version)}</span></div>
                <div>acked nearby replicas: <span className="ws-mono">{result.ackedNearbyReplicas}</span></div>
              </div>
            ) : (
              <div>
                write failed{result.coordinatorMemberId ? ` (coordinator ${result.coordinatorMemberId})` : ""}:{" "}
                <span className="ws-mono">{result.error}</span>
              </div>
            )}
          </InlineMessage>
        </div>
      )}
    </div>
  );
}
