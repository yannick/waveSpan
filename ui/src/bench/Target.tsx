// Target panel: pick the cluster data port + the admin endpoints of each node, then probe them for
// reachability and profiling support. The probe result is lifted up via `onProbed` so the parent can
// gate the rest of the dashboard (and the Profiling panel) on it.
import { useEffect, useState } from "react";
import {
  Badge,
  Button,
  Chip,
  FieldLabel,
  InlineMessage,
  Input,
  Panel,
  Spinner,
} from "../components";
import { getConfig, probeTarget, type NodeRef, type ProbeResult } from "./api";

interface TargetProps {
  /** Reports the probe result plus the node refs that were probed (the API's ProbeResult has no
   *  admin addresses, but the parent needs them for profiling, so we pass them alongside). */
  onProbed(result: ProbeResult, nodes: NodeRef[]): void;
}

const DEFAULT_DATA_ADDR = "localhost:7811";

let rowSeq = 0;
interface Row extends NodeRef {
  id: number;
}
function newRow(name = "", adminAddr = ""): Row {
  return { id: ++rowSeq, name, adminAddr };
}

export function Target({ onProbed }: TargetProps) {
  const [dataAddr, setDataAddr] = useState(DEFAULT_DATA_ADDR);
  const [rows, setRows] = useState<Row[]>([newRow("n1", "localhost:7812")]);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [result, setResult] = useState<ProbeResult | null>(null);

  const patch = (id: number, p: Partial<NodeRef>) =>
    setRows((rs) => rs.map((r) => (r.id === id ? { ...r, ...p } : r)));
  const remove = (id: number) => setRows((rs) => rs.filter((r) => r.id !== id));
  const add = () => setRows((rs) => [...rs, newRow()]);

  const runProbe = async (addr: string, nodes: NodeRef[]) => {
    setBusy(true);
    setErr(null);
    try {
      const res = await probeTarget({ dataAddr: addr.trim(), nodes });
      setResult(res);
      onProbed(res, nodes);
    } catch (e) {
      setErr(String(e instanceof Error ? e.message : e));
    } finally {
      setBusy(false);
    }
  };

  const probe = () =>
    runProbe(
      dataAddr,
      rows
        .filter((r) => r.name.trim() && r.adminAddr.trim())
        .map(({ name, adminAddr }) => ({ name: name.trim(), adminAddr: adminAddr.trim() })),
    );

  // Pre-fill from server-provided defaults (set in-cluster via env) and auto-probe once, so a
  // benchmark works out of the box without typing any addresses. No-op in local dev (empty config).
  useEffect(() => {
    let live = true;
    getConfig()
      .then((cfg) => {
        if (!live || !cfg.defaultDataAddr) return;
        setDataAddr(cfg.defaultDataAddr);
        const node: NodeRef = { name: "n1", adminAddr: cfg.defaultAdminAddr || "" };
        setRows([newRow(node.name, node.adminAddr)]);
        void runProbe(cfg.defaultDataAddr, node.adminAddr ? [node] : []);
      })
      .catch(() => {
        /* no server config (local dev) — keep the localhost defaults */
      });
    return () => {
      live = false;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const canProbe = !busy && dataAddr.trim().length > 0;

  return (
    <Panel
      title="Target"
      actions={
        <Button variant="primary" onClick={probe} disabled={!canProbe}>
          {busy ? <Spinner /> : null}
          {busy ? "Probing…" : "Probe"}
        </Button>
      }
    >
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "140px 1fr",
          gap: "var(--ws-space-md)",
          alignItems: "center",
          maxWidth: 640,
        }}
      >
        <FieldLabel>Data port</FieldLabel>
        <Input
          mono
          value={dataAddr}
          onChange={(e) => setDataAddr(e.target.value)}
          placeholder={DEFAULT_DATA_ADDR}
        />
      </div>

      <div style={{ marginTop: "var(--ws-space-lg)" }}>
        <div className="ws-field-label" style={{ marginBottom: "var(--ws-space-sm)" }}>
          Nodes <span className="ws-muted">(name = admin address)</span>
        </div>
        <div style={{ display: "flex", flexDirection: "column", gap: "var(--ws-space-sm)" }}>
          {rows.map((r) => (
            <div
              key={r.id}
              style={{ display: "flex", gap: "var(--ws-space-sm)", alignItems: "center" }}
            >
              <Input
                mono
                style={{ flex: "0 0 140px" }}
                value={r.name}
                placeholder="name"
                onChange={(e) => patch(r.id, { name: e.target.value })}
              />
              <span className="ws-muted">=</span>
              <Input
                mono
                style={{ flex: 1 }}
                value={r.adminAddr}
                placeholder="host:adminPort"
                onChange={(e) => patch(r.id, { adminAddr: e.target.value })}
              />
              <Button
                variant="ghost"
                size="sm"
                icon
                aria-label="remove node"
                onClick={() => remove(r.id)}
                disabled={rows.length <= 1}
              >
                ✕
              </Button>
            </div>
          ))}
        </div>
        <div style={{ marginTop: "var(--ws-space-sm)" }}>
          <Button variant="ghost" size="sm" onClick={add}>
            + Add node
          </Button>
        </div>
      </div>

      {err && (
        <div style={{ marginTop: "var(--ws-space-lg)" }}>
          <InlineMessage tone="danger">
            probe failed: <span className="ws-mono">{err}</span>
          </InlineMessage>
        </div>
      )}

      {result && (
        <div style={{ marginTop: "var(--ws-space-lg)" }}>
          <div className="ws-field-label" style={{ marginBottom: "var(--ws-space-sm)" }}>
            Probe result <span className="ws-muted ws-mono">· {result.dataAddr}</span>
          </div>
          {result.nodes.length === 0 ? (
            <span className="ws-body-sm ws-muted">No nodes returned.</span>
          ) : (
            <div style={{ display: "flex", flexWrap: "wrap", gap: "var(--ws-space-sm)" }}>
              {result.nodes.map((n) => (
                <span
                  key={n.name}
                  style={{ display: "inline-flex", alignItems: "center", gap: "var(--ws-space-xs)" }}
                >
                  <Badge tone={n.reachable ? "success" : "danger"} dot>
                    {n.name} · {n.reachable ? "reachable" : "unreachable"}
                  </Badge>
                  {n.profiling && <Chip aria-label="profiling supported">profiling ✓</Chip>}
                </span>
              ))}
            </div>
          )}
        </div>
      )}
    </Panel>
  );
}
