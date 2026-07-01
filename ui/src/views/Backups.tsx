import { useEffect, useState } from "react";
import { backup } from "../transport";
import { Badge, Button, Checkbox, FieldLabel, InlineMessage, Input, Modal, Panel, Select, Spinner, Table } from "../components";
import type { BackupState, BackupSummary } from "../gen/wavespan/v1/backup_pb";
import {
  backupHelp,
  buildBeginRequest,
  emptyForm,
  fmtBytes,
  isTerminal,
  phaseLabel,
  pctLabel,
  statusLabel,
  statusTone,
  summaryRow,
  type BackupForm,
  type HelpKey,
} from "./backupModel";

// HelpButton is a small "?" affordance that opens the option-help modal. type="button" + preventDefault so
// it never activates the surrounding <label>'s control.
function HelpButton({ onClick }: { onClick: () => void }) {
  return (
    <button
      type="button"
      onClick={(e) => {
        e.preventDefault();
        onClick();
      }}
      title="Help"
      aria-label="Help"
      style={{
        width: 18,
        height: 18,
        padding: 0,
        lineHeight: "16px",
        fontSize: 11,
        borderRadius: "50%",
        border: "1px solid var(--ws-border, #ccc)",
        background: "transparent",
        color: "inherit",
        cursor: "pointer",
      }}
    >
      ?
    </button>
  );
}

// Backups is the admin console for cluster backups (design/backup §11): trigger a backup (full or
// incremental, any plane, to the default / a named / an explicit destination), watch a RUNNING backup's
// live per-node progress, list known backups, and delete one (chain-aware). All via the BackupService
// Connect endpoint (admin-auth enforced). Object-store credentials are entered in the trigger form and
// sent only in the request — never stored in component state beyond submit, never rendered in any list.
export function Backups() {
  const [list, setList] = useState<BackupSummary[]>([]);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [msg, setMsg] = useState<string | null>(null);
  const [watch, setWatch] = useState<string | null>(null);
  const [form, setForm] = useState<BackupForm>(emptyForm());

  const set = <K extends keyof BackupForm>(k: K, v: BackupForm[K]) => setForm((f) => ({ ...f, [k]: v }));

  const load = async () => {
    const r = await backup.listBackups({});
    setList(r.backups);
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

  useEffect(() => {
    run(load);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const childrenOf = (id: string) => list.filter((b) => b.parent === id).map((b) => b.backupId);

  const submit = () =>
    run(async () => {
      const req = buildBeginRequest(form);
      // Drop transient inline creds from UI state BEFORE the request leaves, so they are never retained
      // past submit on ANY path (success, network failure, or a named-only rejection).
      setForm((f) => ({ ...f, accessKey: "", secretKey: "" }));
      const r = await backup.beginBackup(req);
      setMsg(`started ${r.backupId}`);
      setWatch(r.backupId);
      await load();
    });

  // setDestMode switches the destination mode, clearing any entered inline creds when leaving "explicit".
  const setDestMode = (v: BackupForm["destMode"]) =>
    setForm((f) => ({ ...f, destMode: v, accessKey: v === "explicit" ? f.accessKey : "", secretKey: v === "explicit" ? f.secretKey : "" }));

  const del = (id: string) => {
    const kids = childrenOf(id);
    let force = false;
    if (kids.length > 0) {
      if (!window.confirm(`${id} has incremental children (${kids.join(", ")}).\nForce-delete the whole chain?`)) return;
      force = true;
    } else if (!window.confirm(`Delete backup ${id}?`)) {
      return;
    }
    run(async () => {
      await backup.deleteBackup({ backupId: id, force });
      if (watch === id) setWatch(null);
      await load();
    });
  };

  return (
    <div style={{ display: "grid", gap: 16 }}>
      <TriggerForm form={form} set={set} setDestMode={setDestMode} backups={list} busy={busy} onSubmit={submit} />
      {watch && <BackupProgress backupId={watch} onClose={() => setWatch(null)} />}
      <Panel
        title="Backups"
        actions={
          <Button onClick={() => run(load)} disabled={busy}>
            Refresh
          </Button>
        }
      >
        {err && <InlineMessage tone="danger">{err}</InlineMessage>}
        {msg && <InlineMessage tone="success">{msg}</InlineMessage>}
        {busy && list.length === 0 ? (
          <Spinner />
        ) : list.length === 0 ? (
          <InlineMessage tone="info">No backups yet.</InlineMessage>
        ) : (
          <Table mono>
            <thead>
              <tr>
                <th>backup</th>
                <th>status</th>
                <th>kind</th>
                <th>planes</th>
                <th>size</th>
                <th>destination</th>
                <th>retain until</th>
                <th>started</th>
                <th>finished</th>
                <th />
              </tr>
            </thead>
            <tbody>
              {list.map((b) => {
                const row = summaryRow(b);
                return (
                  <tr key={row.id}>
                    <td>{row.id}</td>
                    <td>
                      <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
                        <Badge tone={row.statusTone} dot>
                          {row.statusLabel}
                        </Badge>
                        {row.partial && (
                          <span title={row.gaps.join("\n")} style={{ color: "var(--tone-warning, #b45309)", fontSize: 12 }}>
                            {row.gapsLabel}
                          </span>
                        )}
                      </div>
                    </td>
                    <td>{row.kind}</td>
                    <td>{row.planes}</td>
                    <td>{row.size}</td>
                    <td>{row.destination}</td>
                    <td>{row.retainUntil}</td>
                    <td>{row.started}</td>
                    <td>{row.finished}</td>
                    <td style={{ display: "flex", gap: 6 }}>
                      <Button onClick={() => setWatch(row.id)}>Watch</Button>
                      <Button variant="danger" onClick={() => del(row.id)} disabled={busy}>
                        Delete
                      </Button>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </Table>
        )}
      </Panel>
    </div>
  );
}

// TriggerForm builds a BeginBackupRequest from operator inputs and submits it.
function TriggerForm({
  form,
  set,
  setDestMode,
  backups,
  busy,
  onSubmit,
}: {
  form: BackupForm;
  set: <K extends keyof BackupForm>(k: K, v: BackupForm[K]) => void;
  setDestMode: (v: BackupForm["destMode"]) => void;
  backups: BackupSummary[];
  busy: boolean;
  onSubmit: () => void;
}) {
  const [help, setHelp] = useState<HelpKey | null>(null);
  // labelHelp renders a field label with a "?" affordance opening that option's help modal.
  const labelHelp = (text: string, topic: HelpKey) => (
    <span style={{ display: "flex", alignItems: "center", gap: 6 }}>
      <FieldLabel>{text}</FieldLabel>
      <HelpButton onClick={() => setHelp(topic)} />
    </span>
  );
  return (
    <Panel title="New backup" actions={<Button variant="primary" onClick={onSubmit} disabled={busy}>Start backup</Button>}>
      {help && (
        <Modal open title={backupHelp[help].title} onClose={() => setHelp(null)}>
          <div style={{ display: "grid", gap: 8, maxWidth: 520 }}>
            {backupHelp[help].paragraphs.map((p, i) => (
              <p key={i} style={{ margin: 0 }}>
                {p}
              </p>
            ))}
          </div>
        </Modal>
      )}
      <div style={{ display: "grid", gap: 10, maxWidth: 640 }}>
        <label>
          {labelHelp("Selection", "selection")}
          <Select value={form.selectionMode} onChange={(e) => set("selectionMode", e.target.value as BackupForm["selectionMode"])}>
            <option value="full">Full (everything)</option>
            <option value="subset">Subset (namespaces / graphs / vector collections)</option>
          </Select>
        </label>
        {form.selectionMode === "subset" && (
          <>
            <Input placeholder="namespaces (comma-separated)" value={form.namespaces} onChange={(e) => set("namespaces", e.target.value)} />
            <Input placeholder="graphs (comma-separated)" value={form.graphs} onChange={(e) => set("graphs", e.target.value)} />
            <Input placeholder="vector collections (comma-separated)" value={form.vectorCollections} onChange={(e) => set("vectorCollections", e.target.value)} />
          </>
        )}
        <label>
          {labelHelp("Planes", "planes")}
          <Select value={form.planesMode} onChange={(e) => set("planesMode", e.target.value as BackupForm["planesMode"])}>
            <option value="logical">Logical</option>
            <option value="physical">Physical</option>
            <option value="both">Logical + Physical</option>
          </Select>
        </label>
        <label>
          {labelHelp("Type", "type")}
          <Select value={form.parent} onChange={(e) => set("parent", e.target.value)}>
            <option value="">Full</option>
            {backups.map((b) => (
              <option key={b.backupId} value={b.backupId}>
                Incremental ← {b.backupId}
              </option>
            ))}
          </Select>
        </label>
        <label>
          {labelHelp("Destination", "destination")}
          <Select value={form.destMode} onChange={(e) => setDestMode(e.target.value as BackupForm["destMode"])}>
            <option value="default">Default (node config)</option>
            <option value="named">Named</option>
            <option value="explicit">Explicit (ad-hoc bucket)</option>
          </Select>
        </label>
        {form.destMode === "named" && (
          <Input placeholder="destination name" value={form.destName} onChange={(e) => set("destName", e.target.value)} />
        )}
        {form.destMode === "explicit" && (
          <>
            <Input placeholder="bucket" value={form.bucket} onChange={(e) => set("bucket", e.target.value)} />
            <Input placeholder="prefix (optional)" value={form.prefix} onChange={(e) => set("prefix", e.target.value)} />
            <Input placeholder="region" value={form.region} onChange={(e) => set("region", e.target.value)} />
            <Input placeholder="endpoint host:port" value={form.endpoint} onChange={(e) => set("endpoint", e.target.value)} />
            <Checkbox label="use SSL" checked={form.useSsl} onChange={(e) => set("useSsl", e.target.checked)} />
            <Checkbox label="path-style" checked={form.usePathStyle} onChange={(e) => set("usePathStyle", e.target.checked)} />
            <Input placeholder="credential reference (secret name) — preferred" value={form.secretRef} onChange={(e) => set("secretRef", e.target.value)} />
            <InlineMessage tone="info">
              Inline credentials below are sent only with this request (never stored or logged). Leave
              blank to use the credential reference. A node in named-only mode rejects inline creds.
            </InlineMessage>
            <Input type="password" placeholder="access key (transient)" value={form.accessKey} onChange={(e) => set("accessKey", e.target.value)} />
            <Input type="password" placeholder="secret key (transient)" value={form.secretKey} onChange={(e) => set("secretKey", e.target.value)} />
          </>
        )}
      </div>
    </Panel>
  );
}

// BackupProgress polls BackupStatus for one backup and renders its phase, overall %, and per-node
// breakdown. It polls every 3s while the backup is RUNNING and STOPS (clears the interval) once the
// status is terminal — the canonical TierPanel poll pattern.
function BackupProgress({ backupId, onClose }: { backupId: string; onClose: () => void }) {
  const [state, setState] = useState<BackupState | null>(null);
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    let live = true;
    let id: ReturnType<typeof setInterval> | undefined;
    const load = () => {
      backup
        .backupStatus({ backupId })
        .then((s) => {
          if (!live) return;
          setState(s);
          setErr(null);
          if (id !== undefined && isTerminal(s.status)) {
            clearInterval(id); // terminal → stop polling
            id = undefined;
          }
        })
        .catch((e: unknown) => {
          if (live) setErr(e instanceof Error ? e.message : String(e));
        });
    };
    load();
    id = setInterval(load, 3000);
    return () => {
      live = false;
      if (id !== undefined) clearInterval(id);
    };
  }, [backupId]);

  return (
    <Panel title={`Progress · ${backupId}`} actions={<Button onClick={onClose}>Close</Button>}>
      {err && <InlineMessage tone="danger">{err}</InlineMessage>}
      {!state ? (
        <Spinner />
      ) : (
        <>
          <div style={{ display: "flex", gap: 12, alignItems: "center", marginBottom: 8 }}>
            <Badge tone={statusTone(state.status)} dot>
              {statusLabel(state.status)}
            </Badge>
            <span>phase: {phaseLabel(state.phase)}</span>
            <span>overall: {pctLabel(state.overallPct)}</span>
            {state.destination?.bucket && <span>→ {state.destination.bucket}</span>}
          </div>
          {state.gaps.length > 0 && (
            <InlineMessage tone="warning">PARTIAL — uncovered ranges: {state.gaps.join(", ")}</InlineMessage>
          )}
          <Table mono>
            <thead>
              <tr>
                <th>node</th>
                <th>phase</th>
                <th>objects</th>
                <th>bytes</th>
                <th>done</th>
              </tr>
            </thead>
            <tbody>
              {state.perNode.map((n) => (
                <tr key={n.memberId}>
                  <td>{n.memberId}</td>
                  <td>{phaseLabel(n.phase)}</td>
                  <td>{n.objects.toString()}</td>
                  <td>{fmtBytes(n.bytes)}</td>
                  <td>{n.done ? <Badge tone="success">done</Badge> : <Badge tone="neutral">…</Badge>}</td>
                </tr>
              ))}
            </tbody>
          </Table>
        </>
      )}
    </Panel>
  );
}
