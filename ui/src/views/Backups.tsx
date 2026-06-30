import { useEffect, useState } from "react";
import { backup } from "../transport";
import { Badge, Button, Checkbox, FieldLabel, InlineMessage, Input, Panel, Select, Spinner, Table } from "../components";
import type { BackupState, BackupSummary } from "../gen/wavespan/v1/backup_pb";
import {
  buildBeginRequest,
  emptyForm,
  fmtBytes,
  fmtTime,
  isTerminal,
  kindLabel,
  phaseLabel,
  pctLabel,
  statusLabel,
  statusTone,
  type BackupForm,
} from "./backupModel";

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
      const r = await backup.beginBackup(req);
      setForm((f) => ({ ...f, accessKey: "", secretKey: "" })); // drop transient creds from UI state
      setMsg(`started ${r.backupId}`);
      setWatch(r.backupId);
      await load();
    });

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
      <TriggerForm form={form} set={set} backups={list} busy={busy} onSubmit={submit} />
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
                <th>started</th>
                <th>finished</th>
                <th />
              </tr>
            </thead>
            <tbody>
              {list.map((b) => (
                <tr key={b.backupId}>
                  <td>{b.backupId}</td>
                  <td>
                    <Badge tone={statusTone(b.status)} dot>
                      {statusLabel(b.status)}
                    </Badge>
                  </td>
                  <td>{kindLabel(b.parent)}</td>
                  <td>{fmtTime(b.startedMs)}</td>
                  <td>{fmtTime(b.finishedMs)}</td>
                  <td style={{ display: "flex", gap: 6 }}>
                    <Button onClick={() => setWatch(b.backupId)}>Watch</Button>
                    <Button variant="danger" onClick={() => del(b.backupId)} disabled={busy}>
                      Delete
                    </Button>
                  </td>
                </tr>
              ))}
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
  backups,
  busy,
  onSubmit,
}: {
  form: BackupForm;
  set: <K extends keyof BackupForm>(k: K, v: BackupForm[K]) => void;
  backups: BackupSummary[];
  busy: boolean;
  onSubmit: () => void;
}) {
  return (
    <Panel title="New backup" actions={<Button variant="primary" onClick={onSubmit} disabled={busy}>Start backup</Button>}>
      <div style={{ display: "grid", gap: 10, maxWidth: 640 }}>
        <label>
          <FieldLabel>Selection</FieldLabel>
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
          <FieldLabel>Planes</FieldLabel>
          <Select value={form.planesMode} onChange={(e) => set("planesMode", e.target.value as BackupForm["planesMode"])}>
            <option value="logical">Logical</option>
            <option value="physical">Physical</option>
            <option value="both">Logical + Physical</option>
          </Select>
        </label>
        <label>
          <FieldLabel>Type</FieldLabel>
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
          <FieldLabel>Destination</FieldLabel>
          <Select value={form.destMode} onChange={(e) => set("destMode", e.target.value as BackupForm["destMode"])}>
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
            <Input placeholder="access key (transient)" value={form.accessKey} onChange={(e) => set("accessKey", e.target.value)} />
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
