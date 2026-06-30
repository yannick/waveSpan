import { useEffect, useState } from "react";
import { backup } from "../transport";
import { Badge, Button, InlineMessage, Panel, Spinner, Table } from "../components";
import type { BackupState, BackupSummary } from "../gen/wavespan/v1/backup_pb";
import {
  fmtBytes,
  fmtTime,
  isTerminal,
  kindLabel,
  phaseLabel,
  pctLabel,
  statusLabel,
  statusTone,
} from "./backupModel";

// Backups is the admin console for cluster backups (design/backup §11): it lists known backups and
// watches an in-progress backup's live per-node progress, all via the BackupService Connect endpoint
// (admin-auth enforced). Object-store credentials are never present in any of these responses.
export function Backups() {
  const [list, setList] = useState<BackupSummary[]>([]);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [watch, setWatch] = useState<string | null>(null);

  const load = async () => {
    const r = await backup.listBackups({});
    setList(r.backups);
  };

  const run = async (fn: () => Promise<void>) => {
    setBusy(true);
    setErr(null);
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

  return (
    <div style={{ display: "grid", gap: 16 }}>
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
                  <td>
                    <Button onClick={() => setWatch(b.backupId)}>Watch</Button>
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
    <Panel
      title={`Progress · ${backupId}`}
      actions={<Button onClick={onClose}>Close</Button>}
    >
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
            <InlineMessage tone="warning">
              PARTIAL — uncovered ranges: {state.gaps.join(", ")}
            </InlineMessage>
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
