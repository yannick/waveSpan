import { useEffect, useState } from "react";
import { backup } from "../transport";
import { Badge, Button, InlineMessage, Panel, Spinner, Table } from "../components";
import type { BackupSummary } from "../gen/wavespan/v1/backup_pb";
import { fmtTime, kindLabel, statusLabel, statusTone } from "./backupModel";

// Backups is the admin console for cluster backups (design/backup §11): it lists known backups via the
// BackupService Connect endpoint (admin-auth enforced). Per-backup planes/destination/gaps + live
// progress come from BackupStatus (the progress panel, Task 3) — the list summary carries only
// id/status/kind/times. Object-store credentials are never present in any of these responses.
export function Backups() {
  const [list, setList] = useState<BackupSummary[]>([]);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);

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
              </tr>
            ))}
          </tbody>
        </Table>
      )}
    </Panel>
  );
}
