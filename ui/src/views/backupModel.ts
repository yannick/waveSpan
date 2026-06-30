// Pure presentation/derivation helpers for the Backups view, kept separate from the React component so
// they are unit-testable with vitest (the repo has no DOM test harness; logic lives here, the JSX shell
// stays thin — mirrors src/lib/valuecodec.ts + its test).
import { BackupStatus, BackupPhase, BackupPlane } from "../gen/wavespan/v1/backup_pb";

export type Tone = "neutral" | "success" | "warning" | "danger" | "info";

// statusLabel maps the backup status enum to its short display label.
export function statusLabel(s: BackupStatus): string {
  switch (s) {
    case BackupStatus.BACKUP_RUNNING:
      return "RUNNING";
    case BackupStatus.BACKUP_COMPLETE:
      return "COMPLETE";
    case BackupStatus.BACKUP_PARTIAL:
      return "PARTIAL";
    case BackupStatus.BACKUP_FAILED:
      return "FAILED";
    default:
      return "—";
  }
}

// statusTone maps a status to a Badge tone: complete=success, partial=warning, failed=danger,
// running=info.
export function statusTone(s: BackupStatus): Tone {
  switch (s) {
    case BackupStatus.BACKUP_COMPLETE:
      return "success";
    case BackupStatus.BACKUP_PARTIAL:
      return "warning";
    case BackupStatus.BACKUP_FAILED:
      return "danger";
    case BackupStatus.BACKUP_RUNNING:
      return "info";
    default:
      return "neutral";
  }
}

// isRunning reports whether a backup is still in progress (poll while true).
export function isRunning(s: BackupStatus): boolean {
  return s === BackupStatus.BACKUP_RUNNING;
}

// isTerminal reports whether a backup has reached a final state (stop polling).
export function isTerminal(s: BackupStatus): boolean {
  return (
    s === BackupStatus.BACKUP_COMPLETE ||
    s === BackupStatus.BACKUP_PARTIAL ||
    s === BackupStatus.BACKUP_FAILED
  );
}

// kindLabel distinguishes a full backup from an incremental (which records its parent).
export function kindLabel(parent: string): string {
  return parent ? `incremental ← ${parent}` : "full";
}

// phaseLabel maps the coordinator phase enum to a label.
export function phaseLabel(p: BackupPhase): string {
  switch (p) {
    case BackupPhase.ASSIGN:
      return "assign";
    case BackupPhase.PREPARE:
      return "prepare";
    case BackupPhase.EXPORT:
      return "export";
    case BackupPhase.COMMIT:
      return "commit";
    default:
      return "—";
  }
}

// planesLabel renders the export planes ("logical", "physical", or "logical+physical").
export function planesLabel(planes: BackupPlane[]): string {
  const names = planes
    .map((p) => (p === BackupPlane.PHYSICAL ? "physical" : p === BackupPlane.LOGICAL ? "logical" : ""))
    .filter((n) => n !== "");
  return names.length > 0 ? names.join("+") : "—";
}

// fmtTime renders an epoch-millis timestamp (bigint) as a locale string, or "—" when unset (0).
export function fmtTime(ms: bigint): string {
  if (ms === 0n) return "—";
  return new Date(Number(ms)).toLocaleString();
}

// pctLabel renders an overall-percent value (0..100) as a rounded percentage.
export function pctLabel(pct: number): string {
  return `${Math.round(pct)}%`;
}

// fmtBytes renders a byte count (bigint) in human units.
export function fmtBytes(n: bigint): string {
  const units = ["B", "KiB", "MiB", "GiB", "TiB"];
  let v = Number(n);
  let i = 0;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i++;
  }
  return `${i === 0 ? v : v.toFixed(1)} ${units[i]}`;
}
