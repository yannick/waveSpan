// Pure presentation/derivation helpers for the Backups view, kept separate from the React component so
// they are unit-testable with vitest (the repo has no DOM test harness; logic lives here, the JSX shell
// stays thin — mirrors src/lib/valuecodec.ts + its test).
import type { MessageInitShape } from "@bufbuild/protobuf";
import { BackupStatus, BackupPhase, BackupPlane, BeginBackupRequestSchema } from "../gen/wavespan/v1/backup_pb";
import type { BackupSummary, Destination, ListDestinationsResult } from "../gen/wavespan/v1/backup_pb";

export type Tone = "neutral" | "success" | "warning" | "danger" | "info";

// In-UI help for the four trigger-form option groups (mirrors the Backup & Restore docs page). Kept as
// pure data so it is unit-testable and the JSX stays a thin Modal shell. Each topic is a title plus a few
// short paragraphs (rendered as text).
export type HelpKey = "selection" | "planes" | "destination" | "type";

export interface HelpTopic {
  title: string;
  paragraphs: string[];
}

export const backupHelp: Record<HelpKey, HelpTopic> = {
  selection: {
    title: "Selection — what to back up",
    paragraphs: [
      "Full (the default) captures everything.",
      "Subset narrows to any combination of namespaces (KV), graphs, and vector collections — use it to extract just part of the cluster.",
      "Cluster/system configuration is always included regardless of selection.",
    ],
  },
  planes: {
    title: "Planes — how it is captured",
    paragraphs: [
      "Logical: record-level (key/value/version) streams. Portable and re-shardable (restore into a different shard count), full-only, and carries the cluster-wide consistency cut.",
      "Physical: per-node SSTable checkpoints. Same-shape (restore to the same topology) and support incrementals.",
      "Pick logical, physical, or both — both gives a portable snapshot plus a fast physical restore from one run.",
    ],
  },
  destination: {
    title: "Destination — where it is written",
    paragraphs: [
      "Default: the node's configured store (WAVESPAN_BACKUP_* env + credentials from a Secret).",
      "Named: an operator-pre-registered alternate selected by name (backup.namedDestinations), with its own credentials — works in named-only mode.",
      "Explicit: an ad-hoc bucket/prefix/region/endpoint with a credential reference or transient inline keys (inline requires allowInlineDestinationCreds).",
      "Credentials are never persisted or logged — the catalog stores only a non-secret descriptor plus a credential reference.",
    ],
  },
  type: {
    title: "Type — full vs incremental",
    paragraphs: [
      "Full: standalone; restores on its own. Logical backups are always full.",
      "Incremental: physical-only. Pick a parent backup id; only SSTables new since the parent are uploaded. Backups chain full → incremental → incremental, and a restore replays the chain.",
    ],
  },
};

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

export type PlanesMode = "logical" | "physical" | "both";

// planesFromMode maps the form's plane selector to the proto plane list.
export function planesFromMode(m: PlanesMode): BackupPlane[] {
  switch (m) {
    case "physical":
      return [BackupPlane.PHYSICAL];
    case "both":
      return [BackupPlane.LOGICAL, BackupPlane.PHYSICAL];
    default:
      return [BackupPlane.LOGICAL];
  }
}

// splitCsv parses a comma/whitespace-separated list into trimmed, non-empty entries.
export function splitCsv(s: string): string[] {
  return s
    .split(/[,\s]+/)
    .map((x) => x.trim())
    .filter((x) => x !== "");
}

// BackupForm is the trigger form's input state.
export interface BackupForm {
  selectionMode: "full" | "subset";
  namespaces: string;
  graphs: string;
  vectorCollections: string;
  planesMode: PlanesMode;
  parent: string; // "" = full backup; otherwise an incremental's parent backup id
  destMode: "default" | "named" | "explicit";
  destName: string; // named destination
  bucket: string;
  prefix: string;
  region: string;
  endpoint: string;
  useSsl: boolean;
  usePathStyle: boolean;
  secretRef: string; // explicit destination: a server-side credential reference (preferred)
  accessKey: string; // explicit destination: transient inline credential (request-only, never stored)
  secretKey: string;
}

// buildBeginRequest builds the BeginBackupRequest from the form. Selection is omitted for a full backup;
// the destination is omitted (→ node default), a {name}, or an explicit {bucket,...,credential}. Inline
// credentials, when supplied, ride in the request only (the caller must not retain them).
export function buildBeginRequest(f: BackupForm): MessageInitShape<typeof BeginBackupRequestSchema> {
  const spec: NonNullable<MessageInitShape<typeof BeginBackupRequestSchema>["spec"]> = {
    planes: planesFromMode(f.planesMode),
    parent: f.parent,
  };
  if (f.selectionMode === "subset") {
    spec.selection = {
      namespaces: splitCsv(f.namespaces),
      graphs: splitCsv(f.graphs),
      vectorCollections: splitCsv(f.vectorCollections),
    };
  }
  if (f.destMode === "named") {
    spec.destination = { name: f.destName };
  } else if (f.destMode === "explicit") {
    const credential =
      f.accessKey || f.secretKey
        ? { accessKey: f.accessKey, secretKey: f.secretKey }
        : f.secretRef
          ? { secretName: f.secretRef }
          : undefined;
    spec.destination = {
      bucket: f.bucket,
      prefix: f.prefix,
      region: f.region,
      endpoint: f.endpoint,
      useSsl: f.useSsl,
      usePathStyle: f.usePathStyle,
      credential,
    };
  }
  return { spec };
}

// emptyForm is the default trigger-form state (a full logical backup to the default destination).
export function emptyForm(): BackupForm {
  return {
    selectionMode: "full",
    namespaces: "",
    graphs: "",
    vectorCollections: "",
    planesMode: "logical",
    parent: "",
    destMode: "default",
    destName: "",
    bucket: "",
    prefix: "",
    region: "",
    endpoint: "",
    useSsl: true,
    usePathStyle: false,
    secretRef: "",
    accessKey: "",
    secretKey: "",
  };
}

// defaultDestLabel describes where the DEFAULT destination writes, for the trigger form's "Default"
// option — the bucket [@ endpoint], or the local FS fallback in dev. Reads only descriptor fields.
export function defaultDestLabel(d: ListDestinationsResult | null | undefined): string {
  if (!d) return "…";
  const dd = d.defaultDestination;
  if (d.defaultIsFs || !dd || !dd.bucket) return "local filesystem (dev)";
  return dd.endpoint ? `${dd.bucket} @ ${dd.endpoint}` : dd.bucket;
}

// NamedOption is a dropdown entry for a configured named destination (value = the name to send).
export interface NamedOption {
  value: string;
  label: string;
}

// namedOptions maps the configured named destinations to dropdown entries (name — bucket). No creds.
export function namedOptions(d: ListDestinationsResult | null | undefined): NamedOption[] {
  if (!d) return [];
  return d.named.map((n) => ({ value: n.name, label: n.bucket ? `${n.name} — ${n.bucket}` : n.name }));
}

// destinationLabel renders a backup's destination for the list: the bucket (with prefix), a named
// destination, or "default" (node config). It reads ONLY the non-secret descriptor fields — never the
// credential — so the list can never surface a secret.
export function destinationLabel(d: Destination | undefined): string {
  if (!d) return "default";
  if (d.bucket) return d.prefix ? `${d.bucket}/${d.prefix}` : d.bucket;
  if (d.name) return d.name;
  return "default";
}

// gapsLabel summarizes a PARTIAL backup's coverage gaps as a compact count ("" when none).
export function gapsLabel(gaps: string[]): string {
  if (gaps.length === 0) return "";
  return `${gaps.length} gap${gaps.length === 1 ? "" : "s"}`;
}

// BackupRow is the display-only projection of a BackupSummary for the list table. It is all strings/flags
// (never the raw summary), so credentials cannot leak into the rendered list.
export interface BackupRow {
  id: string;
  statusLabel: string;
  statusTone: Tone;
  kind: string;
  planes: string;
  size: string;
  destination: string;
  retainUntil: string;
  started: string;
  finished: string;
  partial: boolean;
  gaps: string[];
  gapsLabel: string;
}

// summaryRow projects a BackupSummary to its display row (pure — the JSX just renders these fields).
export function summaryRow(s: BackupSummary): BackupRow {
  return {
    id: s.backupId,
    statusLabel: statusLabel(s.status),
    statusTone: statusTone(s.status),
    kind: kindLabel(s.parent),
    planes: planesLabel(s.planes),
    size: fmtBytes(s.sizeBytes),
    destination: destinationLabel(s.destination),
    retainUntil: fmtTime(s.retainUntilMs),
    started: fmtTime(s.startedMs),
    finished: fmtTime(s.finishedMs),
    partial: s.partial,
    gaps: s.gaps,
    gapsLabel: gapsLabel(s.gaps),
  };
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
