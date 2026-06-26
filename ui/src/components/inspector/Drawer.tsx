import { useState, type ReactNode } from "react";
import { Badge, Button, InlineMessage } from "../index";
import type { InspectHolder } from "../../gen/wavespan/v1/observability_pb";
import type { EdgeRecord, NodeRecord } from "../../gen/wavespan/v1/cypher_pb";
import type { Version } from "../../gen/wavespan/v1/common_pb";
import { KvBody } from "./KvBody";
import { CollectionBody } from "./CollectionBody";
import { GraphBody } from "./GraphBody";
import { VectorBody } from "./VectorBody";

// DrawerTarget is the discriminated union the workbench (and Cypher / Node Explorer) pass to open the
// inspector on a concrete thing. Each kind selects a body and carries exactly the data that body needs.
export type DrawerTarget =
  | {
      kind: "kv";
      namespace: string;
      key: Uint8Array;
      value: Uint8Array;
      version?: Version;
      tombstone: boolean;
      expiresAtUnixMs?: bigint;
      holders: InspectHolder[];
      keyLabel: string;
    }
  | { kind: "kv-new"; namespace: string }
  | { kind: "collection"; namespace: string; collection: string; ctype: "set" | "hash" | "zset" }
  | { kind: "graph-node"; graphId: string; record: NodeRecord }
  | { kind: "graph-edge"; graphId: string; record: EdgeRecord }
  | {
      kind: "vector";
      name: string;
      dims: number;
      dtype: string;
      values: number[];
      metadata: Record<string, import("../../gen/wavespan/v1/cypher_pb").Value>;
    };

interface DrawerProps {
  target: DrawerTarget;
  onClose: () => void;
  /** Called after a successful save or delete so the host can refresh its results. */
  onSaved: () => void;
}

function fmtVersion(v: Version | undefined): string {
  return v ? `${v.hlcPhysicalMs}.${v.hlcLogical}@${v.writerMemberId}` : "—";
}

// ---- shared chrome used by the Drawer shell and reused descriptors per kind ----

function kindBadge(target: DrawerTarget): ReactNode {
  switch (target.kind) {
    case "kv":
    case "kv-new":
      return <Badge tone="primary">KV</Badge>;
    case "collection":
      return <Badge tone="olive">{target.ctype}</Badge>;
    case "graph-node":
      return <Badge tone="purple">node</Badge>;
    case "graph-edge":
      return <Badge tone="purple">edge</Badge>;
    case "vector":
      return <Badge tone="info">vector</Badge>;
  }
}

function identity(target: DrawerTarget): string {
  switch (target.kind) {
    case "kv":
      return target.keyLabel;
    case "kv-new":
      return "new key";
    case "collection":
      return `${target.namespace} / ${target.collection}`;
    case "graph-node":
      return target.record.nodeId || "(new node)";
    case "graph-edge":
      return target.record.edgeId || "(new edge)";
    case "vector":
      return target.name;
  }
}

function scope(target: DrawerTarget): ReactNode {
  switch (target.kind) {
    case "kv":
      return (
        <>
          <Meta label="version" value={fmtVersion(target.version)} />
          <Meta label="holders" value={target.holders.map((h) => h.memberId).join(", ") || "—"} />
          {target.tombstone && <Badge tone="danger">tombstone</Badge>}
        </>
      );
    case "graph-node":
    case "graph-edge":
      return <Meta label="version" value={fmtVersion(target.record.version)} />;
    case "collection":
      return <Meta label="namespace" value={target.namespace} />;
    case "vector":
      return <Meta label="dims" value={`${target.dims} · ${target.dtype}`} />;
    default:
      return null;
  }
}

function Meta({ label, value }: { label: string; value: string }) {
  return (
    <span className="ws-caption">
      <span className="ws-muted">{label}:</span> <span className="ws-mono">{value}</span>
    </span>
  );
}

// InspectorDrawer is a right-anchored, type-aware view+editor panel. It renders inline within the host
// view (no portal) for simplicity, owns the header/footer chrome, and dispatches to a body by kind. The
// Save→diff→confirm and Delete→confirm flows live in the bodies (each type validates differently); the
// bodies report errors and call onSaved on success.
export function InspectorDrawer({ target, onClose, onSaved }: DrawerProps) {
  let body: ReactNode;
  switch (target.kind) {
    case "kv":
    case "kv-new":
      body = <KvBody target={target} onSaved={onSaved} />;
      break;
    case "collection":
      body = <CollectionBody target={target} />;
      break;
    case "graph-node":
    case "graph-edge":
      body = <GraphBody target={target} onSaved={onSaved} />;
      break;
    case "vector":
      body = <VectorBody target={target} />;
      break;
  }

  return (
    <aside
      className="ws-card"
      style={{
        width: 420,
        flex: "0 0 420px",
        alignSelf: "flex-start",
        position: "sticky",
        top: "var(--ws-space-md)",
        maxHeight: "calc(100vh - 120px)",
        overflowY: "auto",
        display: "flex",
        flexDirection: "column",
        gap: "var(--ws-space-md)",
      }}
    >
      <div style={{ display: "flex", alignItems: "center", gap: "var(--ws-space-sm)" }}>
        {kindBadge(target)}
        <span className="ws-title-sm ws-mono" style={{ flex: 1, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }} title={identity(target)}>
          {identity(target)}
        </span>
        <Button variant="ghost" size="sm" onClick={onClose} title="close">
          ✕
        </Button>
      </div>
      <div style={{ display: "flex", flexWrap: "wrap", gap: "var(--ws-space-sm)", alignItems: "center" }}>{scope(target)}</div>

      <div>{body}</div>
    </aside>
  );
}

// ---- shared editor scaffolding the bodies use ----

interface SaveDeleteBarProps {
  canSave: boolean;
  saving: boolean;
  onSave: () => void;
  /** omit to hide the delete affordance (e.g. kv-new). */
  onDelete?: () => void;
  deleting?: boolean;
  deleteLabel?: string;
}

/** A footer bar with a confirming Delete (danger) + a Save (primary). */
export function SaveDeleteBar({ canSave, saving, onSave, onDelete, deleting, deleteLabel = "Delete" }: SaveDeleteBarProps) {
  const [confirmDelete, setConfirmDelete] = useState(false);
  return (
    <div style={{ display: "flex", gap: "var(--ws-space-sm)", alignItems: "center", marginTop: "var(--ws-space-sm)" }}>
      {onDelete &&
        (confirmDelete ? (
          <>
            <Button variant="danger" size="sm" disabled={deleting} onClick={onDelete}>
              {deleting ? "Deleting…" : "Confirm delete"}
            </Button>
            <Button variant="ghost" size="sm" onClick={() => setConfirmDelete(false)}>
              Cancel
            </Button>
          </>
        ) : (
          <Button variant="danger" size="sm" onClick={() => setConfirmDelete(true)} disabled={deleting}>
            {deleteLabel}
          </Button>
        ))}
      <div style={{ flex: 1 }} />
      <Button variant="primary" onClick={onSave} disabled={!canSave || saving}>
        {saving ? "Saving…" : "Save"}
      </Button>
    </div>
  );
}

interface DiffConfirmProps {
  oldText: string;
  newText: string;
  onConfirm: () => void;
  onCancel: () => void;
  saving: boolean;
}

/** A small old→new diff with an explicit Confirm, shown after Save validates. */
export function DiffConfirm({ oldText, newText, onConfirm, onCancel, saving }: DiffConfirmProps) {
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "var(--ws-space-xs)", marginTop: "var(--ws-space-sm)" }}>
      <InlineMessage tone="warning">Confirm this change:</InlineMessage>
      <div className="ws-caption ws-muted">old</div>
      <pre className="ws-mono" style={preStyle}>{oldText || "(empty)"}</pre>
      <div className="ws-caption ws-muted">new</div>
      <pre className="ws-mono" style={preStyle}>{newText || "(empty)"}</pre>
      <div style={{ display: "flex", gap: "var(--ws-space-sm)" }}>
        <Button variant="primary" size="sm" onClick={onConfirm} disabled={saving}>
          {saving ? "Saving…" : "Confirm save"}
        </Button>
        <Button variant="ghost" size="sm" onClick={onCancel} disabled={saving}>
          Cancel
        </Button>
      </div>
    </div>
  );
}

const preStyle = {
  margin: 0,
  padding: "var(--ws-space-xs)",
  background: "var(--ws-color-surface-alt)",
  border: "var(--ws-stroke-hairline) solid var(--ws-color-border)",
  borderRadius: "var(--ws-radius-sm)",
  fontSize: "var(--ws-text-body-sm-size)",
  whiteSpace: "pre-wrap" as const,
  wordBreak: "break-all" as const,
  maxHeight: 160,
  overflow: "auto",
};
