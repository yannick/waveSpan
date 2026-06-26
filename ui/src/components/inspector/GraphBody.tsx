import { useState } from "react";
import { obs } from "../../transport";
import type { EdgeRecord, NodeRecord } from "../../gen/wavespan/v1/cypher_pb";
import { Button, Chip, InlineMessage, Input } from "../index";
import { TypedRows } from "./TypedRows";
import { rowsToProperties, type TypedRow, valueToRow } from "./value";
import { SaveDeleteBar, type DrawerTarget } from "./Drawer";

type GraphTarget = Extract<DrawerTarget, { kind: "graph-node" }> | Extract<DrawerTarget, { kind: "graph-edge" }>;

const labelStyle = { fontWeight: 700 as const, fontSize: "var(--ws-text-body-sm-size)" };

function propsToRows(properties: { [k: string]: import("../../gen/wavespan/v1/cypher_pb").Value }): TypedRow[] {
  return Object.entries(properties).map(([k, v]) => valueToRow(k, v));
}

// GraphBody views & edits a graph node or edge: labels as add/remove chips (nodes), properties via the
// typed-rows editor, and start→end+type for edges (type editable for a new edge). Save upserts via
// AdminPutGraph; Delete tombstones the same record. Complex property values are preserved verbatim.
export function GraphBody({ target, onSaved }: { target: GraphTarget; onSaved: () => void }) {
  const isNode = target.kind === "graph-node";
  const rec = target.record;
  const isNew = (isNode ? (rec as NodeRecord).nodeId : (rec as EdgeRecord).edgeId) === "";

  const [labels, setLabels] = useState<string[]>(isNode ? (rec as NodeRecord).labels : []);
  const [newLabel, setNewLabel] = useState("");
  const [rows, setRows] = useState<TypedRow[]>(() => propsToRows(rec.properties));
  const [edgeType, setEdgeType] = useState(isNode ? "" : (rec as EdgeRecord).type);
  const [error, setError] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);
  const [deleting, setDeleting] = useState(false);

  const addLabel = () => {
    const l = newLabel.trim();
    if (l && !labels.includes(l)) setLabels([...labels, l]);
    setNewLabel("");
  };

  const buildAndSave = async (tombstone: boolean) => {
    setError(null);
    let properties: ReturnType<typeof rowsToProperties>;
    try {
      properties = rowsToProperties(rows);
    } catch (e) {
      setError((e as Error).message);
      return;
    }
    const setBusy = tombstone ? setDeleting : setSaving;
    setBusy(true);
    try {
      if (isNode) {
        const n = rec as NodeRecord;
        const res = await obs.adminPutGraph({
          graphId: target.graphId,
          target: { case: "node", value: { graphId: target.graphId, nodeId: n.nodeId, labels, properties, tombstone } },
          delete: tombstone,
          targetMemberId: "",
        });
        if (!res.ok) {
          setError(res.error || "graph write failed");
          return;
        }
      } else {
        const e = rec as EdgeRecord;
        const res = await obs.adminPutGraph({
          graphId: target.graphId,
          target: {
            case: "edge",
            value: { graphId: target.graphId, edgeId: e.edgeId, startNode: e.startNode, endNode: e.endNode, type: edgeType, properties, tombstone },
          },
          delete: tombstone,
          targetMemberId: "",
        });
        if (!res.ok) {
          setError(res.error || "graph write failed");
          return;
        }
      }
      onSaved();
    } catch (e) {
      setError(String(e));
    } finally {
      setBusy(false);
    }
  };

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "var(--ws-space-md)" }}>
      {!isNode && (
        <div className="ws-caption">
          <span className="ws-mono">{(rec as EdgeRecord).startNode}</span>
          {" → "}
          <span className="ws-mono">{(rec as EdgeRecord).endNode}</span>
          <div style={{ marginTop: "var(--ws-space-xs)", display: "flex", alignItems: "center", gap: "var(--ws-space-xs)" }}>
            <span style={labelStyle}>type</span>
            <Input value={edgeType} onChange={(e) => setEdgeType(e.target.value)} mono disabled={!isNew} style={{ flex: 1 }} />
          </div>
          {!isNew && <span className="ws-caption ws-muted">edge type is read-only for existing edges</span>}
        </div>
      )}

      {isNode && (
        <div>
          <div style={labelStyle}>Labels</div>
          <div style={{ display: "flex", flexWrap: "wrap", gap: "var(--ws-space-xs)", margin: "var(--ws-space-xs) 0" }}>
            {labels.map((l) => (
              <Chip key={l} onClick={() => setLabels(labels.filter((x) => x !== l))} title="remove label">
                {l} ✕
              </Chip>
            ))}
            {labels.length === 0 && <span className="ws-caption ws-muted">no labels</span>}
          </div>
          <div style={{ display: "flex", gap: "var(--ws-space-xs)" }}>
            <Input
              value={newLabel}
              onChange={(e) => setNewLabel(e.target.value)}
              onKeyDown={(e) => e.key === "Enter" && addLabel()}
              placeholder="add label"
              mono
              style={{ flex: 1 }}
            />
            <Button variant="ghost" size="sm" onClick={addLabel}>
              + add
            </Button>
          </div>
        </div>
      )}

      <div>
        <div style={labelStyle}>Properties</div>
        <div style={{ marginTop: "var(--ws-space-xs)" }}>
          <TypedRows rows={rows} onChange={setRows} />
        </div>
      </div>

      {error && <InlineMessage tone="danger"><span className="ws-mono">{error}</span></InlineMessage>}

      <SaveDeleteBar
        canSave={!saving}
        saving={saving}
        onSave={() => void buildAndSave(false)}
        onDelete={isNew ? undefined : () => void buildAndSave(true)}
        deleting={deleting}
      />
    </div>
  );
}
