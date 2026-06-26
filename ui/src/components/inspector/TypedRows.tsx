import { Badge, Button, Input, Select } from "../index";
import { emptyRow, type RowType, type TypedRow } from "./value";

interface TypedRowsProps {
  rows: TypedRow[];
  onChange: (rows: TypedRow[]) => void;
  /** when true the whole editor is read-only (e.g. vector metadata in v1). */
  readOnly?: boolean;
}

const labelStyle = { fontWeight: 700 as const, fontSize: "var(--ws-text-body-sm-size)" };

// TypedRows edits a property map as key / type / value rows mapping to the proto Value scalar oneof.
// Non-scalar values (bytes/list/map/null) come in as read-only rows that are preserved on save.
export function TypedRows({ rows, onChange, readOnly = false }: TypedRowsProps) {
  const set = (i: number, patch: Partial<TypedRow>) => {
    const next = rows.slice();
    next[i] = { ...next[i], ...patch };
    onChange(next);
  };
  const remove = (i: number) => onChange(rows.filter((_, j) => j !== i));
  const add = () => onChange([...rows, emptyRow()]);

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "var(--ws-space-xs)" }}>
      {rows.length === 0 && <span className="ws-caption ws-muted">no properties</span>}
      {rows.map((row, i) => (
        <div key={i} style={{ display: "flex", gap: "var(--ws-space-xs)", alignItems: "center" }}>
          <Input
            value={row.key}
            onChange={(e) => set(i, { key: e.target.value })}
            placeholder="key"
            mono
            disabled={readOnly || !row.editable}
            style={{ width: 130 }}
          />
          {row.editable ? (
            <>
              <Select
                value={row.type}
                onChange={(e) => set(i, { type: e.target.value as RowType })}
                disabled={readOnly}
                style={{ width: 90 }}
              >
                <option value="string">string</option>
                <option value="int">int</option>
                <option value="double">double</option>
                <option value="bool">bool</option>
              </Select>
              {row.type === "bool" ? (
                <Select value={row.value || "false"} onChange={(e) => set(i, { value: e.target.value })} disabled={readOnly} style={{ flex: 1 }}>
                  <option value="true">true</option>
                  <option value="false">false</option>
                </Select>
              ) : (
                <Input value={row.value} onChange={(e) => set(i, { value: e.target.value })} placeholder="value" mono style={{ flex: 1 }} disabled={readOnly} />
              )}
            </>
          ) : (
            <>
              <Badge tone="neutral">complex</Badge>
              <span className="ws-mono ws-muted" style={{ flex: 1, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }} title="preserved verbatim on save">
                {row.value}
              </span>
            </>
          )}
          {!readOnly && (
            <Button variant="ghost" size="sm" onClick={() => remove(i)} title="remove property">
              ✕
            </Button>
          )}
        </div>
      ))}
      {!readOnly && (
        <div>
          <Button variant="ghost" size="sm" onClick={add}>
            + property
          </Button>
        </div>
      )}
      {rows.some((r) => !r.editable) && (
        <span className="ws-caption ws-muted" style={labelStyle}>
          complex values (bytes / list / map / null) are read-only and preserved on save.
        </span>
      )}
    </div>
  );
}
