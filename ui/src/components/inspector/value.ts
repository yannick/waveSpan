/*
 * Helpers between the cypher `Value` proto union and the editable typed rows used by TypedRows.
 *
 * The graph property model is the proto `Value` oneof (string / int / double / bool / bytes / list /
 * map / null). The drawer's TypedRows editor only edits the four scalar types (string/int/double/bool);
 * the structured/opaque variants (bytes/list/map/null) are surfaced read-only and preserved verbatim
 * on save. This module is the single place that translates between the two representations.
 */

import type { Value } from "../../gen/wavespan/v1/cypher_pb";

/** The scalar types TypedRows can edit. */
export type RowType = "string" | "int" | "double" | "bool";

/** One editable property row. `editable=false` means a bytes/list/map/null value we only display. */
export interface TypedRow {
  key: string;
  /** the editable scalar type, when editable; otherwise a display-only label of the underlying kind. */
  type: RowType;
  /** the text the user edits (for editable rows) or a read-only rendering (for non-editable rows). */
  value: string;
  editable: boolean;
  /** the original proto Value, preserved verbatim for non-editable rows so save round-trips them. */
  original?: Value;
}

/** A plain init object the connect-es client accepts for a Value oneof. */
export type ValueInit =
  | { value: { case: "stringValue"; value: string } }
  | { value: { case: "intValue"; value: bigint } }
  | { value: { case: "doubleValue"; value: number } }
  | { value: { case: "boolValue"; value: boolean } }
  | Value;

/** Render any Value as a compact, human-readable display string. */
export function valueToDisplay(v: Value | undefined): string {
  const c = v?.value;
  if (!c) return "";
  switch (c.case) {
    case "stringValue":
      return c.value;
    case "intValue":
      return String(c.value);
    case "doubleValue":
      return String(c.value);
    case "boolValue":
      return String(c.value);
    case "bytesValue":
      return `‹${c.value.length} bytes›`;
    case "listValue":
      return `[${c.value.values.map(valueToDisplay).join(", ")}]`;
    case "mapValue":
      return `{${Object.entries(c.value.entries)
        .map(([k, vv]) => `${k}: ${valueToDisplay(vv)}`)
        .join(", ")}}`;
    case "null":
      return "null";
    default:
      return "";
  }
}

/** Convert a proto Value into an editable TypedRow body (key is filled in by the caller). */
export function valueToRow(key: string, v: Value | undefined): TypedRow {
  const c = v?.value;
  switch (c?.case) {
    case "stringValue":
      return { key, type: "string", value: c.value, editable: true };
    case "intValue":
      return { key, type: "int", value: String(c.value), editable: true };
    case "doubleValue":
      return { key, type: "double", value: String(c.value), editable: true };
    case "boolValue":
      return { key, type: "bool", value: String(c.value), editable: true };
    // Structured/opaque variants are preserved verbatim and shown read-only.
    case "bytesValue":
    case "listValue":
    case "mapValue":
    case "null":
      return { key, type: "string", value: valueToDisplay(v), editable: false, original: v };
    default:
      return { key, type: "string", value: "", editable: true };
  }
}

/** A fresh, empty editable row. */
export function emptyRow(): TypedRow {
  return { key: "", type: "string", value: "", editable: true };
}

/**
 * Convert an editable scalar row into a Value init object. Throws on invalid int/double text so the
 * editor can surface a clear inline error rather than silently writing a bad value.
 */
function rowToValueInit(row: TypedRow): ValueInit {
  switch (row.type) {
    case "string":
      return { value: { case: "stringValue", value: row.value } };
    case "int": {
      const t = row.value.trim();
      if (!/^-?\d+$/.test(t)) throw new Error(`property "${row.key}": "${row.value}" is not an integer`);
      return { value: { case: "intValue", value: BigInt(t) } };
    }
    case "double": {
      const n = Number(row.value.trim());
      if (!Number.isFinite(n)) throw new Error(`property "${row.key}": "${row.value}" is not a number`);
      return { value: { case: "doubleValue", value: n } };
    }
    case "bool": {
      const t = row.value.trim().toLowerCase();
      if (t !== "true" && t !== "false") throw new Error(`property "${row.key}": "${row.value}" is not a boolean`);
      return { value: { case: "boolValue", value: t === "true" } };
    }
  }
}

/**
 * Build the properties map for a NodeRecord/EdgeRecord from typed rows. Editable rows are encoded from
 * their text; non-editable rows are preserved from their original Value. Empty keys are skipped; a
 * duplicate key throws. Validation errors (bad int/double/bool) propagate to the caller.
 */
export function rowsToProperties(rows: TypedRow[]): { [k: string]: ValueInit } {
  const out: { [k: string]: ValueInit } = {};
  for (const row of rows) {
    const key = row.key.trim();
    if (key === "") continue;
    if (key in out) throw new Error(`duplicate property key "${key}"`);
    if (!row.editable && row.original) {
      out[key] = row.original;
    } else {
      out[key] = rowToValueInit(row);
    }
  }
  return out;
}
