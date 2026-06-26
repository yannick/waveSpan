/*
 * Value codec — sniff / decode / encode / preview for opaque KV & collection values.
 *
 * Values are stored server-side as raw bytes; the UI infers ("sniffs") a display codec but lets the
 * user override it so binary is never silently mangled. This module is intentionally PURE — no React,
 * no DOM beyond TextEncoder/TextDecoder — so it can be unit-tested in isolation and reused by the
 * preview column and the KV/collection editors alike (see data-workbench design §4.3).
 *
 * Codecs:
 *   - "json"   pretty-printed (2-space) JSON; encode requires JSON.parse to succeed.
 *   - "number" the UTF-8 text of a single finite JS number.
 *   - "text"   the raw UTF-8 text.
 *   - "hex"    space-separated lowercase hex byte pairs (the escape hatch for binary).
 */

export type Codec = "json" | "number" | "text" | "hex";

const enc = new TextEncoder();
// fatal:true makes decode throw on invalid UTF-8 so we can distinguish text from binary.
const strictDec = new TextDecoder("utf-8", { fatal: true });
const lenientDec = new TextDecoder("utf-8");

/** Decode bytes as UTF-8, or return null if the bytes are not valid UTF-8. */
function tryUtf8(bytes: Uint8Array): string | null {
  try {
    return strictDec.decode(bytes);
  } catch {
    return null;
  }
}

/** Does `text` parse as a JSON object or array? (Scalars are handled by the "number"/"text" codecs.) */
function isJsonObjectOrArray(text: string): boolean {
  const trimmed = text.trim();
  // Cheap structural gate before the (relatively) expensive JSON.parse.
  if (trimmed === "" || (trimmed[0] !== "{" && trimmed[0] !== "[")) return false;
  try {
    const v = JSON.parse(trimmed);
    return typeof v === "object" && v !== null;
  } catch {
    return false;
  }
}

/** Is `text` a single finite JS number (e.g. "42", "-3.14", "1e9")? Rejects "", "NaN", "Infinity". */
function isFiniteNumber(text: string): boolean {
  const trimmed = text.trim();
  if (trimmed === "") return false;
  const n = Number(trimmed);
  return Number.isFinite(n);
}

/** Fraction of characters that are printable/whitespace (vs. control chars), used to split text/binary. */
function printableRatio(text: string): number {
  if (text.length === 0) return 1;
  let printable = 0;
  for (const ch of text) {
    const c = ch.codePointAt(0)!;
    // Allow tab/newline/carriage-return + everything from space up, excluding the DEL control char.
    if (c === 0x09 || c === 0x0a || c === 0x0d || (c >= 0x20 && c !== 0x7f)) printable++;
  }
  // Iterating by code point and counting by code point keeps multi-byte glyphs from skewing the ratio.
  return printable / [...text].length;
}

/**
 * Infer a display codec for raw bytes. Heuristic order:
 *   empty → "text"; valid-UTF-8 JSON object/array → "json"; finite number → "number";
 *   mostly-printable UTF-8 → "text"; otherwise → "hex" (binary).
 */
export function sniff(bytes: Uint8Array): Codec {
  if (bytes.length === 0) return "text";
  const text = tryUtf8(bytes);
  if (text === null) return "hex"; // not valid UTF-8 → binary
  if (isJsonObjectOrArray(text)) return "json";
  if (isFiniteNumber(text)) return "number";
  if (printableRatio(text) >= 0.9) return "text";
  return "hex";
}

/** Format a byte count as a compact human-readable size ("0 B", "512 B", "4.2 KB", "1.0 MB"). */
export function formatBytes(n: number): string {
  if (n < 1024) return `${n} B`;
  const units = ["KB", "MB", "GB", "TB"];
  let v = n / 1024;
  let i = 0;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i++;
  }
  return `${v.toFixed(1)} ${units[i]}`;
}

/** Render bytes as space-separated lowercase hex pairs ("9f 3c 00"). */
function toHex(bytes: Uint8Array): string {
  let out = "";
  for (let i = 0; i < bytes.length; i++) {
    if (i > 0) out += " ";
    out += bytes[i].toString(16).padStart(2, "0");
  }
  return out;
}

/**
 * Decode bytes into an editable string for the given codec.
 *   json   → pretty-printed (2-space); falls back to raw UTF-8 if it does not parse.
 *   number → the UTF-8 text.
 *   text   → the UTF-8 text.
 *   hex    → space-separated lowercase hex bytes.
 */
export function decode(bytes: Uint8Array, codec: Codec): string {
  switch (codec) {
    case "hex":
      return toHex(bytes);
    case "json": {
      const text = lenientDec.decode(bytes);
      try {
        return JSON.stringify(JSON.parse(text), null, 2);
      } catch {
        // Not parseable: surface the raw text so the user can fix it rather than losing data.
        return text;
      }
    }
    case "number":
    case "text":
      return lenientDec.decode(bytes);
  }
}

/** Strip whitespace and an optional leading "0x"/"0X" prefix from a hex string. */
function normalizeHex(text: string): string {
  let s = text.replace(/\s+/g, "");
  if (s.startsWith("0x") || s.startsWith("0X")) s = s.slice(2);
  return s;
}

/**
 * Encode an editable string back into bytes for the given codec, VALIDATING the input.
 * Throws Error with a clear message on invalid input:
 *   json   → must JSON.parse; re-serialized compactly.
 *   number → must be a single finite JS number.
 *   hex    → even-length hex (after stripping whitespace/0x), valid hex pairs.
 *   text   → any string (UTF-8 encoded).
 */
export function encode(text: string, codec: Codec): Uint8Array {
  switch (codec) {
    case "json": {
      let parsed: unknown;
      try {
        parsed = JSON.parse(text);
      } catch (e) {
        throw new Error(`Invalid JSON: ${(e as Error).message}`);
      }
      return enc.encode(JSON.stringify(parsed));
    }
    case "number": {
      const trimmed = text.trim();
      if (trimmed === "") throw new Error("Invalid number: empty value");
      const n = Number(trimmed);
      if (!Number.isFinite(n)) throw new Error(`Invalid number: "${text}" is not a finite number`);
      return enc.encode(trimmed);
    }
    case "hex": {
      const s = normalizeHex(text);
      if (s.length % 2 !== 0) {
        throw new Error(`Invalid hex: odd number of digits (${s.length})`);
      }
      if (s.length > 0 && !/^[0-9a-fA-F]+$/.test(s)) {
        throw new Error("Invalid hex: contains non-hex characters");
      }
      const out = new Uint8Array(s.length / 2);
      for (let i = 0; i < out.length; i++) {
        out[i] = parseInt(s.slice(i * 2, i * 2 + 2), 16);
      }
      return out;
    }
    case "text":
      return enc.encode(text);
  }
}

/**
 * Produce a compact, single-line preview for a table cell.
 *   json   → whitespace collapsed, then truncated.
 *   number/text → single line (newlines collapsed), truncated with "…".
 *   hex    → first few bytes + total size, e.g. "‹0x9f 3c … 4.2 KB›".
 */
export function preview(bytes: Uint8Array, codec: Codec, maxLen = 80): string {
  if (codec === "hex") {
    // Show as many leading bytes as fit, then the human size — readable without dumping the blob.
    const shown = Math.min(bytes.length, 8);
    const head = toHex(bytes.subarray(0, shown));
    const ellipsis = bytes.length > shown ? " …" : "";
    return `‹0x${head}${ellipsis} ${formatBytes(bytes.length)}›`;
  }

  let text = decode(bytes, codec);
  if (codec === "json") {
    // Collapse the pretty-print back to a dense one-liner for the preview.
    try {
      text = JSON.stringify(JSON.parse(text));
    } catch {
      text = text.replace(/\s+/g, " ").trim();
    }
  } else {
    text = text.replace(/\s+/g, " ").trim();
  }

  if (text.length > maxLen) return text.slice(0, maxLen - 1) + "…";
  return text;
}
