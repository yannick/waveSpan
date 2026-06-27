import { describe, it, expect } from "vitest";
import { type Codec, sniff, decode, encode, preview, formatBytes } from "./valuecodec";

const utf8 = (s: string) => new TextEncoder().encode(s);

/** Round-trip helper: encode(decode(bytes)) should equal the canonical bytes for that codec. */
function roundTrip(bytes: Uint8Array, codec: Codec): Uint8Array {
  return encode(decode(bytes, codec), codec);
}

describe("sniff", () => {
  it("empty bytes → text", () => {
    expect(sniff(new Uint8Array())).toBe("text");
  });

  it("JSON object → json", () => {
    expect(sniff(utf8('{"a":1,"b":[2,3]}'))).toBe("json");
  });

  it("JSON array → json", () => {
    expect(sniff(utf8("[1, 2, 3]"))).toBe("json");
  });

  it("plain integer → number", () => {
    expect(sniff(utf8("42"))).toBe("number");
  });

  it("float → number", () => {
    expect(sniff(utf8("-3.14"))).toBe("number");
  });

  it("plain text → text", () => {
    expect(sniff(utf8("hello world"))).toBe("text");
  });

  it("unicode text → text", () => {
    expect(sniff(utf8("héllo 世界"))).toBe("text");
  });

  it("non-UTF-8 binary → hex", () => {
    expect(sniff(new Uint8Array([0xff, 0x00, 0x9f]))).toBe("hex");
  });

  it("mostly-control-char bytes → hex", () => {
    expect(sniff(new Uint8Array([0x01, 0x02, 0x03, 0x04, 0x05]))).toBe("hex");
  });

  it("JSON scalar string is not treated as json object/array", () => {
    // A bare quoted string parses as JSON but is not an object/array → falls through to text.
    expect(sniff(utf8('"just a string"'))).toBe("text");
  });
});

describe("decode", () => {
  it("json pretty-prints with 2-space indent", () => {
    expect(decode(utf8('{"a":1}'), "json")).toBe('{\n  "a": 1\n}');
  });

  it("text decodes UTF-8", () => {
    expect(decode(utf8("héllo"), "text")).toBe("héllo");
  });

  it("number decodes UTF-8 text", () => {
    expect(decode(utf8("42"), "number")).toBe("42");
  });

  it("hex renders space-separated lowercase pairs", () => {
    expect(decode(new Uint8Array([0xff, 0x00, 0x9f]), "hex")).toBe("ff 00 9f");
  });

  it("json falls back to raw text when unparseable", () => {
    expect(decode(utf8("not json"), "json")).toBe("not json");
  });
});

describe("encode round-trips", () => {
  it("json object", () => {
    const bytes = utf8('{"a":1,"b":[2,3]}');
    expect(new TextDecoder().decode(roundTrip(bytes, "json"))).toBe('{"a":1,"b":[2,3]}');
  });

  it("json array", () => {
    const bytes = utf8("[1,2,3]");
    expect(new TextDecoder().decode(roundTrip(bytes, "json"))).toBe("[1,2,3]");
  });

  it("integer", () => {
    expect(new TextDecoder().decode(roundTrip(utf8("42"), "number"))).toBe("42");
  });

  it("float", () => {
    expect(new TextDecoder().decode(roundTrip(utf8("-3.14"), "number"))).toBe("-3.14");
  });

  it("utf-8 text", () => {
    expect(roundTrip(utf8("héllo 世界"), "text")).toEqual(utf8("héllo 世界"));
  });

  it("binary via hex", () => {
    const bytes = new Uint8Array([0xff, 0x00, 0x9f]);
    expect(roundTrip(bytes, "hex")).toEqual(bytes);
  });

  it("hex accepts a 0x prefix and whitespace", () => {
    expect(encode("0xff 00 9f", "hex")).toEqual(new Uint8Array([0xff, 0x00, 0x9f]));
  });

  it("empty hex → empty bytes", () => {
    expect(encode("", "hex")).toEqual(new Uint8Array());
  });
});

describe("encode validation", () => {
  it("invalid json throws", () => {
    expect(() => encode("{not valid}", "json")).toThrow(/Invalid JSON/);
  });

  it("invalid number throws", () => {
    expect(() => encode("not a number", "number")).toThrow(/Invalid number/);
  });

  it("non-finite number throws", () => {
    expect(() => encode("Infinity", "number")).toThrow(/Invalid number/);
  });

  it("empty number throws", () => {
    expect(() => encode("   ", "number")).toThrow(/Invalid number/);
  });

  it("odd-length hex throws", () => {
    expect(() => encode("abc", "hex")).toThrow(/Invalid hex/);
  });

  it("non-hex characters throw", () => {
    expect(() => encode("zz zz", "hex")).toThrow(/Invalid hex/);
  });
});

describe("preview", () => {
  it("json collapses whitespace into one line", () => {
    expect(preview(utf8('{\n  "a":  1\n}'), "json")).toBe('{"a":1}');
  });

  it("text truncates with an ellipsis", () => {
    const long = "x".repeat(200);
    const out = preview(utf8(long), "text", 20);
    expect(out.length).toBe(20);
    expect(out.endsWith("…")).toBe(true);
  });

  it("text collapses newlines to a single line", () => {
    expect(preview(utf8("a\nb\nc"), "text")).toBe("a b c");
  });

  it("hex shows leading bytes + size", () => {
    const bytes = new Uint8Array(4200).fill(0);
    bytes[0] = 0x9f;
    bytes[1] = 0x3c;
    const out = preview(bytes, "hex");
    expect(out).toContain("‹0x9f 3c");
    expect(out).toContain("…");
    expect(out).toContain("4.1 KB");
    expect(out.endsWith("›")).toBe(true);
  });

  it("short hex has no ellipsis", () => {
    expect(preview(new Uint8Array([0x01, 0x02]), "hex")).toBe("‹0x01 02 2 B›");
  });
});

describe("formatBytes", () => {
  it("bytes", () => expect(formatBytes(512)).toBe("512 B"));
  it("kilobytes", () => expect(formatBytes(4200)).toBe("4.1 KB"));
  it("megabytes", () => expect(formatBytes(1024 * 1024)).toBe("1.0 MB"));
});
