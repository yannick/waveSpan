/*
 * Minimal Prometheus text-exposition parser (https://prometheus.io/docs/instrumenting/exposition_formats/).
 * Enough to render the node's /metrics endpoint in the UI: it captures the `# HELP` / `# TYPE` lines and
 * the `name{labels} value [timestamp]` samples. Histograms/summaries surface as their individual
 * `_bucket` / `_sum` / `_count` series (we do not aggregate them), which is fine for a flat table view.
 */

export interface MetricSample {
  labels: Record<string, string>;
  value: number;
}

export interface MetricFamily {
  name: string;
  help: string;
  type: string; // counter | gauge | histogram | summary | untyped
  samples: MetricSample[];
}

/** Parse a label block like `code="200",method="get"` into a record. Values may contain escapes. */
function parseLabels(block: string): Record<string, string> {
  const labels: Record<string, string> = {};
  // Matches key="value" pairs, honouring \" and \\ escapes inside the value.
  const re = /([a-zA-Z_][a-zA-Z0-9_]*)\s*=\s*"((?:\\.|[^"\\])*)"/g;
  let m: RegExpExecArray | null;
  while ((m = re.exec(block)) !== null) {
    labels[m[1]] = m[2].replace(/\\(["\\n])/g, (_s, c) => (c === "n" ? "\n" : c));
  }
  return labels;
}

/** Parse a Prometheus exposition payload into metric families, preserving first-seen order. */
export function parsePrometheus(text: string): MetricFamily[] {
  const families = new Map<string, MetricFamily>();
  const order: string[] = [];

  const family = (name: string): MetricFamily => {
    let f = families.get(name);
    if (!f) {
      f = { name, help: "", type: "untyped", samples: [] };
      families.set(name, f);
      order.push(name);
    }
    return f;
  };

  for (const rawLine of text.split("\n")) {
    const line = rawLine.trim();
    if (line === "") continue;

    if (line.startsWith("#")) {
      // `# HELP name text...` or `# TYPE name kind`
      const meta = /^#\s+(HELP|TYPE)\s+(\S+)\s*(.*)$/.exec(line);
      if (!meta) continue;
      const [, kind, name, rest] = meta;
      if (kind === "HELP") family(name).help = rest;
      else family(name).type = rest.trim() || "untyped";
      continue;
    }

    // `name{labels} value [timestamp]` — labels and timestamp optional.
    const sample = /^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+(.+)$/.exec(line);
    if (!sample) continue;
    const [, name, labelBlock, tail] = sample;
    const valueToken = tail.trim().split(/\s+/)[0];
    let value: number;
    if (valueToken === "+Inf") value = Infinity;
    else if (valueToken === "-Inf") value = -Infinity;
    else if (valueToken === "NaN") value = NaN;
    else value = Number(valueToken);

    family(name).samples.push({
      labels: labelBlock ? parseLabels(labelBlock.slice(1, -1)) : {},
      value,
    });
  }

  return order.map((n) => families.get(n)!);
}
