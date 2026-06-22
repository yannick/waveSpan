/*
 * Documentation registry. Markdown pages live in ./content/*.md and are bundled
 * at build time via Vite's import.meta.glob (eager, raw). Each file carries a
 * small frontmatter block:
 *
 *   ---
 *   title: KV API
 *   section: Reference
 *   order: 4
 *   summary: Put / Get / Delete / Scan and the consistency metadata.
 *   ---
 *
 * The registry parses that frontmatter into an ordered, section-grouped nav model.
 */

export interface DocPage {
  /** slug derived from filename, e.g. "04-kv-api" -> "kv-api" */
  slug: string;
  title: string;
  section: string;
  order: number;
  summary: string;
  body: string;
}

export interface DocSection {
  name: string;
  pages: DocPage[];
}

// Raw markdown keyed by file path.
const raw = import.meta.glob("./content/*.md", {
  query: "?raw",
  import: "default",
  eager: true,
}) as Record<string, string>;

/** Minimal frontmatter parser — supports the flat `key: value` block we author. */
function parseFrontmatter(text: string): { meta: Record<string, string>; body: string } {
  const match = /^---\r?\n([\s\S]*?)\r?\n---\r?\n?([\s\S]*)$/.exec(text);
  if (!match) return { meta: {}, body: text };
  const meta: Record<string, string> = {};
  for (const line of match[1].split(/\r?\n/)) {
    const kv = /^([A-Za-z0-9_]+):\s*(.*)$/.exec(line);
    if (kv) meta[kv[1]] = kv[2].replace(/^["']|["']$/g, "").trim();
  }
  return { meta, body: match[2] };
}

function slugFromPath(path: string): string {
  const file = path.split("/").pop() ?? path;
  return file.replace(/\.md$/, "").replace(/^\d+[-_]/, "");
}

const pages: DocPage[] = Object.entries(raw)
  .map(([path, text]) => {
    const { meta, body } = parseFrontmatter(text);
    return {
      slug: slugFromPath(path),
      title: meta.title ?? slugFromPath(path),
      section: meta.section ?? "Docs",
      order: Number(meta.order ?? 999),
      summary: meta.summary ?? "",
      body,
    };
  })
  .sort((a, b) => a.order - b.order);

// Group into sections, preserving first-seen section order (driven by page order).
const sectionOrder: string[] = [];
const bySection = new Map<string, DocPage[]>();
for (const p of pages) {
  if (!bySection.has(p.section)) {
    bySection.set(p.section, []);
    sectionOrder.push(p.section);
  }
  bySection.get(p.section)!.push(p);
}

export const DOC_SECTIONS: DocSection[] = sectionOrder.map((name) => ({
  name,
  pages: bySection.get(name)!,
}));

export const DOC_PAGES: DocPage[] = pages;

export function getDoc(slug: string): DocPage | undefined {
  return pages.find((p) => p.slug === slug);
}

export const FIRST_DOC = pages[0]?.slug ?? "";
