import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useUrlState } from "../router";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
import rehypeSlug from "rehype-slug";
import rehypeHighlight from "rehype-highlight";
import type { Components } from "react-markdown";
import { DOC_PAGES, DOC_SECTIONS, FIRST_DOC, getDoc } from "../docs/registry";
import { Button } from "../components";
import "../docs/docs.css";

// CopyAnchor is the "#" affordance beside a heading: clicking it deep-links to that section (writes
// &section=<id> into the URL) and copies the resulting shareable link to the clipboard.
function CopyAnchor({ id, onLink }: { id: string; onLink: (id: string) => void }) {
  const [copied, setCopied] = useState(false);
  return (
    <button
      type="button"
      className="ws-prose__anchor"
      title="Copy link to this section"
      aria-label="Copy link to this section"
      onClick={() => {
        onLink(id); // updates the URL synchronously (replaceState) before we read it
        try {
          void navigator.clipboard?.writeText(window.location.href);
        } catch {
          /* clipboard unavailable — the URL is still updated */
        }
        setCopied(true);
        window.setTimeout(() => setCopied(false), 1200);
      }}
    >
      {copied ? "✓ copied" : "#"}
    </button>
  );
}

export function Documentation() {
  // The open doc and the focused section both live in the URL so a reload / shared link restores them.
  const [slug, setSlug] = useUrlState("doc", FIRST_DOC);
  const [section, setSection] = useUrlState("section", "");
  const page = getDoc(slug) ?? DOC_PAGES[0];

  // Keep the latest URL setters in refs so the memoized markdown renderers stay stable across renders.
  const setSlugRef = useRef(setSlug);
  setSlugRef.current = setSlug;
  const setSectionRef = useRef(setSection);
  setSectionRef.current = setSection;

  // Open a doc (clearing any section, resetting scroll) — nav links, pager, and cross-references.
  const goDoc = useCallback((s: string) => {
    setSectionRef.current("");
    setSlugRef.current(s);
    window.scrollTo({ top: 0 });
  }, []);
  const linkSection = useCallback((id: string) => setSectionRef.current(id), []);

  // Deep link: once the doc has rendered, scroll the focused section into view.
  useEffect(() => {
    if (!section) return;
    const el = document.getElementById(section);
    if (!el) return;
    const raf = window.requestAnimationFrame(() =>
      el.scrollIntoView({ behavior: "smooth", block: "start" }),
    );
    return () => window.cancelAnimationFrame(raf);
  }, [slug, section, page.body]);

  const components: Components = useMemo(() => {
    const heading = (level: 2 | 3 | 4) =>
      function Heading({ id, children }: { id?: string; children?: React.ReactNode }) {
        const Tag = `h${level}` as "h2" | "h3" | "h4";
        return (
          <Tag id={id} className="ws-prose__heading">
            {children}
            {id ? <CopyAnchor id={id} onLink={linkSection} /> : null}
          </Tag>
        );
      };
    return {
      // Intercept `doc:<slug>` links so cross-references navigate within the browser.
      a({ href, children, ...props }) {
        if (href && href.startsWith("doc:")) {
          const target = href.slice(4);
          return (
            <a
              href={`#/docs?doc=${target}`}
              onClick={(e) => {
                e.preventDefault();
                goDoc(target);
              }}
              {...props}
            >
              {children}
            </a>
          );
        }
        const external = href?.startsWith("http");
        return (
          <a href={href} target={external ? "_blank" : undefined} rel={external ? "noreferrer" : undefined} {...props}>
            {children}
          </a>
        );
      },
      h2: heading(2),
      h3: heading(3),
      h4: heading(4),
    };
  }, [goDoc, linkSection]);

  const idx = DOC_PAGES.findIndex((p) => p.slug === page.slug);
  const prev = idx > 0 ? DOC_PAGES[idx - 1] : null;
  const next = idx < DOC_PAGES.length - 1 ? DOC_PAGES[idx + 1] : null;

  return (
    <div className="ws-docs">
      <nav className="ws-docs__nav" aria-label="Documentation">
        {DOC_SECTIONS.map((sec) => (
          <div className="ws-docs__section" key={sec.name}>
            <div className="ws-docs__section-title">{sec.name}</div>
            {sec.pages.map((p) => (
              <button
                key={p.slug}
                className="ws-docs__link"
                aria-current={p.slug === page.slug}
                onClick={() => goDoc(p.slug)}
              >
                {p.title}
              </button>
            ))}
          </div>
        ))}
      </nav>

      <article className="ws-prose">
        {page.summary && <p className="ws-prose__lead">{page.summary}</p>}
        <ReactMarkdown
          remarkPlugins={[remarkGfm]}
          rehypePlugins={[rehypeSlug, [rehypeHighlight, { detect: true, ignoreMissing: true }]]}
          components={components}
        >
          {page.body}
        </ReactMarkdown>

        <div className="ws-docs__pager">
          {prev ? (
            <Button variant="ghost" onClick={() => goDoc(prev.slug)}>← {prev.title}</Button>
          ) : (
            <span />
          )}
          {next ? (
            <Button variant="ghost" onClick={() => goDoc(next.slug)}>{next.title} →</Button>
          ) : (
            <span />
          )}
        </div>
      </article>
    </div>
  );
}
