import { useMemo } from "react";
import { useUrlState } from "../router";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
import rehypeSlug from "rehype-slug";
import rehypeHighlight from "rehype-highlight";
import type { Components } from "react-markdown";
import { DOC_PAGES, DOC_SECTIONS, FIRST_DOC, getDoc } from "../docs/registry";
import { Button } from "../components";
import "../docs/docs.css";

export function Documentation() {
  // The open doc lives in the URL so a reload / shared link opens the same page.
  const [slug, setSlug] = useUrlState("doc", FIRST_DOC);
  const page = getDoc(slug) ?? DOC_PAGES[0];

  // Intercept `doc:<slug>` links so cross-references navigate within the browser.
  const components: Components = useMemo(
    () => ({
      a({ href, children, ...props }) {
        if (href && href.startsWith("doc:")) {
          const target = href.slice(4);
          return (
            <a
              href={`#${target}`}
              onClick={(e) => {
                e.preventDefault();
                setSlug(target);
                window.scrollTo({ top: 0 });
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
    }),
    [],
  );

  const idx = DOC_PAGES.findIndex((p) => p.slug === page.slug);
  const prev = idx > 0 ? DOC_PAGES[idx - 1] : null;
  const next = idx < DOC_PAGES.length - 1 ? DOC_PAGES[idx + 1] : null;

  const go = (s: string) => {
    setSlug(s);
    window.scrollTo({ top: 0 });
  };

  return (
    <div className="ws-docs">
      <nav className="ws-docs__nav" aria-label="Documentation">
        {DOC_SECTIONS.map((section) => (
          <div className="ws-docs__section" key={section.name}>
            <div className="ws-docs__section-title">{section.name}</div>
            {section.pages.map((p) => (
              <button
                key={p.slug}
                className="ws-docs__link"
                aria-current={p.slug === page.slug}
                onClick={() => go(p.slug)}
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
            <Button variant="ghost" onClick={() => go(prev.slug)}>← {prev.title}</Button>
          ) : (
            <span />
          )}
          {next ? (
            <Button variant="ghost" onClick={() => go(next.slug)}>{next.title} →</Button>
          ) : (
            <span />
          )}
        </div>
      </article>
    </div>
  );
}
