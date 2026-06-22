import type { ReactNode } from "react";

export function Spinner() {
  return <span className="ws-spinner" role="status" aria-label="loading" />;
}

interface EmptyStateProps {
  title?: ReactNode;
  children?: ReactNode;
  icon?: ReactNode;
}

export function EmptyState({ title, children, icon }: EmptyStateProps) {
  return (
    <div className="ws-empty">
      {icon && <div style={{ fontSize: 28 }}>{icon}</div>}
      {title && <div className="ws-title-sm">{title}</div>}
      {children && <div className="ws-body-sm">{children}</div>}
    </div>
  );
}

interface InlineMessageProps {
  tone?: "danger" | "success" | "warning" | "info";
  children: ReactNode;
}

const TONE_VAR: Record<NonNullable<InlineMessageProps["tone"]>, string> = {
  danger: "var(--ws-color-danger)",
  success: "var(--ws-color-success)",
  warning: "var(--ws-color-mustard)",
  info: "var(--ws-color-blue)",
};

/** A soft-tinted, accent-barred message box for errors / results. */
export function InlineMessage({ tone = "info", children }: InlineMessageProps) {
  const c = TONE_VAR[tone];
  return (
    <div
      className="ws-body-sm"
      style={{
        borderRadius: "var(--ws-radius-sm)",
        borderLeft: `var(--ws-stroke-accent) solid ${c}`,
        border: "var(--ws-stroke-hairline) solid var(--ws-color-border)",
        borderLeftWidth: "var(--ws-stroke-accent)",
        borderLeftColor: c,
        background: `color-mix(in srgb, ${c} 12%, var(--ws-color-paper))`,
        padding: "var(--ws-space-md) var(--ws-space-lg)",
        color: "var(--ws-color-ink)",
      }}
    >
      {children}
    </div>
  );
}
