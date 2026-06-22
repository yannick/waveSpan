import type { CSSProperties, HTMLAttributes, ReactNode } from "react";

interface CardProps extends HTMLAttributes<HTMLDivElement> {
  /** When set, draws an 8px Linea accent bar down the left edge in this colour. */
  accent?: string;
  flat?: boolean;
  children?: ReactNode;
}

/** A bordered cream surface. `accent` adds the signature left accent bar. */
export function Card({ accent, flat, className, style, children, ...rest }: CardProps) {
  const classes = [
    "ws-card",
    flat ? "ws-card--flat" : "",
    accent ? "ws-card--accent" : "",
    className ?? "",
  ]
    .filter(Boolean)
    .join(" ");
  const mergedStyle = accent
    ? ({ ["--_accent" as string]: accent, ...style } as CSSProperties)
    : style;
  return (
    <div className={classes} style={mergedStyle} {...rest}>
      {children}
    </div>
  );
}

interface PanelProps extends Omit<HTMLAttributes<HTMLDivElement>, "title"> {
  title?: ReactNode;
  actions?: ReactNode;
  bodyPadding?: boolean;
  children?: ReactNode;
}

/** A panel with an optional header strip (title + actions) and padded body. */
export function Panel({
  title,
  actions,
  bodyPadding = true,
  className,
  children,
  ...rest
}: PanelProps) {
  return (
    <div className={["ws-panel", className ?? ""].filter(Boolean).join(" ")} {...rest}>
      {(title || actions) && (
        <div className="ws-panel__head">
          <div className="ws-title-sm">{title}</div>
          {actions && <div className="ws-toolbar">{actions}</div>}
        </div>
      )}
      <div style={bodyPadding ? undefined : { padding: 0 }} className={bodyPadding ? "ws-panel__body" : ""}>
        {children}
      </div>
    </div>
  );
}

/** A horizontal strip of controls. */
export function Toolbar({ className, children, ...rest }: HTMLAttributes<HTMLDivElement>) {
  return (
    <div className={["ws-toolbar", className ?? ""].filter(Boolean).join(" ")} {...rest}>
      {children}
    </div>
  );
}
