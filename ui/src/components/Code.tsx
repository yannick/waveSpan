import type { HTMLAttributes, ReactNode } from "react";

export function CodeInline({ children }: { children: ReactNode }) {
  return <code className="ws-code-inline">{children}</code>;
}

export function Kbd({ children }: { children: ReactNode }) {
  return <kbd className="ws-kbd">{children}</kbd>;
}

export function CodeBlock({ className, children, ...rest }: HTMLAttributes<HTMLPreElement>) {
  return (
    <pre className={["ws-code-block", className ?? ""].filter(Boolean).join(" ")} {...rest}>
      {children}
    </pre>
  );
}
