import type { ButtonHTMLAttributes, ReactNode } from "react";

type Variant = "primary" | "teal" | "secondary" | "ghost" | "danger";

interface ButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: Variant;
  size?: "sm" | "md";
  icon?: boolean;
  children?: ReactNode;
}

const VARIANT_CLASS: Record<Variant, string> = {
  primary: "ws-btn--primary",
  teal: "ws-btn--teal",
  secondary: "ws-btn--secondary",
  ghost: "ws-btn--ghost",
  danger: "ws-btn--danger",
};

/** Linea-style bordered button. Default variant = secondary (outline). */
export function Button({
  variant = "secondary",
  size = "md",
  icon = false,
  className,
  children,
  ...rest
}: ButtonProps) {
  const classes = [
    "ws-btn",
    VARIANT_CLASS[variant],
    size === "sm" ? "ws-btn--sm" : "",
    icon ? "ws-btn--icon" : "",
    className ?? "",
  ]
    .filter(Boolean)
    .join(" ");
  return (
    <button className={classes} {...rest}>
      {children}
    </button>
  );
}

interface ChipProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  children: ReactNode;
}

/** A small mono pill used for quick picks (e.g. example queries). */
export function Chip({ className, children, ...rest }: ChipProps) {
  return (
    <button className={["ws-chip", className ?? ""].filter(Boolean).join(" ")} {...rest}>
      {children}
    </button>
  );
}
