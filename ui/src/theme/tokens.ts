/*
 * Typed mirror of the CSS design tokens (tokens.css). Values are `var(--ws-*)`
 * references so anything consumed from TS (SVG fills, inline styles for dynamic
 * values) still resolves through the live theme and switches with light/dark.
 *
 * For raw colour values needed by canvas/SVG maths that cannot use `var()`,
 * use `readToken()` to resolve a custom property to its computed string.
 */

export const color = {
  paper: "var(--ws-color-paper)",
  surface: "var(--ws-color-surface)",
  surfaceAlt: "var(--ws-color-surface-alt)",
  ink: "var(--ws-color-ink)",
  inkMuted: "var(--ws-color-ink-muted)",
  border: "var(--ws-color-border)",
  borderStrong: "var(--ws-color-border-strong)",
  orange: "var(--ws-color-orange)",
  teal: "var(--ws-color-teal)",
  olive: "var(--ws-color-olive)",
  mustard: "var(--ws-color-mustard)",
  red: "var(--ws-color-red)",
  blue: "var(--ws-color-blue)",
  purple: "var(--ws-color-purple)",
  primary: "var(--ws-color-primary)",
  accent: "var(--ws-color-accent)",
  success: "var(--ws-color-success)",
  warning: "var(--ws-color-warning)",
  danger: "var(--ws-color-danger)",
  info: "var(--ws-color-info)",
  onAccent: "var(--ws-color-on-accent)",
} as const;

export const space = {
  xxs: "var(--ws-space-xxs)",
  xs: "var(--ws-space-xs)",
  sm: "var(--ws-space-sm)",
  md: "var(--ws-space-md)",
  lg: "var(--ws-space-lg)",
  xl: "var(--ws-space-xl)",
  xxl: "var(--ws-space-xxl)",
  x3l: "var(--ws-space-3xl)",
} as const;

export const radius = {
  xs: "var(--ws-radius-xs)",
  sm: "var(--ws-radius-sm)",
  md: "var(--ws-radius-md)",
  lg: "var(--ws-radius-lg)",
  pill: "var(--ws-radius-pill)",
} as const;

export const stroke = {
  hairline: "var(--ws-stroke-hairline)",
  regular: "var(--ws-stroke-regular)",
  strong: "var(--ws-stroke-strong)",
  accent: "var(--ws-stroke-accent)",
} as const;

export const font = {
  sans: "var(--ws-font-sans)",
  mono: "var(--ws-font-mono)",
} as const;

export const tokens = { color, space, radius, stroke, font } as const;

/**
 * Stable categorical palette for charts / graph node colouring. Cycles through
 * the Linea accent hues so adjacent categories stay visually distinct.
 */
export const CATEGORICAL = [
  "var(--ws-color-teal)",
  "var(--ws-color-orange)",
  "var(--ws-color-olive)",
  "var(--ws-color-mustard)",
  "var(--ws-color-blue)",
  "var(--ws-color-purple)",
  "var(--ws-color-red)",
] as const;

/** Pick a stable categorical colour for an arbitrary string key. */
export function categoricalColor(key: string): string {
  let h = 0;
  for (let i = 0; i < key.length; i++) h = (h * 31 + key.charCodeAt(i)) >>> 0;
  return CATEGORICAL[h % CATEGORICAL.length];
}

/** Resolve a CSS custom property (e.g. "--ws-color-teal") to its computed value. */
export function readToken(name: string, el: Element = document.documentElement): string {
  return getComputedStyle(el).getPropertyValue(name).trim();
}
