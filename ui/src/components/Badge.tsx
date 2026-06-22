import type { CSSProperties, ReactNode } from "react";
import { color } from "../theme/tokens";

/** Semantic tone → token colour. */
export const TONE = {
  neutral: color.inkMuted,
  primary: color.teal,
  accent: color.orange,
  success: color.teal,
  warning: color.mustard,
  danger: color.red,
  info: color.blue,
  olive: color.olive,
  purple: color.purple,
} as const;

export type Tone = keyof typeof TONE;

interface BadgeProps {
  tone?: Tone | string;
  solid?: boolean;
  dot?: boolean;
  children: ReactNode;
}

/** Pill-shaped semantic label. `tone` accepts a named tone or any CSS colour. */
export function Badge({ tone = "neutral", solid, dot, children }: BadgeProps) {
  const toneColor = (TONE as Record<string, string>)[tone] ?? tone;
  const style = { ["--_tone" as string]: toneColor } as CSSProperties;
  return (
    <span className={["ws-badge", solid ? "ws-badge--solid" : ""].filter(Boolean).join(" ")} style={style}>
      {dot && <span className="ws-badge__dot" />}
      {children}
    </span>
  );
}
