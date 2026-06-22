import { useTheme } from "../theme/ThemeProvider";

/** Light/dark switch. Sun = light "paper", moon = dark "ink". */
export function ThemeToggle() {
  const { mode, toggle } = useTheme();
  const isDark = mode === "dark";
  return (
    <button
      className="ws-btn ws-btn--secondary ws-btn--sm"
      onClick={toggle}
      aria-label={`Switch to ${isDark ? "light" : "dark"} theme`}
      title={`Switch to ${isDark ? "light" : "dark"} theme`}
    >
      <span aria-hidden style={{ fontSize: 14, lineHeight: 1 }}>{isDark ? "◐" : "◑"}</span>
      {isDark ? "Ink" : "Paper"}
    </button>
  );
}
