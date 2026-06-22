// Lightweight hash-based router + URL-state hooks for the embedded console.
//
// The admin port serves this SPA at "/" alongside reserved server routes (/metrics, /admin,
// /debug/pprof, …), so hash routing (#/screen?filter=…) is used deliberately: the fragment is never
// sent to the server, so there are no route collisions and no SPA-fallback needed — a reload or a
// pasted URL restores the exact screen AND its filter state.
import { useEffect, useSyncExternalStore } from "react";

export const DEFAULT_SCREEN = "cypher";

export interface Route {
  screen: string;
  params: URLSearchParams;
}

function parseHash(): Route {
  const raw = window.location.hash.replace(/^#\/?/, ""); // "screen?filter=…"
  const qIdx = raw.indexOf("?");
  const screen = (qIdx === -1 ? raw : raw.slice(0, qIdx)) || DEFAULT_SCREEN;
  const qs = qIdx === -1 ? "" : raw.slice(qIdx + 1);
  return { screen, params: new URLSearchParams(qs) };
}

// --- a tiny external store so every hook re-renders on any URL change (real or synthetic) ---
const listeners = new Set<() => void>();
function emit() {
  for (const l of listeners) l();
}
function subscribe(cb: () => void) {
  const onChange = () => cb();
  window.addEventListener("hashchange", onChange);
  window.addEventListener("popstate", onChange);
  listeners.add(cb);
  return () => {
    window.removeEventListener("hashchange", onChange);
    window.removeEventListener("popstate", onChange);
    listeners.delete(cb);
  };
}
// the snapshot is the current hash string (cheap, stable identity per URL)
function snapshot() {
  return window.location.hash;
}

/** useRoute re-renders the caller whenever the screen or any query param changes. */
export function useRoute(): Route {
  useSyncExternalStore(subscribe, snapshot, snapshot);
  return parseHash();
}

function writeHash(screen: string, params: URLSearchParams, push: boolean) {
  const qs = params.toString();
  const next = `#/${screen}${qs ? "?" + qs : ""}`;
  if (push) {
    window.history.pushState(null, "", next);
  } else {
    window.history.replaceState(null, "", next);
  }
  emit(); // replace/pushState don't fire hashchange — notify subscribers ourselves
}

/** navigate switches screens (a history entry), starting that screen with `params` (default none). */
export function navigate(screen: string, params: Record<string, string> = {}) {
  writeHash(screen, new URLSearchParams(params), true);
}

/** Ensure the URL names a screen on first load so it is always a proper, shareable URL. */
export function useEnsureScreen() {
  useEffect(() => {
    if (!window.location.hash || window.location.hash === "#" || window.location.hash === "#/") {
      writeHash(DEFAULT_SCREEN, new URLSearchParams(), false);
    }
  }, []);
}

function setParam(key: string, value: string, def: string) {
  const { screen, params } = parseHash();
  if (value === def) params.delete(key);
  else params.set(key, value);
  writeHash(screen, params, false); // replace: filter typing must not spam history
}

/** A string query param bound to the current screen. Omitted from the URL when equal to `def`. */
export function useUrlState(key: string, def = ""): [string, (v: string) => void] {
  const { params } = useRoute();
  const value = params.get(key) ?? def;
  return [value, (v: string) => setParam(key, v, def)];
}

/** A boolean query param ("1"/"0"). */
export function useUrlBool(key: string, def: boolean): [boolean, (v: boolean) => void] {
  const [s, set] = useUrlState(key, def ? "1" : "0");
  return [s === "1", (v: boolean) => set(v ? "1" : "0")];
}

/** A numeric query param. */
export function useUrlNumber(key: string, def: number): [number, (v: number) => void] {
  const [s, set] = useUrlState(key, String(def));
  const n = Number(s);
  return [Number.isFinite(n) ? n : def, (v: number) => set(String(v))];
}

/** A set of string values stored as a comma list (e.g. selected table filters). */
export function useUrlStringSet(key: string): [Set<string>, (next: Set<string>) => void] {
  const [s, set] = useUrlState(key, "");
  const current = new Set(s ? s.split(",").filter(Boolean) : []);
  return [current, (next: Set<string>) => set([...next].sort().join(","))];
}
