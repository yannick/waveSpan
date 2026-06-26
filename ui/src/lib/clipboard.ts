// Copy text to the clipboard, returning whether it succeeded.
//
// The async Clipboard API is only available in secure contexts (https or
// localhost). The admin console is often served over plain http on a node
// port, where `navigator.clipboard` is undefined — so we fall back to a
// hidden <textarea> + execCommand("copy"), which still works there.
export async function copyText(text: string): Promise<boolean> {
  if (navigator.clipboard?.writeText) {
    try {
      await navigator.clipboard.writeText(text);
      return true;
    } catch {
      // fall through to the legacy path (e.g. permission denied)
    }
  }
  try {
    const ta = document.createElement("textarea");
    ta.value = text;
    ta.setAttribute("readonly", "");
    ta.style.position = "fixed";
    ta.style.opacity = "0";
    ta.style.pointerEvents = "none";
    document.body.appendChild(ta);
    ta.select();
    const ok = document.execCommand("copy");
    document.body.removeChild(ta);
    return ok;
  } catch {
    return false;
  }
}
