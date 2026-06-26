import { useEffect, type ReactNode } from "react";

interface ModalProps {
  /** Whether the modal is mounted/visible. */
  open: boolean;
  /** Title shown in the header strip. */
  title?: ReactNode;
  /** Controls in the header strip, right-aligned (e.g. a Copy button). */
  actions?: ReactNode;
  /** Called on backdrop click, Escape, or the close affordance. */
  onClose: () => void;
  children?: ReactNode;
}

/**
 * A centered modal dialog over a dimmed backdrop. Closes on Escape and
 * backdrop click; body scroll is locked while open. Matches the Linea
 * bordered-card aesthetic via `.ws-modal`.
 */
export function Modal({ open, title, actions, onClose, children }: ModalProps) {
  useEffect(() => {
    if (!open) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    document.addEventListener("keydown", onKey);
    const prevOverflow = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    return () => {
      document.removeEventListener("keydown", onKey);
      document.body.style.overflow = prevOverflow;
    };
  }, [open, onClose]);

  if (!open) return null;

  return (
    <div className="ws-modal__backdrop" onClick={onClose} role="presentation">
      <div
        className="ws-modal"
        role="dialog"
        aria-modal="true"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="ws-modal__head">
          <div className="ws-title-sm">{title}</div>
          <div className="ws-toolbar">
            {actions}
            <button className="ws-modal__close" onClick={onClose} aria-label="Close" title="Close (Esc)">
              ✕
            </button>
          </div>
        </div>
        <div className="ws-modal__body">{children}</div>
      </div>
    </div>
  );
}
