import type { ReactNode, TableHTMLAttributes } from "react";

interface TableProps extends TableHTMLAttributes<HTMLTableElement> {
  mono?: boolean;
  children: ReactNode;
}

/** Bordered table inside a horizontally-scrollable wrapper. */
export function Table({ mono, className, children, ...rest }: TableProps) {
  return (
    <div className="ws-table-wrap">
      <table
        className={["ws-table", mono ? "ws-table--mono" : "", className ?? ""].filter(Boolean).join(" ")}
        {...rest}
      >
        {children}
      </table>
    </div>
  );
}
