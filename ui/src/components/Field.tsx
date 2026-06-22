import type {
  InputHTMLAttributes,
  LabelHTMLAttributes,
  ReactNode,
  SelectHTMLAttributes,
  TextareaHTMLAttributes,
} from "react";

interface InputProps extends InputHTMLAttributes<HTMLInputElement> {
  mono?: boolean;
}

export function Input({ mono, className, ...rest }: InputProps) {
  return (
    <input
      className={["ws-field", mono ? "ws-field--mono" : "", className ?? ""].filter(Boolean).join(" ")}
      {...rest}
    />
  );
}

interface TextareaProps extends TextareaHTMLAttributes<HTMLTextAreaElement> {
  mono?: boolean;
}

export function Textarea({ mono, className, ...rest }: TextareaProps) {
  return (
    <textarea
      className={["ws-field", mono ? "ws-field--mono" : "", className ?? ""].filter(Boolean).join(" ")}
      spellCheck={false}
      {...rest}
    />
  );
}

export function Select({ className, children, ...rest }: SelectHTMLAttributes<HTMLSelectElement>) {
  return (
    <select className={["ws-field", className ?? ""].filter(Boolean).join(" ")} {...rest}>
      {children}
    </select>
  );
}

interface CheckboxProps extends Omit<InputHTMLAttributes<HTMLInputElement>, "type"> {
  label: ReactNode;
}

export function Checkbox({ label, ...rest }: CheckboxProps) {
  return (
    <label className="ws-checkbox">
      <input type="checkbox" {...rest} />
      {label}
    </label>
  );
}

interface FieldLabelProps extends LabelHTMLAttributes<HTMLLabelElement> {
  children: ReactNode;
}

/** Inline caption-styled label wrapper, e.g. `<FieldLabel>graph <Input/></FieldLabel>`. */
export function FieldLabel({ className, children, ...rest }: FieldLabelProps) {
  return (
    <label className={["ws-field-label", className ?? ""].filter(Boolean).join(" ")} {...rest}>
      {children}
    </label>
  );
}
