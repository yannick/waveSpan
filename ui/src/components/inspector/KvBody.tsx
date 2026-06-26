import { useEffect, useMemo, useState } from "react";
import { obs } from "../../transport";
import { MemberLiveness, type MemberState } from "../../gen/wavespan/v1/admin_pb";
import { Button, InlineMessage, Input, Select, Textarea } from "../index";
import { type Codec, decode, encode, formatBytes, preview, sniff } from "../../lib/valuecodec";
import { DiffConfirm, SaveDeleteBar, type DrawerTarget } from "./Drawer";

type KvTarget = Extract<DrawerTarget, { kind: "kv" }> | Extract<DrawerTarget, { kind: "kv-new" }>;

const CODECS: Codec[] = ["json", "text", "number", "hex"];
const labelStyle = { fontWeight: 700 as const, fontSize: "var(--ws-text-body-sm-size)" };

// KvBody views & edits a single KV value with a user-overridable display codec, a save→diff→confirm
// flow, and a coordinator + TTL picker (reusing the KvWriter selector). New keys (kind "kv-new") let
// the user type a key; existing keys edit the value in place. Delete writes a tombstone via AdminDelete.
export function KvBody({ target, onSaved }: { target: KvTarget; onSaved: () => void }) {
  const isNew = target.kind === "kv-new";
  const existingValue = isNew ? new Uint8Array() : target.value;

  const [codec, setCodec] = useState<Codec>(() => (isNew ? "text" : sniff(existingValue)));
  const [text, setText] = useState<string>(() => (isNew ? "" : decode(existingValue, sniff(existingValue))));
  const [keyText, setKeyText] = useState<string>(isNew ? "" : "");
  const [coordinator, setCoordinator] = useState("");
  const [ttl, setTtl] = useState("");
  const [members, setMembers] = useState<MemberState[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [pending, setPending] = useState<Uint8Array | null>(null); // encoded bytes awaiting confirm
  const [saving, setSaving] = useState(false);
  const [deleting, setDeleting] = useState(false);

  useEffect(() => {
    let live = true;
    obs.getClusterView({}).then((v) => live && setMembers(v.members)).catch(() => {});
    return () => {
      live = false;
    };
  }, []);

  // Switching codec re-decodes the original bytes through the new codec (best-effort, never lossy here).
  const switchCodec = (next: Codec) => {
    setError(null);
    setPending(null);
    setCodec(next);
    setText(decode(existingValue, next));
  };

  const oldPreview = useMemo(() => (isNew ? "(new key)" : preview(existingValue, codec, 400)), [existingValue, codec, isNew]);

  const beginSave = () => {
    setError(null);
    if (isNew && keyText.trim() === "") {
      setError("key is required");
      return;
    }
    try {
      setPending(encode(text, codec));
    } catch (e) {
      setError((e as Error).message);
    }
  };

  const confirmSave = async () => {
    if (!pending) return;
    setSaving(true);
    setError(null);
    try {
      const key = isNew ? new TextEncoder().encode(keyText) : target.key;
      const res = await obs.adminPut({
        namespace: target.namespace,
        key,
        value: pending,
        ttlMs: ttl.trim() ? BigInt(ttl.trim()) : undefined,
        targetMemberId: coordinator,
      });
      if (!res.ok) {
        setError(res.error || "write failed");
        return;
      }
      setPending(null);
      onSaved();
    } catch (e) {
      setError(String(e));
    } finally {
      setSaving(false);
    }
  };

  const onDelete = async () => {
    if (isNew) return;
    setDeleting(true);
    setError(null);
    try {
      const res = await obs.adminDelete({ namespace: target.namespace, key: target.key, targetMemberId: coordinator });
      if (!res.ok) {
        setError(res.error || "delete failed");
        return;
      }
      onSaved();
    } catch (e) {
      setError(String(e));
    } finally {
      setDeleting(false);
    }
  };

  const byteLen = useMemo(() => {
    try {
      return encode(text, codec).length;
    } catch {
      return null;
    }
  }, [text, codec]);

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "var(--ws-space-sm)" }}>
      {isNew && (
        <label style={{ display: "flex", flexDirection: "column", gap: "var(--ws-space-xxs)" }}>
          <span style={labelStyle}>Key</span>
          <Input value={keyText} onChange={(e) => setKeyText(e.target.value)} placeholder="e.g. user/42" mono />
        </label>
      )}

      <div style={{ display: "flex", gap: "var(--ws-space-xs)", flexWrap: "wrap" }}>
        {CODECS.map((c) => (
          <Button key={c} variant={c === codec ? "primary" : "ghost"} size="sm" onClick={() => switchCodec(c)}>
            {c.toUpperCase()}
          </Button>
        ))}
        <div style={{ flex: 1 }} />
        <span className="ws-caption ws-muted">{byteLen != null ? formatBytes(byteLen) : "invalid"}</span>
      </div>

      <Textarea
        value={text}
        onChange={(e) => {
          setText(e.target.value);
          setPending(null);
          setError(null);
        }}
        rows={8}
        mono
      />

      <div style={{ display: "grid", gridTemplateColumns: "90px 1fr", gap: "var(--ws-space-sm)", alignItems: "center" }}>
        <span style={labelStyle}>Coordinator</span>
        <Select value={coordinator} onChange={(e) => setCoordinator(e.target.value)}>
          <option value="">This node (auto)</option>
          {members.map((m, i) => {
            const id = m.member?.memberId ?? "?";
            const alive = m.state === MemberLiveness.MEMBER_ALIVE;
            return (
              <option key={i} value={id} disabled={!alive}>
                {id} {m.member?.zone ? `· ${m.member.zone}` : ""} {alive ? "" : "(not alive)"}
              </option>
            );
          })}
        </Select>
        <span style={labelStyle}>TTL (ms)</span>
        <Input value={ttl} onChange={(e) => setTtl(e.target.value)} placeholder="optional, blank = none" mono />
      </div>

      {error && <InlineMessage tone="danger"><span className="ws-mono">{error}</span></InlineMessage>}

      {pending ? (
        <DiffConfirm
          oldText={oldPreview}
          newText={preview(pending, codec, 400)}
          onConfirm={confirmSave}
          onCancel={() => setPending(null)}
          saving={saving}
        />
      ) : (
        <SaveDeleteBar
          canSave={!saving}
          saving={saving}
          onSave={beginSave}
          onDelete={isNew ? undefined : onDelete}
          deleting={deleting}
          deleteLabel="Delete (tombstone)"
        />
      )}
    </div>
  );
}
