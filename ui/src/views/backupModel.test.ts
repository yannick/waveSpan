import { describe, it, expect } from "vitest";
import { create } from "@bufbuild/protobuf";
import { BackupStatus, BackupPhase, BackupPlane, BackupSummarySchema } from "../gen/wavespan/v1/backup_pb";
import {
  statusLabel,
  statusTone,
  isRunning,
  isTerminal,
  kindLabel,
  phaseLabel,
  planesLabel,
  fmtTime,
  pctLabel,
  fmtBytes,
  buildBeginRequest,
  emptyForm,
  planesFromMode,
  splitCsv,
  destinationLabel,
  summaryRow,
} from "./backupModel";

describe("backupModel status helpers", () => {
  it("labels each status", () => {
    expect(statusLabel(BackupStatus.BACKUP_RUNNING)).toBe("RUNNING");
    expect(statusLabel(BackupStatus.BACKUP_COMPLETE)).toBe("COMPLETE");
    expect(statusLabel(BackupStatus.BACKUP_PARTIAL)).toBe("PARTIAL");
    expect(statusLabel(BackupStatus.BACKUP_FAILED)).toBe("FAILED");
    expect(statusLabel(BackupStatus.BACKUP_STATUS_UNSPECIFIED)).toBe("—");
  });

  it("tones complete/partial/failed/running distinctly", () => {
    expect(statusTone(BackupStatus.BACKUP_COMPLETE)).toBe("success");
    expect(statusTone(BackupStatus.BACKUP_PARTIAL)).toBe("warning");
    expect(statusTone(BackupStatus.BACKUP_FAILED)).toBe("danger");
    expect(statusTone(BackupStatus.BACKUP_RUNNING)).toBe("info");
  });

  it("classifies running vs terminal", () => {
    expect(isRunning(BackupStatus.BACKUP_RUNNING)).toBe(true);
    expect(isRunning(BackupStatus.BACKUP_COMPLETE)).toBe(false);
    expect(isTerminal(BackupStatus.BACKUP_COMPLETE)).toBe(true);
    expect(isTerminal(BackupStatus.BACKUP_PARTIAL)).toBe(true);
    expect(isTerminal(BackupStatus.BACKUP_FAILED)).toBe(true);
    expect(isTerminal(BackupStatus.BACKUP_RUNNING)).toBe(false);
  });
});

describe("backupModel poll-stop contract (live progress)", () => {
  // The progress panel polls while RUNNING and clears its interval once isTerminal — drive that gate
  // over a mocked RUNNING→RUNNING→COMPLETE sequence and assert it stops after the terminal status.
  it("keeps polling while RUNNING and stops at the first terminal status", () => {
    const sequence = [
      BackupStatus.BACKUP_RUNNING,
      BackupStatus.BACKUP_RUNNING,
      BackupStatus.BACKUP_COMPLETE,
      BackupStatus.BACKUP_RUNNING, // would never be observed — polling already stopped
    ];
    let polls = 0;
    for (const s of sequence) {
      polls++;
      if (isTerminal(s)) break; // mirrors the component clearing its interval
    }
    expect(polls).toBe(3); // two RUNNING reads + the COMPLETE read, then stop
  });
});

describe("backupModel formatting", () => {
  it("distinguishes full vs incremental", () => {
    expect(kindLabel("")).toBe("full");
    expect(kindLabel("bk-parent")).toBe("incremental ← bk-parent");
  });

  it("labels phases", () => {
    expect(phaseLabel(BackupPhase.EXPORT)).toBe("export");
    expect(phaseLabel(BackupPhase.COMMIT)).toBe("commit");
  });

  it("renders planes", () => {
    expect(planesLabel([BackupPlane.LOGICAL])).toBe("logical");
    expect(planesLabel([BackupPlane.PHYSICAL])).toBe("physical");
    expect(planesLabel([BackupPlane.LOGICAL, BackupPlane.PHYSICAL])).toBe("logical+physical");
    expect(planesLabel([])).toBe("—");
  });

  it("formats time, percent, bytes", () => {
    expect(fmtTime(0n)).toBe("—");
    expect(fmtTime(1719720000000n)).not.toBe("—");
    expect(pctLabel(42.6)).toBe("43%");
    expect(fmtBytes(0n)).toBe("0 B");
    expect(fmtBytes(2048n)).toBe("2.0 KiB");
  });
});

describe("buildBeginRequest", () => {
  it("full logical backup to the default destination omits selection + destination", () => {
    const req = buildBeginRequest(emptyForm());
    expect(req.spec?.selection).toBeUndefined();
    expect(req.spec?.destination).toBeUndefined();
    expect(req.spec?.parent).toBe("");
    expect(req.spec?.planes).toEqual(planesFromMode("logical"));
  });

  it("subset selection parses csv lists; both planes; incremental parent", () => {
    const req = buildBeginRequest({
      ...emptyForm(),
      selectionMode: "subset",
      namespaces: "app, logs",
      graphs: "g1",
      vectorCollections: "",
      planesMode: "both",
      parent: "bk-base",
    });
    expect(req.spec?.selection?.namespaces).toEqual(["app", "logs"]);
    expect(req.spec?.selection?.graphs).toEqual(["g1"]);
    expect(req.spec?.selection?.vectorCollections).toEqual([]);
    expect(req.spec?.planes?.length).toBe(2);
    expect(req.spec?.parent).toBe("bk-base");
  });

  it("named destination carries only the name (no secrets)", () => {
    const req = buildBeginRequest({ ...emptyForm(), destMode: "named", destName: "cold" });
    expect(req.spec?.destination?.name).toBe("cold");
    expect(req.spec?.destination?.bucket ?? "").toBe("");
    expect(req.spec?.destination?.credential).toBeUndefined();
  });

  it("explicit destination with a secret reference (no inline creds)", () => {
    const req = buildBeginRequest({
      ...emptyForm(),
      destMode: "explicit",
      bucket: "adhoc",
      endpoint: "s3.ovh.net",
      region: "de",
      secretRef: "OPS",
    });
    expect(req.spec?.destination?.bucket).toBe("adhoc");
    expect(req.spec?.destination?.credential?.secretName).toBe("OPS");
    expect(req.spec?.destination?.credential?.accessKey ?? "").toBe("");
  });

  it("explicit destination with transient inline creds passes them in the request", () => {
    const req = buildBeginRequest({
      ...emptyForm(),
      destMode: "explicit",
      bucket: "adhoc",
      accessKey: "AK",
      secretKey: "SK",
    });
    expect(req.spec?.destination?.credential?.accessKey).toBe("AK");
    expect(req.spec?.destination?.credential?.secretKey).toBe("SK");
    expect(req.spec?.destination?.credential?.secretName ?? "").toBe("");
  });
});

describe("destinationLabel", () => {
  it("shows bucket (+prefix), name, or default — never credentials", () => {
    expect(destinationLabel(undefined)).toBe("default");
    expect(destinationLabel(create(BackupSummarySchema, {}).destination)).toBe("default");
    expect(destinationLabel({ bucket: "b", prefix: "p" } as never)).toBe("b/p");
    expect(destinationLabel({ bucket: "b" } as never)).toBe("b");
    expect(destinationLabel({ name: "cold" } as never)).toBe("cold");
  });
});

describe("summaryRow", () => {
  it("maps a PARTIAL incremental summary to display fields with NO credentials", () => {
    const s = create(BackupSummarySchema, {
      backupId: "bk-1",
      status: BackupStatus.BACKUP_PARTIAL,
      startedMs: 1000n,
      finishedMs: 2000n,
      parent: "bk-0",
      planes: [BackupPlane.LOGICAL, BackupPlane.PHYSICAL],
      sizeBytes: 2048n,
      retainUntilMs: 3000n,
      partial: true,
      gaps: ["collections-shard:5", "member:m3"],
      destination: {
        bucket: "my-bucket",
        prefix: "pfx",
        credential: { secretName: "s3-secret", accessKey: "LEAK-AK", secretKey: "LEAK-SK" },
      },
    });
    const row = summaryRow(s);
    expect(row.id).toBe("bk-1");
    expect(row.statusLabel).toBe("PARTIAL");
    expect(row.statusTone).toBe("warning");
    expect(row.kind).toBe("incremental ← bk-0");
    expect(row.planes).toBe("logical+physical");
    expect(row.size).toBe("2.0 KiB");
    expect(row.destination).toBe("my-bucket/pfx");
    expect(row.retainUntil).not.toBe("—");
    expect(row.partial).toBe(true);
    expect(row.gaps).toEqual(["collections-shard:5", "member:m3"]);
    expect(row.gapsLabel).toBe("2 gaps");
    // Defence-in-depth: the row is display-only strings — no raw credentials must appear anywhere in it.
    expect(JSON.stringify(row)).not.toContain("LEAK-AK");
    expect(JSON.stringify(row)).not.toContain("LEAK-SK");
  });

  it("a full COMPLETE backup: kind=full, not partial, no gaps label", () => {
    const s = create(BackupSummarySchema, {
      backupId: "bk-9",
      status: BackupStatus.BACKUP_COMPLETE,
      planes: [BackupPlane.LOGICAL],
      sizeBytes: 0n,
    });
    const row = summaryRow(s);
    expect(row.kind).toBe("full");
    expect(row.partial).toBe(false);
    expect(row.gapsLabel).toBe("");
    expect(row.destination).toBe("default");
    expect(row.retainUntil).toBe("—");
  });
});

describe("splitCsv", () => {
  it("trims and drops empties", () => {
    expect(splitCsv(" a, b ,,c ")).toEqual(["a", "b", "c"]);
    expect(splitCsv("")).toEqual([]);
  });
});
