import { describe, it, expect } from "vitest";
import { BackupStatus, BackupPhase, BackupPlane } from "../gen/wavespan/v1/backup_pb";
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
