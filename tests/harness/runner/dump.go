package runner

import (
	"fmt"
	"sort"
	"strings"

	"github.com/yannick/wavespan/internal/version"
)

// Violation is a checker finding (mirrors testing-waves dumpFailure forensics).
type Violation struct {
	Property string   // the checker name, e.g. "convergence"
	Detail   string   // human-readable explanation
	Key      string   // the offending key (if any)
	Window   [2]int64 // [startMs, endMs] of the offending op window
}

// Dump renders a forensic report for a violation: the seed, the offending op window, per-replica
// disagreement for the key, and the nemesis timeline (design/25 "forensic dump").
func Dump(h *History, v Violation) string {
	var b strings.Builder
	fmt.Fprintf(&b, "=== HARNESS VIOLATION: %s ===\n", v.Property)
	fmt.Fprintf(&b, "seed: %d\n", h.Seed)
	fmt.Fprintf(&b, "detail: %s\n", v.Detail)
	if v.Key != "" {
		fmt.Fprintf(&b, "key: %s\n", v.Key)
	}
	fmt.Fprintf(&b, "window: [%d, %d]ms\n", v.Window[0], v.Window[1])

	if v.Key != "" {
		fmt.Fprintf(&b, "\nper-replica reads of %q post-heal (healed@%dms):\n", v.Key, h.HealedAtMs())
		reads := h.PostHealReads(v.Key)
		sort.Slice(reads, func(i, j int) bool { return reads[i].ServedBy < reads[j].ServedBy })
		for _, r := range reads {
			fmt.Fprintf(&b, "  %-10s value=%q version=%s\n", r.ServedBy, r.Value, verStr(r.ObservedVersion))
		}
	}

	fmt.Fprintf(&b, "\nnemesis timeline:\n")
	faults := append([]Fault(nil), h.Faults...)
	sort.Slice(faults, func(i, j int) bool { return faults[i].StartMs < faults[j].StartMs })
	for _, f := range faults {
		fmt.Fprintf(&b, "  [%d-%d]ms %s targets=%v\n", f.StartMs, f.EndMs, f.Kind, f.Targets)
	}
	return b.String()
}

func verStr(v *version.Version) string {
	if v == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%d.%d@%s/%s#%d", v.HLCPhysicalMs, v.HLCLogical, v.WriterClusterID, v.WriterMemberID, v.WriterSequence)
}
