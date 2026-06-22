package profile

import (
	"fmt"
	"strings"
)

// human formats a value with its unit (time → s/ms/µs, bytes → GB/MB/KB, else thousands).
func human(unit string, v int64) string {
	switch unit {
	case "nanoseconds":
		switch {
		case v >= 1e9:
			return fmt.Sprintf("%.2fs", float64(v)/1e9)
		case v >= 1e6:
			return fmt.Sprintf("%.1fms", float64(v)/1e6)
		case v >= 1e3:
			return fmt.Sprintf("%.1fµs", float64(v)/1e3)
		default:
			return fmt.Sprintf("%dns", v)
		}
	case "bytes":
		switch {
		case v >= 1<<30:
			return fmt.Sprintf("%.2fGB", float64(v)/(1<<30))
		case v >= 1<<20:
			return fmt.Sprintf("%.1fMB", float64(v)/(1<<20))
		case v >= 1<<10:
			return fmt.Sprintf("%.1fKB", float64(v)/(1<<10))
		default:
			return fmt.Sprintf("%dB", v)
		}
	default:
		return commas(v)
	}
}

func commas(v int64) string {
	s := fmt.Sprintf("%d", v)
	var out []byte
	for i, c := range []byte(s) {
		if i > 0 && (len(s)-i)%3 == 0 {
			out = append(out, ',')
		}
		out = append(out, c)
	}
	return string(out)
}

func pct(v, total int64) string {
	if total == 0 {
		return "0.0%"
	}
	return fmt.Sprintf("%.1f%%", 100*float64(v)/float64(total))
}

// shortFn trims a fully-qualified Go function name to package/Type.Method.
func shortFn(fn string) string {
	if i := strings.LastIndex(fn, "/"); i >= 0 {
		fn = fn[i+1:]
	}
	return fn
}

func stripMarkdown(s string) string {
	return strings.NewReplacer("**", "", "`", "").Replace(s)
}

// pattern flags a known cost driver when its substrings appear among the top functions.
type pattern struct {
	match []string
	note  string
}

var costPatterns = map[string][]pattern{
	"block": {
		{[]string{"fsync", ".Sync", "Commit", "WAL", "flush", "Flush"}, "**fsync/WAL durability** dominates blocking — every committed write waits on a disk sync. Batching commits or group-commit would cut write latency the most."},
		{[]string{"AntiEntropy", "Reconcile", "ScanRecords", "FetchReplica"}, "**background intra-cluster anti-entropy** is on the blocking path — it scans keys and fetches from peers, stealing time from foreground requests. Make it incremental/range-hashed and rate-limited."},
		{[]string{"ScanLabel", "ScanRecords", "Iterator", "Seek"}, "**storage scans** block significantly — queries may be doing full scans instead of indexed seeks."},
	},
	"mutex": {
		{[]string{"AntiEntropy", "Reconcile", "holder", "Holder", "Directory"}, "lock contention around **holder/anti-entropy state** — a shared structure is serializing requests."},
		{[]string{"recordstore", "Store", "Apply"}, "contention in the **record store** write path — commits may share one lock."},
	},
	"cpu": {
		{[]string{"AntiEntropy", "Reconcile", "ScanRecords"}, "**background anti-entropy burns CPU** scanning the keyspace; with many keys this scales O(keys×peers) per pass."},
		{[]string{"Marshal", "Unmarshal", "proto"}, "**protobuf (de)serialization** is a large CPU share — consider fewer/larger RPCs or caching decoded forms."},
		{[]string{"runtime.", "mallocgc", "gcBgMarkWorker"}, "the Go **runtime/GC** is a notable CPU share — driven by allocation volume (see the Allocations section)."},
	},
	"alloc": {
		{[]string{"Marshal", "Unmarshal", "proto", "connect"}, "**RPC (de)serialization** allocates heavily — the main GC driver on the hot path."},
		{[]string{"ScanRecords", "Scan", "Iterator", "append"}, "**scan/result buffering** allocates a lot — large intermediate slices per query."},
	},
}

// notesFor produces interpretation bullets for a section: pattern-matched drivers plus the single
// biggest function called out with its share.
func notesFor(kind string, agg []FuncStat, total int64, unit string) []string {
	var notes []string
	if len(agg) > 0 && total > 0 {
		top := agg[0]
		notes = append(notes, fmt.Sprintf("Biggest single cost (cum): `%s` at **%s** (%s of the sampled %s).",
			shortFn(top.Function), human(unit, top.Cum), pct(top.Cum, total), kind))
		// The hottest LEAF (highest flat) is where the work/allocation actually happens — cum just
		// shows the call chain above it. This is usually the real fix target.
		if leaf := maxFlat(agg); leaf != nil && leaf.Function != top.Function && leaf.Flat*5 > total {
			notes = append(notes, fmt.Sprintf("**Hottest leaf: `%s` at %s flat (%s)** — the actual %s happens here, not just in callers above it.",
				shortFn(leaf.Function), human(unit, leaf.Flat), pct(leaf.Flat, total), leafVerb(kind)))
		}
	}
	fired := map[string]bool{}
	for _, p := range costPatterns[kind] {
		for _, fs := range topN(agg, 15) {
			if containsAny(fs.Function, p.match) && !fired[p.note] {
				fired[p.note] = true
				notes = append(notes, p.note)
				break
			}
		}
	}
	return notes
}

func maxFlat(stats []FuncStat) *FuncStat {
	var best *FuncStat
	for i := range stats {
		if best == nil || stats[i].Flat > best.Flat {
			best = &stats[i]
		}
	}
	return best
}

func leafVerb(kind string) string {
	switch kind {
	case "alloc":
		return "allocation"
	case "cpu":
		return "CPU work"
	case "block":
		return "blocking"
	case "mutex":
		return "lock wait"
	default:
		return "cost"
	}
}

func containsAny(s string, subs []string) bool {
	for _, sub := range subs {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}

// appFrames are the substrings that mark a function as application/storage/RPC code worth
// attributing cost to. Pure Go runtime scheduling and net/http server plumbing are noise for a
// "where does it go" view — they sit at the top of every stack — so they are filtered out and
// reported only as an aggregate overhead share.
var appFrames = []string{
	"cwire/wavespan", "wavesdb", "connectrpc", "protobuf", "protoimpl", "protowire",
	"syscall.", "os.(*File)", // fsync / disk I/O — the real write-latency cost
}

// IsAppFrame reports whether a function belongs to code we can act on (vs Go runtime/HTTP plumbing).
func IsAppFrame(fn string) bool { return containsAny(fn, appFrames) }

// filterApp keeps only application/storage/RPC frames.
func filterApp(stats []FuncStat) []FuncStat {
	out := make([]FuncStat, 0, len(stats))
	for _, s := range stats {
		if IsAppFrame(s.Function) {
			out = append(out, s)
		}
	}
	return out
}

// frameworkFlatShare is the fraction of leaf (flat) cost spent in non-application frames (Go runtime,
// scheduler, net/http server) — i.e. unavoidable plumbing overhead.
func frameworkFlatShare(stats []FuncStat, total int64) float64 {
	if total == 0 {
		return 0
	}
	var fw int64
	for _, s := range stats {
		if !IsAppFrame(s.Function) {
			fw += s.Flat
		}
	}
	return float64(fw) / float64(total)
}
