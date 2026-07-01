package backup

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/yannick/wavespan/internal/collections"
)

// Held-range coverage (F1). A cluster backup is PARTIAL when the live cluster did not cover the full
// expected keyspace, not only when the assigner enumerated a gap. Coverage is checked per tier because
// the tiers differ fundamentally:
//
//   - Collections (CP/raft) has a DETERMINISTIC partition: the data shards [FirstDataShard, +N). Each
//     node reports the data shards it actually hosts; a shard hosted by no exporting node is a real gap.
//   - KV/global (AP) has NO deterministic per-node ownership — keys are replicated by placement/holder
//     directory, not partitioned, so "which node must hold key X" is not enumerable. The only honest
//     cluster-wide guarantee for the AP tier is therefore MEMBER COMPLETENESS: every expected member
//     exported. A missing member may have held the only replica of some normal-namespace key
//     (replicate-everywhere namespaces survive it, but normal ones may not), so an absent member is the
//     correct conservative PARTIAL signal for KV. PARTIAL for KV means "a member didn't export", NOT a
//     per-key coverage proof (which is not computable here).
//
// shardTokenPrefix tags a hosted-data-shard entry in a NodeRecord's HeldRanges (e.g. "shard:2").
const shardTokenPrefix = "shard:"

// shardToken encodes a data-shard id as a HeldRanges token.
func shardToken(id uint64) string { return shardTokenPrefix + strconv.FormatUint(id, 10) }

// formatHeldShards encodes hosted data-shard ids as HeldRanges tokens, ascending.
func formatHeldShards(ids []uint64) []string {
	out := make([]string, 0, len(ids))
	sorted := append([]uint64(nil), ids...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	for _, id := range sorted {
		out = append(out, shardToken(id))
	}
	return out
}

// parseShardToken decodes a "shard:<id>" HeldRanges token; ok is false for any other token.
func parseShardToken(s string) (uint64, bool) {
	if !strings.HasPrefix(s, shardTokenPrefix) {
		return 0, false
	}
	id, err := strconv.ParseUint(strings.TrimPrefix(s, shardTokenPrefix), 10, 64)
	if err != nil {
		return 0, false
	}
	return id, true
}

// coverageGaps returns the coverage gaps for a committed backup: data shards hosted by no exporting node
// ("collections-shard:<id>") and expected members that did not export ("member:<id>"). Only nodes that
// finished export (Done) count as covering their shards / being present. expectedDataShards == 0 means the
// data-shard count is unknown → the collections check is skipped (never a false PARTIAL). Order is stable:
// shard gaps ascending, then member gaps sorted.
func coverageGaps(perNode []NodeRecord, expectedDataShards uint64, expectedMembers []string) []string {
	held := map[uint64]bool{}
	exported := map[string]bool{}
	for _, n := range perNode {
		if !n.Done {
			continue // reported held ranges at prepare but did not finish export → does not cover
		}
		exported[n.MemberID] = true
		for _, r := range n.HeldRanges {
			if id, ok := parseShardToken(r); ok {
				held[id] = true
			}
		}
	}

	var gaps []string
	if expectedDataShards > 0 {
		for id := collections.FirstDataShard; id < collections.FirstDataShard+expectedDataShards; id++ {
			if !held[id] {
				gaps = append(gaps, fmt.Sprintf("collections-shard:%d", id))
			}
		}
	}
	missing := make([]string, 0)
	for _, m := range expectedMembers {
		if !exported[m] {
			missing = append(missing, m)
		}
	}
	sort.Strings(missing)
	for _, m := range missing {
		gaps = append(gaps, "member:"+m)
	}
	return gaps
}
