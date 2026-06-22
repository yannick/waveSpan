package conflict

import (
	"sort"

	"github.com/cwire/wavespan/internal/version"
	wavespanv1 "github.com/cwire/wavespan/proto/wavespan/v1"
)

// KeepSiblings keeps concurrent writes rather than silently discarding them (design/06
// "Keep siblings"). Concurrency is detected by writer identity: a higher-versioned write from the
// SAME writer (cluster+member) is a causal successor and supersedes that writer's earlier value,
// but writes from DIFFERENT writers are concurrent and both survive. The client resolves siblings
// by issuing a new write. (Full vector-clock causality is a later refinement.)
type KeepSiblings struct{}

// Resolve returns one record per writer (its highest version). A single surviving writer collapses
// to a Winner; multiple surviving writers are returned as Siblings, sorted deterministically.
func (KeepSiblings) Resolve(existing []*wavespanv1.StoredRecord, incoming *wavespanv1.StoredRecord) ResolveResult {
	byWriter := map[string]*wavespanv1.StoredRecord{}
	consider := func(r *wavespanv1.StoredRecord) {
		if r == nil {
			return
		}
		v := version.FromProto(r.GetVersion())
		w := v.WriterClusterID + "\x00" + v.WriterMemberID
		if cur, ok := byWriter[w]; !ok || v.Compare(version.FromProto(cur.GetVersion())) > 0 {
			byWriter[w] = r
		}
	}
	for _, e := range existing {
		consider(e)
	}
	consider(incoming)

	if len(byWriter) == 0 {
		return ResolveResult{Kind: KindReject, Reason: "no records to resolve"}
	}
	sibs := make([]*wavespanv1.StoredRecord, 0, len(byWriter))
	for _, r := range byWriter {
		sibs = append(sibs, r)
	}
	// deterministic order by version compare
	sort.Slice(sibs, func(i, j int) bool {
		return version.FromProto(sibs[i].GetVersion()).Compare(version.FromProto(sibs[j].GetVersion())) < 0
	})
	if len(sibs) == 1 {
		if sibs[0].GetTombstone() {
			return ResolveResult{Kind: KindTombstone, Record: sibs[0]}
		}
		return ResolveResult{Kind: KindWinner, Record: sibs[0]}
	}
	return ResolveResult{Kind: KindSiblings, Siblings: sibs}
}
