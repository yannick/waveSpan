package conflict

import (
	"github.com/yannick/wavespan/internal/version"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

// HLCLastWriteWins picks the single record with the highest version under the deterministic
// hlc-last-write-wins order (design/06). A tombstone wins iff its version wins ("Delete conflicts":
// a delete wins only if its version is the winner).
type HLCLastWriteWins struct{}

// Resolve returns the highest-versioned record (which may be a tombstone).
func (HLCLastWriteWins) Resolve(existing []*wavespanv1.StoredRecord, incoming *wavespanv1.StoredRecord) ResolveResult {
	winner := incoming
	for _, e := range existing {
		if e == nil {
			continue
		}
		if winner == nil || version.FromProto(e.GetVersion()).Compare(version.FromProto(winner.GetVersion())) > 0 {
			winner = e
		}
	}
	if winner == nil {
		return ResolveResult{Kind: KindReject, Reason: "no records to resolve"}
	}
	if winner.GetTombstone() {
		return ResolveResult{Kind: KindTombstone, Record: winner}
	}
	return ResolveResult{Kind: KindWinner, Record: winner}
}
