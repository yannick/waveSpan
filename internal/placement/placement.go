// Package placement selects nearby durable-replica candidates for the write coordinator,
// applying hard filters then scoring over the latency graph (design/04 "Latency graph",
// design/05 "Candidate selection"). Measured RTT dominates; static topology is a hint.
package placement

import (
	"cmp"
	"errors"
	"slices"
	"strings"

	"github.com/yannick/wavespan/internal/latencygraph"
	"github.com/yannick/wavespan/internal/membership"
)

// GeoPolicy controls cross-geo replica placement (design/00 "Geo policy modes").
type GeoPolicy int

const (
	// PreferLocalGeo prefers same-geo replicas, spilling to the nearest allowed geo only when
	// needed and permitted.
	PreferLocalGeo GeoPolicy = iota
	// RequireLocalGeo never replicates outside the compliance geo; it fails if it cannot.
	RequireLocalGeo
	// LatencyOnly ignores geo labels and chooses purely by latency, subject to node diversity.
	LatencyOnly
)

// Policy is the placement policy for a write.
type Policy struct {
	TargetNearbyReplicas        int
	MinAckNearbyReplicas        int
	RequireDistinctNodes        bool
	Geo                         GeoPolicy
	AllowSpilloverForDurability bool
	ComplianceGeo               string // required local geo for RequireLocalGeo (defaults to self.Geo)
}

// Candidate is a scored placement target.
type Candidate struct {
	Member       membership.Member
	Score        float64
	GeoSpillover bool
}

var (
	// ErrNoCandidates means no peer can host a nearby durable replica (origin+1 must fail).
	ErrNoCandidates = errors.New("placement: no nearby replica candidates")
	// ErrInsufficientLocalReplicas means require-local-geo cannot meet minAck within the geo.
	ErrInsufficientLocalReplicas = errors.New("placement: insufficient local-geo replicas")
)

// Select returns placement candidates ordered best (lowest score) first. It returns an error
// when the policy cannot be satisfied so the coordinator can fail the write.
func Select(self membership.Member, members []membership.MemberView, graph *latencygraph.Graph, policy Policy) ([]Candidate, error) {
	complianceGeo := policy.ComplianceGeo
	if complianceGeo == "" {
		complianceGeo = self.Geo
	}

	// One backing buffer, partitioned in place: all[:nSame] holds compliance-geo candidates,
	// all[nSame:] the rest. Select runs on every write, so it must not allocate beyond this
	// single make (pinned by TestSelectAllocatesAtMostOnce).
	all := make([]Candidate, 0, len(members))
	nSame := 0
	for _, mv := range members {
		m := mv.Member
		if m.MemberID == self.MemberID || mv.State != membership.StateAlive {
			continue // hard filter: self, non-alive
		}
		if policy.RequireDistinctNodes && self.SameNode(m) {
			continue // hard filter: distinct node for durable replicas
		}
		all = append(all, Candidate{
			Member: m,
			Score:  graph.Score(m.MemberID, 0, 0, latencygraph.TopologyPenalty(self.Zone, self.Region, self.Geo, m.Zone, m.Region, m.Geo)),
		})
		if m.Geo != "" && self.Geo != "" && m.Geo == complianceGeo {
			last := len(all) - 1
			all[last], all[nSame] = all[nSame], all[last]
			nSame++
		}
	}

	switch policy.Geo {
	case RequireLocalGeo:
		sameGeo := all[:nSame]
		if len(sameGeo) < policy.MinAckNearbyReplicas {
			return nil, ErrInsufficientLocalReplicas
		}
		sortByScore(sameGeo)
		return sameGeo, nil

	case LatencyOnly:
		if len(all) == 0 {
			return nil, ErrNoCandidates
		}
		sortByScore(all)
		return all, nil

	default: // PreferLocalGeo
		sameGeo, otherGeo := all[:nSame], all[nSame:]
		sortByScore(sameGeo)
		if policy.AllowSpilloverForDurability {
			// spillover follows local: it helps reach target-N when minAck is satisfied
			// locally, and covers minAck itself when the local geo cannot.
			sortByScore(otherGeo)
			markSpillover(otherGeo)
			if len(sameGeo) < policy.MinAckNearbyReplicas && len(all) == 0 {
				return nil, ErrNoCandidates
			}
			return all, nil
		}
		if len(sameGeo) < policy.MinAckNearbyReplicas && len(sameGeo) == 0 {
			return nil, ErrNoCandidates
		}
		return sameGeo, nil
	}
}

func markSpillover(cs []Candidate) {
	for i := range cs {
		cs[i].GeoSpillover = true
	}
}

// sortByScore orders candidates best (lowest score) first; the unique MemberID tie-break
// makes the order fully deterministic without needing a stable sort.
func sortByScore(cs []Candidate) {
	slices.SortFunc(cs, func(a, b Candidate) int {
		if a.Score != b.Score {
			return cmp.Compare(a.Score, b.Score)
		}
		return strings.Compare(a.Member.MemberID, b.Member.MemberID)
	})
}
