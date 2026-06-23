// Package placement selects nearby durable-replica candidates for the write coordinator,
// applying hard filters then scoring over the latency graph (design/04 "Latency graph",
// design/05 "Candidate selection"). Measured RTT dominates; static topology is a hint.
package placement

import (
	"errors"
	"sort"

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

	var sameGeo, otherGeo []Candidate
	for _, mv := range members {
		m := mv.Member
		if m.MemberID == self.MemberID || mv.State != membership.StateAlive {
			continue // hard filter: self, non-alive
		}
		if policy.RequireDistinctNodes && self.SameNode(m) {
			continue // hard filter: distinct node for durable replicas
		}
		c := Candidate{
			Member: m,
			Score:  graph.Score(m.MemberID, 0, 0, latencygraph.TopologyPenalty(self.Zone, self.Region, self.Geo, m.Zone, m.Region, m.Geo)),
		}
		if m.Geo != "" && self.Geo != "" && m.Geo == complianceGeo {
			sameGeo = append(sameGeo, c)
		} else {
			otherGeo = append(otherGeo, c)
		}
	}
	sortByScore(sameGeo)
	sortByScore(otherGeo)

	switch policy.Geo {
	case RequireLocalGeo:
		if len(sameGeo) < policy.MinAckNearbyReplicas {
			return nil, ErrInsufficientLocalReplicas
		}
		return sameGeo, nil

	case LatencyOnly:
		all := append(append([]Candidate(nil), sameGeo...), otherGeo...)
		sortByScore(all)
		if len(all) == 0 {
			return nil, ErrNoCandidates
		}
		return all, nil

	default: // PreferLocalGeo
		if len(sameGeo) >= policy.MinAckNearbyReplicas {
			// enough local; append spillover only to help reach target-N if allowed
			if policy.AllowSpilloverForDurability {
				return append(sameGeo, markSpillover(otherGeo)...), nil
			}
			return sameGeo, nil
		}
		// not enough same-geo to satisfy minAck: spill if allowed
		if policy.AllowSpilloverForDurability {
			out := append(append([]Candidate(nil), sameGeo...), markSpillover(otherGeo)...)
			if len(out) == 0 {
				return nil, ErrNoCandidates
			}
			return out, nil
		}
		if len(sameGeo) == 0 {
			return nil, ErrNoCandidates
		}
		return sameGeo, nil
	}
}

func markSpillover(cs []Candidate) []Candidate {
	out := make([]Candidate, len(cs))
	for i, c := range cs {
		c.GeoSpillover = true
		out[i] = c
	}
	return out
}

func sortByScore(cs []Candidate) {
	sort.SliceStable(cs, func(i, j int) bool {
		if cs[i].Score != cs[j].Score {
			return cs[i].Score < cs[j].Score
		}
		return cs[i].Member.MemberID < cs[j].Member.MemberID // deterministic tie-break
	})
}
