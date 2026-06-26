package observability

import (
	"context"
	"net/http"
	"sort"

	"connectrpc.com/connect"
	"github.com/yannick/wavespan/internal/rpcopts"
	"github.com/yannick/wavespan/internal/security"
	"github.com/yannick/wavespan/internal/version"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
	"github.com/yannick/wavespan/proto/wavespan/v1/wavespanv1connect"
)

// InspectGlobal resolves a key across the cluster's holders (and peer clusters) and streams one
// InspectKey listing every known holder (design/26). It is best-effort/eventual: the trailer is
// COMPLETE only if every candidate holder answered, else PARTIAL with warnings naming the
// unreachable holders. Values are redacted unless include_value AND admin.
func (s *ObsService) InspectGlobal(ctx context.Context, req *connect.Request[wavespanv1.InspectGlobalRequest], stream *connect.ServerStream[wavespanv1.InspectRow]) error {
	m := req.Msg
	ns, key := m.GetNamespace(), m.GetKey()
	role := security.RoleFrom(ctx)
	reveal := m.GetIncludeValue() && role == security.RoleAdmin

	if err := stream.Send(&wavespanv1.InspectRow{Row: &wavespanv1.InspectRow_Header{Header: &wavespanv1.ResponseMeta{
		ServedByClusterId: s.self.ClusterID, ServedByMemberId: s.self.MemberID, Source: wavespanv1.ReadSource_FETCHED_CLOSEST_HOLDER,
	}}}); err != nil {
		return err
	}

	ik := &wavespanv1.InspectKey{LogicalPath: ns + "/" + string(key), KeyHash: security.KeyHash(ns, key), LogicalKey: key}
	complete := true
	var warnings []string
	var best *wavespanv1.StoredRecord

	if s.clusterResolver == nil {
		complete = false
		warnings = append(warnings, "global holder resolution not configured on this node")
	} else {
		holders, b, c, w := s.clusterResolver.ResolveKey(ctx, ns, key, reveal)
		ik.Holders = append(ik.Holders, holders...)
		best, complete, warnings = b, c, w

		if m.GetIncludePeerClusters() && s.peerInspector != nil {
			ph, pb, pc, pw := s.peerInspector.InspectPeers(ctx, ns, key, reveal)
			ik.Holders = append(ik.Holders, ph...)
			warnings = append(warnings, pw...)
			complete = complete && pc
			if pb != nil && (best == nil || version.FromProto(pb.GetVersion()).Compare(version.FromProto(best.GetVersion())) > 0) {
				best = pb
			}
		}
	}

	if best != nil {
		ik.Version = best.GetVersion()
		ik.Tombstone = best.GetTombstone()
		if best.ExpiresAtUnixMs != nil {
			ik.ExpiresAtUnixMs = best.ExpiresAtUnixMs
		}
		if v := best.GetValue().GetInline(); len(v) > 0 {
			ik.Value = v
		}
	}
	ik.Holders = dedupAndSortHolders(ik.Holders)

	if err := stream.Send(&wavespanv1.InspectRow{Row: &wavespanv1.InspectRow_Key{Key: ik}}); err != nil {
		return err
	}
	completeness := wavespanv1.Completeness_COMPLETE
	if !complete {
		completeness = wavespanv1.Completeness_PARTIAL
	}
	return stream.Send(&wavespanv1.InspectRow{Row: &wavespanv1.InspectRow_Trailer{Trailer: &wavespanv1.InspectTrailer{
		RowsReturned: 1, FinalCompleteness: completeness, Warnings: warnings,
	}}})
}

// dedupAndSortHolders collapses duplicate (peer_cluster_id, member_id) holders — which arise when
// several configured endpoints belong to the SAME peer cluster and each returns that cluster's full
// holder set — keeping the highest version seen, then orders deterministically by
// (peer_cluster_id, member_id) so identical requests yield identical lists.
func dedupAndSortHolders(hs []*wavespanv1.InspectHolder) []*wavespanv1.InspectHolder {
	type key struct{ cluster, member string }
	idx := make(map[key]int, len(hs))
	out := make([]*wavespanv1.InspectHolder, 0, len(hs))
	for _, h := range hs {
		k := key{h.GetPeerClusterId(), h.GetMemberId()}
		if i, ok := idx[k]; ok {
			if version.FromProto(h.GetVersion()).Compare(version.FromProto(out[i].GetVersion())) > 0 {
				out[i] = h
			}
			continue
		}
		idx[k] = len(out)
		out = append(out, h)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].GetPeerClusterId() != out[j].GetPeerClusterId() {
			return out[i].GetPeerClusterId() < out[j].GetPeerClusterId()
		}
		return out[i].GetMemberId() < out[j].GetMemberId()
	})
	return out
}

// Handler returns the mountable ObservabilityService Connect handler for the admin port.
func (s *ObsService) Handler() (string, http.Handler) {
	return wavespanv1connect.NewObservabilityServiceHandler(s, rpcopts.Handler()...)
}
