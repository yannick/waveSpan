package observability

import (
	"context"
	"net/http"

	"connectrpc.com/connect"
	"github.com/yannick/wavespan/internal/rpcopts"
	"github.com/yannick/wavespan/internal/security"
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

	// the local node's own copy, if present.
	complete := true
	var warnings []string
	if rec, found, err := s.rstore.GetRecord(ns, key); err == nil && found {
		ik.Version = rec.GetVersion()
		ik.Tombstone = rec.GetTombstone()
		if rec.ExpiresAtUnixMs != nil {
			ik.ExpiresAtUnixMs = rec.ExpiresAtUnixMs
		}
		if reveal && !rec.GetTombstone() {
			ik.Value = rec.GetValue().GetInline()
		}
		ik.Holders = append(ik.Holders, &wavespanv1.InspectHolder{MemberId: s.self.MemberID, HolderClass: wavespanv1.HolderClass_HOLDER_DURABLE, Version: rec.GetVersion()})
	}

	// cross-holder (and cross-cluster) resolution, when an inspector is wired.
	if s.globalInspector != nil {
		holders, w, c := s.globalInspector.InspectKey(ctx, ns, key, m.GetIncludePeerClusters(), reveal)
		ik.Holders = append(ik.Holders, holders...)
		warnings = append(warnings, w...)
		complete = complete && c
	} else {
		complete = false
		warnings = append(warnings, "global holder resolution not configured on this node")
	}

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

// Handler returns the mountable ObservabilityService Connect handler for the admin port.
func (s *ObsService) Handler() (string, http.Handler) {
	return wavespanv1connect.NewObservabilityServiceHandler(s, rpcopts.Handler()...)
}
