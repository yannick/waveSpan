package observability

import (
	"context"

	"connectrpc.com/connect"
	"github.com/cwire/wavespan/internal/membership"
	wavespanv1 "github.com/cwire/wavespan/proto/wavespan/v1"
)

// AdminPut writes a KV record for testing from the node UI, coordinated by a chosen cluster member
// (design/26). It forwards a Put to the target member's data port so that member becomes the write
// origin (origin+1 then replicates). Failures are reported in the response body (ok=false, error) so
// the UI can show them inline rather than as transport errors. The admin port's authorization
// (SurfaceAdmin) gates who may call it.
func (s *ObsService) AdminPut(ctx context.Context, req *connect.Request[wavespanv1.AdminPutRequest]) (*connect.Response[wavespanv1.AdminPutResponse], error) {
	m := req.Msg
	if s.kvWriter == nil {
		return connect.NewResponse(&wavespanv1.AdminPutResponse{Error: "admin write not enabled on this node"}), nil
	}
	target, ok := s.resolveTarget(m.GetTargetMemberId())
	if !ok {
		return connect.NewResponse(&wavespanv1.AdminPutResponse{Error: "unknown target member: " + m.GetTargetMemberId()}), nil
	}

	put := &wavespanv1.PutRequest{
		Namespace: m.GetNamespace(), Key: m.GetKey(), Value: m.GetValue(), RequireOriginPlusOne: true,
	}
	if m.TtlMs != nil {
		put.TtlMs = m.TtlMs
	}

	res, err := s.kvWriter(ctx, target, put)
	if err != nil {
		return connect.NewResponse(&wavespanv1.AdminPutResponse{CoordinatorMemberId: target.MemberID, Error: err.Error()}), nil
	}
	return connect.NewResponse(&wavespanv1.AdminPutResponse{
		Ok:                  true,
		Version:             res.GetVersion(),
		AckedNearbyReplicas: res.GetAckedNearbyReplicas(),
		CoordinatorMemberId: target.MemberID,
	}), nil
}

// resolveTarget maps a requested member id to a member: empty or self resolves to this node;
// otherwise it must be a current cluster member.
func (s *ObsService) resolveTarget(id string) (membership.Member, bool) {
	if id == "" || id == s.self.MemberID {
		return s.self, true
	}
	for _, mv := range s.cluster.Members() {
		if mv.Member.MemberID == id {
			return mv.Member, true
		}
	}
	return membership.Member{}, false
}
