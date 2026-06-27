package observability

import (
	"context"

	"connectrpc.com/connect"
	"github.com/yannick/wavespan/internal/membership"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
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

// AdminDelete writes a tombstone for a KV record from the Data Browser, coordinated by a chosen
// cluster member (mirrors AdminPut). A delete is a Put(tombstone) on the target's data port, so the
// chosen member becomes the write origin and the tombstone replicates from there. Failures are
// reported in the response body so the UI can show them inline.
func (s *ObsService) AdminDelete(ctx context.Context, req *connect.Request[wavespanv1.AdminDeleteRequest]) (*connect.Response[wavespanv1.AdminDeleteResponse], error) {
	m := req.Msg
	if s.kvDeleter == nil {
		return connect.NewResponse(&wavespanv1.AdminDeleteResponse{Error: "admin delete not enabled on this node"}), nil
	}
	target, ok := s.resolveTarget(m.GetTargetMemberId())
	if !ok {
		return connect.NewResponse(&wavespanv1.AdminDeleteResponse{Error: "unknown target member: " + m.GetTargetMemberId()}), nil
	}

	del := &wavespanv1.DeleteRequest{
		Namespace: m.GetNamespace(), Key: m.GetKey(), RequireOriginPlusOne: true,
	}
	res, err := s.kvDeleter(ctx, target, del)
	if err != nil {
		return connect.NewResponse(&wavespanv1.AdminDeleteResponse{CoordinatorMemberId: target.MemberID, Error: err.Error()}), nil
	}
	return connect.NewResponse(&wavespanv1.AdminDeleteResponse{
		Ok:                  true,
		Version:             res.GetVersion(),
		AckedNearbyReplicas: res.GetAckedNearbyReplicas(),
		CoordinatorMemberId: target.MemberID,
	}), nil
}

// DeleteNamespace tombstones every live key in a namespace cluster-wide. It enumerates the namespace
// the same way the Data Browser does (a cluster-wide fan-out scan, merged by key) and issues a
// coordinated Delete for each live key via this node as coordinator, so the tombstones replicate.
// It is best-effort: a key whose delete fails is skipped and not counted. The namespace may still
// appear in the gossiped list until holder summaries age out (an emptied namespace is acceptable).
func (s *ObsService) DeleteNamespace(ctx context.Context, req *connect.Request[wavespanv1.DeleteNamespaceRequest]) (*connect.Response[wavespanv1.DeleteNamespaceResponse], error) {
	ns := req.Msg.GetNamespace()
	if ns == "" {
		return connect.NewResponse(&wavespanv1.DeleteNamespaceResponse{Error: "namespace is required"}), nil
	}
	if s.kvDeleter == nil {
		return connect.NewResponse(&wavespanv1.DeleteNamespaceResponse{Error: "admin delete not enabled on this node"}), nil
	}
	target, ok := s.resolveTarget("")
	if !ok {
		return connect.NewResponse(&wavespanv1.DeleteNamespaceResponse{Error: "no coordinator available"}), nil
	}

	// Enumerate every key in the namespace across the cluster (values not revealed — we only need keys).
	keys, err := s.collectInspectKeys(ctx, &wavespanv1.InspectLocalRequest{Namespace: ns, ClusterWide: true}, ns, nil, nil, false)
	if err != nil {
		return connect.NewResponse(&wavespanv1.DeleteNamespaceResponse{Error: err.Error()}), nil
	}

	var deleted uint64
	for _, ik := range keys {
		if ik.GetTombstone() {
			continue // already dead
		}
		del := &wavespanv1.DeleteRequest{Namespace: ns, Key: ik.GetLogicalKey(), RequireOriginPlusOne: true}
		if _, derr := s.kvDeleter(ctx, target, del); derr != nil {
			continue // best-effort: skip keys that fail to delete
		}
		deleted++
	}
	return connect.NewResponse(&wavespanv1.DeleteNamespaceResponse{Ok: true, DeletedKeys: deleted}), nil
}

// AdminPutGraph upserts (or, on delete, tombstones) a single graph node or edge from the node UI,
// mirroring AdminPut/AdminDelete so graph editing is symmetric with the other models. The record is
// version-stamped via newGraphVersion (the same stamping the sample-dataset loader uses) and written
// through the local graph store's atomic Batch (PutNode/PutEdge + Commit).
//
// v1 CAVEAT: this writes to THIS node's LOCAL graph store only — graph mutations are not yet
// coordinator-forwarded/replicated like KV (target_member_id is accepted but not used to forward).
// This matches the existing graph write path (LoadSampleDataset), which also applies locally.
// Failures are reported in the response body (ok=false, error) so the UI can show them inline rather
// than as transport errors.
func (s *ObsService) AdminPutGraph(_ context.Context, req *connect.Request[wavespanv1.AdminPutGraphRequest]) (*connect.Response[wavespanv1.AdminPutGraphResponse], error) {
	m := req.Msg
	if s.graph == nil || s.newGraphVersion == nil {
		return connect.NewResponse(&wavespanv1.AdminPutGraphResponse{Error: "graph write not enabled"}), nil
	}
	if m.GetGraphId() == "" {
		return connect.NewResponse(&wavespanv1.AdminPutGraphResponse{Error: "graph_id is required"}), nil
	}
	node, edge := m.GetNode(), m.GetEdge()
	if (node == nil) == (edge == nil) {
		return connect.NewResponse(&wavespanv1.AdminPutGraphResponse{Error: "exactly one of node or edge must be set"}), nil
	}

	v := s.newGraphVersion()
	b := s.graph.NewBatch()
	if node != nil {
		node.GraphId = m.GetGraphId()
		node.Version = v
		node.Tombstone = m.GetDelete()
		if err := b.PutNode(node); err != nil {
			return connect.NewResponse(&wavespanv1.AdminPutGraphResponse{Error: err.Error()}), nil
		}
	} else {
		edge.GraphId = m.GetGraphId()
		edge.Version = v
		edge.Tombstone = m.GetDelete()
		if err := b.PutEdge(edge); err != nil {
			return connect.NewResponse(&wavespanv1.AdminPutGraphResponse{Error: err.Error()}), nil
		}
	}
	if err := b.Commit(s.graph); err != nil {
		return connect.NewResponse(&wavespanv1.AdminPutGraphResponse{Error: err.Error()}), nil
	}
	return connect.NewResponse(&wavespanv1.AdminPutGraphResponse{Ok: true, Version: v}), nil
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
