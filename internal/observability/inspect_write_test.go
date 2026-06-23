package observability

import (
	"context"
	"errors"
	"testing"

	"connectrpc.com/connect"
	"github.com/yannick/wavespan/internal/membership"
	"github.com/yannick/wavespan/internal/recordstore"
	"github.com/yannick/wavespan/internal/storage"
	"github.com/yannick/wavespan/internal/version"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

func writerObs(t *testing.T, w KvWriter) *ObsService {
	t.Helper()
	mem := storage.NewMemStore()
	t.Cleanup(func() { _ = mem.Close() })
	rs := recordstore.NewStore(mem, "dev", "node1", version.NewClock(nil, 500), version.NewSequencer(0))
	cluster := membersCluster{members: []membership.MemberView{
		{Member: membership.Member{MemberID: "node1", DataAddr: "node1:7800"}, State: membership.StateAlive},
		{Member: membership.Member{MemberID: "node2", DataAddr: "node2:7800"}, State: membership.StateAlive},
	}}
	self := membership.Member{ClusterID: "dev", MemberID: "node1", DataAddr: "node1:7800"}
	return NewObsService(NewGossipRing(64), cluster, self, rs).WithKvWriter(w)
}

func TestAdminPutForwardsToSelectedCoordinator(t *testing.T) {
	var gotTarget membership.Member
	var gotReq *wavespanv1.PutRequest
	obs := writerObs(t, func(_ context.Context, target membership.Member, req *wavespanv1.PutRequest) (*wavespanv1.PutResult, error) {
		gotTarget, gotReq = target, req
		return &wavespanv1.PutResult{Version: &wavespanv1.Version{WriterMemberId: target.MemberID}, AckedNearbyReplicas: 1}, nil
	})

	resp, err := obs.AdminPut(context.Background(), connect.NewRequest(&wavespanv1.AdminPutRequest{
		Namespace: "default", Key: []byte("k"), Value: []byte("v"), TargetMemberId: "node2",
	}))
	if err != nil {
		t.Fatal(err)
	}
	m := resp.Msg
	if !m.GetOk() || m.GetCoordinatorMemberId() != "node2" {
		t.Fatalf("expected ok via node2, got %+v", m)
	}
	if gotTarget.MemberID != "node2" || gotTarget.DataAddr != "node2:7800" {
		t.Fatalf("forwarded to wrong member: %+v", gotTarget)
	}
	if !gotReq.GetRequireOriginPlusOne() || string(gotReq.GetValue()) != "v" {
		t.Fatalf("forwarded request malformed: %+v", gotReq)
	}
	if m.GetAckedNearbyReplicas() != 1 {
		t.Fatalf("ack count not surfaced: %+v", m)
	}
}

func TestAdminPutEmptyTargetUsesSelf(t *testing.T) {
	var gotTarget membership.Member
	obs := writerObs(t, func(_ context.Context, target membership.Member, _ *wavespanv1.PutRequest) (*wavespanv1.PutResult, error) {
		gotTarget = target
		return &wavespanv1.PutResult{}, nil
	})
	resp, err := obs.AdminPut(context.Background(), connect.NewRequest(&wavespanv1.AdminPutRequest{Namespace: "default", Key: []byte("k"), Value: []byte("v")}))
	if err != nil {
		t.Fatal(err)
	}
	if !resp.Msg.GetOk() || gotTarget.MemberID != "node1" {
		t.Fatalf("empty target should coordinate on self (node1), got target=%q ok=%v", gotTarget.MemberID, resp.Msg.GetOk())
	}
}

func TestAdminPutUnknownTargetReported(t *testing.T) {
	obs := writerObs(t, func(_ context.Context, _ membership.Member, _ *wavespanv1.PutRequest) (*wavespanv1.PutResult, error) {
		t.Fatal("writer must not be called for an unknown target")
		return nil, nil
	})
	resp, err := obs.AdminPut(context.Background(), connect.NewRequest(&wavespanv1.AdminPutRequest{Namespace: "default", Key: []byte("k"), TargetMemberId: "ghost"}))
	if err != nil {
		t.Fatal(err)
	}
	if resp.Msg.GetOk() || resp.Msg.GetError() == "" {
		t.Fatalf("unknown target must report ok=false with an error: %+v", resp.Msg)
	}
}

func TestAdminPutForwardErrorReported(t *testing.T) {
	obs := writerObs(t, func(_ context.Context, _ membership.Member, _ *wavespanv1.PutRequest) (*wavespanv1.PutResult, error) {
		return nil, errors.New("data port unreachable")
	})
	resp, err := obs.AdminPut(context.Background(), connect.NewRequest(&wavespanv1.AdminPutRequest{Namespace: "default", Key: []byte("k"), TargetMemberId: "node2"}))
	if err != nil {
		t.Fatal(err)
	}
	if resp.Msg.GetOk() || resp.Msg.GetError() == "" || resp.Msg.GetCoordinatorMemberId() != "node2" {
		t.Fatalf("forward error must surface in the response: %+v", resp.Msg)
	}
}
