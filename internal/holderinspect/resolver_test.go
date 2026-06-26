package holderinspect

import (
	"context"
	"errors"
	"testing"

	"github.com/yannick/wavespan/internal/membership"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

type fakeFetcher struct {
	recs map[string]*wavespanv1.FetchReplicaResponse
	errs map[string]error
}

func (f *fakeFetcher) FetchReplica(_ context.Context, t membership.Member, _ string, _ []byte) (*wavespanv1.FetchReplicaResponse, error) {
	if e := f.errs[t.DataAddr]; e != nil {
		return nil, e
	}
	if r := f.recs[t.DataAddr]; r != nil {
		return r, nil
	}
	return &wavespanv1.FetchReplicaResponse{Found: false}, nil
}

type fakeMembers struct{ views []membership.MemberView }

func (f *fakeMembers) Members() []membership.MemberView { return f.views }

func ver(ms uint64) *wavespanv1.Version { return &wavespanv1.Version{HlcPhysicalMs: ms, HlcLogical: 0} }

func aliveView(id, addr string) membership.MemberView {
	return membership.MemberView{Member: membership.Member{MemberID: id, DataAddr: addr}, State: membership.StateAlive}
}

func inline(b string) *wavespanv1.ValueBody {
	return &wavespanv1.ValueBody{Body: &wavespanv1.ValueBody_Inline{Inline: []byte(b)}}
}

// self holds v=10; m2 holds v=20 (newer) and answers; m3 is unreachable.
func TestResolveKey_MergesHoldersLatestWinsPartialOnUnreachable(t *testing.T) {
	self := membership.Member{MemberID: "self", DataAddr: "self:1"}
	members := &fakeMembers{views: []membership.MemberView{
		aliveView("self", "self:1"), aliveView("m2", "m2:1"), aliveView("m3", "m3:1"),
	}}
	fetch := &fakeFetcher{
		recs: map[string]*wavespanv1.FetchReplicaResponse{
			"m2:1": {Found: true, Record: &wavespanv1.StoredRecord{Version: ver(20), Value: inline("newer")}},
		},
		errs: map[string]error{"m3:1": errors.New("dial timeout")},
	}
	selfRec := &wavespanv1.StoredRecord{Version: ver(10), Value: inline("old")}
	r := New(self, members, fetch, func(_ string, _ []byte) (*wavespanv1.StoredRecord, bool, error) { return selfRec, true, nil })

	holders, best, complete, warnings := r.ResolveKey(context.Background(), "ns", []byte("k"), true)

	if complete {
		t.Fatal("expected PARTIAL: m3 unreachable")
	}
	if len(warnings) != 1 {
		t.Fatalf("want 1 warning, got %v", warnings)
	}
	if len(holders) != 2 {
		t.Fatalf("want 2 holders (self,m2), got %d", len(holders))
	}
	if holders[0].MemberId != "m2" || holders[1].MemberId != "self" {
		t.Fatalf("holders not sorted deterministically by member_id: %v", []string{holders[0].MemberId, holders[1].MemberId})
	}
	if best.GetVersion().GetHlcPhysicalMs() != 20 {
		t.Fatalf("best should be v20, got %v", best.GetVersion())
	}
	if got := string(best.GetValue().GetInline()); got != "newer" {
		t.Fatalf("best value should be revealed as 'newer', got %q", got)
	}
}

func TestResolveKey_CompleteWhenAllAnswer(t *testing.T) {
	self := membership.Member{MemberID: "self", DataAddr: "self:1"}
	members := &fakeMembers{views: []membership.MemberView{aliveView("self", "self:1"), aliveView("m2", "m2:1")}}
	fetch := &fakeFetcher{recs: map[string]*wavespanv1.FetchReplicaResponse{"m2:1": {Found: true, Record: &wavespanv1.StoredRecord{Version: ver(5)}}}}
	r := New(self, members, fetch, func(_ string, _ []byte) (*wavespanv1.StoredRecord, bool, error) {
		return &wavespanv1.StoredRecord{Version: ver(7)}, true, nil
	})
	_, _, complete, warnings := r.ResolveKey(context.Background(), "ns", []byte("k"), false)
	if !complete || len(warnings) != 0 {
		t.Fatalf("want COMPLETE no warnings, got complete=%v warns=%v", complete, warnings)
	}
}

// reveal=false must not surface inline values on the best record.
func TestResolveKey_RedactsWhenNotRevealed(t *testing.T) {
	self := membership.Member{MemberID: "self", DataAddr: "self:1"}
	members := &fakeMembers{views: []membership.MemberView{aliveView("self", "self:1")}}
	r := New(self, members, &fakeFetcher{}, func(_ string, _ []byte) (*wavespanv1.StoredRecord, bool, error) {
		return &wavespanv1.StoredRecord{Version: ver(3), Value: inline("secret")}, true, nil
	})
	_, best, _, _ := r.ResolveKey(context.Background(), "ns", []byte("k"), false)
	if v := best.GetValue().GetInline(); len(v) != 0 {
		t.Fatalf("value must be redacted when reveal=false, got %q", v)
	}
}
