package kv

import (
	"context"
	"testing"

	"github.com/yannick/wavespan/internal/cache"
	"github.com/yannick/wavespan/internal/membership"
	"github.com/yannick/wavespan/internal/recordstore"
	"github.com/yannick/wavespan/internal/storage"
	"github.com/yannick/wavespan/internal/version"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

func scanStore(t *testing.T, keys ...string) *recordstore.Store {
	t.Helper()
	mem := storage.NewMemStore()
	t.Cleanup(func() { _ = mem.Close() })
	s := recordstore.NewStore(mem, "dev", "node1", version.NewClock(func() uint64 { return 1000 }, 500), version.NewSequencer(0))
	for _, k := range keys {
		v := s.NextVersion()
		if _, err := s.Apply(s.BuildRecord("default", []byte(k), []byte("V"+k), v, false, nil), wavespanv1.MutationKind_MUTATION_KIND_PUT); err != nil {
			t.Fatal(err)
		}
	}
	return s
}

// runScan collects the header completeness and the ordered row keys from a scan.
func runScan(t *testing.T, sc *Scanner, req *wavespanv1.ScanRequest) (wavespanv1.Completeness, []string) {
	t.Helper()
	var comp wavespanv1.Completeness
	var keys []string
	err := sc.Scan(context.Background(), req, func(r *wavespanv1.ScanResponse) error {
		switch m := r.Msg.(type) {
		case *wavespanv1.ScanResponse_Header:
			comp = m.Header.GetCompleteness()
		case *wavespanv1.ScanResponse_Row:
			keys = append(keys, string(m.Row.GetKey()))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return comp, keys
}

func eqStrs(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestScanCacheFastNeverComplete(t *testing.T) {
	store := scanStore(t, "a", "b", "c")
	sc := NewScanner(store, member("node1"), staticCluster{aliveView("node1")}, nil, nil)
	comp, keys := runScan(t, sc, &wavespanv1.ScanRequest{Namespace: "default", Mode: wavespanv1.ScanMode_CACHE_FAST})
	if comp != wavespanv1.Completeness_BEST_EFFORT {
		t.Fatalf("cache-fast completeness = %v, want BEST_EFFORT", comp)
	}
	if !eqStrs(keys, []string{"a", "b", "c"}) {
		t.Fatalf("cache-fast keys = %v", keys)
	}
}

func TestScanCacheCompleteRequiresValidCertificate(t *testing.T) {
	store := scanStore(t, "a", "b")
	certs := cache.NewCertStore(nil)
	sc := NewScanner(store, member("node1"), staticCluster{aliveView("node1")}, nil, certs)
	sc.nowMs = func() int64 { return 5000 }

	// no certificate -> downgrade to BEST_EFFORT
	if comp, _ := runScan(t, sc, &wavespanv1.ScanRequest{Namespace: "default", Mode: wavespanv1.ScanMode_CACHE_COMPLETE}); comp != wavespanv1.Completeness_BEST_EFFORT {
		t.Fatalf("cache-complete without cert = %v, want BEST_EFFORT", comp)
	}
	// valid certificate covering the whole namespace -> COMPLETE
	certs.Put(&wavespanv1.RangeCoverageCertificate{Namespace: "default", ValidUntilUnixMs: 10000})
	if comp, _ := runScan(t, sc, &wavespanv1.ScanRequest{Namespace: "default", Mode: wavespanv1.ScanMode_CACHE_COMPLETE}); comp != wavespanv1.Completeness_COMPLETE {
		t.Fatalf("cache-complete with valid cert = %v, want COMPLETE", comp)
	}
	// after expiry -> downgrade again (property 4: never claim complete without a valid cert)
	sc.nowMs = func() int64 { return 20000 }
	if comp, _ := runScan(t, sc, &wavespanv1.ScanRequest{Namespace: "default", Mode: wavespanv1.ScanMode_CACHE_COMPLETE}); comp != wavespanv1.Completeness_BEST_EFFORT {
		t.Fatalf("cache-complete with expired cert = %v, want BEST_EFFORT (downgrade)", comp)
	}
}

type fakeHolderScanner struct {
	rows      map[string][]*wavespanv1.ScanLocalRow
	gotLimits []int
}

func (f *fakeHolderScanner) ScanLocal(_ context.Context, target membership.Member, _ string, _, _ []byte, limit int) ([]*wavespanv1.ScanLocalRow, error) {
	f.gotLimits = append(f.gotLimits, limit)
	rows := f.rows[target.MemberID]
	if limit > 0 && len(rows) > limit {
		rows = rows[:limit]
	}
	return rows, nil
}

// TestScanRoutedPushesLimitToHolders pins design/37 P0.2: a limited routed scan must ask each
// holder for a bounded row count (2× the caller limit as merge slack), not the whole namespace,
// and still return the correct first-limit merged rows.
func TestScanRoutedPushesLimitToHolders(t *testing.T) {
	store := scanStore(t, "a", "e") // self holds a, e
	v := version.Version{HLCPhysicalMs: 1, WriterClusterID: "dev", WriterMemberID: "node2", WriterSequence: 1}
	holder := &fakeHolderScanner{rows: map[string][]*wavespanv1.ScanLocalRow{
		"node2": {
			{Key: []byte("b"), Value: []byte("Vb"), Version: v.ToProto()},
			{Key: []byte("c"), Value: []byte("Vc"), Version: v.ToProto()},
			{Key: []byte("d"), Value: []byte("Vd"), Version: v.ToProto()},
			{Key: []byte("f"), Value: []byte("Vf"), Version: v.ToProto()},
		},
	}}
	sc := NewScanner(store, member("node1"), staticCluster{aliveView("node1"), aliveView("node2")}, holder, nil)
	_, keys := runScan(t, sc, &wavespanv1.ScanRequest{Namespace: "default", Mode: wavespanv1.ScanMode_ROUTED_EVENTUAL, Limit: 2})
	if !eqStrs(keys, []string{"a", "b"}) {
		t.Fatalf("routed limited keys = %v, want a,b", keys)
	}
	if len(holder.gotLimits) != 1 || holder.gotLimits[0] != 4 {
		t.Fatalf("holder scan limits = %v, want one call with limit 4 (2x caller limit)", holder.gotLimits)
	}
	// limit 0 (scan-all) stays unbounded by contract.
	holder.gotLimits = nil
	_, keys = runScan(t, sc, &wavespanv1.ScanRequest{Namespace: "default", Mode: wavespanv1.ScanMode_ROUTED_EVENTUAL})
	if !eqStrs(keys, []string{"a", "b", "c", "d", "e", "f"}) {
		t.Fatalf("routed unlimited keys = %v", keys)
	}
	if len(holder.gotLimits) != 1 || holder.gotLimits[0] != 0 {
		t.Fatalf("holder scan limits = %v, want one call with limit 0", holder.gotLimits)
	}
}

func TestScanRoutedMergesHoldersSorted(t *testing.T) {
	store := scanStore(t, "a", "c") // self holds a, c
	v := version.Version{HLCPhysicalMs: 1, WriterClusterID: "dev", WriterMemberID: "node2", WriterSequence: 1}
	holder := &fakeHolderScanner{rows: map[string][]*wavespanv1.ScanLocalRow{
		"node2": {
			{Key: []byte("b"), Value: []byte("Vb"), Version: v.ToProto()},
			{Key: []byte("c"), Value: []byte("Vc"), Version: v.ToProto()}, // duplicate of self's c (older)
			{Key: []byte("d"), Value: []byte("Vd"), Version: v.ToProto()},
		},
	}}
	sc := NewScanner(store, member("node1"), staticCluster{aliveView("node1"), aliveView("node2")}, holder, nil)
	comp, keys := runScan(t, sc, &wavespanv1.ScanRequest{Namespace: "default", Mode: wavespanv1.ScanMode_ROUTED_EVENTUAL})
	if comp != wavespanv1.Completeness_PARTIAL {
		t.Fatalf("routed completeness = %v, want PARTIAL", comp)
	}
	if !eqStrs(keys, []string{"a", "b", "c", "d"}) {
		t.Fatalf("routed merged keys = %v, want sorted+deduped a,b,c,d", keys)
	}
}
