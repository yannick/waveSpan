package recordstore

import (
	"fmt"
	"testing"

	"github.com/yannick/wavespan/internal/version"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

func lkPut(t *testing.T, s *Store, ns, key, val string) {
	t.Helper()
	v := s.NextVersion()
	if _, err := s.Apply(s.BuildRecord(ns, []byte(key), []byte(val), v, false, nil), wavespanv1.MutationKind_MUTATION_KIND_PUT); err != nil {
		t.Fatal(err)
	}
}

func lkDel(t *testing.T, s *Store, ns, key string) {
	t.Helper()
	v := s.NextVersion()
	if _, err := s.Apply(s.BuildRecord(ns, []byte(key), nil, v, true, nil), wavespanv1.MutationKind_MUTATION_KIND_DELETE); err != nil {
		t.Fatal(err)
	}
}

func wantLive(t *testing.T, s *Store, want int64) {
	t.Helper()
	if got := s.LiveKeys(); got != want {
		t.Fatalf("LiveKeys() = %d, want %d", got, want)
	}
}

func TestLiveKeysInsertsAndOverwrites(t *testing.T) {
	s := newTestStore(t)
	wantLive(t, s, 0)
	lkPut(t, s, "default", "a", "1")
	lkPut(t, s, "default", "b", "1")
	wantLive(t, s, 2)
	// Overwriting an existing key must not change the live count.
	lkPut(t, s, "default", "a", "2")
	lkPut(t, s, "default", "a", "3")
	wantLive(t, s, 2)
}

func TestLiveKeysDeleteAndResurrect(t *testing.T) {
	s := newTestStore(t)
	lkPut(t, s, "default", "k", "v")
	wantLive(t, s, 1)
	lkDel(t, s, "default", "k")
	wantLive(t, s, 0)
	// Deleting an already-dead key stays at 0.
	lkDel(t, s, "default", "k")
	wantLive(t, s, 0)
	// Resurrecting (writing over a tombstone) brings it back to 1.
	lkPut(t, s, "default", "k", "again")
	wantLive(t, s, 1)
}

func TestLiveKeysNamespaceIsolation(t *testing.T) {
	s := newTestStore(t)
	lkPut(t, s, "ns1", "k", "v")
	lkPut(t, s, "ns2", "k", "v") // same user key, different namespace = distinct key
	wantLive(t, s, 2)
}

func TestLiveKeysStaleWriteNoDoubleCount(t *testing.T) {
	s := newTestStore(t)
	high := s.NextVersion()
	if _, err := s.Apply(s.BuildRecord("default", []byte("k"), []byte("new"), high, false, nil), wavespanv1.MutationKind_MUTATION_KIND_PUT); err != nil {
		t.Fatal(err)
	}
	wantLive(t, s, 1)
	// A losing (older) write must not change the count.
	low := version.Version{HLCPhysicalMs: 1, WriterClusterID: "dev", WriterMemberID: "node1", WriterSequence: 1}
	if _, err := s.Apply(s.BuildRecord("default", []byte("k"), []byte("old"), low, false, nil), wavespanv1.MutationKind_MUTATION_KIND_PUT); err != nil {
		t.Fatal(err)
	}
	wantLive(t, s, 1)
}

// TestLiveKeysSurvivesCacheReset forces the per-stripe version cache past its cap so a stripe resets,
// then re-touches evicted keys: the count must stay exact because the slow path re-reads the prior
// pointer from storage.
func TestLiveKeysSurvivesCacheReset(t *testing.T) {
	s := newTestStore(t)
	const n = maxVerCachePer + 200 // exceed one stripe is unlikely, but exceed the global cap surely
	for i := 0; i < n; i++ {
		lkPut(t, s, "default", fmt.Sprintf("key-%06d", i), "v")
	}
	wantLive(t, s, int64(n))
	// Delete half; count must track exactly even after any cache resets.
	for i := 0; i < n; i += 2 {
		lkDel(t, s, "default", fmt.Sprintf("key-%06d", i))
	}
	wantLive(t, s, int64(n-(n+1)/2))
}
