package ttl

import (
	"context"
	"testing"

	"github.com/cwire/wavespan/internal/recordstore"
	"github.com/cwire/wavespan/internal/storage"
	"github.com/cwire/wavespan/internal/version"
	wavespanv1 "github.com/cwire/wavespan/proto/wavespan/v1"
)

func newStore(t *testing.T, wallMs uint64) *recordstore.Store {
	t.Helper()
	mem := storage.NewMemStore()
	t.Cleanup(func() { _ = mem.Close() })
	return recordstore.NewStore(mem, "dev", "node1", version.NewClock(func() uint64 { return wallMs }, 500), version.NewSequencer(0))
}

func putTTL(t *testing.T, s *recordstore.Store, key string, ttlMs int64) {
	t.Helper()
	v := s.NextVersion()
	if _, err := s.Apply(s.BuildRecord("default", []byte(key), []byte("v"), v, false, &ttlMs), wavespanv1.MutationKind_MUTATION_KIND_PUT); err != nil {
		t.Fatal(err)
	}
}

// tombstoneCounter returns a TombstoneFunc that applies a real tombstone (so reads converge) and
// counts invocations.
func tombstoneCounter(s *recordstore.Store, calls *int) TombstoneFunc {
	return func(_ context.Context, ns string, key []byte) error {
		*calls++
		v := s.NextVersion()
		_, err := s.Apply(s.BuildRecord(ns, key, nil, v, true, nil), wavespanv1.MutationKind_MUTATION_KIND_DELETE)
		return err
	}
}

func TestExpiredFilter(t *testing.T) {
	exp := int64(100)
	if Expired(nil, 200) {
		t.Fatal("nil expiry is never expired")
	}
	if Expired(&exp, 50) {
		t.Fatal("not expired before deadline")
	}
	if !Expired(&exp, 100) {
		t.Fatal("expired at deadline")
	}
}

func TestSweeperEmitsTombstoneForExpiredOnly(t *testing.T) {
	s := newStore(t, 1_000_000)
	putTTL(t, s, "soon", 1000)        // expires 1_001_000
	putTTL(t, s, "later", 10_000_000) // expires far in the future
	calls := 0
	sw := NewSweeper(s, tombstoneCounter(s, &calls), func() int64 { return 1_002_000 }) // past 'soon', before 'later'

	emitted := sw.SweepOnce(context.Background())
	if emitted != 1 || calls != 1 {
		t.Fatalf("sweeper should tombstone exactly the expired key, emitted=%d calls=%d", emitted, calls)
	}
	// 'soon' is now a tombstone (hidden); 'later' is untouched
	if out, _ := s.Get("default", []byte("soon")); out.Found {
		t.Fatal("expired key should be tombstoned/hidden after sweep")
	}
	if out, _ := s.Get("default", []byte("later")); !out.Found {
		t.Fatal("non-expired key must not be swept")
	}
	// the due ttl index entry was cleared, so a second sweep does nothing
	if again := sw.SweepOnce(context.Background()); again != 0 {
		t.Fatalf("second sweep should be a no-op, emitted=%d", again)
	}
}

func TestExpiryDoesNotBreakConvergence(t *testing.T) {
	// a concurrent write with a higher version must still win over an expired record's tombstone
	s := newStore(t, 1_000_000)
	putTTL(t, s, "k", 1000) // low version, expires soon
	calls := 0
	NewSweeper(s, tombstoneCounter(s, &calls), func() int64 { return 1_002_000 }).SweepOnce(context.Background())

	// a brand-new write (higher HLC) lands after the tombstone
	hi := version.Version{HLCPhysicalMs: 9_000_000, WriterClusterID: "dev", WriterMemberID: "node1", WriterSequence: 99}
	if _, err := s.Apply(s.BuildRecord("default", []byte("k"), []byte("fresh"), hi, false, nil), wavespanv1.MutationKind_MUTATION_KIND_PUT); err != nil {
		t.Fatal(err)
	}
	out, _ := s.Get("default", []byte("k"))
	if !out.Found || string(out.Value) != "fresh" {
		t.Fatalf("a newer write must win over an expired-key tombstone: %+v", out)
	}
}
