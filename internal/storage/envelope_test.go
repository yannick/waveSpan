package storage

import (
	"math/rand"
	"testing"

	wavespanv1 "github.com/cwire/wavespan/proto/wavespan/v1"
	"google.golang.org/protobuf/proto"
)

func ver(phys uint64, logical uint32, cluster, member string, seq uint64) *wavespanv1.Version {
	return &wavespanv1.Version{
		HlcPhysicalMs:   phys,
		HlcLogical:      logical,
		WriterClusterId: cluster,
		WriterMemberId:  member,
		WriterSequence:  seq,
	}
}

func rec(v *wavespanv1.Version, value string, tombstone bool) *wavespanv1.StoredRecord {
	r := &wavespanv1.StoredRecord{
		LogicalKey: []byte("k"),
		Version:    v,
		Tombstone:  tombstone,
		Namespace:  "default",
		Kind:       wavespanv1.RecordKind_RECORD_KIND_KV,
	}
	if !tombstone {
		r.Value = &wavespanv1.ValueBody{Body: &wavespanv1.ValueBody_Inline{Inline: []byte(value)}}
	}
	return r
}

func TestStoredRecordRoundTrip(t *testing.T) {
	r := rec(ver(100, 1, "c", "m", 5), "hello", false)
	r.ExpiresAtUnixMs = proto.Int64(123456)
	b, err := EncodeStoredRecord(r)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeStoredRecord(b)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(r, got) {
		t.Fatalf("round trip mismatch:\n %v\n %v", r, got)
	}
}

func TestMutationEnvelopeRoundTrip(t *testing.T) {
	m := &wavespanv1.MutationEnvelope{
		MutationId: "c\x1fm\x1f5",
		Kind:       wavespanv1.MutationKind_MUTATION_KIND_PUT,
		LogicalKey: []byte("k"),
		Version:    ver(100, 1, "c", "m", 5),
		Namespace:  "default",
	}
	b, err := EncodeMutationEnvelope(m)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeMutationEnvelope(b)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(m, got) {
		t.Fatalf("round trip mismatch")
	}
}

func TestRebuildLatestPointerPicksLWWWinner(t *testing.T) {
	records := []*wavespanv1.StoredRecord{
		rec(ver(100, 0, "a", "m1", 1), "old", false),
		rec(ver(200, 0, "a", "m1", 2), "newest", false),
		rec(ver(150, 0, "a", "m1", 3), "middle", false),
	}
	// shuffle to prove order-independence
	rng := rand.New(rand.NewSource(7))
	for iter := 0; iter < 50; iter++ {
		shuf := append([]*wavespanv1.StoredRecord(nil), records...)
		rng.Shuffle(len(shuf), func(i, j int) { shuf[i], shuf[j] = shuf[j], shuf[i] })
		lp := RebuildLatestPointer(shuf)
		if lp.GetWinner().GetHlcPhysicalMs() != 200 {
			t.Fatalf("winner not the highest HLC: %+v", lp.GetWinner())
		}
		if lp.GetTombstone() {
			t.Fatalf("winner should not be a tombstone")
		}
	}
}

func TestRebuildLatestPointerWinningTombstoneHidesValue(t *testing.T) {
	// A tombstone whose version wins must become the winner (delete wins under LWW).
	records := []*wavespanv1.StoredRecord{
		rec(ver(100, 0, "a", "m1", 1), "value", false),
		rec(ver(300, 0, "a", "m1", 4), "", true), // tombstone, higher HLC
	}
	lp := RebuildLatestPointer(records)
	if !lp.GetTombstone() {
		t.Fatalf("winning tombstone did not hide the older value: %+v", lp)
	}
	if lp.GetWinner().GetHlcPhysicalMs() != 300 {
		t.Fatalf("tombstone version should win: %+v", lp.GetWinner())
	}

	// Conversely, an older tombstone must not win over a newer value.
	records2 := []*wavespanv1.StoredRecord{
		rec(ver(100, 0, "a", "m1", 1), "", true),
		rec(ver(300, 0, "a", "m1", 4), "live", false),
	}
	lp2 := RebuildLatestPointer(records2)
	if lp2.GetTombstone() {
		t.Fatalf("older tombstone should not win over newer value: %+v", lp2)
	}
}

func TestKeyBuildersAreInjective(t *testing.T) {
	// length-prefixing must keep composite keys unambiguous
	a := KVDataKey("ns", []byte("ab"), ver(1, 0, "c", "m", 1))
	b := KVDataKey("ns", []byte("a"), ver(1, 0, "c", "m", 1))
	if string(a) == string(b) {
		t.Fatal("KVDataKey collided across different user keys")
	}
	l1 := KVLatestKey("nsa", []byte("b"))
	l2 := KVLatestKey("ns", []byte("ab"))
	if string(l1) == string(l2) {
		t.Fatal("KVLatestKey collided across namespace/key boundary")
	}
}
