package recordstore

import (
	"bytes"
	"testing"

	"github.com/yannick/wavespan/internal/storage"
	"github.com/yannick/wavespan/internal/version"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	mem := storage.NewMemStore()
	t.Cleanup(func() { _ = mem.Close() })
	clock := version.NewClock(nil, 500)
	return NewStore(mem, "dev", "node1", clock, version.NewSequencer(0))
}

func putLocal(t *testing.T, s *Store, ns string, key, val []byte) version.Version {
	t.Helper()
	v := s.NextVersion()
	rec := s.BuildRecord(ns, key, val, v, false, nil)
	if _, err := s.Apply(rec, wavespanv1.MutationKind_MUTATION_KIND_PUT); err != nil {
		t.Fatal(err)
	}
	return v
}

func TestLocalPutGet(t *testing.T) {
	s := newTestStore(t)
	v := putLocal(t, s, "default", []byte("foo"), []byte("bar"))

	out, err := s.Get("default", []byte("foo"))
	if err != nil {
		t.Fatal(err)
	}
	if !out.Found || !bytes.Equal(out.Value, []byte("bar")) {
		t.Fatalf("get foo = %+v, want found bar", out)
	}
	if !out.Version.Equal(v) {
		t.Fatalf("get version %+v != put version %+v", out.Version, v)
	}
	// absent
	if out, _ := s.Get("default", []byte("missing")); out.Found {
		t.Fatal("missing key reported found")
	}
}

func TestLocalDeleteTombstone(t *testing.T) {
	s := newTestStore(t)
	putLocal(t, s, "default", []byte("k"), []byte("v"))

	// delete = tombstone put with a newer version
	v := s.NextVersion()
	rec := s.BuildRecord("default", []byte("k"), nil, v, true, nil)
	if _, err := s.Apply(rec, wavespanv1.MutationKind_MUTATION_KIND_DELETE); err != nil {
		t.Fatal(err)
	}
	out, err := s.Get("default", []byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	if out.Found {
		t.Fatal("deleted key should report not found")
	}
	if !out.Tombstone {
		t.Fatal("latest pointer should be a tombstone")
	}
}

func TestLWWOlderVersionDoesNotOverwrite(t *testing.T) {
	s := newTestStore(t)

	// apply a high-version write, then a lower-version write for the same key
	high := version.Version{HLCPhysicalMs: 1000, WriterClusterID: "dev", WriterMemberID: "x", WriterSequence: 1}
	low := version.Version{HLCPhysicalMs: 100, WriterClusterID: "dev", WriterMemberID: "x", WriterSequence: 2}

	recHigh := s.BuildRecord("default", []byte("k"), []byte("new"), high, false, nil)
	if _, err := s.Apply(recHigh, wavespanv1.MutationKind_MUTATION_KIND_PUT); err != nil {
		t.Fatal(err)
	}
	recLow := s.BuildRecord("default", []byte("k"), []byte("old"), low, false, nil)
	if _, err := s.Apply(recLow, wavespanv1.MutationKind_MUTATION_KIND_PUT); err != nil {
		t.Fatal(err)
	}

	out, _ := s.Get("default", []byte("k"))
	if !bytes.Equal(out.Value, []byte("new")) {
		t.Fatalf("LWW winner should be the higher version: got %q", out.Value)
	}
	if !out.Version.Equal(high) {
		t.Fatalf("winner version = %+v, want high", out.Version)
	}
}
