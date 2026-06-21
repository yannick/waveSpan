package local

import (
	"bytes"
	"testing"

	"github.com/cwire/wavespan/internal/recordstore"
	"github.com/cwire/wavespan/internal/storage"
	"github.com/cwire/wavespan/internal/version"
	wavespanv1 "github.com/cwire/wavespan/proto/wavespan/v1"
)

func newStore(t *testing.T, member string) *recordstore.Store {
	t.Helper()
	mem := storage.NewMemStore()
	t.Cleanup(func() { _ = mem.Close() })
	return recordstore.NewStore(mem, "dev", member, version.NewClock(nil, 500), version.NewSequencer(0))
}

func TestReceiverStoresDurableAndReturnsVersion(t *testing.T) {
	st := newStore(t, "node2")
	recv := NewReceiver(st, "node2", NewIdempotency(0))

	v := version.Version{HLCPhysicalMs: 100, WriterClusterID: "dev", WriterMemberID: "node1", WriterSequence: 1}
	rec := &wavespanv1.StoredRecord{
		LogicalKey: []byte("k"), Namespace: "default", Version: v.ToProto(),
		Value: &wavespanv1.ValueBody{Body: &wavespanv1.ValueBody_Inline{Inline: []byte("val")}},
	}
	req := BuildRequest("default", []byte("k"), rec, "node1")

	resp, err := recv.Apply(req)
	if err != nil {
		t.Fatal(err)
	}
	if !resp.GetDurable() || resp.GetMemberId() != "node2" {
		t.Fatalf("unexpected response: %+v", resp)
	}
	if version.FromProto(resp.GetAppliedVersion()).Compare(v) != 0 {
		t.Fatalf("applied version mismatch")
	}
	// durably stored
	got, _ := st.Get("default", []byte("k"))
	if !got.Found || !bytes.Equal(got.Value, []byte("val")) {
		t.Fatalf("replica not durable: %+v", got)
	}
}

func TestReceiverIdempotentOnDuplicate(t *testing.T) {
	st := newStore(t, "node2")
	recv := NewReceiver(st, "node2", NewIdempotency(0))
	v := version.Version{HLCPhysicalMs: 100, WriterClusterID: "dev", WriterMemberID: "node1", WriterSequence: 1}
	rec := &wavespanv1.StoredRecord{
		LogicalKey: []byte("k"), Namespace: "default", Version: v.ToProto(),
		Value: &wavespanv1.ValueBody{Body: &wavespanv1.ValueBody_Inline{Inline: []byte("val")}},
	}
	req := BuildRequest("default", []byte("k"), rec, "node1")

	if _, err := recv.Apply(req); err != nil {
		t.Fatal(err)
	}
	// duplicate apply must be a no-op acknowledged durable (one logical mutation, property 5)
	resp, err := recv.Apply(req)
	if err != nil || !resp.GetDurable() {
		t.Fatalf("duplicate apply should ack durable: %+v %v", resp, err)
	}
}

func TestIdempotencyEvictsOldest(t *testing.T) {
	i := NewIdempotency(2)
	v := version.Version{WriterSequence: 1}
	i.Record("a", v)
	i.Record("b", v)
	i.Record("c", v) // evicts "a"
	if _, ok := i.Check("a"); ok {
		t.Fatal("oldest entry should have been evicted")
	}
	if _, ok := i.Check("c"); !ok {
		t.Fatal("newest entry should be present")
	}
}
