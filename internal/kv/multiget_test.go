package kv

import (
	"context"
	"fmt"
	"testing"

	"github.com/yannick/wavespan/internal/membership"
	"github.com/yannick/wavespan/internal/recordstore"
	"github.com/yannick/wavespan/internal/storage"
	"github.com/yannick/wavespan/internal/version"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

func TestMultiGetReturnsPerKeyInOrder(t *testing.T) {
	mem := storage.NewMemStore()
	t.Cleanup(func() { _ = mem.Close() })
	rstore := recordstore.NewStore(mem, "dev", "node1", version.NewClock(nil, 500), version.NewSequencer(0))
	reader := NewReader(rstore, membership.Member{ClusterID: "dev", MemberID: "node1"})

	// write the even-indexed keys; odd keys are absent
	for i := 0; i < 10; i += 2 {
		v := rstore.NextVersion()
		rec := rstore.BuildRecord("default", []byte(fmt.Sprintf("k%d", i)), []byte(fmt.Sprintf("v%d", i)), v, false, nil)
		if _, err := rstore.Apply(rec, wavespanv1.MutationKind_MUTATION_KIND_PUT); err != nil {
			t.Fatal(err)
		}
	}

	keys := make([][]byte, 10)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("k%d", i))
	}
	results, err := reader.MultiGet(context.Background(), "default", keys, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 10 {
		t.Fatalf("expected 10 results, got %d", len(results))
	}
	for i, r := range results {
		if i%2 == 0 {
			if !r.GetFound() || string(r.GetValue()) != fmt.Sprintf("v%d", i) {
				t.Fatalf("key k%d: found=%v value=%q (want v%d)", i, r.GetFound(), r.GetValue(), i)
			}
		} else if r.GetFound() {
			t.Fatalf("key k%d should be absent, got value %q", i, r.GetValue())
		}
	}
}

func TestMultiGetEmpty(t *testing.T) {
	mem := storage.NewMemStore()
	t.Cleanup(func() { _ = mem.Close() })
	rstore := recordstore.NewStore(mem, "dev", "node1", version.NewClock(nil, 500), version.NewSequencer(0))
	reader := NewReader(rstore, membership.Member{ClusterID: "dev", MemberID: "node1"})
	res, err := reader.MultiGet(context.Background(), "default", nil, false)
	if err != nil || len(res) != 0 {
		t.Fatalf("empty MultiGet = (%v, len %d)", err, len(res))
	}
}
