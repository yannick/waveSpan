package collections

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/yannick/wavespan/internal/storage"
)

// TestHIncr covers atomic integer/float hash counters: basic accumulation, the value HGet sees, type
// and non-number errors, and — the point of doing it in one Raft entry — exactness under concurrency.
func TestHIncr(t *testing.T) {
	mem := storage.NewMemStore()
	t.Cleanup(func() { _ = mem.Close() })
	addr := freeAddr(t)
	m := newMgr(t, t.TempDir(), addr, mem)
	if err := m.StartShard(1, 1, map[uint64]string{1: addr}, false); err != nil {
		t.Fatalf("StartShard: %v", err)
	}
	defer m.Stop()
	c := New(m, SingleShardDirectory(1))
	waitReady(t, c)
	ns := []byte("app")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// Integer counter: new field starts from 0; increments accumulate; HGet sees the decimal string.
	coll := []byte("counts")
	if v, err := c.HIncrBy(ctx, ns, coll, []byte("hits"), 5); err != nil || v != 5 {
		t.Fatalf("HIncrBy +5 = %d,%v want 5", v, err)
	}
	if v, err := c.HIncrBy(ctx, ns, coll, []byte("hits"), -2); err != nil || v != 3 {
		t.Fatalf("HIncrBy -2 = %d,%v want 3", v, err)
	}
	if got, found, err := c.HGet(ctx, ns, coll, []byte("hits"), true); err != nil || !found || string(got) != "3" {
		t.Fatalf("HGet hits = %q,%v,%v want \"3\"", got, found, err)
	}

	// Float counter.
	if v, err := c.HIncrByFloat(ctx, ns, coll, []byte("ratio"), 1.5); err != nil || v != 1.5 {
		t.Fatalf("HIncrByFloat +1.5 = %v,%v want 1.5", v, err)
	}
	if v, err := c.HIncrByFloat(ctx, ns, coll, []byte("ratio"), 0.25); err != nil || v != 1.75 {
		t.Fatalf("HIncrByFloat +0.25 = %v,%v want 1.75", v, err)
	}

	// Non-number field → ErrNotNumber (and the value is untouched).
	if _, err := c.HSet(ctx, ns, coll, FieldValue{Field: []byte("name"), Value: []byte("alice")}); err != nil {
		t.Fatalf("HSet: %v", err)
	}
	if _, err := c.HIncrBy(ctx, ns, coll, []byte("name"), 1); !errors.Is(err, ErrNotNumber) {
		t.Fatalf("HIncrBy on non-number = %v want ErrNotNumber", err)
	}
	if got, _, _ := c.HGet(ctx, ns, coll, []byte("name"), true); string(got) != "alice" {
		t.Fatalf("non-number field mutated to %q", got)
	}

	// Wrong type: HIncrBy on a set → ErrWrongType.
	set := []byte("aset")
	if _, err := c.SAdd(ctx, ns, set, []byte("x")); err != nil {
		t.Fatalf("SAdd: %v", err)
	}
	if _, err := c.HIncrBy(ctx, ns, set, []byte("x"), 1); !errors.Is(err, ErrWrongType) {
		t.Fatalf("HIncrBy on a set = %v want ErrWrongType", err)
	}

	// Concurrency: 8 writers × 50 increments of the same field must total exactly 400 — no lost updates,
	// because each read-add-write is one Raft entry applied in order.
	const writers, per = 8, 50
	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < per; i++ {
				if _, err := c.HIncrBy(ctx, ns, coll, []byte("concurrent"), 1); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent HIncrBy: %v", err)
	}
	if got, _, err := c.HGet(ctx, ns, coll, []byte("concurrent"), true); err != nil || string(got) != "400" {
		t.Fatalf("concurrent counter = %q,%v want \"400\" (lost updates)", got, err)
	}
}
