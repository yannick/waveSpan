package collections

import (
	"context"
	"encoding/binary"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/yannick/wavespan/internal/storage"
)

// TestConcurrentSAddCoalescedCorrect drives many concurrent SAdds through the real Manager (so the
// batching proposer coalesces them into opBatch entries the SM expands) and asserts exact set semantics:
// distinct members all land, duplicates dedup to one, and the cardinality is exact. This is the
// concurrency-correctness check the design calls for — the single-writer bench cannot show batching.
func TestConcurrentSAddCoalescedCorrect(t *testing.T) {
	c := concurrentShard(t)
	ns, coll := []byte("bench"), []byte("set")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const writers, perWriter = 32, 50 // 1600 ops; many in flight at once => coalescing
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				member := []byte(fmt.Sprintf("m-%d-%d", w, i))
				if _, err := c.SAdd(ctx, ns, coll, member); err != nil {
					t.Errorf("SAdd: %v", err)
					return
				}
			}
		}(w)
	}
	wg.Wait()

	if n, err := c.SCard(ctx, ns, coll, true); err != nil || n != writers*perWriter {
		t.Fatalf("SCard = %d,%v want %d", n, err, writers*perWriter)
	}

	// Re-add a subset concurrently; all are duplicates, so cardinality must not change (dedup correctness
	// under coalescing).
	wg = sync.WaitGroup{}
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			added, err := c.SAdd(ctx, ns, coll, []byte(fmt.Sprintf("m-%d-0", w)))
			if err != nil {
				t.Errorf("SAdd dup: %v", err)
			}
			if added != 0 {
				t.Errorf("re-add reported %d new, want 0", added)
			}
		}(w)
	}
	wg.Wait()
	if n, err := c.SCard(ctx, ns, coll, true); err != nil || n != writers*perWriter {
		t.Fatalf("SCard after dups = %d,%v want %d", n, err, writers*perWriter)
	}

	// CardCheck: stored counter must equal counted elements (internal invariant) after the coalesced load.
	if cc, err := c.CardCheck(ctx, ns, coll, true); err != nil || cc.Stored != cc.Counted || cc.Stored != writers*perWriter {
		t.Fatalf("CardCheck = %+v,%v want stored==counted==%d", cc, err, writers*perWriter)
	}
}

// TestConcurrentIdempotentWritesCoalesced checks that idempotency keys still dedup when proposals
// coalesce into one entry: firing the SAME keyed write N times concurrently must apply once.
func TestConcurrentIdempotentWritesCoalesced(t *testing.T) {
	c := concurrentShard(t)
	ns, coll := []byte("idem"), []byte("set")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 64 concurrent copies of the same idempotent SAdd of one member.
	const copies = 64
	var wg sync.WaitGroup
	results := make([]uint64, copies)
	for i := 0; i < copies; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			n, err := c.proposeCount(ctx, command{Op: opSAdd, NS: ns, Coll: coll, Idem: []byte("k1"), Items: []item{{Key: []byte("only")}}})
			if err != nil {
				t.Errorf("idem SAdd: %v", err)
			}
			results[i] = n
		}(i)
	}
	wg.Wait()

	// The member exists exactly once.
	if n, err := c.SCard(ctx, ns, coll, true); err != nil || n != 1 {
		t.Fatalf("SCard = %d,%v want 1 (idempotent)", n, err)
	}
	// Exactly one copy should report "1 added"; the rest return the cached result (also 1) — but the set
	// must be size 1, which is the real invariant. Confirm all results are the cached "added" value 1.
	for i, r := range results {
		if r != 1 {
			t.Fatalf("idem result[%d] = %d want 1 (cached)", i, r)
		}
	}
}

// TestConcurrentIdempotentHIncrByCoalesced is the strict idempotency check: HIncrBy is NOT idempotent,
// so if coalescing applied a keyed increment more than once the counter would overshoot. Firing the
// same keyed +5 from many goroutines (which coalesce into shared entries) must leave the counter at
// exactly 5.
func TestConcurrentIdempotentHIncrByCoalesced(t *testing.T) {
	c := concurrentShard(t)
	ns, coll, field := []byte("idem"), []byte("ctr"), []byte("n")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const copies = 64
	var wg sync.WaitGroup
	for i := 0; i < copies; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, data, err := c.proposeCmd(ctx, command{Op: opHIncrBy, NS: ns, Coll: coll, Idem: []byte("inc1"),
				Items: []item{{Key: field, Val: int64Bytes(5)}}})
			if err != nil {
				t.Errorf("HIncrBy: %v", err)
			}
			if len(data) != 8 {
				t.Errorf("HIncrBy data len = %d want 8", len(data))
			}
		}()
	}
	wg.Wait()

	v, err := c.HIncrBy(ctx, ns, coll, field, 0) // read current value via a +0 with no idem key
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if v != 5 {
		t.Fatalf("counter = %d want 5 (idempotent increment applied exactly once)", v)
	}
}

func int64Bytes(n int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(n))
	return b
}

// concurrentShard stands up a single-node data shard with a short coalescing window so concurrent
// writers actually coalesce in tests.
func concurrentShard(t *testing.T) *Collections {
	t.Helper()
	mem := storage.NewMemStore()
	t.Cleanup(func() { _ = mem.Close() })
	addr := freeAddr(t)
	m := newMgr(t, t.TempDir(), addr, mem)
	if err := m.StartShard(firstDataShard, 1, map[uint64]string{1: addr}, false); err != nil {
		t.Fatalf("StartShard: %v", err)
	}
	t.Cleanup(m.Stop)
	c := New(m, SingleShardDirectory(firstDataShard))
	waitReadyAt(t, c)
	return c
}

func waitReadyAt(t *testing.T, c *Collections) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, err := c.SAdd(ctx, []byte("__probe__"), []byte("__probe__"+strconv.Itoa(int(time.Now().UnixNano()))))
		cancel()
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("shard never became ready: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
