//go:build chaos

// Split-under-load: range split must not lose acknowledged writes. The migrate-on-split sequence
// (scan subrange -> ingest -> cut directory over -> purge) has a window between the scan and the
// cutover where a write committed to the OLD shard is neither migrated nor routed to afterward — it is
// silently lost on purge. A correct CP system must not drop an acked write, so the splitting subrange
// must be frozen for the migration window (design/30 §6.1). This test drives concurrent writes through
// a split and asserts every acked write survives.
//
// Run:  go test -tags chaos -run TestSplitUnderLoad ./tests/chaos -v
package chaos

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/yannick/wavespan/internal/collections"
	"github.com/yannick/wavespan/internal/storage"
)

func TestSplitUnderLoadNoLostWrites(t *testing.T) {
	addr := freeAddr(t)
	store := storage.NewMemStore()
	t.Cleanup(func() { _ = store.Close() })
	mgr, err := collections.NewManager(t.TempDir(), addr, store)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer mgr.Stop()

	bctx, bcancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer bcancel()
	ctrl, err := collections.Bootstrap(bctx, mgr, 1, map[uint64]string{1: addr}, map[uint64]string{1: addr})
	if err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
	c := ctrl.Collections()

	// "zzz" has a high route key, so a split at "m" migrates its shard to a new one.
	ns, coll := []byte("app"), []byte("zzz")
	// Warm up until the data shard accepts writes.
	for {
		wctx, wcancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, e := c.SAdd(wctx, ns, coll, []byte("warmup"))
		wcancel()
		if e == nil {
			break
		}
		time.Sleep(150 * time.Millisecond)
	}

	var ackedMu sync.Mutex
	acked := map[string]bool{"warmup": true} // the warmup write is also an acked write
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for seq := 0; ; seq++ {
				select {
				case <-stop:
					return
				default:
				}
				m := []byte(fmt.Sprintf("w%d-%d", w, seq))
				// Retry briefly so a frozen subrange (once implemented) is awaited, not dropped.
				deadline := time.Now().Add(6 * time.Second)
				for time.Now().Before(deadline) {
					ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
					_, e := c.SAdd(ctx, ns, coll, m)
					cancel()
					if e == nil {
						ackedMu.Lock()
						acked[string(m)] = true
						ackedMu.Unlock()
						break
					}
					time.Sleep(40 * time.Millisecond)
				}
			}
		}(w)
	}

	time.Sleep(400 * time.Millisecond) // let writes flow
	// Split while writes are in flight: "zzz" migrates to a new shard on the same node.
	sctx, scancel := context.WithTimeout(context.Background(), 30*time.Second)
	_, serr := ctrl.Split(sctx, collections.RouteKey(ns, []byte("m")), 1, map[uint64]string{1: addr})
	scancel()
	if serr != nil {
		t.Fatalf("Split: %v", serr)
	}
	time.Sleep(400 * time.Millisecond) // more writes after the cutover
	close(stop)
	wg.Wait()
	time.Sleep(500 * time.Millisecond) // quiesce

	ackedMu.Lock()
	want := len(acked)
	missing := []string{}
	got := readMembers(t, c, ns, coll, true) // linearizable, routed to the (now new) shard
	set := map[string]bool{}
	for _, m := range got {
		set[m] = true
	}
	for m := range acked {
		if !set[m] {
			missing = append(missing, m)
		}
	}
	ackedMu.Unlock()

	t.Logf("acked=%d present=%d", want, len(got))
	if len(missing) > 0 {
		t.Fatalf("split LOST %d acknowledged write(s) (e.g. %q); acked=%d present=%d", len(missing), missing[0], want, len(got))
	}
	if n := card(t, c, ns, coll, true); n != uint64(want) {
		t.Fatalf("SCard=%d but acked=%d after split", n, want)
	}
}
