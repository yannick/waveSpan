package collections

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestConcurrentWritersNoBufferCorruption reproduces the incident scenario: a high-concurrency flood of
// writes (each encoded into a POOLED buffer that proposeCmd releases as soon as Propose returns) driven
// through the coalescing proposer. Before the fix, a pooled buffer could be reused before dragonboat had
// durably copied the proposed bytes, corrupting a committed entry (the observed "collections: short
// command" that crash-looped the voters). With the proposer copying off the pooled buffer at enqueue,
// every committed entry is well-formed: cardinality is EXACT and there are zero decode failures.
//
// Run under -race to catch the pooled-buffer reuse race directly.
func TestConcurrentWritersNoBufferCorruption(t *testing.T) {
	c := concurrentShard(t)
	ns, coll := []byte("incident"), []byte("set")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const writers, perWriter = 100, 30 // 3000 distinct members, ~incident-scale concurrency
	skippedBefore := CorruptEntriesSkipped()

	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				member := []byte(fmt.Sprintf("m-%d-%d", w, i))
				if _, err := c.SAdd(ctx, ns, coll, member); err != nil {
					t.Errorf("SAdd(%s): %v", member, err)
					return
				}
			}
		}(w)
	}
	wg.Wait()

	// Exact cardinality: every distinct member landed, none corrupted into a different/short command.
	if n, err := c.SCard(ctx, ns, coll, true); err != nil || n != writers*perWriter {
		t.Fatalf("SCard = %d,%v want %d (a corrupted entry would lose or mangle a member)", n, err, writers*perWriter)
	}
	cc, err := c.CardCheck(ctx, ns, coll, true)
	if err != nil || cc.Stored != cc.Counted || cc.Stored != writers*perWriter {
		t.Fatalf("CardCheck = %+v,%v want stored==counted==%d", cc, err, writers*perWriter)
	}
	// No committed entry was ever skipped as corrupt — the source-of-corruption fix means the SM never
	// even has to fall back to its skip path under a clean concurrent flood.
	if skipped := CorruptEntriesSkipped() - skippedBefore; skipped != 0 {
		t.Fatalf("%d entries skipped as corrupt under a clean concurrent flood — buffer corruption regressed", skipped)
	}
}

// TestConcurrentWritersWithCancellation stresses the exact release-race window: writers fire with very
// short per-call deadlines so many proposeCmd calls return early (ctx cancellation) and release their
// pooled buffer while the flusher may still hold it. The node must never crash and committed members
// must never be corrupt; whatever DID commit must be a real member (no short-command poison entries).
func TestConcurrentWritersWithCancellation(t *testing.T) {
	c := concurrentShard(t)
	ns, coll := []byte("cancel"), []byte("set")
	skippedBefore := CorruptEntriesSkipped()

	const writers, perWriter = 64, 40
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				// A deliberately tight, jittered deadline so a good fraction of calls cancel mid-flight.
				d := time.Duration(50+(w+i)%200) * time.Microsecond
				cctx, ccancel := context.WithTimeout(context.Background(), d)
				_, _ = c.SAdd(cctx, ns, coll, []byte(fmt.Sprintf("m-%d-%d", w, i)))
				ccancel()
			}
		}(w)
	}
	wg.Wait()

	// The node stayed up; now a clean write must still succeed and the collection must be internally
	// consistent (stored counter == counted elements), proving no poison entry corrupted the state.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := c.SAdd(ctx, ns, coll, []byte("after-flood")); err != nil {
		t.Fatalf("SAdd after cancellation flood failed (node did not stay healthy): %v", err)
	}
	cc, err := c.CardCheck(ctx, ns, coll, true)
	if err != nil {
		t.Fatalf("CardCheck: %v", err)
	}
	if cc.Stored != cc.Counted {
		t.Fatalf("CardCheck = %+v: stored != counted — buffer corruption leaked inconsistent state", cc)
	}
	if skipped := CorruptEntriesSkipped() - skippedBefore; skipped != 0 {
		t.Fatalf("%d corrupt entries skipped during the cancellation flood — pooled buffer reuse corrupted committed entries", skipped)
	}
}
