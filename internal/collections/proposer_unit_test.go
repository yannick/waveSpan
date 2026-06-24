package collections

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestEncodeCommandPooledRoundTrip checks the pooled encoder (A1) produces the same wire bytes as the
// allocating one and survives a release/reuse cycle.
func TestEncodeCommandPooledRoundTrip(t *testing.T) {
	c := command{Op: opSAdd, NS: []byte("ns"), Coll: []byte("coll"), Idem: []byte("idem"),
		Items: []item{{Key: []byte("a"), ExpiryMs: 7}, {Key: []byte("b"), Score: 1.5}}}
	want := encodeCommand(c)
	eb := encodeCommandPooled(c)
	if !bytes.Equal(eb.bytes(), want) {
		t.Fatalf("pooled encode = %x want %x", eb.bytes(), want)
	}
	// Decode must round-trip.
	got, err := decodeCommand(eb.bytes())
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Op != c.Op || !bytes.Equal(got.NS, c.NS) || !bytes.Equal(got.Coll, c.Coll) || len(got.Items) != 2 {
		t.Fatalf("decoded = %+v", got)
	}
	eb.release()
	// Reuse: a different command must encode correctly off the recycled buffer.
	c2 := command{Op: opHSet, NS: []byte("x"), Coll: []byte("y"), Items: []item{{Key: []byte("f"), Val: []byte("v")}}}
	eb2 := encodeCommandPooled(c2)
	if !bytes.Equal(eb2.bytes(), encodeCommand(c2)) {
		t.Fatalf("reused buffer encoded wrong")
	}
	eb2.release()
}

// TestDecodeCommandIntoScratchReuse checks decodeCommandInto reuses the scratch Items slice (A1) and
// still decodes correctly across calls.
func TestDecodeCommandIntoScratchReuse(t *testing.T) {
	a := encodeCommand(command{Op: opSAdd, NS: []byte("n"), Coll: []byte("c"), Items: []item{{Key: []byte("m1")}, {Key: []byte("m2")}}})
	b := encodeCommand(command{Op: opSAdd, NS: []byte("n"), Coll: []byte("c"), Items: []item{{Key: []byte("z")}}})
	scratch := make([]item, 0, 8)
	c1, err := decodeCommandInto(a, scratch)
	if err != nil || len(c1.Items) != 2 || !bytes.Equal(c1.Items[0].Key, []byte("m1")) {
		t.Fatalf("decode a = %+v err=%v", c1, err)
	}
	c2, err := decodeCommandInto(b, scratch)
	if err != nil || len(c2.Items) != 1 || !bytes.Equal(c2.Items[0].Key, []byte("z")) {
		t.Fatalf("decode b = %+v err=%v", c2, err)
	}
}

// TestBatchEncodeDecodeRoundTrip checks the opBatch wrapper (QW2) splits back into its sub-commands.
func TestBatchEncodeDecodeRoundTrip(t *testing.T) {
	subs := [][]byte{
		encodeCommand(command{Op: opSAdd, NS: []byte("n"), Coll: []byte("c"), Items: []item{{Key: []byte("a")}}}),
		encodeCommand(command{Op: opHSet, NS: []byte("n"), Coll: []byte("h"), Items: []item{{Key: []byte("f"), Val: []byte("v")}}}),
	}
	enc := encodeBatchInto(nil, subs)
	if opKind(enc[0]) != opBatch {
		t.Fatalf("first byte = %d want opBatch", enc[0])
	}
	out, err := decodeBatch(enc)
	if err != nil {
		t.Fatalf("decodeBatch: %v", err)
	}
	if len(out) != 2 || !bytes.Equal(out[0], subs[0]) || !bytes.Equal(out[1], subs[1]) {
		t.Fatalf("decodeBatch mismatch")
	}
}

// TestBatchResultCodec checks the packed per-op result encoding round-trips (QW2 result routing).
func TestBatchResultCodec(t *testing.T) {
	results := []ProposeResult{
		{Value: 1, Data: nil},
		{Value: 0, Data: wrongType},
		{Value: 42, Data: []byte{1, 2, 3, 4, 5, 6, 7, 8}},
	}
	enc := encodeBatchResult(nil, results)
	got, err := decodeBatchResult(enc, len(results))
	if err != nil {
		t.Fatalf("decodeBatchResult: %v", err)
	}
	for i := range results {
		if got[i].Value != results[i].Value || !bytes.Equal(got[i].Data, results[i].Data) {
			t.Fatalf("result[%d] = %+v want %+v", i, got[i], results[i])
		}
	}
	if _, err := decodeBatchResult(enc, len(results)+1); err == nil {
		t.Fatal("expected count mismatch error")
	}
}

// fakeShard records every cmd it sees, so a proposer test can assert coalescing happened (one opBatch
// entry carrying many ops, instead of many single entries). It echoes a per-op count result.
type fakeShard struct {
	mu       sync.Mutex
	proposed [][]byte
}

func (f *fakeShard) Propose(_ context.Context, _ uint64, cmd []byte) (ProposeResult, error) {
	f.mu.Lock()
	f.proposed = append(f.proposed, append([]byte(nil), cmd...))
	f.mu.Unlock()
	if opKind(cmd[0]) == opBatch {
		subs, err := decodeBatch(cmd)
		if err != nil {
			return ProposeResult{}, err
		}
		rs := make([]ProposeResult, len(subs))
		for i := range subs {
			rs[i] = ProposeResult{Value: 1} // pretend each op added one element
		}
		return ProposeResult{Data: encodeBatchResult(nil, rs)}, nil
	}
	return ProposeResult{Value: 1}, nil
}

func (f *fakeShard) entries() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.proposed)
}

func (f *fakeShard) batchedOps() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, c := range f.proposed {
		if opKind(c[0]) == opBatch {
			subs, _ := decodeBatch(c)
			n += len(subs)
		}
	}
	return n
}

// TestProposerCoalesces fires many concurrent single-op proposals at one shard and asserts the proposer
// collapsed them into far fewer Raft entries (coalescing) while every caller got its correct result.
func TestProposerCoalesces(t *testing.T) {
	fs := &fakeShard{}
	p := newProposer(fs, 2*time.Millisecond, 256)
	const n = 500
	var wg sync.WaitGroup
	errs := make([]error, n)
	vals := make([]uint64, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			cmd := encodeCommand(command{Op: opSAdd, NS: []byte("n"), Coll: []byte("c"),
				Items: []item{{Key: []byte(fmt.Sprintf("m%d", i))}}})
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			res, err := p.Propose(ctx, firstDataShard, cmd)
			errs[i] = err
			vals[i] = res.Value
		}(i)
	}
	wg.Wait()
	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Fatalf("op %d: %v", i, errs[i])
		}
		if vals[i] != 1 {
			t.Fatalf("op %d value = %d want 1", i, vals[i])
		}
	}
	// Every op must have been delivered exactly once across entries.
	if got := fs.batchedOps() + (fs.entries() - countBatched(fs)); got < n {
		t.Fatalf("delivered ops = %d want >= %d", got, n)
	}
	// Coalescing must have happened: with 500 concurrent ops we expect well under 500 entries.
	if fs.entries() >= n {
		t.Fatalf("no coalescing: %d entries for %d ops", fs.entries(), n)
	}
	t.Logf("coalesced %d ops into %d Raft entries", n, fs.entries())
}

func countBatched(f *fakeShard) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, c := range f.proposed {
		if opKind(c[0]) == opBatch {
			n++
		}
	}
	return n
}

// TestHashDirectoryDeterministic checks routing is stable, in-range, and identical across instances
// (every node must agree, D1).
func TestHashDirectoryDeterministic(t *testing.T) {
	const n = 8
	d1 := NewHashDirectory(n)
	d2 := NewHashDirectory(n)
	seen := map[uint64]int{}
	for i := 0; i < 2000; i++ {
		ns := []byte(fmt.Sprintf("ns%d", i%17))
		coll := []byte(fmt.Sprintf("coll%d", i))
		s1 := d1.ShardFor(ns, coll)
		s2 := d2.ShardFor(ns, coll)
		if s1 != s2 {
			t.Fatalf("non-deterministic routing for %s/%s: %d vs %d", ns, coll, s1, s2)
		}
		if s1 < firstDataShard || s1 >= firstDataShard+n {
			t.Fatalf("shard %d out of range [%d,%d)", s1, firstDataShard, firstDataShard+n)
		}
		seen[s1]++
	}
	if len(seen) != n {
		t.Fatalf("only %d of %d shards used; routing not spread", len(seen), n)
	}
	if shards := d1.Shards(); len(shards) != n {
		t.Fatalf("Shards() = %d want %d", len(shards), n)
	}
}
