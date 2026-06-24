package collections

import (
	"context"
	"sync/atomic"
	"testing"

	sm "github.com/lni/dragonboat/v4/statemachine"

	"github.com/yannick/wavespan/internal/storage"
)

// TestUpdateSkipsCorruptCommittedEntry is THE crash-fix regression: a corrupt/short/garbage committed
// entry must NOT make Update return an error or panic (dragonboat treats either as fatal → node death →
// crash-loop on replay). The poison entry is skipped deterministically (applied nothing), and a valid
// entry in the same batch still applies. This is the exact incident shape: a corrupt entry followed by
// good traffic.
func TestUpdateSkipsCorruptCommittedEntry(t *testing.T) {
	mem := storage.NewMemStore()
	t.Cleanup(func() { _ = mem.Close() })
	s := newShardSM(mem, 1)
	if _, err := s.Open(nil); err != nil {
		t.Fatalf("Open: %v", err)
	}

	ns, coll := []byte("app"), []byte("set")
	good := encodeCommand(command{Op: opSAdd, NS: ns, Coll: coll, Items: itemsFromKeys([][]byte{[]byte("ok")})})

	before := CorruptEntriesSkipped()

	// A batch of poison entries followed by one valid entry. Each poison must be skipped, not crash.
	poison := [][]byte{
		{},                              // empty command
		{byte(opSAdd)},                  // op byte only — truncated, no chunks
		{0xff, 0xff, 0xff, 0xff},        // unknown op + garbage
		append([]byte{byte(opSAdd)}, []byte{0x00, 0x00, 0x00, 0x10, 0x01}...), // claims a 16-byte NS chunk but is short
		encodeBatchTruncated(t),         // a valid opBatch header claiming a sub-command it doesn't carry
		encodeHIncrNoItems(t, ns, coll), // HINCRBY with zero items (would panic on Items[0])
	}
	entries := make([]sm.Entry, 0, len(poison)+1)
	var idx uint64
	for _, p := range poison {
		idx++
		entries = append(entries, sm.Entry{Index: idx, Cmd: p})
	}
	idx++
	entries = append(entries, sm.Entry{Index: idx, Cmd: good})

	out, err := func() (res []sm.Entry, err error) {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("Update panicked on corrupt entry: %v", r)
			}
		}()
		return s.Update(entries)
	}()
	if err != nil {
		t.Fatalf("Update returned error on corrupt entries (would crash the node): %v", err)
	}
	if len(out) != len(entries) {
		t.Fatalf("Update returned %d entries, want %d", len(out), len(entries))
	}
	// Every poison entry got a benign empty result; the good entry got Value 1 (one member added).
	for i := 0; i < len(poison); i++ {
		if out[i].Result.Value != 0 || len(out[i].Result.Data) != 0 {
			t.Fatalf("poison entry %d got non-benign result %+v", i, out[i].Result)
		}
	}
	if got := out[len(out)-1].Result.Value; got != 1 {
		t.Fatalf("trailing valid SADD result = %d, want 1 (corrupt entries broke apply)", got)
	}
	if skipped := CorruptEntriesSkipped() - before; skipped != uint64(len(poison)) {
		t.Fatalf("skipped %d corrupt entries, want %d", skipped, len(poison))
	}

	// The SM stays consistent: only the good member exists, cardinality is exact, no partial state leaked
	// from the skipped entries.
	res, err := s.Lookup(cardCheckQuery{NS: ns, Coll: coll})
	if err != nil {
		t.Fatalf("CardCheck lookup: %v", err)
	}
	cc := res.(CardCheck)
	if cc.Stored != 1 || cc.Counted != 1 {
		t.Fatalf("CardCheck = %+v, want stored==counted==1 (skipped entries leaked state)", cc)
	}
	got, err := s.Lookup(isMemberQuery{NS: ns, Coll: coll, Member: []byte("ok")})
	if err != nil || got.(bool) != true {
		t.Fatalf("SIsMember(ok) = %v,%v want true (valid entry did not apply)", got, err)
	}
}

// TestUpdateSkipsCorruptSubCommandInBatch checks that a poisoned coalesced opBatch (one corrupt
// sub-command) is skipped whole, deterministically, without crashing — and a following good entry still
// applies.
func TestUpdateSkipsCorruptSubCommandInBatch(t *testing.T) {
	mem := storage.NewMemStore()
	t.Cleanup(func() { _ = mem.Close() })
	s := newShardSM(mem, 1)
	if _, err := s.Open(nil); err != nil {
		t.Fatalf("Open: %v", err)
	}
	ns, coll := []byte("app"), []byte("set")

	goodSub := encodeCommand(command{Op: opSAdd, NS: ns, Coll: coll, Items: itemsFromKeys([][]byte{[]byte("a")})})
	badSub := []byte{byte(opSAdd), 0x00, 0x00, 0x00, 0x10} // claims a 16-byte NS chunk, carries none
	batch := encodeBatchInto(nil, [][]byte{goodSub, badSub})

	good := encodeCommand(command{Op: opSAdd, NS: ns, Coll: coll, Items: itemsFromKeys([][]byte{[]byte("b")})})

	out, err := s.Update([]sm.Entry{{Index: 1, Cmd: batch}, {Index: 2, Cmd: good}})
	if err != nil {
		t.Fatalf("Update error on poisoned batch: %v", err)
	}
	if out[0].Result.Value != 0 || len(out[0].Result.Data) != 0 {
		t.Fatalf("poisoned batch entry got non-benign result %+v", out[0].Result)
	}
	// The whole poisoned batch was skipped, so "a" must NOT exist; only the trailing good "b" applies.
	if got, _ := s.Lookup(isMemberQuery{NS: ns, Coll: coll, Member: []byte("a")}); got.(bool) {
		t.Fatalf("member 'a' from a skipped poisoned batch leaked into state")
	}
	if got, _ := s.Lookup(isMemberQuery{NS: ns, Coll: coll, Member: []byte("b")}); !got.(bool) {
		t.Fatalf("trailing good entry did not apply after a poisoned batch")
	}
	cc := mustCardCheck(t, s, ns, coll)
	if cc.Stored != 1 || cc.Counted != 1 {
		t.Fatalf("CardCheck = %+v, want stored==counted==1", cc)
	}
}

// TestUpdateStorageFaultStaysFatal confirms a GENUINE storage fault still propagates as a fatal Update
// error (it is not a corrupt-entry skip): a storage fault is real, local, and non-deterministic, so the
// replica must stop rather than silently diverge.
func TestUpdateStorageFaultStaysFatal(t *testing.T) {
	mem := storage.NewMemStore()
	fs := &faultyStore{LocalStore: mem}
	s := newShardSM(fs, 1)
	if _, err := s.Open(nil); err != nil {
		t.Fatalf("Open: %v", err)
	}
	ns, coll := []byte("app"), []byte("set")
	good := encodeCommand(command{Op: opSAdd, NS: ns, Coll: coll, Items: itemsFromKeys([][]byte{[]byte("x")})})
	fs.failGets.Store(true) // every store Get now faults — a genuine storage fault during apply
	_, err := s.Update([]sm.Entry{{Index: 1, Cmd: good}})
	if err == nil {
		t.Fatalf("Update with a storage fault returned nil; a real storage fault must stay fatal")
	}
	if !isFatal(err) {
		t.Fatalf("storage fault was reclassified as non-fatal: %v", err)
	}
}

// --- helpers ---

func encodeBatchTruncated(t *testing.T) []byte {
	t.Helper()
	// opBatch || count=1 || (no chunk follows) — decodeBatch must fail on the missing sub-command chunk.
	return []byte{byte(opBatch), 0x00, 0x00, 0x00, 0x01}
}

func encodeHIncrNoItems(t *testing.T, ns, coll []byte) []byte {
	t.Helper()
	return encodeCommand(command{Op: opHIncrBy, NS: ns, Coll: coll, Items: nil})
}

func mustCardCheck(t *testing.T, s *shardSM, ns, coll []byte) CardCheck {
	t.Helper()
	res, err := s.Lookup(cardCheckQuery{NS: ns, Coll: coll})
	if err != nil {
		t.Fatalf("CardCheck: %v", err)
	}
	return res.(CardCheck)
}

// faultyStore wraps a LocalStore and can be told to fault every Get, to exercise the fatal path.
type faultyStore struct {
	storage.LocalStore
	failGets atomic.Bool
}

func (f *faultyStore) Get(cf storage.ColumnFamily, key []byte) ([]byte, bool, error) {
	if f.failGets.Load() {
		return nil, false, context.DeadlineExceeded
	}
	return f.LocalStore.Get(cf, key)
}
