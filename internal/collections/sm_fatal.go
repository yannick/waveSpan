package collections

import (
	"errors"
	"fmt"
	"log"
	"sync/atomic"

	sm "github.com/lni/dragonboat/v4/statemachine"

	"github.com/yannick/wavespan/internal/storage"
)

// CRITICAL ROBUSTNESS (design/30 §5.3, incident: a corrupt committed entry crash-looped all voters).
//
// dragonboat treats ANY error returned from a state machine's Update as FATAL — it panics the node, and
// on recovery the poison entry replays, crash-looping the whole shard. The hard invariant is therefore:
// applying a COMMITTED entry must never return an error and never panic for any reason attributable to
// the entry's own bytes (malformed / truncated / corrupt). Such an entry is SKIPPED deterministically
// (every replica skips the same entry, so state stays consistent), leaving a benign result.
//
// Only a genuine local STORAGE fault (store.Get / store.Batch / store.Snapshot failing) is a real fatal
// condition: it is not caused by the entry and is not deterministic across replicas, so it must still
// propagate and stop this replica rather than silently diverge its state. To tell the two apart we wrap
// store-layer errors in fatalErr at their source; anything not wrapped is a "skip this entry" error.

// fatalErr marks an error that must remain fatal to the state machine (a genuine storage-layer fault),
// as opposed to a decode/corruption error on a committed entry (which is skipped, never fatal).
type fatalErr struct{ err error }

func (e fatalErr) Error() string { return e.err.Error() }
func (e fatalErr) Unwrap() error { return e.err }

// fatal wraps a storage-layer error so the Update loop keeps treating it as fatal. A nil error stays nil.
func fatal(err error) error {
	if err == nil {
		return nil
	}
	return fatalErr{err: err}
}

// isFatal reports whether err (or anything it wraps) is a genuine storage-layer fault that must stop the
// replica. Everything else is a per-entry decode/corruption error that is skipped deterministically.
func isFatal(err error) bool {
	var fe fatalErr
	return errors.As(err, &fe)
}

// getData reads a CFReplData key, marking any storage error as fatal so it is never mistaken for a
// per-entry decode/corruption error. All per-entry store reads go through this so the Update loop can
// cleanly classify fatal (storage) vs skippable (corrupt entry) failures.
func (b *baseSM) getData(key []byte) (val []byte, found bool, err error) {
	v, ok, gerr := b.store.Get(storage.CFReplData, key)
	if gerr != nil {
		return nil, false, fatal(gerr)
	}
	return v, ok, nil
}

// corruptEntry is the non-fatal error describing why a committed entry could not be applied; it is
// logged once and the entry is skipped. It wraps the underlying decode error for testing/observability.
type corruptEntry struct {
	index uint64
	err   error
}

func (e corruptEntry) Error() string {
	return fmt.Sprintf("collections: skipping corrupt committed entry index=%d: %v", e.index, e.err)
}
func (e corruptEntry) Unwrap() error { return e.err }

// corruptEntriesSkipped counts committed entries skipped as corrupt — exported for metrics/tests.
var corruptEntriesSkipped atomic.Uint64

// CorruptEntriesSkipped reports how many committed entries this process has skipped as corrupt. A
// non-zero value means a malformed/poison entry was deterministically dropped instead of crashing.
func CorruptEntriesSkipped() uint64 { return corruptEntriesSkipped.Load() }

// logCorruptEntry records a skipped corrupt entry: it bumps the counter and logs once per entry (these
// are rare and operationally important — they mean a poison entry was contained, not a crash-loop).
func logCorruptEntry(e corruptEntry) {
	corruptEntriesSkipped.Add(1)
	log.Printf("collections: WARNING %s — entry skipped (state kept consistent across replicas)", e.Error())
}

// updateSnapshot captures the accumulated effects of an Update so a single entry can be rolled back
// (skipped) without disturbing entries already applied in the same batch. ops is restored by length
// (append-only within an entry); the overlay maps are restored from shallow clones, and the dedup
// sequence is reset. Cloning the small per-batch overlay maps is cheap relative to never crash-looping.
type updateSnapshot struct {
	opsLen       int
	dedupSeq     uint64
	exists       map[string]bool
	zscore       map[string]*float64
	cardDelta    map[string]int64
	htype        map[string]collType
	vals         map[string][]byte
	inBatchDedup map[string]ProposeResult
}

func (u *updateCtx) snapshot() updateSnapshot {
	return updateSnapshot{
		opsLen:       len(u.ops),
		dedupSeq:     u.dedupSeq,
		exists:       cloneMap(u.exists),
		zscore:       cloneMap(u.zscore),
		cardDelta:    cloneMap(u.cardDelta),
		htype:        cloneMap(u.htype),
		vals:         cloneMap(u.vals),
		inBatchDedup: cloneMap(u.inBatchDedup),
	}
}

func (u *updateCtx) restore(s updateSnapshot) {
	if len(u.ops) > s.opsLen {
		u.ops = u.ops[:s.opsLen]
	}
	u.dedupSeq = s.dedupSeq
	u.exists = s.exists
	u.zscore = s.zscore
	u.cardDelta = s.cardDelta
	u.htype = s.htype
	u.vals = s.vals
	u.inBatchDedup = s.inBatchDedup
}

func cloneMap[K comparable, V any](m map[K]V) map[K]V {
	out := make(map[K]V, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// applyEntrySafe applies one committed entry under a recover() so a panic from corrupt bytes (nil deref,
// index-out-of-range) becomes a deterministic skip rather than a node crash. It returns the entry's
// result, or a non-fatal error (corrupt entry → skipped) / fatal error (storage fault → propagated). The
// caller has snapshotted u's state and rolls it back on any error.
func (s *shardSM) applyEntrySafe(u *updateCtx, e *sm.Entry, frozen []frozenRange, scratch []item, subResults *[]ProposeResult) (res sm.Result, err error) {
	defer func() {
		if r := recover(); r != nil {
			// A panic here is only reachable from corrupt entry bytes (every store read is bounds-checked
			// or fatal-wrapped); turn it into a non-fatal skip so the node never dies on poison input.
			err = corruptEntry{index: e.Index, err: fmt.Errorf("panic applying entry: %v", r)}
		}
	}()
	return s.applyEntry(u, e, frozen, scratch, subResults)
}

// applyEntry decodes and applies one committed entry (migrate/freeze/batch/single). Decode/corruption
// failures are returned as a non-fatal corruptEntry so the caller skips the entry; storage faults arrive
// already wrapped as fatalErr and propagate.
func (s *shardSM) applyEntry(u *updateCtx, e *sm.Entry, frozen []frozenRange, scratch []item, subResults *[]ProposeResult) (sm.Result, error) {
	cmd := e.Cmd
	wrap := func(err error) error { // tag a non-fatal (decode) error with the entry index for logging
		if err == nil || isFatal(err) {
			return err
		}
		return corruptEntry{index: e.Index, err: err}
	}
	if len(cmd) > 0 && (opKind(cmd[0]) == opIngest || opKind(cmd[0]) == opPurge) {
		changed, err := u.applyMigrate(cmd) // raw subrange copy/purge (design/30 §6)
		if err != nil {
			return sm.Result{}, wrap(err)
		}
		return sm.Result{Value: uint64(changed)}, nil
	}
	if len(cmd) > 0 && (opKind(cmd[0]) == opFreeze || opKind(cmd[0]) == opUnfreeze) {
		if err := u.applyFreeze(cmd); err != nil {
			return sm.Result{}, wrap(err)
		}
		return sm.Result{Value: 1}, nil
	}
	if len(cmd) > 0 && opKind(cmd[0]) == opBatch { // QW2: a coalesced batch of single commands
		subCmds, err := decodeBatch(cmd)
		if err != nil {
			return sm.Result{}, wrap(err)
		}
		rs := (*subResults)[:0]
		for _, sub := range subCmds {
			r, aerr := u.applyOne(sub, frozen, scratch)
			if aerr != nil {
				return sm.Result{}, wrap(aerr)
			}
			rs = append(rs, r)
		}
		*subResults = rs
		return sm.Result{Value: 0, Data: encodeBatchResult(nil, rs)}, nil
	}
	r, err := u.applyOne(cmd, frozen, scratch)
	if err != nil {
		return sm.Result{}, wrap(err)
	}
	return sm.Result{Value: r.Value, Data: r.Data}, nil
}
