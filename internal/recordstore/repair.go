package recordstore

import (
	"encoding/binary"
	"log/slog"

	"github.com/yannick/wavespan/internal/storage"
	"github.com/yannick/wavespan/internal/version"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
	"google.golang.org/protobuf/proto"
)

// RepairCutMeta fixes the CFKVMeta latest-pointer index after a logical bootstrap-restore of an HLC ≤T
// cut. The cut filters CFKVData to records with Version ≤ T but exports CFKVMeta VERBATIM, so the
// restored latest pointers (and their SiblingVersions / conflict state) are preserved exactly — EXCEPT
// for the rare key whose winning (or sibling) version was written after T and was therefore dropped from
// CFKVData. Such a pointer would dangle (point at an absent record → silent key loss), so this pass
// repairs ONLY those:
//
//   - Winner present in CFKVData → the pointer stands verbatim; only dangling sibling refs (a >T-cut
//     sibling) are filtered out. Winner ≤T present and no sibling cut → the pointer is left untouched
//     (siblings/conflict state fully preserved — the common case, and ALL keys when the cut excluded
//     nothing, which is the norm since T = now + lease is ~5s in the future).
//   - Winner absent (it was >T, cut) → repoint to the max surviving ≤T version of the key via
//     RebuildLatestPointer (carrying its tombstone/expiry, rebuilding its TTL bucket entry, and dropping
//     the stale one). Concurrent SiblingVersions are dropped for this repointed key — the LWW selector
//     cannot reconstruct conflict state once the winner is gone (documented cut-only limitation,
//     design/backup §5.2). If NO ≤T version survives, the key did not exist at/below T → drop the pointer.
//
// This bounds sibling/conflict loss to keys whose latest write was genuinely after the cut, instead of
// collapsing every conflicted key on every restore.
func RepairCutMeta(store storage.LocalStore) error {
	// Read pass: collect every latest pointer. The scan ends (exclusive) at the 0xff TTL sentinel, so the
	// TTL bucket entries are excluded — latest-pointer keys begin with a uvarint namespace length < 0x80
	// (encode.go invariant), so this bound is exact.
	type lpEntry struct {
		ns string
		uk []byte
		lp *wavespanv1.LatestPointer
	}
	it, err := store.Scan(storage.CFKVMeta, nil, ttlLowBound(), 0)
	if err != nil {
		return err
	}
	var entries []lpEntry
	for it.Valid() {
		ns, uk, ok := parseLatestKey(it.Key())
		if ok {
			lp, derr := storage.DecodeLatestPointer(it.Value())
			if derr != nil {
				// Skip + log rather than aborting the whole restore on one bad pointer (consistent with the
				// CFKVData decode policy below). The pointer is left as-is; the data is preserved.
				slog.Warn("recordstore: skipping undecodable CFKVMeta latest pointer during cut repair",
					"namespace", ns, "err", derr)
			} else {
				entries = append(entries, lpEntry{ns: ns, uk: uk, lp: lp})
			}
		}
		it.Next()
	}
	serr := it.Err()
	_ = it.Close()
	if serr != nil {
		return serr
	}

	const batch = 1000
	ops := make([]storage.StoreOp, 0, batch)
	flush := func() error {
		if len(ops) == 0 {
			return nil
		}
		e := store.BatchRC(ops)
		ops = ops[:0]
		return e
	}

	for _, e := range entries {
		ns, uk, lp := e.ns, e.uk, e.lp
		winnerPresent, err := dataPresent(store, ns, uk, version.FromProto(lp.GetWinner()))
		if err != nil {
			return err
		}

		if winnerPresent {
			// Winner survived the cut: keep the pointer; filter only sibling refs whose record was cut.
			kept, changed, err := presentSiblings(store, ns, uk, lp.GetSiblingVersions())
			if err != nil {
				return err
			}
			if !changed {
				continue // fully verbatim — winner + all siblings present, conflict state intact
			}
			nlp := &wavespanv1.LatestPointer{Winner: lp.GetWinner(), Tombstone: lp.GetTombstone(), SiblingVersions: kept}
			if lp.ExpiresAtUnixMs != nil {
				nlp.ExpiresAtUnixMs = proto.Int64(lp.GetExpiresAtUnixMs())
			}
			b, err := storage.EncodeLatestPointer(nlp)
			if err != nil {
				return err
			}
			ops = append(ops, storage.StoreOp{CF: storage.CFKVMeta, Key: latestKey(ns, uk), Value: b, ExpiresAtUnixMs: lpNativeTTL(nlp)})
		} else {
			// Winner was after T (cut): repoint to the surviving ≤T records (or drop the key).
			survivors, err := scanKeyRecords(store, ns, uk)
			if err != nil {
				return err
			}
			// Drop the stale TTL bucket entry indexed under the old (cut) winner's expiry, if any.
			if lp.ExpiresAtUnixMs != nil {
				ops = append(ops, storage.StoreOp{CF: storage.CFKVMeta, Key: ttlKey(lp.GetExpiresAtUnixMs(), ns, uk), Delete: true})
			}
			nlp := storage.RebuildLatestPointer(survivors)
			if nlp == nil {
				// No ≤T version survived → the key did not exist at/below T. Drop the dangling pointer.
				ops = append(ops, storage.StoreOp{CF: storage.CFKVMeta, Key: latestKey(ns, uk), Delete: true})
			} else {
				b, err := storage.EncodeLatestPointer(nlp)
				if err != nil {
					return err
				}
				ops = append(ops, storage.StoreOp{CF: storage.CFKVMeta, Key: latestKey(ns, uk), Value: b, ExpiresAtUnixMs: lpNativeTTL(nlp)})
				if nlp.ExpiresAtUnixMs != nil && !nlp.GetTombstone() {
					win := version.FromProto(nlp.GetWinner())
					ops = append(ops, storage.StoreOp{CF: storage.CFKVMeta, Key: ttlKey(nlp.GetExpiresAtUnixMs(), ns, uk), Value: encodeVersion(win)})
				}
			}
		}
		if len(ops) >= batch {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	return flush()
}

// lpNativeTTL is the engine-native per-row TTL for a latest pointer (mirrors Apply): the winner's expiry
// unless it is a tombstone, else 0 (no native TTL).
func lpNativeTTL(lp *wavespanv1.LatestPointer) int64 {
	if lp.ExpiresAtUnixMs != nil && !lp.GetTombstone() {
		return lp.GetExpiresAtUnixMs()
	}
	return 0
}

// dataPresent reports whether the versioned CFKVData record for (ns,userKey,v) survives in store.
func dataPresent(store storage.LocalStore, ns string, userKey []byte, v version.Version) (bool, error) {
	_, found, err := store.Get(storage.CFKVData, dataKey(ns, userKey, v))
	return found, err
}

// presentSiblings filters sibling versions to those still present in CFKVData, reporting whether any were
// dropped (a >T-cut sibling). kept preserves input order.
func presentSiblings(store storage.LocalStore, ns string, userKey []byte, sibs []*wavespanv1.Version) (kept []*wavespanv1.Version, changed bool, err error) {
	for _, s := range sibs {
		present, perr := dataPresent(store, ns, userKey, version.FromProto(s))
		if perr != nil {
			return nil, false, perr
		}
		if present {
			kept = append(kept, s)
		} else {
			changed = true
		}
	}
	return kept, changed, nil
}

// scanKeyRecords returns all surviving CFKVData records for one user key (all already ≤T). An undecodable
// record is skipped + logged rather than aborting the restore (consistent with the latest-pointer policy).
func scanKeyRecords(store storage.LocalStore, ns string, userKey []byte) ([]*wavespanv1.StoredRecord, error) {
	pfx := dataKeyPrefix(ns, userKey)
	it, err := store.Scan(storage.CFKVData, pfx, prefixEnd(pfx), 0)
	if err != nil {
		return nil, err
	}
	var recs []*wavespanv1.StoredRecord
	for it.Valid() {
		rec, derr := storage.DecodeStoredRecord(it.Value())
		if derr != nil {
			slog.Warn("recordstore: skipping undecodable CFKVData record during cut repair", "namespace", ns, "err", derr)
		} else {
			recs = append(recs, rec)
		}
		it.Next()
	}
	cerr := it.Err()
	_ = it.Close()
	if cerr != nil {
		return nil, cerr
	}
	return recs, nil
}

// parseLatestKey extracts (namespace, userKey) from a CFKVMeta latest-pointer key
// (lenPrefix(ns) || userKey). It is the inverse of latestKey.
func parseLatestKey(k []byte) (namespace string, userKey []byte, ok bool) {
	nsLen, n := binary.Uvarint(k)
	// Unsigned compare (no lossy int cast): a length in (2^63, 2^64) would slip past a signed guard.
	if n <= 0 || nsLen > uint64(len(k)-n) {
		return "", nil, false
	}
	ns := string(k[n : n+int(nsLen)])
	uk := append([]byte(nil), k[n+int(nsLen):]...)
	return ns, uk, true
}
