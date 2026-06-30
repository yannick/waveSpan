package recordstore

import (
	"encoding/binary"
	"log/slog"

	"github.com/yannick/wavespan/internal/storage"
	"github.com/yannick/wavespan/internal/version"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

// parseDataKey extracts (namespace, userKey) from a CFKVData key
// (lenPrefix(ns) || lenPrefix(userKey) || versionSuffix), ignoring the version suffix. It is the inverse
// of dataKey for the namespace + user-key components.
func parseDataKey(k []byte) (namespace string, userKey []byte, ok bool) {
	nsLen, n := binary.Uvarint(k)
	// Unsigned compare (no lossy int cast): a length in (2^63, 2^64) would slip past a signed guard.
	if n <= 0 || nsLen > uint64(len(k)-n) {
		return "", nil, false
	}
	ns := string(k[n : n+int(nsLen)])
	rest := k[n+int(nsLen):]
	ukLen, m := binary.Uvarint(rest)
	if m <= 0 || ukLen > uint64(len(rest)-m) {
		return "", nil, false
	}
	uk := append([]byte(nil), rest[m:m+int(ukLen)]...)
	return ns, uk, true
}

// RebuildMetaIfAbsent rebuilds CFKVMeta from CFKVData ONLY when CFKVMeta is empty. A full (non-cut) backup
// exports CFKVMeta verbatim — including SiblingVersions / conflict-tracking state the LWW rebuild cannot
// reconstruct — so when it is present it must be preserved as restored, and this is a no-op. A ≤T cut
// backup omits CFKVMeta (it would dangle), leaving it empty here, so it is rebuilt from the surviving ≤T
// CFKVData. Presence (not a cut flag) is the trigger so the restore path needs no out-of-band signal.
func RebuildMetaIfAbsent(store storage.LocalStore) error {
	present, err := metaPresent(store)
	if err != nil {
		return err
	}
	if present {
		return nil // full backup restored CFKVMeta verbatim — keep siblings/conflict state intact
	}
	return RebuildMeta(store)
}

// metaPresent reports whether CFKVMeta holds any entry (a single key is enough to decide).
func metaPresent(store storage.LocalStore) (bool, error) {
	it, err := store.Scan(storage.CFKVMeta, nil, nil, 1)
	if err != nil {
		return false, err
	}
	present := it.Valid()
	cerr := it.Err()
	_ = it.Close()
	return present, cerr
}

// RebuildMeta reconstructs the CFKVMeta index — the per-key latest pointer plus the TTL bucket entries —
// from the CFKVData versioned records. It is the restore-side counterpart to Apply: after a logical
// bootstrap-restore of a ≤T cut where CFKVMeta is omitted, the surviving versioned records (each already
// ≤ the backup frontier T) are the source of truth, so the latest-as-of-T LWW winner is recomputed per
// (ns,userKey). This is what keeps the read path correct with NO dangling latest pointers: copying CFKVMeta
// verbatim while CFKVData dropped its >T winner would point at an absent record (silent key loss).
//
// Limitation (cut-only): RebuildLatestPointer recovers only the LWW winner / tombstone / expiry. A key with
// concurrent siblings (keep-siblings policy) is collapsed to its winner — SiblingVersions and the
// SIBLINGS_PRESENT conflict flag are NOT reconstructed (the winner's value is correct; sibling VALUES
// survive as distinct CFKVData versions). Reconstructing siblings needs causality/conflict-policy state the
// LWW selector lacks; it is a tracked follow-up. This regression applies ONLY to a ≤T cut — full backups
// export CFKVMeta verbatim (see RebuildMetaIfAbsent / CFSpec.RebuildWhenCut), preserving siblings.
//
// A key whose every version was dropped (all > T) has no surviving record → no pointer is written (the key
// is correctly absent). The winner's expiry is carried into the engine's native TTL + the TTL bucket index
// (mirroring Apply) so restored TTL'd keys still expire.
func RebuildMeta(store storage.LocalStore) error {
	it, err := store.Scan(storage.CFKVData, nil, nil, 0)
	if err != nil {
		return err
	}
	type group struct {
		ns      string
		userKey []byte
		recs    []*wavespanv1.StoredRecord
	}
	byKey := map[string]*group{}
	var order []*group
	for it.Valid() {
		if ns, uk, ok := parseDataKey(it.Key()); ok {
			rec, derr := storage.DecodeStoredRecord(it.Value())
			if derr != nil {
				// Skip + log rather than aborting the whole restore on one bad record — consistent with
				// kvVersionOf, which keeps an undecodable record (does not drop it on the cut). The record
				// itself is preserved in CFKVData; it simply cannot contribute a latest pointer here.
				slog.Warn("recordstore: skipping undecodable CFKVData record during CFKVMeta rebuild",
					"namespace", ns, "err", derr)
				it.Next()
				continue
			}
			gk := ns + "\x00" + string(uk)
			g := byKey[gk]
			if g == nil {
				g = &group{ns: ns, userKey: uk}
				byKey[gk] = g
				order = append(order, g)
			}
			g.recs = append(g.recs, rec)
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
	for _, g := range order {
		lp := storage.RebuildLatestPointer(g.recs)
		if lp == nil {
			continue
		}
		lpBytes, err := storage.EncodeLatestPointer(lp)
		if err != nil {
			return err
		}
		var lpExpiry int64
		if lp.ExpiresAtUnixMs != nil && !lp.GetTombstone() {
			lpExpiry = lp.GetExpiresAtUnixMs()
		}
		ops = append(ops, storage.StoreOp{CF: storage.CFKVMeta, Key: latestKey(g.ns, g.userKey), Value: lpBytes, ExpiresAtUnixMs: lpExpiry})
		if lp.ExpiresAtUnixMs != nil && !lp.GetTombstone() {
			win := version.FromProto(lp.GetWinner())
			ops = append(ops, storage.StoreOp{CF: storage.CFKVMeta, Key: ttlKey(lp.GetExpiresAtUnixMs(), g.ns, g.userKey), Value: encodeVersion(win)})
		}
		if len(ops) >= batch {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	return flush()
}
