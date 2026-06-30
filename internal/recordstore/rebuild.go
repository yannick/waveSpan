package recordstore

import (
	"encoding/binary"

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

// RebuildMeta reconstructs the CFKVMeta index — the per-key latest pointer plus the TTL bucket entries —
// from the CFKVData versioned records. It is the restore-side counterpart to Apply: after a logical
// bootstrap-restore where CFKVMeta is a DERIVED CF (not backed up), the surviving versioned records (each
// already ≤ the backup frontier T) are the source of truth, so the latest-as-of-T LWW winner is recomputed
// per (ns,userKey). This is what keeps the read path correct with NO dangling latest pointers: copying
// CFKVMeta verbatim while CFKVData dropped its >T winner would point at an absent record (silent key loss).
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
				_ = it.Close()
				return derr
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
