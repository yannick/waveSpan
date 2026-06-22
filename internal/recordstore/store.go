package recordstore

import (
	"sync"
	"sync/atomic"

	"github.com/cwire/wavespan/internal/storage"
	"github.com/cwire/wavespan/internal/version"
	wavespanv1 "github.com/cwire/wavespan/proto/wavespan/v1"
	"google.golang.org/protobuf/proto"
)

// Store is the local KV engine: it assigns versions and applies mutations atomically across the
// versioned-record, latest-pointer, and mutation-log column families (design/05 write algorithm;
// design/02 "Required local invariants"). It is the primitive the coordinator (origin write) and
// the StoreReplica receiver both use.
type Store struct {
	local     storage.LocalStore
	clock     *version.Clock
	seq       *version.Sequencer
	clusterID string
	memberID  string
	logSeq    atomic.Uint64
	// stripes serialize the latest-pointer read-modify-write PER KEY so that, under the engine's
	// ReadCommitted commits (which run concurrently), two writes to the same key still produce a
	// correct LWW pointer while writes to different keys proceed in parallel.
	stripes [numStripes]sync.Mutex
	// latestVer caches the current winning version per key (guarded by the key's stripe). The common
	// monotonic write (incoming version > the cached winner) then skips the read-modify-write storage
	// Get + pointer decode entirely. Capped per stripe; on overflow a stripe's cache is cleared (a
	// miss simply falls back to the storage read, so correctness is unaffected).
	latestVer [numStripes]map[string]version.Version

	// applyObserver, if set, fires after every durable Apply (origin, replica, anti-entropy,
	// bootstrap, and cross-cluster all route through Apply). `won` reports whether the applied record
	// is the LWW winner for its key, so a derived index (e.g. the vector ANN) can mirror the winner
	// and ignore losing/older writes.
	applyObserver func(rec *wavespanv1.StoredRecord, won bool)
}

// SetApplyObserver installs a post-apply hook (nil clears it). It is the single integration point for
// derived state that must mirror every replicated write regardless of the path it arrived on.
func (s *Store) SetApplyObserver(fn func(rec *wavespanv1.StoredRecord, won bool)) {
	s.applyObserver = fn
}

const (
	numStripes     = 512
	maxVerCachePer = 8192 // per-stripe cap before the cache is reset (bounds memory)
)

// NewStore wires a local KV store.
func NewStore(local storage.LocalStore, clusterID, memberID string, clock *version.Clock, seq *version.Sequencer) *Store {
	s := &Store{local: local, clock: clock, seq: seq, clusterID: clusterID, memberID: memberID}
	for i := range s.latestVer {
		s.latestVer[i] = make(map[string]version.Version)
	}
	return s
}

// stripeIdx hashes a namespace/key to its stripe (FNV-1a).
func stripeIdx(ns string, key []byte) uint32 {
	h := uint32(2166136261)
	for i := 0; i < len(ns); i++ {
		h = (h ^ uint32(ns[i])) * 16777619
	}
	for _, b := range key {
		h = (h ^ uint32(b)) * 16777619
	}
	// the stripe only distributes locks, so a ns/key boundary collision just shares a lock (harmless);
	// the authoritative cache key (ckey) carries an explicit separator.
	return h % numStripes
}

// NextVersion stamps a new version for an originated mutation (coordinator path).
func (s *Store) NextVersion() version.Version {
	ts := s.clock.Now()
	return version.Version{
		HLCPhysicalMs:   ts.PhysicalMs,
		HLCLogical:      ts.Logical,
		WriterClusterID: s.clusterID,
		WriterMemberID:  s.memberID,
		WriterSequence:  s.seq.Next(),
	}
}

// BuildRecord constructs a StoredRecord for a put or tombstone.
func (s *Store) BuildRecord(namespace string, key, value []byte, v version.Version, tombstone bool, ttlMs *int64) *wavespanv1.StoredRecord {
	rec := &wavespanv1.StoredRecord{
		LogicalKey:      key,
		Version:         v.ToProto(),
		Tombstone:       tombstone,
		Kind:            wavespanv1.RecordKind_RECORD_KIND_KV,
		Namespace:       namespace,
		OriginClusterId: v.WriterClusterID,
		OriginMemberId:  v.WriterMemberID,
	}
	if !tombstone {
		rec.Value = &wavespanv1.ValueBody{Body: &wavespanv1.ValueBody_Inline{Inline: value}}
	}
	if ttlMs != nil {
		exp := s.clock.Now().PhysicalMs + uint64(*ttlMs)
		e := int64(exp)
		rec.ExpiresAtUnixMs = &e
	}
	return rec
}

// Apply writes a versioned record durably: it stores the record, advances the latest pointer
// under hlc-last-write-wins, and appends a mutation-log entry — all in one atomic batch. It is
// idempotent: re-applying the same version is a no-op for the winner. Returns the resulting
// winning version.
func (s *Store) Apply(rec *wavespanv1.StoredRecord, kind wavespanv1.MutationKind) (version.Version, error) {
	ns := rec.GetNamespace()
	key := rec.GetLogicalKey()
	recVer := version.FromProto(rec.GetVersion())

	// Serialize the read-modify-write of THIS key's latest pointer (different keys run in parallel).
	si := stripeIdx(ns, key)
	s.stripes[si].Lock()
	defer s.stripes[si].Unlock()
	ckey := ns + "\x00" + string(key)

	// current latest pointer
	winner := recVer
	winnerTombstone := rec.GetTombstone()
	var winnerExpiry *int64
	if rec.ExpiresAtUnixMs != nil {
		e := rec.GetExpiresAtUnixMs()
		winnerExpiry = &e
	}
	// Fast path: if the cached winner is older than the incoming version (the monotonic common case),
	// the incoming record wins outright — skip the storage read + pointer decode. Otherwise read the
	// authoritative pointer to pick the winner and carry its tombstone/expiry.
	if cached, ok := s.latestVer[si][ckey]; !ok || recVer.Compare(cached) <= 0 {
		if cur, found, err := s.local.Get(storage.CFKVMeta, latestKey(ns, key)); err != nil {
			return version.Version{}, err
		} else if found {
			lp, err := storage.DecodeLatestPointer(cur)
			if err != nil {
				return version.Version{}, err
			}
			curWin := version.FromProto(lp.GetWinner())
			if curWin.Compare(recVer) >= 0 {
				winner = curWin
				winnerTombstone = lp.GetTombstone()
				winnerExpiry = nil
				if lp.ExpiresAtUnixMs != nil {
					e := lp.GetExpiresAtUnixMs()
					winnerExpiry = &e
				}
			}
		}
	}

	lp := &wavespanv1.LatestPointer{Winner: winner.ToProto(), Tombstone: winnerTombstone}
	if winnerExpiry != nil {
		lp.ExpiresAtUnixMs = proto.Int64(*winnerExpiry)
	}
	env := &wavespanv1.MutationEnvelope{
		MutationId: recVer.MutationID(), Kind: kind, LogicalKey: key, Value: rec.GetValue(),
		Version: rec.GetVersion(), Tombstone: rec.GetTombstone(), Namespace: ns,
		OriginClusterId: rec.GetOriginClusterId(), OriginMemberId: rec.GetOriginMemberId(),
		OriginSequence: recVer.WriterSequence,
	}
	if rec.ExpiresAtUnixMs != nil {
		env.ExpiresAtUnixMs = proto.Int64(rec.GetExpiresAtUnixMs())
	}

	// Marshal the three records into ONE pooled buffer (MarshalAppend), then slice out each section.
	// The storage layer copies each value on commit, so the buffer is transient and recycled. Offsets
	// are captured first, then sliced after all appends, since appending may reallocate the buffer.
	bp := encBufPool.Get().(*[]byte)
	buf := (*bp)[:0]
	var off1, off2, off3 int
	var err error
	if buf, err = storage.AppendStoredRecord(buf, rec); err != nil {
		encBufPool.Put(bp)
		return version.Version{}, err
	}
	off1 = len(buf)
	if buf, err = storage.AppendLatestPointer(buf, lp); err != nil {
		encBufPool.Put(bp)
		return version.Version{}, err
	}
	off2 = len(buf)
	if buf, err = storage.AppendMutationEnvelope(buf, env); err != nil {
		encBufPool.Put(bp)
		return version.Version{}, err
	}
	off3 = len(buf)
	recBytes, lpBytes, envBytes := buf[:off1], buf[off1:off2], buf[off2:off3]

	// Carry the record's expiry into the engine's native per-key TTL so wavesdb physically reclaims
	// the versioned record and its latest pointer on compaction once expired (design/02). The lazy
	// sweeper below (cross-replica tombstones) and the read-path expiry check remain the logical
	// authority; this just stops expired bytes from lingering on disk indefinitely.
	var recExpiry int64
	if rec.ExpiresAtUnixMs != nil && !rec.GetTombstone() {
		recExpiry = rec.GetExpiresAtUnixMs()
	}
	var lpExpiry int64
	if winnerExpiry != nil && !winnerTombstone {
		lpExpiry = *winnerExpiry
	}
	ops := []storage.StoreOp{
		{CF: storage.CFKVData, Key: dataKey(ns, key, recVer), Value: recBytes, ExpiresAtUnixMs: recExpiry},
		{CF: storage.CFKVMeta, Key: latestKey(ns, key), Value: lpBytes, ExpiresAtUnixMs: lpExpiry},
		{CF: storage.CFReplLog, Key: storage.ReplLogKey(ns, s.logSeq.Add(1)), Value: envBytes},
	}
	// index into the lazy-TTL bucket so the sweeper can find it (design/03 "TTL storage").
	if rec.ExpiresAtUnixMs != nil && !rec.GetTombstone() {
		ops = append(ops, storage.StoreOp{
			CF: storage.CFKVMeta, Key: ttlKey(rec.GetExpiresAtUnixMs(), ns, key), Value: encodeVersion(recVer),
		})
	}
	err = s.local.BatchRC(ops)
	*bp = buf
	encBufPool.Put(bp) // safe: Batch has copied every value
	if err != nil {
		return version.Version{}, err
	}
	// Record the new winner so subsequent monotonic writes to this key skip the storage read.
	if m := s.latestVer[si]; len(m) < maxVerCachePer {
		m[ckey] = winner
	} else {
		s.latestVer[si] = map[string]version.Version{ckey: winner} // bounded reset
	}
	if s.applyObserver != nil {
		s.applyObserver(rec, winner.Compare(recVer) == 0)
	}
	return winner, nil
}

// encBufPool recycles the per-write proto-encode buffer (StoredRecord + LatestPointer +
// MutationEnvelope marshaled together), keeping the hot write path allocation-light.
var encBufPool = sync.Pool{New: func() any { b := make([]byte, 0, 1024); return &b }}

// ScanRow is one row returned by a local range scan.
type ScanRow struct {
	Key         []byte
	Value       []byte
	Version     version.Version
	ExpiresAtMs *int64
}

// ScanRange iterates the namespace's latest pointers in user-key order over [start, end), reading
// each winner's value. It skips tombstones and (best-effort) records detected as expired at nowMs.
// limit 0 means unbounded.
func (s *Store) ScanRange(namespace string, start, end []byte, limit int, nowMs int64) ([]ScanRow, error) {
	lo := latestKey(namespace, start)
	if start == nil {
		lo = namespacePrefix(namespace)
	}
	var hi []byte
	if end == nil {
		hi = prefixEnd(namespacePrefix(namespace))
	} else {
		hi = latestKey(namespace, end)
	}
	it, err := s.local.Scan(storage.CFKVMeta, lo, hi, 0)
	if err != nil {
		return nil, err
	}
	defer func() { _ = it.Close() }()

	nsPrefixLen := len(namespacePrefix(namespace))
	var rows []ScanRow
	for it.Valid() {
		k := it.Key()
		lp, derr := storage.DecodeLatestPointer(it.Value())
		if derr != nil {
			it.Next()
			continue
		}
		userKey := append([]byte(nil), k[nsPrefixLen:]...)
		win := version.FromProto(lp.GetWinner())
		hidden := lp.GetTombstone()
		if lp.ExpiresAtUnixMs != nil && lp.GetExpiresAtUnixMs() <= nowMs {
			hidden = true // best-effort hide-expired on read (design/03 "TTL semantics")
		}
		if !hidden {
			recBytes, found, gerr := s.local.Get(storage.CFKVData, dataKey(namespace, userKey, win))
			if gerr == nil && found {
				if rec, rerr := storage.DecodeStoredRecord(recBytes); rerr == nil {
					row := ScanRow{Key: userKey, Value: rec.GetValue().GetInline(), Version: win}
					if lp.ExpiresAtUnixMs != nil {
						e := lp.GetExpiresAtUnixMs()
						row.ExpiresAtMs = &e
					}
					rows = append(rows, row)
					if limit > 0 && len(rows) >= limit {
						return rows, nil
					}
				}
			}
		}
		it.Next()
	}
	return rows, it.Err()
}

// ScanRecords returns the full winning StoredRecords in [start, end) for a namespace, including
// tombstones (used by anti-entropy to compare and ship records). Expired records are not hidden —
// anti-entropy compares authoritative state.
func (s *Store) ScanRecords(namespace string, start, end []byte) ([]*wavespanv1.StoredRecord, error) {
	lo := latestKey(namespace, start)
	if start == nil {
		lo = namespacePrefix(namespace)
	}
	var hi []byte
	if end == nil {
		hi = prefixEnd(namespacePrefix(namespace))
	} else {
		hi = latestKey(namespace, end)
	}
	it, err := s.local.Scan(storage.CFKVMeta, lo, hi, 0)
	if err != nil {
		return nil, err
	}
	defer func() { _ = it.Close() }()
	nsPrefixLen := len(namespacePrefix(namespace))
	var out []*wavespanv1.StoredRecord
	for it.Valid() {
		k := it.Key()
		lp, derr := storage.DecodeLatestPointer(it.Value())
		if derr == nil {
			userKey := append([]byte(nil), k[nsPrefixLen:]...)
			win := version.FromProto(lp.GetWinner())
			if rb, found, gerr := s.local.Get(storage.CFKVData, dataKey(namespace, userKey, win)); gerr == nil && found {
				if rec, rerr := storage.DecodeStoredRecord(rb); rerr == nil {
					out = append(out, rec)
				}
			}
		}
		it.Next()
	}
	return out, it.Err()
}

// ScanRecordsFrom scans up to limit winning records whose user key is >= start, returning them and
// the cursor to resume strictly after the last one (nil when the namespace end is reached). It bounds
// per-call work + allocation for incremental sweeps (e.g. intra-cluster anti-entropy) instead of
// materializing the whole namespace.
func (s *Store) ScanRecordsFrom(namespace string, start []byte, limit int) (recs []*wavespanv1.StoredRecord, next []byte, err error) {
	lo := latestKey(namespace, start)
	if start == nil {
		lo = namespacePrefix(namespace)
	}
	hi := prefixEnd(namespacePrefix(namespace))
	it, err := s.local.Scan(storage.CFKVMeta, lo, hi, limit)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = it.Close() }()
	nsPrefixLen := len(namespacePrefix(namespace))
	var lastKey []byte
	count := 0
	for it.Valid() {
		k := it.Key()
		userKey := append([]byte(nil), k[nsPrefixLen:]...)
		lastKey = userKey
		count++
		if lp, derr := storage.DecodeLatestPointer(it.Value()); derr == nil {
			win := version.FromProto(lp.GetWinner())
			if rb, found, gerr := s.local.Get(storage.CFKVData, dataKey(namespace, userKey, win)); gerr == nil && found {
				if rec, rerr := storage.DecodeStoredRecord(rb); rerr == nil {
					recs = append(recs, rec)
				}
			}
		}
		it.Next()
	}
	if ierr := it.Err(); ierr != nil {
		return nil, nil, ierr
	}
	// A short page means we reached the end of the namespace — resume from the top next sweep.
	if limit > 0 && count >= limit && lastKey != nil {
		next = append(append([]byte(nil), lastKey...), 0x00) // strictly after lastKey
	}
	return recs, next, nil
}

// ExpiredEntry is a ttl-indexed key whose bucket is due, for the sweeper. IndexKey is the raw
// ttl-index key, used to clear the entry after sweeping.
type ExpiredEntry struct {
	Namespace string
	Key       []byte
	IndexKey  []byte
}

// ExpiredEntries returns ttl-index entries in buckets due by nowMs (design/03 lazy TTL sweeper).
func (s *Store) ExpiredEntries(nowMs int64, limit int) ([]ExpiredEntry, error) {
	it, err := s.local.Scan(storage.CFKVMeta, ttlLowBound(), ttlScanBound(nowMs), 0)
	if err != nil {
		return nil, err
	}
	defer func() { _ = it.Close() }()
	var out []ExpiredEntry
	for it.Valid() {
		ns, key, ok := parseTTLKey(it.Key())
		if ok {
			out = append(out, ExpiredEntry{
				Namespace: ns, Key: append([]byte(nil), key...), IndexKey: append([]byte(nil), it.Key()...),
			})
			if limit > 0 && len(out) >= limit {
				return out, nil
			}
		}
		it.Next()
	}
	return out, it.Err()
}

// ClearTTLIndex removes a swept ttl-index entry by its raw key.
func (s *Store) ClearTTLIndex(indexKey []byte) error {
	return s.local.Batch([]storage.StoreOp{{CF: storage.CFKVMeta, Key: indexKey, Delete: true}})
}

// ExpiresAt returns the latest pointer's expiry for a key, if any (used to clear the ttl entry).
func (s *Store) ExpiresAt(namespace string, key []byte) (int64, bool) {
	cur, found, err := s.local.Get(storage.CFKVMeta, latestKey(namespace, key))
	if err != nil || !found {
		return 0, false
	}
	lp, err := storage.DecodeLatestPointer(cur)
	if err != nil || lp.ExpiresAtUnixMs == nil {
		return 0, false
	}
	return lp.GetExpiresAtUnixMs(), true
}

// ApplySiblings stores a set of concurrent sibling records for a key (keep-siblings resolution,
// design/06): every sibling's versioned record is written, and the latest pointer records the
// highest-versioned sibling as winner with the rest in sibling_versions. Conflict state becomes
// SIBLINGS_PRESENT so reads can surface them.
func (s *Store) ApplySiblings(namespace string, key []byte, siblings []*wavespanv1.StoredRecord) error {
	if len(siblings) == 0 {
		return nil
	}
	si := stripeIdx(namespace, key) // order against concurrent Apply/ApplySiblings on this key
	s.stripes[si].Lock()
	defer s.stripes[si].Unlock()
	delete(s.latestVer[si], namespace+"\x00"+string(key)) // siblings rewrite the pointer; force a re-read next Apply
	winner := siblings[0]
	for _, r := range siblings[1:] {
		if version.FromProto(r.GetVersion()).Compare(version.FromProto(winner.GetVersion())) > 0 {
			winner = r
		}
	}
	lp := &wavespanv1.LatestPointer{Winner: winner.GetVersion(), Tombstone: winner.GetTombstone()}
	var ops []storage.StoreOp
	for _, r := range siblings {
		rv := version.FromProto(r.GetVersion())
		b, err := EncodeStoredRecordRaw(r)
		if err != nil {
			return err
		}
		ops = append(ops, storage.StoreOp{CF: storage.CFKVData, Key: dataKey(namespace, key, rv), Value: b})
		if !rv.Equal(version.FromProto(winner.GetVersion())) {
			lp.SiblingVersions = append(lp.SiblingVersions, r.GetVersion())
		}
	}
	lpBytes, err := storage.EncodeLatestPointer(lp)
	if err != nil {
		return err
	}
	ops = append(ops, storage.StoreOp{CF: storage.CFKVMeta, Key: latestKey(namespace, key), Value: lpBytes})
	return s.local.BatchRC(ops)
}

// EncodeStoredRecordRaw marshals a StoredRecord (exposed for ApplySiblings).
func EncodeStoredRecordRaw(r *wavespanv1.StoredRecord) ([]byte, error) {
	return storage.EncodeStoredRecord(r)
}

// GetRecord returns the winning StoredRecord for a key (used by repair to re-replicate the
// latest record). found is false when the key is absent locally.
func (s *Store) GetRecord(namespace string, key []byte) (*wavespanv1.StoredRecord, bool, error) {
	cur, found, err := s.local.Get(storage.CFKVMeta, latestKey(namespace, key))
	if err != nil || !found {
		return nil, false, err
	}
	lp, err := storage.DecodeLatestPointer(cur)
	if err != nil {
		return nil, false, err
	}
	win := version.FromProto(lp.GetWinner())
	recBytes, rfound, err := s.local.Get(storage.CFKVData, dataKey(namespace, key, win))
	if err != nil || !rfound {
		return nil, false, err
	}
	rec, err := storage.DecodeStoredRecord(recBytes)
	if err != nil {
		return nil, false, err
	}
	return rec, true, nil
}

// Forget physically removes a key's local records and latest pointer without writing a tombstone
// (used to evict a dynamic cache replica — derived, disposable state; design/05 "Cache eviction").
// It does not append a mutation-log entry: forgetting is a local-only drop, not a replicated delete.
func (s *Store) Forget(namespace string, key []byte) error {
	ops := []storage.StoreOp{{CF: storage.CFKVMeta, Key: latestKey(namespace, key), Delete: true}}
	it, err := s.local.Scan(storage.CFKVData, dataKeyPrefix(namespace, key), prefixEnd(dataKeyPrefix(namespace, key)), 0)
	if err != nil {
		return err
	}
	for it.Valid() {
		ops = append(ops, storage.StoreOp{CF: storage.CFKVData, Key: append([]byte(nil), it.Key()...), Delete: true})
		it.Next()
	}
	_ = it.Close()
	return s.local.Batch(ops)
}

// prefixEnd returns the smallest key greater than every key with the given prefix.
func prefixEnd(prefix []byte) []byte {
	end := append([]byte(nil), prefix...)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] != 0xff {
			end[i]++
			return end[:i+1]
		}
	}
	return nil // prefix is all 0xff: unbounded
}

// GetOutcome is the result of a local read.
type GetOutcome struct {
	Found        bool
	Value        []byte
	Version      version.Version
	Tombstone    bool
	ConflictNone bool
	ExpiresAtMs  *int64
}

// Get resolves a key from the latest pointer and its winning versioned record. A tombstone
// winner reports Found=false but carries the version.
func (s *Store) Get(namespace string, key []byte) (GetOutcome, error) {
	cur, found, err := s.local.Get(storage.CFKVMeta, latestKey(namespace, key))
	if err != nil || !found {
		return GetOutcome{Found: false}, err
	}
	lp, err := storage.DecodeLatestPointer(cur)
	if err != nil {
		return GetOutcome{}, err
	}
	win := version.FromProto(lp.GetWinner())
	out := GetOutcome{Version: win, Tombstone: lp.GetTombstone(), ConflictNone: len(lp.GetSiblingVersions()) == 0}
	if lp.ExpiresAtUnixMs != nil {
		e := lp.GetExpiresAtUnixMs()
		out.ExpiresAtMs = &e
	}
	if lp.GetTombstone() {
		return out, nil // deleted: found=false
	}
	recBytes, rfound, err := s.local.Get(storage.CFKVData, dataKey(namespace, key, win))
	if err != nil || !rfound {
		return out, err
	}
	rec, err := storage.DecodeStoredRecord(recBytes)
	if err != nil {
		return out, err
	}
	out.Found = true
	out.Value = rec.GetValue().GetInline()
	return out, nil
}
