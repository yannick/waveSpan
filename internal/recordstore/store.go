package recordstore

import (
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
}

// NewStore wires a local KV store.
func NewStore(local storage.LocalStore, clusterID, memberID string, clock *version.Clock, seq *version.Sequencer) *Store {
	return &Store{local: local, clock: clock, seq: seq, clusterID: clusterID, memberID: memberID}
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

	// current latest pointer
	winner := recVer
	winnerTombstone := rec.GetTombstone()
	var winnerExpiry *int64
	if rec.ExpiresAtUnixMs != nil {
		e := rec.GetExpiresAtUnixMs()
		winnerExpiry = &e
	}
	if cur, found, err := s.local.Get(storage.CFKVMeta, latestKey(ns, key)); err != nil {
		return version.Version{}, err
	} else if found {
		lp, err := storage.DecodeLatestPointer(cur)
		if err != nil {
			return version.Version{}, err
		}
		curWin := version.FromProto(lp.GetWinner())
		if curWin.Compare(recVer) >= 0 {
			// existing winner stands; still persist the incoming record version below
			winner = curWin
			winnerTombstone = lp.GetTombstone()
			winnerExpiry = nil
			if lp.ExpiresAtUnixMs != nil {
				e := lp.GetExpiresAtUnixMs()
				winnerExpiry = &e
			}
		}
	}

	recBytes, err := storage.EncodeStoredRecord(rec)
	if err != nil {
		return version.Version{}, err
	}
	lp := &wavespanv1.LatestPointer{Winner: winner.ToProto(), Tombstone: winnerTombstone}
	if winnerExpiry != nil {
		lp.ExpiresAtUnixMs = proto.Int64(*winnerExpiry)
	}
	lpBytes, err := storage.EncodeLatestPointer(lp)
	if err != nil {
		return version.Version{}, err
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
	envBytes, err := storage.EncodeMutationEnvelope(env)
	if err != nil {
		return version.Version{}, err
	}

	ops := []storage.StoreOp{
		{CF: storage.CFKVData, Key: dataKey(ns, key, recVer), Value: recBytes},
		{CF: storage.CFKVMeta, Key: latestKey(ns, key), Value: lpBytes},
		{CF: storage.CFReplLog, Key: storage.ReplLogKey(ns, s.logSeq.Add(1)), Value: envBytes},
	}
	if err := s.local.Batch(ops); err != nil {
		return version.Version{}, err
	}
	return winner, nil
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
