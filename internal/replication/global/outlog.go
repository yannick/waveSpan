// Package global implements WaveSpan's cross-cluster active-active replication (design/06): a
// per-peer outbound log, streaming sender/receiver, an idempotent applier driven by the conflict
// resolvers, and anti-entropy. No cross-cluster consensus on the hot path.
package global

import (
	"encoding/binary"
	"errors"
	"sync"

	"github.com/yannick/wavespan/internal/storage"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
	"google.golang.org/protobuf/proto"
)

// ErrOutLogFull is returned to a globalDurabilityRequired caller when the per-peer out-log is over
// budget and no anti-entropy checkpoint has advanced to free space (design/06 "Backpressure").
var ErrOutLogFull = errors.New("global: out-log over budget")

// numPartitions partitions the out-log so a stalled key range does not block others.
const numPartitions = 16

// OutLog is the per-peer, partitioned outbound replication log
// (`/repl/global/out/{peer}/{partition}/{seq}`), stored in CFReplLog behind a "gout" prefix.
// Retention rule: entries are dropped ONLY after an anti-entropy checkpoint confirms the peer
// caught up past them (CompactBelowCheckpoint); Append never drops un-checkpointed entries.
type OutLog struct {
	store  storage.LocalStore
	budget int64

	mu         sync.Mutex
	seq        map[string]uint64 // (peer,partition) -> last appended seq
	bytes      map[string]int64  // (peer,partition) -> live bytes
	checkpoint map[string]uint64 // (peer,partition) -> seq the peer has applied through
}

// NewOutLog builds an out-log over the local store with a per-(peer,partition) byte budget
// (0 = unbounded).
func NewOutLog(store storage.LocalStore, budgetBytes int64) *OutLog {
	return &OutLog{
		store: store, budget: budgetBytes,
		seq: map[string]uint64{}, bytes: map[string]int64{}, checkpoint: map[string]uint64{},
	}
}

// Partition assigns a mutation to a partition by hashing its namespace+key.
func Partition(namespace string, key []byte) uint32 {
	h := uint32(2166136261)
	for _, b := range append([]byte(namespace+"\x00"), key...) {
		h = (h ^ uint32(b)) * 16777619
	}
	return h % numPartitions
}

func pk(peer string, partition uint32) string {
	var p [4]byte
	binary.BigEndian.PutUint32(p[:], partition)
	return peer + "\x00" + string(p[:])
}

func outKey(peer string, partition uint32, seq uint64) []byte {
	out := []byte("gout\x00")
	out = append(out, peer...)
	out = append(out, 0)
	var p [4]byte
	binary.BigEndian.PutUint32(p[:], partition)
	out = append(out, p[:]...)
	var s [8]byte
	binary.BigEndian.PutUint64(s[:], seq)
	return append(out, s[:]...)
}

func outPrefix(peer string, partition uint32) []byte {
	out := []byte("gout\x00")
	out = append(out, peer...)
	out = append(out, 0)
	var p [4]byte
	binary.BigEndian.PutUint32(p[:], partition)
	return append(out, p[:]...)
}

func prefixUpper(p []byte) []byte {
	end := append([]byte(nil), p...)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] != 0xff {
			end[i]++
			return end[:i+1]
		}
	}
	return nil
}

// Append writes a mutation to the peer's partitioned out-log. For a globalDurabilityRequired
// caller it returns ErrOutLogFull when over budget; the default path always appends (best-effort,
// budget reclaimed only via post-checkpoint compaction).
func (l *OutLog) Append(peer string, m *wavespanv1.GlobalMutation, globalDurabilityRequired bool) error {
	key := pk(peer, m.GetPartition())
	l.mu.Lock()
	defer l.mu.Unlock()
	if globalDurabilityRequired && l.budget > 0 && l.bytes[key] >= l.budget {
		return ErrOutLogFull
	}
	seq := l.seq[key] + 1
	b, err := proto.Marshal(m)
	if err != nil {
		return err
	}
	if err := l.store.Put(storage.CFReplLog, outKey(peer, m.GetPartition(), seq), b); err != nil {
		return err
	}
	l.seq[key] = seq
	l.bytes[key] += int64(len(b))
	return nil
}

// OutEntry is an out-log entry with its sequence.
type OutEntry struct {
	Seq      uint64
	Mutation *wavespanv1.GlobalMutation
}

// IterateFrom returns entries for (peer, partition) with seq > fromSeq, in order, up to limit.
func (l *OutLog) IterateFrom(peer string, partition uint32, fromSeq uint64, limit int) ([]OutEntry, error) {
	lo := outKey(peer, partition, fromSeq+1)
	hi := prefixUpper(outPrefix(peer, partition))
	it, err := l.store.Scan(storage.CFReplLog, lo, hi, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = it.Close() }()
	var out []OutEntry
	for it.Valid() {
		k := it.Key()
		seq := binary.BigEndian.Uint64(k[len(k)-8:])
		m := &wavespanv1.GlobalMutation{}
		if proto.Unmarshal(it.Value(), m) == nil {
			out = append(out, OutEntry{Seq: seq, Mutation: m})
		}
		it.Next()
	}
	return out, it.Err()
}

// Checkpoint records that the peer has applied (peer, partition) through seq, allowing
// CompactBelowCheckpoint to reclaim entries at or below it.
func (l *OutLog) Checkpoint(peer string, partition uint32, seq uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if seq > l.checkpoint[pk(peer, partition)] {
		l.checkpoint[pk(peer, partition)] = seq
	}
}

// CompactBelowCheckpoint deletes entries at or below the checkpoint for (peer, partition) and
// returns how many were removed. Entries above the checkpoint are retained even over budget.
func (l *OutLog) CompactBelowCheckpoint(peer string, partition uint32) (int, error) {
	l.mu.Lock()
	cp := l.checkpoint[pk(peer, partition)]
	l.mu.Unlock()
	if cp == 0 {
		return 0, nil
	}
	entries, err := l.IterateFrom(peer, partition, 0, 0)
	if err != nil {
		return 0, err
	}
	removed := 0
	var freed int64
	for _, e := range entries {
		if e.Seq > cp {
			break
		}
		b, _ := proto.Marshal(e.Mutation)
		if derr := l.store.Delete(storage.CFReplLog, outKey(peer, partition, e.Seq)); derr == nil {
			removed++
			freed += int64(len(b))
		}
	}
	l.mu.Lock()
	l.bytes[pk(peer, partition)] -= freed
	if l.bytes[pk(peer, partition)] < 0 {
		l.bytes[pk(peer, partition)] = 0
	}
	l.mu.Unlock()
	return removed, nil
}

// Bytes returns the live byte count for (peer, partition).
func (l *OutLog) Bytes(peer string, partition uint32) int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.bytes[pk(peer, partition)]
}

// LastSeq returns the last appended seq for (peer, partition).
func (l *OutLog) LastSeq(peer string, partition uint32) uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.seq[pk(peer, partition)]
}
