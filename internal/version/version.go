package version

import (
	"strconv"
	"strings"
	"sync"

	wavespanv1 "github.com/cwire/wavespan/proto/wavespan/v1"
)

// Version is the per-mutation HLC version stamped by the writing coordinator. It mirrors
// wavespanv1.Version and carries the deterministic last-write-wins order from
// design/22_versioning_and_hlc.md / design/03_kv_store.md.
type Version struct {
	HLCPhysicalMs   uint64
	HLCLogical      uint32
	WriterClusterID string
	WriterMemberID  string
	WriterSequence  uint64
}

// Timestamp returns the HLC pair embedded in the version.
func (a Version) Timestamp() Timestamp {
	return Timestamp{PhysicalMs: a.HLCPhysicalMs, Logical: a.HLCLogical}
}

// Compare returns -1 if a < b, 0 if equal, +1 if a > b under the hlc-last-write-wins
// total order. Higher means "wins". The HLC pair decides causality/concurrency; the
// writer fields are a deterministic tie-break (design/22 "Version compare").
func (a Version) Compare(b Version) int {
	if a.HLCPhysicalMs != b.HLCPhysicalMs {
		return cmpUint64(a.HLCPhysicalMs, b.HLCPhysicalMs)
	}
	if a.HLCLogical != b.HLCLogical {
		return cmpUint32(a.HLCLogical, b.HLCLogical)
	}
	if c := strings.Compare(a.WriterClusterID, b.WriterClusterID); c != 0 {
		return c
	}
	if c := strings.Compare(a.WriterMemberID, b.WriterMemberID); c != 0 {
		return c
	}
	return cmpUint64(a.WriterSequence, b.WriterSequence)
}

// Equal reports whether two versions are the same mutation identity-wise.
func (a Version) Equal(b Version) bool {
	return a.HLCPhysicalMs == b.HLCPhysicalMs &&
		a.HLCLogical == b.HLCLogical &&
		a.WriterClusterID == b.WriterClusterID &&
		a.WriterMemberID == b.WriterMemberID &&
		a.WriterSequence == b.WriterSequence
}

// MutationID is the stable, idempotent replication identity (design/06): it depends only
// on (writer_cluster_id, writer_member_id, writer_sequence). The field separator keeps the
// id unambiguous across field boundaries. Re-sending the same originated mutation yields
// the same id, so a receiver that already applied it can deduplicate.
func (a Version) MutationID() string {
	return mutationID(a.WriterClusterID, a.WriterMemberID, a.WriterSequence)
}

func mutationID(cluster, member string, seq uint64) string {
	var b strings.Builder
	b.WriteString(cluster)
	b.WriteByte(idSep)
	b.WriteString(member)
	b.WriteByte(idSep)
	b.WriteString(strconv.FormatUint(seq, 10))
	return b.String()
}

// idSep is a separator byte that cannot appear in the strconv-formatted sequence and is
// disallowed in cluster/member identifiers, keeping MutationID injective.
const idSep = '\x1f' // ASCII unit separator

// ToProto converts to the wire Version.
func (a Version) ToProto() *wavespanv1.Version {
	return &wavespanv1.Version{
		HlcPhysicalMs:   a.HLCPhysicalMs,
		HlcLogical:      a.HLCLogical,
		WriterClusterId: a.WriterClusterID,
		WriterMemberId:  a.WriterMemberID,
		WriterSequence:  a.WriterSequence,
	}
}

// FromProto converts from the wire Version.
func FromProto(p *wavespanv1.Version) Version {
	if p == nil {
		return Version{}
	}
	return Version{
		HLCPhysicalMs:   p.GetHlcPhysicalMs(),
		HLCLogical:      p.GetHlcLogical(),
		WriterClusterID: p.GetWriterClusterId(),
		WriterMemberID:  p.GetWriterMemberId(),
		WriterSequence:  p.GetWriterSequence(),
	}
}

// Sequencer is a per-member monotonic writer-sequence counter. It is seeded on startup
// from the persisted high-water mark (column family sys; design/22), so it never regresses
// across restarts. Safe for concurrent use.
type Sequencer struct {
	mu   sync.Mutex
	last uint64
}

// NewSequencer resumes from the highest previously issued value (0 for a fresh member).
func NewSequencer(start uint64) *Sequencer { return &Sequencer{last: start} }

// Next returns the next strictly increasing sequence value.
func (s *Sequencer) Next() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.last++
	return s.last
}

// Last returns the highest value issued so far (the value to persist).
func (s *Sequencer) Last() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.last
}
