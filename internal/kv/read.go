package kv

import (
	"time"

	"github.com/cwire/wavespan/internal/membership"
	"github.com/cwire/wavespan/internal/recordstore"
	wavespanv1 "github.com/cwire/wavespan/proto/wavespan/v1"
)

// Reader serves local-first reads (design/03 "Get path"). The dynamic-cache and holder-fetch
// paths arrive in M5; M3 reads only the local durable copy.
type Reader struct {
	store *recordstore.Store
	self  membership.Member
}

// NewReader builds a local reader.
func NewReader(store *recordstore.Store, self membership.Member) *Reader {
	return &Reader{store: store, self: self}
}

// Get reads a key from the local store and attaches response metadata declaring the read mode.
func (r *Reader) Get(namespace string, key []byte, hideExpired bool) (*wavespanv1.GetResult, error) {
	out, err := r.store.Get(namespace, key)
	if err != nil {
		return nil, err
	}
	meta := &wavespanv1.ResponseMeta{
		ServedByClusterId: r.self.ClusterID,
		ServedByMemberId:  r.self.MemberID,
		Source:            wavespanv1.ReadSource_LOCAL_DURABLE,
		ConflictState:     wavespanv1.ConflictState_CONFLICT_NONE,
		Completeness:      wavespanv1.Completeness_COMPLETE,
		ObservedAtUnixMs:  time.Now().UnixMilli(),
	}
	if !out.ConflictNone {
		meta.ConflictState = wavespanv1.ConflictState_CONFLICT_SIBLINGS_PRESENT
	}
	res := &wavespanv1.GetResult{Meta: meta, Found: out.Found, Value: out.Value}
	if out.Found || out.Tombstone {
		meta.ObservedVersion = out.Version.ToProto()
	}
	if out.ExpiresAtMs != nil {
		res.ExpiresAtUnixMs = out.ExpiresAtMs
		if hideExpired && *out.ExpiresAtMs <= time.Now().UnixMilli() {
			res.Found = false
			res.Value = nil
		}
	}
	return res, nil
}
