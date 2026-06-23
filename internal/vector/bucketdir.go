package vector

import "sync"

// HeldBucket is one node's advertised bucket set for a collection (the neutral form the node bridges
// to the gossip wire).
type HeldBucket struct {
	Collection        string
	QVer              uint32
	Buckets           []uint32
	GeneratedAtUnixMs int64
}

// BucketDir tracks which coarse buckets each member holds per collection, so a kNN query routes only
// to the holders of its probed buckets (design/29). This node's own set is maintained incrementally
// as vectors are applied and periodically recomputed from the store (so emptied/migrated buckets are
// de-advertised — the explicit-set property the holder bloom lacks). Peers' sets arrive via gossip,
// kept newest-generation-wins.
type BucketDir struct {
	self string
	mu   sync.RWMutex
	own  map[string]*bucketSet            // collection -> own buckets
	peer map[string]map[string]*bucketSet // collection -> memberID -> buckets
}

type bucketSet struct {
	qver uint32
	set  map[uint32]bool
	gen  int64
}

// NewBucketDir builds an empty directory for this node.
func NewBucketDir(self string) *BucketDir {
	return &BucketDir{self: self, own: map[string]*bucketSet{}, peer: map[string]map[string]*bucketSet{}}
}

// AddOwn records that this node now holds a bucket (called as vectors are applied).
func (d *BucketDir) AddOwn(collection string, qver, bucket uint32) {
	d.mu.Lock()
	defer d.mu.Unlock()
	bs := d.own[collection]
	if bs == nil || bs.qver != qver {
		bs = &bucketSet{qver: qver, set: map[uint32]bool{}}
		d.own[collection] = bs
	}
	bs.set[bucket] = true
}

// SetOwn replaces this node's bucket set for a collection (periodic recompute from the store). It is
// what de-advertises buckets that no longer have any local vector.
func (d *BucketDir) SetOwn(collection string, qver uint32, buckets []uint32, nowMs int64) {
	set := make(map[uint32]bool, len(buckets))
	for _, b := range buckets {
		set[b] = true
	}
	d.mu.Lock()
	d.own[collection] = &bucketSet{qver: qver, set: set, gen: nowMs}
	d.mu.Unlock()
}

// OwnAdvert returns this node's bucket sets (one per collection) for gossip.
func (d *BucketDir) OwnAdvert(nowMs int64) []HeldBucket {
	d.mu.RLock()
	defer d.mu.RUnlock()
	out := make([]HeldBucket, 0, len(d.own))
	for coll, bs := range d.own {
		buckets := make([]uint32, 0, len(bs.set))
		for b := range bs.set {
			buckets = append(buckets, b)
		}
		out = append(out, HeldBucket{Collection: coll, QVer: bs.qver, Buckets: buckets, GeneratedAtUnixMs: nowMs})
	}
	return out
}

// ApplyPeer merges a peer's advertised bucket set (newest generation wins).
func (d *BucketDir) ApplyPeer(memberID string, h HeldBucket) {
	d.mu.Lock()
	defer d.mu.Unlock()
	byMember := d.peer[h.Collection]
	if byMember == nil {
		byMember = map[string]*bucketSet{}
		d.peer[h.Collection] = byMember
	}
	if cur := byMember[memberID]; cur != nil && cur.gen >= h.GeneratedAtUnixMs {
		return
	}
	set := make(map[uint32]bool, len(h.Buckets))
	for _, b := range h.Buckets {
		set[b] = true
	}
	byMember[memberID] = &bucketSet{qver: h.QVer, set: set, gen: h.GeneratedAtUnixMs}
}

// Holders returns the members (this node + peers) that hold any of the probed buckets for a
// collection at the given quantizer version. ok is false when no routing info is available (the
// caller then falls back to scattering to all holders).
func (d *BucketDir) Holders(collection string, qver uint32, buckets []uint32) (members []string, ok bool) {
	want := make(map[uint32]bool, len(buckets))
	for _, b := range buckets {
		want[b] = true
	}
	d.mu.RLock()
	defer d.mu.RUnlock()
	byMember := d.peer[collection]
	if byMember == nil && d.own[collection] == nil {
		return nil, false // nothing advertised yet
	}
	seen := map[string]bool{}
	intersects := func(bs *bucketSet) bool {
		if bs == nil || bs.qver != qver {
			return false
		}
		for b := range want {
			if bs.set[b] {
				return true
			}
		}
		return false
	}
	if intersects(d.own[collection]) {
		members = append(members, d.self)
		seen[d.self] = true
	}
	for mid, bs := range byMember {
		if !seen[mid] && intersects(bs) {
			members = append(members, mid)
			seen[mid] = true
		}
	}
	return members, true
}
