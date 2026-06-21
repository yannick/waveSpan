package local

import (
	"context"

	"github.com/cwire/wavespan/internal/membership"
	"github.com/cwire/wavespan/internal/recordstore"
	"github.com/cwire/wavespan/internal/version"
	wavespanv1 "github.com/cwire/wavespan/proto/wavespan/v1"
)

// Receiver applies StoreReplica requests durably (design/05 "StoreReplica protocol" receiver
// rules): compare the incoming version, apply the conflict policy (hlc-last-write-wins via
// recordstore), persist the record + latest pointer + mutation-log entry atomically, and
// acknowledge only after the durable local store. It is idempotent by mutation id.
type Receiver struct {
	store    *recordstore.Store
	memberID string
	idem     *Idempotency
}

// NewReceiver builds a StoreReplica receiver over a local record store.
func NewReceiver(store *recordstore.Store, memberID string, idem *Idempotency) *Receiver {
	if idem == nil {
		idem = NewIdempotency(0)
	}
	return &Receiver{store: store, memberID: memberID, idem: idem}
}

// Apply stores a replica durably and returns the applied (winning) version.
func (r *Receiver) Apply(req *wavespanv1.StoreReplicaRequest) (*wavespanv1.StoreReplicaResponse, error) {
	if v, ok := r.idem.Check(req.GetMutationId()); ok {
		return &wavespanv1.StoreReplicaResponse{Durable: true, MemberId: r.memberID, AppliedVersion: v.ToProto()}, nil
	}
	rec := req.GetRecord()
	kind := wavespanv1.MutationKind_MUTATION_KIND_PUT
	if rec.GetTombstone() {
		kind = wavespanv1.MutationKind_MUTATION_KIND_DELETE
	}
	winner, err := r.store.Apply(rec, kind)
	if err != nil {
		return nil, err
	}
	r.idem.Record(req.GetMutationId(), version.FromProto(rec.GetVersion()))
	return &wavespanv1.StoreReplicaResponse{
		Durable: true, MemberId: r.memberID, AppliedVersion: winner.ToProto(),
		ConflictState: wavespanv1.ConflictState_CONFLICT_NONE,
	}, nil
}

// Replicator sends a StoreReplica to a target member (the coordinator's fanout seam). The real
// implementation dials over Connect; tests route in-process.
type Replicator interface {
	StoreReplica(ctx context.Context, target membership.Member, req *wavespanv1.StoreReplicaRequest) (*wavespanv1.StoreReplicaResponse, error)
}

// BuildRequest assembles a StoreReplica request for a record (NEARBY_DURABLE class).
func BuildRequest(namespace string, key []byte, rec *wavespanv1.StoredRecord, coordinatorMemberID string) *wavespanv1.StoreReplicaRequest {
	return &wavespanv1.StoreReplicaRequest{
		Namespace: namespace, Key: key, Record: rec,
		ReplicaClass:        wavespanv1.ReplicaClass_REPLICA_CLASS_NEARBY_DURABLE,
		CoordinatorMemberId: coordinatorMemberID,
		MutationId:          version.FromProto(rec.GetVersion()).MutationID(),
	}
}
