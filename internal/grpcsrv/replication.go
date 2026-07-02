package grpcsrv

import (
	"context"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/yannick/wavespan/internal/replication/local"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

// Replication is the gRPC ReplicationService adapter (inter-node, data-port). It mirrors the Connect
// ReplicaServer in internal/replication/local, delegating to the SAME exported cores: a *Receiver for
// inbound replica writes, a RecordReader for FetchReplica/Backfill/ScanLocal, and an optional
// SubscriptionSource for live SubscribeKey streaming. Only the transport (gRPC vs Connect) differs.
type Replication struct {
	wavespanv1.UnimplementedReplicationServiceServer
	recv   *local.Receiver
	reader local.RecordReader
	self   string
	dataAd string
	source local.SubscriptionSource
}

// NewReplication wires the gRPC ReplicationService adapter over the same dependencies the Connect
// ReplicaServer takes (see local.NewReplicaServer). reader serves FetchReplica; source (optional)
// serves live SubscribeKey updates.
func NewReplication(recv *local.Receiver, reader local.RecordReader, selfMemberID, selfDataAddr string, source local.SubscriptionSource) *Replication {
	return &Replication{recv: recv, reader: reader, self: selfMemberID, dataAd: selfDataAddr, source: source}
}

const backfillMaxPage = 1024

// StoreReplica handles an inbound replica write.
func (s *Replication) StoreReplica(_ context.Context, m *wavespanv1.StoreReplicaRequest) (*wavespanv1.StoreReplicaResponse, error) {
	resp, err := s.recv.Apply(m)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return resp, nil
}

// storeBatchApplyWorkers bounds concurrent applies for one inbound batch. Entries are applied
// CONCURRENTLY on purpose: the recordstore serializes per key (striped locks) and the WAL group
// commit merges concurrent committers into one fsync — a sequential loop would pay one commit
// group (and under syncMode=full, one fsync) per entry, forfeiting the point of batching.
const storeBatchApplyWorkers = 16

// StoreReplicaBatch applies a coalesced batch of replica writes (design/37 P1.4). Entries are
// independent: a failed entry yields durable=false in its positional response and never fails the
// batch RPC.
func (s *Replication) StoreReplicaBatch(_ context.Context, m *wavespanv1.StoreReplicaBatchRequest) (*wavespanv1.StoreReplicaBatchResponse, error) {
	reqs := m.GetRequests()
	resps := make([]*wavespanv1.StoreReplicaResponse, len(reqs))
	sem := make(chan struct{}, storeBatchApplyWorkers)
	var wg sync.WaitGroup
	for i, r := range reqs {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int, r *wavespanv1.StoreReplicaRequest) {
			defer wg.Done()
			defer func() { <-sem }()
			resp, err := s.recv.Apply(r)
			if err != nil {
				resp = &wavespanv1.StoreReplicaResponse{Durable: false, MemberId: s.self}
			}
			resps[i] = resp
		}(i, r)
	}
	wg.Wait()
	return &wavespanv1.StoreReplicaBatchResponse{Responses: resps}, nil
}

// FetchReplica returns the local winning record for a key.
func (s *Replication) FetchReplica(_ context.Context, m *wavespanv1.FetchReplicaRequest) (*wavespanv1.FetchReplicaResponse, error) {
	rec, found, err := s.reader.GetRecord(m.GetNamespace(), m.GetKey())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	resp := &wavespanv1.FetchReplicaResponse{Found: found, Record: rec}
	if found && m.GetWantSubscriptionOffer() {
		resp.SubscriptionOffer = &wavespanv1.SubscriptionOffer{SourceMemberId: s.self, SourceDataAddr: s.dataAd}
	}
	return resp, nil
}

// RangeDigest returns the content hash of this holder's winning (key, version) tuples in
// [start_key, end_key) — the cheap first phase of digest-based intra-cluster anti-entropy
// (design/37 P2.11): matching digests spare the per-key FetchReplica traffic entirely.
func (s *Replication) RangeDigest(_ context.Context, m *wavespanv1.RangeDigestRequest) (*wavespanv1.RangeDigestResponse, error) {
	recs, err := s.reader.ScanRecords(m.GetNamespace(), m.GetStartKey(), m.GetEndKey())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &wavespanv1.RangeDigestResponse{Digest: local.DigestRecords(recs), Count: uint64(len(recs))}, nil
}

// Backfill streams this holder's full records for a namespace, paginated, so a joining node can
// bootstrap an "everywhere"-replicated namespace.
func (s *Replication) Backfill(_ context.Context, m *wavespanv1.BackfillRequest) (*wavespanv1.BackfillResponse, error) {
	limit := int(m.GetLimit())
	if limit <= 0 || limit > backfillMaxPage {
		limit = backfillMaxPage
	}
	recs, next, err := s.reader.ScanRecordsFrom(m.GetNamespace(), m.GetCursor(), limit)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &wavespanv1.BackfillResponse{Records: recs, NextCursor: next}, nil
}

// ScanLocal scans this holder's local store over a subrange (routed-eventual scan).
func (s *Replication) ScanLocal(_ context.Context, m *wavespanv1.ScanLocalRequest) (*wavespanv1.ScanLocalResponse, error) {
	rows, err := s.reader.ScanRange(m.GetNamespace(), m.GetStartKey(), m.GetEndKey(), int(m.GetLimit()), time.Now().UnixMilli())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	resp := &wavespanv1.ScanLocalResponse{}
	for _, r := range rows {
		row := &wavespanv1.ScanLocalRow{Key: r.Key, Value: r.Value, Version: r.Version.ToProto()}
		if r.ExpiresAtMs != nil {
			row.ExpiresAtUnixMs = r.ExpiresAtMs
		}
		resp.Rows = append(resp.Rows, row)
	}
	return resp, nil
}

// SubscribeKey streams cache updates for a key. With a live source it delegates to the SAME producer
// (source.Subscribe) used by the Connect handler, forwarding each update via stream.Send; the source
// honours stream.Context() cancellation. Without a source it sends the current record once and closes.
func (s *Replication) SubscribeKey(m *wavespanv1.SubscribeKeyRequest, stream grpc.ServerStreamingServer[wavespanv1.CacheUpdate]) error {
	if s.source != nil {
		return s.source.Subscribe(stream.Context(), m, stream.Send)
	}
	rec, found, err := s.reader.GetRecord(m.GetNamespace(), m.GetKey())
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	if found {
		return stream.Send(&wavespanv1.CacheUpdate{Namespace: m.GetNamespace(), Key: m.GetKey(), Record: rec, StreamSequence: 1})
	}
	return nil
}
