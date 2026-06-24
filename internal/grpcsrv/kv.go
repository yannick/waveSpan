package grpcsrv

import (
	"context"
	"errors"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/yannick/wavespan/internal/kv"
	"github.com/yannick/wavespan/internal/membership"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

// KV is the gRPC KvService adapter. It mirrors internal/kv.Service, delegating to the same
// Coordinator/Reader/Scanner cores; only the transport (gRPC vs Connect) differs.
type KV struct {
	wavespanv1.UnimplementedKvServiceServer
	coord   *kv.Coordinator
	reader  *kv.Reader
	scanner *kv.Scanner
	self    membership.Member
}

// NewKV wires the gRPC KV adapter over the coordinator, reader, and (optional) scanner.
func NewKV(coord *kv.Coordinator, reader *kv.Reader, scanner *kv.Scanner, self membership.Member) *KV {
	return &KV{coord: coord, reader: reader, scanner: scanner, self: self}
}

func (s *KV) writeMeta() *wavespanv1.ResponseMeta {
	return &wavespanv1.ResponseMeta{
		ServedByClusterId: s.self.ClusterID,
		ServedByMemberId:  s.self.MemberID,
		Source:            wavespanv1.ReadSource_LOCAL_DURABLE,
		ConflictState:     wavespanv1.ConflictState_CONFLICT_NONE,
		Completeness:      wavespanv1.Completeness_COMPLETE,
		ObservedAtUnixMs:  time.Now().UnixMilli(),
	}
}

func kvWriteError(err error) error {
	if errors.Is(err, kv.ErrInsufficientNearbyReplicas) {
		return status.Error(codes.Unavailable, err.Error())
	}
	return status.Error(codes.Internal, err.Error())
}

// Put coordinates an origin+1 write.
func (s *KV) Put(ctx context.Context, m *wavespanv1.PutRequest) (*wavespanv1.PutResult, error) {
	out, err := s.coord.Put(ctx, m.GetNamespace(), m.GetKey(), m.GetValue(), m.TtlMs, m.GetIdempotencyKey())
	if err != nil {
		return nil, kvWriteError(err)
	}
	meta := s.writeMeta()
	meta.ObservedVersion = out.Version.ToProto()
	return &wavespanv1.PutResult{
		Meta: meta, Version: out.Version.ToProto(),
		AckedNearbyReplicas: uint32(out.AckedNearbyReplicas), GeoSpillover: out.GeoSpillover,
	}, nil
}

// Delete coordinates a tombstone write.
func (s *KV) Delete(ctx context.Context, m *wavespanv1.DeleteRequest) (*wavespanv1.DeleteResult, error) {
	out, err := s.coord.Delete(ctx, m.GetNamespace(), m.GetKey(), m.GetIdempotencyKey())
	if err != nil {
		return nil, kvWriteError(err)
	}
	meta := s.writeMeta()
	meta.ObservedVersion = out.Version.ToProto()
	return &wavespanv1.DeleteResult{
		Meta: meta, Version: out.Version.ToProto(), AckedNearbyReplicas: uint32(out.AckedNearbyReplicas),
	}, nil
}

// Get serves a local-first read (with a closest-holder cache fetch on a miss, M5).
func (s *KV) Get(ctx context.Context, m *wavespanv1.GetRequest) (*wavespanv1.GetResult, error) {
	res, err := s.reader.Get(ctx, m.GetNamespace(), m.GetKey(), m.GetHideExpiredOnRead())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return res, nil
}

// MultiGet serves a batch of reads in one round-trip.
func (s *KV) MultiGet(ctx context.Context, m *wavespanv1.MultiGetRequest) (*wavespanv1.MultiGetResult, error) {
	results, err := s.reader.MultiGet(ctx, m.GetNamespace(), m.GetKeys(), m.GetHideExpiredOnRead())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &wavespanv1.MultiGetResult{Results: results}, nil
}

// Scan streams a range scan (M6). It delegates to the same Scanner core, sending each result on the
// gRPC stream; cancellation is honoured via stream.Context() (passed to the scanner).
func (s *KV) Scan(req *wavespanv1.ScanRequest, stream grpc.ServerStreamingServer[wavespanv1.ScanResponse]) error {
	if s.scanner == nil {
		return status.Error(codes.Unimplemented, "kv: scan not enabled")
	}
	return s.scanner.Scan(stream.Context(), req, stream.Send)
}
