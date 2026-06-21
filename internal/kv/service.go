package kv

import (
	"context"
	"errors"
	"net/http"
	"time"

	"connectrpc.com/connect"
	"github.com/cwire/wavespan/internal/membership"
	wavespanv1 "github.com/cwire/wavespan/proto/wavespan/v1"
	"github.com/cwire/wavespan/proto/wavespan/v1/wavespanv1connect"
)

// Service is the public KvService Connect handler over the coordinator and reader.
type Service struct {
	coord  *Coordinator
	reader *Reader
	self   membership.Member
}

// NewService wires the KV Connect service.
func NewService(coord *Coordinator, reader *Reader, self membership.Member) *Service {
	return &Service{coord: coord, reader: reader, self: self}
}

// Handler returns the mountable Connect handler (path, handler) for the data port.
func (s *Service) Handler() (string, http.Handler) {
	return wavespanv1connect.NewKvServiceHandler(s)
}

func (s *Service) writeMeta() *wavespanv1.ResponseMeta {
	return &wavespanv1.ResponseMeta{
		ServedByClusterId: s.self.ClusterID,
		ServedByMemberId:  s.self.MemberID,
		Source:            wavespanv1.ReadSource_LOCAL_DURABLE,
		ConflictState:     wavespanv1.ConflictState_CONFLICT_NONE,
		Completeness:      wavespanv1.Completeness_COMPLETE,
		ObservedAtUnixMs:  time.Now().UnixMilli(),
	}
}

func writeError(err error) error {
	if errors.Is(err, ErrInsufficientNearbyReplicas) {
		return connect.NewError(connect.CodeUnavailable, err)
	}
	return connect.NewError(connect.CodeInternal, err)
}

// Put coordinates an origin+1 write.
func (s *Service) Put(ctx context.Context, req *connect.Request[wavespanv1.PutRequest]) (*connect.Response[wavespanv1.PutResult], error) {
	m := req.Msg
	out, err := s.coord.Put(ctx, m.GetNamespace(), m.GetKey(), m.GetValue(), m.TtlMs, m.GetIdempotencyKey())
	if err != nil {
		return nil, writeError(err)
	}
	meta := s.writeMeta()
	meta.ObservedVersion = out.Version.ToProto()
	return connect.NewResponse(&wavespanv1.PutResult{
		Meta: meta, Version: out.Version.ToProto(),
		AckedNearbyReplicas: uint32(out.AckedNearbyReplicas), GeoSpillover: out.GeoSpillover,
	}), nil
}

// Delete coordinates a tombstone write.
func (s *Service) Delete(ctx context.Context, req *connect.Request[wavespanv1.DeleteRequest]) (*connect.Response[wavespanv1.DeleteResult], error) {
	m := req.Msg
	out, err := s.coord.Delete(ctx, m.GetNamespace(), m.GetKey(), m.GetIdempotencyKey())
	if err != nil {
		return nil, writeError(err)
	}
	meta := s.writeMeta()
	meta.ObservedVersion = out.Version.ToProto()
	return connect.NewResponse(&wavespanv1.DeleteResult{
		Meta: meta, Version: out.Version.ToProto(), AckedNearbyReplicas: uint32(out.AckedNearbyReplicas),
	}), nil
}

// Get serves a local-first read (with a closest-holder cache fetch on a miss, M5).
func (s *Service) Get(ctx context.Context, req *connect.Request[wavespanv1.GetRequest]) (*connect.Response[wavespanv1.GetResult], error) {
	res, err := s.reader.Get(ctx, req.Msg.GetNamespace(), req.Msg.GetKey(), req.Msg.GetHideExpiredOnRead())
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(res), nil
}
