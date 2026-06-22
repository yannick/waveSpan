package vector

import (
	"context"
	"net/http"

	"connectrpc.com/connect"
	wavespanv1 "github.com/cwire/wavespan/proto/wavespan/v1"
	"github.com/cwire/wavespan/proto/wavespan/v1/wavespanv1connect"
)

// Service is the VectorService Connect handler: it ingests raw vectors into the local store
// (design/08). Search is served via the Cypher vector.searchExact procedure.
type Service struct {
	store      *Store
	newVersion func() *wavespanv1.Version
}

// NewService wires the vector ingest service.
func NewService(store *Store, newVersion func() *wavespanv1.Version) *Service {
	return &Service{store: store, newVersion: newVersion}
}

// Handler returns the mountable Connect handler for the data port.
func (s *Service) Handler() (string, http.Handler) {
	return wavespanv1connect.NewVectorServiceHandler(s)
}

// Put ingests a vector record, stamping a server version when absent and deriving dimensions.
func (s *Service) Put(_ context.Context, req *connect.Request[wavespanv1.PutVectorRequest]) (*connect.Response[wavespanv1.PutVectorResponse], error) {
	rec := req.Msg.GetRecord()
	if rec == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errNoRecord)
	}
	if rec.Version == nil && s.newVersion != nil {
		rec.Version = s.newVersion()
	}
	rec.Dimensions = uint32(len(rec.GetValues()))
	if err := s.store.Put(rec); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&wavespanv1.PutVectorResponse{}), nil
}

var errNoRecord = connectError("vector: put requires a record")

type connectError string

func (e connectError) Error() string { return string(e) }
