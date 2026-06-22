package observability

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"connectrpc.com/connect"
	"github.com/cwire/wavespan/internal/latencygraph"
	"github.com/cwire/wavespan/internal/membership"
	"github.com/cwire/wavespan/internal/recordstore"
	"github.com/cwire/wavespan/internal/security"
	"github.com/cwire/wavespan/internal/storage"
	"github.com/cwire/wavespan/internal/version"
	wavespanv1 "github.com/cwire/wavespan/proto/wavespan/v1"
	"github.com/cwire/wavespan/proto/wavespan/v1/wavespanv1connect"
)

type fakeCluster struct{}

func (fakeCluster) Members() []membership.MemberView { return nil }
func (fakeCluster) Graph() *latencygraph.Graph {
	return latencygraph.New(latencygraph.DefaultConfig())
}

func newObsServer(t *testing.T, inspector GlobalInspector) (wavespanv1connect.ObservabilityServiceClient, *recordstore.Store) {
	t.Helper()
	mem := storage.NewMemStore()
	t.Cleanup(func() { _ = mem.Close() })
	rs := recordstore.NewStore(mem, "dev", "node1", version.NewClock(nil, 500), version.NewSequencer(0))
	obs := NewObsService(NewGossipRing(64), fakeCluster{}, membership.Member{ClusterID: "dev", MemberID: "node1"}, rs)
	if inspector != nil {
		obs.WithGlobalInspector(inspector)
	}
	mux := http.NewServeMux()
	mux.Handle(obs.Handler())
	wrapped := security.Identity{DevMode: true}.EnforceHTTP(mux)
	ts := httptest.NewServer(wrapped)
	t.Cleanup(ts.Close)
	return wavespanv1connect.NewObservabilityServiceClient(ts.Client(), ts.URL), rs
}

func seedKV(t *testing.T, rs *recordstore.Store, key, val string) {
	t.Helper()
	v := rs.NextVersion()
	if _, err := rs.Apply(rs.BuildRecord("default", []byte(key), []byte(val), v, false, nil), wavespanv1.MutationKind_MUTATION_KIND_PUT); err != nil {
		t.Fatal(err)
	}
}

func inspectLocal(t *testing.T, client wavespanv1connect.ObservabilityServiceClient, role string, includeValue bool) []*wavespanv1.InspectKey {
	t.Helper()
	req := connect.NewRequest(&wavespanv1.InspectLocalRequest{Namespace: "default", IncludeValue: includeValue})
	req.Header().Set("X-WaveSpan-Role", role)
	stream, err := client.InspectLocal(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	var keys []*wavespanv1.InspectKey
	for stream.Receive() {
		if k := stream.Msg().GetKey(); k != nil {
			keys = append(keys, k)
		}
	}
	if err := stream.Err(); err != nil {
		t.Fatal(err)
	}
	return keys
}

func TestInspectLocalRedactsByDefault(t *testing.T) {
	client, rs := newObsServer(t, nil)
	seedKV(t, rs, "k1", "secret")

	// reader, include_value -> still redacted (not admin)
	keys := inspectLocal(t, client, "reader", true)
	if len(keys) != 1 {
		t.Fatalf("expected 1 key, got %d", len(keys))
	}
	if len(keys[0].GetValue()) != 0 {
		t.Fatal("non-admin must not see the value")
	}
	if keys[0].GetKeyHash() == "" {
		t.Fatal("key_hash must always be present")
	}

	// admin + include_value -> value revealed
	adminKeys := inspectLocal(t, client, "admin", true)
	if string(adminKeys[0].GetValue()) != "secret" {
		t.Fatalf("admin with include_value should see the value, got %q", adminKeys[0].GetValue())
	}

	// admin WITHOUT include_value -> still redacted
	noVal := inspectLocal(t, client, "admin", false)
	if len(noVal[0].GetValue()) != 0 {
		t.Fatal("include_value=false must redact even for admin")
	}
}

// fakeInspector reports one unreachable holder so completeness is partial.
type fakeInspector struct{}

func (fakeInspector) InspectKey(_ context.Context, _ string, _ []byte, _, _ bool) ([]*wavespanv1.InspectHolder, []string, bool) {
	return nil, []string{"holder node2 unreachable"}, false
}

func TestInspectGlobalCompletenessOnMissedHolder(t *testing.T) {
	client, rs := newObsServer(t, fakeInspector{})
	seedKV(t, rs, "k1", "v")

	req := connect.NewRequest(&wavespanv1.InspectGlobalRequest{Namespace: "default", Key: []byte("k1")})
	req.Header().Set("X-WaveSpan-Role", "reader")
	stream, err := client.InspectGlobal(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	var trailer *wavespanv1.InspectTrailer
	for stream.Receive() {
		if tr := stream.Msg().GetTrailer(); tr != nil {
			trailer = tr
		}
	}
	if err := stream.Err(); err != nil {
		t.Fatal(err)
	}
	if trailer == nil || trailer.GetFinalCompleteness() != wavespanv1.Completeness_PARTIAL {
		t.Fatalf("an unreachable holder must yield PARTIAL completeness: %+v", trailer)
	}
	if len(trailer.GetWarnings()) == 0 {
		t.Fatal("a warning naming the unreachable holder is required")
	}
}
