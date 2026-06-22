package global

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/cwire/wavespan/internal/config"
	"github.com/cwire/wavespan/internal/conflict"
	"github.com/cwire/wavespan/internal/storage"
	wavespanv1 "github.com/cwire/wavespan/proto/wavespan/v1"
)

// receiverFor builds a GlobalReplication httptest server applying into a fresh store for clusterB,
// gated by an "up" flag so a disconnect can be simulated.
func receiverFor(t *testing.T) (*httptest.Server, *Applier, *atomic.Bool) {
	t.Helper()
	bStore := newRecStore(t, "b1")
	applier := NewApplier(bStore, conflict.NewRegistry(), nil)
	srv := NewServer(applier, nil)
	up := &atomic.Bool{}
	up.Store(true)
	path, h := srv.Handler()
	mux := http.NewServeMux()
	mux.Handle(path, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !up.Load() {
			http.Error(w, "peer down", http.StatusServiceUnavailable)
			return
		}
		h.ServeHTTP(w, r)
	}))
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	return ts, applier, up
}

func senderTo(t *testing.T, ts *httptest.Server) (*Sender, *OutLog) {
	t.Helper()
	mem := storage.NewMemStore()
	t.Cleanup(func() { _ = mem.Close() })
	outlog := NewOutLog(mem, 0)
	peer := config.ClusterPeer{ClusterID: "test-b", ReplEndpoint: strings.TrimPrefix(ts.URL, "http://")}
	return NewSender(outlog, []config.ClusterPeer{peer}, ts.Client()), outlog
}

func TestSenderShipsToReceiverIdempotently(t *testing.T) {
	ts, applier, _ := receiverFor(t)
	sender, outlog := senderTo(t, ts)
	ctx := context.Background()

	for i := 1; i <= 3; i++ {
		if err := outlog.Append("test-b", gmut("test-a", "a1", uint64(i), 100, "default", "k"+string(rune('0'+i)), "v", false, nil), false); err != nil {
			t.Fatal(err)
		}
	}
	if n := sender.DrainOnce(ctx); n != 3 {
		t.Fatalf("expected 3 mutations shipped, got %d", n)
	}
	if !applier.Applied(&wavespanv1.GlobalMutationId{ClusterId: "test-a", MemberId: "a1", WriterSequence: 1}) {
		t.Fatal("receiver did not apply mutation 1")
	}
	// cursor advanced -> a second drain ships nothing
	if n := sender.DrainOnce(ctx); n != 0 {
		t.Fatalf("second drain should be a no-op, shipped %d", n)
	}
}

func TestSenderResumesAfterDisconnect(t *testing.T) {
	ts, _, up := receiverFor(t)
	sender, outlog := senderTo(t, ts)
	ctx := context.Background()

	_ = outlog.Append("test-b", gmut("test-a", "a1", 1, 100, "default", "k1", "v", false, nil), false)
	up.Store(false) // peer down
	if n := sender.DrainOnce(ctx); n != 0 {
		t.Fatalf("no mutations should ship while peer is down, got %d", n)
	}
	// add more while down
	_ = outlog.Append("test-b", gmut("test-a", "a1", 2, 100, "default", "k2", "v", false, nil), false)
	up.Store(true) // peer back
	// resumes from cursor 0, ships both with no gaps
	if n := sender.DrainOnce(ctx); n != 2 {
		t.Fatalf("after reconnect both mutations should ship, got %d", n)
	}
}
