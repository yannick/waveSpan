package collections

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/lni/dragonboat/v4/config"
	"github.com/lni/dragonboat/v4/raftio"
	pb "github.com/lni/dragonboat/v4/raftpb"
)

// Cheap-mTLS Raft transport (design/30 §12, Appendix B.4). dragonboat's Expert.TransportFactory lets
// us replace its built-in TCP transport with one that carries Raft message batches and snapshot chunks
// as marshaled protobuf over HTTP, secured by the cluster's existing mTLS — no second TLS stack, no
// separate gossip transport. HTTP gives us framing for free, so this is far simpler than the raw-TCP
// reference transport: POST the marshaled payload, the receiver unmarshals and feeds dragonboat's
// handlers. Targets are "host:port" (the same form as initialMembers); the transport adds the scheme.
const (
	raftMsgPath   = "/wavespan/raft/msg"
	raftChunkPath = "/wavespan/raft/chunk"
)

// TransportFactory creates the cheap-mTLS transport. ServerTLS/ClientTLS are the cluster's mTLS
// configs; when both are nil the transport runs plaintext HTTP (dev / tests).
type TransportFactory struct {
	ServerTLS *tls.Config
	ClientTLS *tls.Config
	// ListenAddr, when set, is the local bind address for the raft listener, separate from the
	// advertised RaftAddress. This lets a node advertise a stable name (e.g. a StatefulSet pod's
	// DNS) while binding 0.0.0.0:port locally — binding directly to the DNS is racy at startup
	// (the record may not resolve yet). Empty = bind the advertised RaftAddress (dev / tests).
	ListenAddr string
	// Gate, when set, is consulted before every send: Gate(src, dst) == false drops the message,
	// simulating a network partition. Test-only (nil in production = all sends allowed).
	Gate func(src, dst string) bool
}

var _ config.TransportFactory = (*TransportFactory)(nil)

// Create builds the transport for a NodeHost, advertising its RaftAddress and binding ListenAddr
// (falling back to RaftAddress when ListenAddr is empty).
func (f *TransportFactory) Create(nh config.NodeHostConfig, mh raftio.MessageHandler, ch raftio.ChunkHandler) raftio.ITransport {
	t := newHTTPTransport(nh.RaftAddress, f.ServerTLS, f.ClientTLS, mh, ch)
	t.gate = f.Gate
	t.bindAddr = f.ListenAddr
	return t
}

// Validate accepts any non-empty RaftAddress (the transport controls the address form).
func (f *TransportFactory) Validate(addr string) bool { return addr != "" }

type httpTransport struct {
	listenAddr   string // advertised RaftAddress (node identity)
	bindAddr     string // local bind address; empty => bind listenAddr
	serverTLS    *tls.Config
	clientTLS    *tls.Config
	msgHandler   raftio.MessageHandler
	chunkHandler raftio.ChunkHandler
	gate         func(src, dst string) bool

	client *http.Client
	srv    *http.Server
}

// blocked reports whether a send from this node to target is partitioned away.
func (t *httpTransport) blocked(target string) bool {
	return t.gate != nil && !t.gate(t.listenAddr, target)
}

var _ raftio.ITransport = (*httpTransport)(nil)

func newHTTPTransport(addr string, srvTLS, cliTLS *tls.Config, mh raftio.MessageHandler, ch raftio.ChunkHandler) *httpTransport {
	return &httpTransport{
		listenAddr: addr, serverTLS: srvTLS, clientTLS: cliTLS,
		msgHandler: mh, chunkHandler: ch,
		client: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig:     cliTLS,
				MaxIdleConns:        64,
				MaxIdleConnsPerHost: 8,
				IdleConnTimeout:     90 * time.Second,
				ForceAttemptHTTP2:   true,
			},
			Timeout: 60 * time.Second, // snapshot chunks are size-bounded, so this is ample
		},
	}
}

func (t *httpTransport) Name() string { return "wavespan-mtls" }

func (t *httpTransport) Start() error {
	bind := t.listenAddr
	if t.bindAddr != "" {
		bind = t.bindAddr
	}
	ln, err := net.Listen("tcp", bind)
	if err != nil {
		return err
	}
	if t.serverTLS != nil {
		ln = tls.NewListener(ln, t.serverTLS)
	}
	mux := http.NewServeMux()
	mux.HandleFunc(raftMsgPath, t.handleMsg)
	mux.HandleFunc(raftChunkPath, t.handleChunk)
	t.srv = &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = t.srv.Serve(ln) }()
	return nil
}

func (t *httpTransport) handleMsg(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var batch pb.MessageBatch
	if err := batch.Unmarshal(body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	t.msgHandler(batch)
	w.WriteHeader(http.StatusNoContent)
}

func (t *httpTransport) handleChunk(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var chunk pb.Chunk
	if err := chunk.Unmarshal(body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if !t.chunkHandler(chunk) {
		http.Error(w, "chunk rejected", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (t *httpTransport) Close() error {
	if t.srv != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = t.srv.Shutdown(ctx)
	}
	t.client.CloseIdleConnections()
	return nil
}

func (t *httpTransport) scheme() string {
	if t.clientTLS != nil {
		return "https"
	}
	return "http"
}

func (t *httpTransport) GetConnection(_ context.Context, target string) (raftio.IConnection, error) {
	return &httpConn{t: t, target: target, url: t.scheme() + "://" + target + raftMsgPath}, nil
}

func (t *httpTransport) GetSnapshotConnection(_ context.Context, target string) (raftio.ISnapshotConnection, error) {
	return &httpSnapshotConn{t: t, target: target, url: t.scheme() + "://" + target + raftChunkPath}, nil
}

type httpConn struct {
	t      *httpTransport
	target string
	url    string
}

func (c *httpConn) Close() {}

func (c *httpConn) SendMessageBatch(batch pb.MessageBatch) error {
	if c.t.blocked(c.target) {
		return errors.New("wavespan raft transport: partitioned")
	}
	data, err := batch.Marshal()
	if err != nil {
		return err
	}
	return c.t.post(c.url, data)
}

type httpSnapshotConn struct {
	t      *httpTransport
	target string
	url    string
}

func (c *httpSnapshotConn) Close() {}

func (c *httpSnapshotConn) SendChunk(chunk pb.Chunk) error {
	if c.t.blocked(c.target) {
		return errors.New("wavespan raft transport: partitioned")
	}
	data, err := chunk.Marshal()
	if err != nil {
		return err
	}
	return c.t.post(c.url, data)
}

func (t *httpTransport) post(url string, body []byte) error {
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := t.client.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusNoContent {
		return errors.New("wavespan raft transport: peer returned " + resp.Status)
	}
	return nil
}
