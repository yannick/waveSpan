//go:build harness

// Package client wraps the WaveSpan public client for the correctness harness: every op is applied
// through the real KvService and recorded into the unified history with its ack state, served-by
// replica, and observed/writer version (design/25). A read targets a SPECIFIC member so workloads
// can compare per-replica views (Jepsen reads each node independently).
package client

import (
	"context"
	"net/http"
	"sync"
	"time"

	"connectrpc.com/connect"
	"github.com/cwire/wavespan/internal/version"
	wavespanv1 "github.com/cwire/wavespan/proto/wavespan/v1"
	"github.com/cwire/wavespan/proto/wavespan/v1/wavespanv1connect"
	"github.com/cwire/wavespan/tests/harness/runner"
)

// Client drives the KvService on each member and appends ops to a shared history.
type Client struct {
	mu      sync.Mutex
	kv      map[string]wavespanv1connect.KvServiceClient
	repl    map[string]wavespanv1connect.ReplicationServiceClient
	history *runner.History
	now     func() int64
}

// New wires a client over member->dataAddr and a shared history.
func New(dataAddrs map[string]string, h *runner.History) *Client {
	kv := map[string]wavespanv1connect.KvServiceClient{}
	repl := map[string]wavespanv1connect.ReplicationServiceClient{}
	for member, addr := range dataAddrs {
		kv[member] = wavespanv1connect.NewKvServiceClient(http.DefaultClient, "http://"+addr)
		repl[member] = wavespanv1connect.NewReplicationServiceClient(http.DefaultClient, "http://"+addr)
	}
	return &Client{kv: kv, repl: repl, history: h, now: func() int64 { return time.Now().UnixMilli() }}
}

func (c *Client) record(op runner.Op) {
	c.mu.Lock()
	c.history.Append(op)
	c.mu.Unlock()
}

func fromProto(v *wavespanv1.Version) *version.Version {
	if v == nil {
		return nil
	}
	vv := version.FromProto(v)
	return &vv
}

// Put writes through member's coordinator, recording the op (ack + writer version).
func (c *Client) Put(ctx context.Context, member, ns, key, val, reqID, session string) bool {
	start := c.now()
	req := &wavespanv1.PutRequest{Namespace: ns, Key: []byte(key), Value: []byte(val), RequireOriginPlusOne: true}
	if reqID != "" {
		req.IdempotencyKey = &reqID
	}
	resp, err := c.kv[member].Put(ctx, connect.NewRequest(req))
	op := runner.Op{Kind: runner.OpPut, Key: key, Value: val, RequestID: reqID, Session: session,
		StartMs: start, EndMs: c.now(), ServedBy: member, Policy: c.policy(ns)}
	if err == nil {
		op.Ack = true
		op.WriterVersion = fromProto(resp.Msg.GetVersion())
	}
	c.record(op)
	return op.Ack
}

// Get reads from a SPECIFIC member (served_by), recording the observed version + value.
func (c *Client) Get(ctx context.Context, member, ns, key, session string) (string, bool) {
	start := c.now()
	resp, err := c.kv[member].Get(ctx, connect.NewRequest(&wavespanv1.GetRequest{Namespace: ns, Key: []byte(key)}))
	op := runner.Op{Kind: runner.OpGet, Key: key, Session: session, StartMs: start, EndMs: c.now(), ServedBy: member, Policy: c.policy(ns)}
	val, found := "", false
	if err == nil {
		op.Ack = true
		found = resp.Msg.GetFound()
		val = string(resp.Msg.GetValue())
		op.Value = val
		op.Tombstone = !found
		op.ObservedVersion = fromProto(resp.Msg.GetMeta().GetObservedVersion())
		op.Cluster = resp.Msg.GetMeta().GetServedByClusterId()
	}
	c.record(op)
	return val, found && op.Ack
}

// Peek reads from a member WITHOUT recording into the history (used to poll for convergence before
// the final recorded read; the polling reads themselves are not part of the verified history).
func (c *Client) Peek(ctx context.Context, member, ns, key string) (string, bool) {
	resp, err := c.kv[member].Get(ctx, connect.NewRequest(&wavespanv1.GetRequest{Namespace: ns, Key: []byte(key)}))
	if err != nil {
		return "", false
	}
	return string(resp.Msg.GetValue()), resp.Msg.GetFound()
}

// LocalCount returns how many records a member holds LOCALLY for a namespace (ScanLocal never
// fetches from peers), so it proves what a node physically holds — used to verify backfill.
func (c *Client) LocalCount(ctx context.Context, member, ns string) int {
	resp, err := c.repl[member].ScanLocal(ctx, connect.NewRequest(&wavespanv1.ScanLocalRequest{Namespace: ns}))
	if err != nil {
		return -1
	}
	return len(resp.Msg.GetRows())
}

// Delete tombstones a key.
func (c *Client) Delete(ctx context.Context, member, ns, key, reqID string) bool {
	start := c.now()
	dreq := &wavespanv1.DeleteRequest{Namespace: ns, Key: []byte(key), RequireOriginPlusOne: true}
	if reqID != "" {
		dreq.IdempotencyKey = &reqID
	}
	resp, err := c.kv[member].Delete(ctx, connect.NewRequest(dreq))
	op := runner.Op{Kind: runner.OpDelete, Key: key, RequestID: reqID, StartMs: start, EndMs: c.now(), ServedBy: member, Tombstone: true, Policy: c.policy(ns)}
	if err == nil {
		op.Ack = true
		op.WriterVersion = fromProto(resp.Msg.GetVersion())
	}
	c.record(op)
	return op.Ack
}

// policy returns the conflict policy for a namespace (the harness uses "siblings" for keep-siblings
// workloads and the default LWW otherwise; mirrors the compose WAVESPAN_KEEP_SIBLINGS_NAMESPACES).
func (c *Client) policy(ns string) string {
	if ns == "siblings" {
		return "keep-siblings"
	}
	return "hlc-last-write-wins"
}
