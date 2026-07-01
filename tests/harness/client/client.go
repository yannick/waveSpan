//go:build harness

// Package client wraps the WaveSpan public client for the correctness harness: every op is applied
// through the real KvService and recorded into the unified history with its ack state, served-by
// replica, and observed/writer version (design/25). A read targets a SPECIFIC member so workloads
// can compare per-replica views (Jepsen reads each node independently).
package client

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/yannick/wavespan/internal/rpcopts"
	"github.com/yannick/wavespan/internal/version"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
	"github.com/yannick/wavespan/tests/harness/runner"
)

// Client drives the KvService on each member and appends ops to a shared history.
type Client struct {
	mu      sync.Mutex
	kv      map[string]wavespanv1.KvServiceClient
	repl    map[string]wavespanv1.ReplicationServiceClient
	history *runner.History
	now     func() int64
}

// New wires a client over member->dataAddr and a shared history. The data port is a real grpc.Server
// (cmd/wavespan-node: RegisterKvServiceServer on grpcDataSrv) — the production data-plane transport — so
// the harness talks to it with a REAL grpc-go client over the shared insecure pooled connection
// (rpcopts.GRPCConn), exactly as internal bench and the inter-node forwarders do. (A Connect-protocol
// client cannot speak to a grpc-go server.)
func New(dataAddrs map[string]string, h *runner.History) *Client {
	kv := map[string]wavespanv1.KvServiceClient{}
	repl := map[string]wavespanv1.ReplicationServiceClient{}
	for member, addr := range dataAddrs {
		conn, err := rpcopts.GRPCConn(addr)
		if err != nil { // a bad advertise addr is a harness setup bug — fail loudly
			panic(fmt.Sprintf("harness client: dial %s: %v", addr, err))
		}
		kv[member] = wavespanv1.NewKvServiceClient(conn)
		repl[member] = wavespanv1.NewReplicationServiceClient(conn)
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
	resp, err := c.kv[member].Put(ctx, req)
	op := runner.Op{Kind: runner.OpPut, Key: key, Value: val, RequestID: reqID, Session: session,
		StartMs: start, EndMs: c.now(), ServedBy: member, Policy: c.policy(ns)}
	if err == nil {
		op.Ack = true
		op.WriterVersion = fromProto(resp.GetVersion())
	}
	c.record(op)
	return op.Ack
}

// Get reads from a SPECIFIC member (served_by), recording the observed version + value.
func (c *Client) Get(ctx context.Context, member, ns, key, session string) (string, bool) {
	start := c.now()
	resp, err := c.kv[member].Get(ctx, &wavespanv1.GetRequest{Namespace: ns, Key: []byte(key)})
	op := runner.Op{Kind: runner.OpGet, Key: key, Session: session, StartMs: start, EndMs: c.now(), ServedBy: member, Policy: c.policy(ns)}
	val, found := "", false
	if err == nil {
		op.Ack = true
		found = resp.GetFound()
		val = string(resp.GetValue())
		op.Value = val
		op.Tombstone = !found
		op.ObservedVersion = fromProto(resp.GetMeta().GetObservedVersion())
		op.Cluster = resp.GetMeta().GetServedByClusterId()
	}
	c.record(op)
	return val, found && op.Ack
}

// Peek reads from a member WITHOUT recording into the history (used to poll for convergence before
// the final recorded read; the polling reads themselves are not part of the verified history).
func (c *Client) Peek(ctx context.Context, member, ns, key string) (string, bool) {
	resp, err := c.kv[member].Get(ctx, &wavespanv1.GetRequest{Namespace: ns, Key: []byte(key)})
	if err != nil {
		return "", false
	}
	return string(resp.GetValue()), resp.GetFound()
}

// LocalCount returns how many records a member holds LOCALLY for a namespace (ScanLocal never
// fetches from peers), so it proves what a node physically holds — used to verify backfill.
func (c *Client) LocalCount(ctx context.Context, member, ns string) int {
	resp, err := c.repl[member].ScanLocal(ctx, &wavespanv1.ScanLocalRequest{Namespace: ns})
	if err != nil {
		return -1
	}
	return len(resp.GetRows())
}

// Delete tombstones a key.
func (c *Client) Delete(ctx context.Context, member, ns, key, reqID string) bool {
	start := c.now()
	dreq := &wavespanv1.DeleteRequest{Namespace: ns, Key: []byte(key), RequireOriginPlusOne: true}
	if reqID != "" {
		dreq.IdempotencyKey = &reqID
	}
	resp, err := c.kv[member].Delete(ctx, dreq)
	op := runner.Op{Kind: runner.OpDelete, Key: key, RequestID: reqID, StartMs: start, EndMs: c.now(), ServedBy: member, Tombstone: true, Policy: c.policy(ns)}
	if err == nil {
		op.Ack = true
		op.WriterVersion = fromProto(resp.GetVersion())
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
