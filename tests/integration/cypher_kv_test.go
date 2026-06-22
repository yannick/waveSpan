//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"connectrpc.com/connect"
	wavespanv1 "github.com/cwire/wavespan/proto/wavespan/v1"
)

// cypherScalar runs a single-row query and returns the string value of one column from the first row
// (empty string if no row or the cell is not a string). Used to read kv.get's scalar result.
func cypherScalar(t *testing.T, port, query, col string) (string, bool) {
	t.Helper()
	stream, err := cypherClient(port).Query(context.Background(), connect.NewRequest(&wavespanv1.CypherRequest{GraphId: "g", Query: query}))
	if err != nil {
		t.Fatalf("cypher %q: %v", query, err)
	}
	var (
		val string
		got bool
	)
	for stream.Receive() {
		if row := stream.Msg().GetRow(); row != nil && !got {
			val = row.GetColumns()[col].GetStringValue()
			got = true
		}
	}
	if err := stream.Err(); err != nil {
		t.Fatalf("cypher stream %q: %v", query, err)
	}
	return val, got
}

// TestCypherKVCoherence proves the Cypher kv.* built-ins and the gRPC KV API share ONE coherent KV,
// in both directions: a value written via the KV API Put is readable via `RETURN kv.get(ns,key)`, and
// a value written via `CALL kv.put(ns,key,value)` is readable via the gRPC KV Get. Both paths route
// through the same Reader/Coordinator, so the same serving node sees the other side's writes.
func TestCypherKVCoherence(t *testing.T) {
	compose(t, "up", "-d")
	t.Cleanup(func() { compose(t, "down", "-v") })
	waitFor(t, "node up", 60*time.Second, func() bool { return len(membership(t, "7901")) == 3 })

	const port = "7811" // node1 data port; serves both the KV and Cypher services
	ctx := context.Background()

	// direction 1: KV API write -> Cypher read.
	if _, err := kvClient(port).Put(ctx, connect.NewRequest(&wavespanv1.PutRequest{
		Namespace: "profile", Key: []byte("u1"), Value: []byte("hello"), RequireOriginPlusOne: true,
	})); err != nil {
		t.Fatalf("kv put u1: %v", err)
	}
	// Poll: the Reader is local-first with a closest-holder fetch on miss, so visibility is bounded
	// but not necessarily instant.
	waitFor(t, "kv.get sees KV-API write", 30*time.Second, func() bool {
		v, ok := cypherScalar(t, port, "RETURN kv.get('profile','u1') AS v", "v")
		return ok && v == "hello"
	})

	// direction 2: Cypher write -> KV API read.
	if _, ok := cypherScalar(t, port, "CALL kv.put('profile','u2','world') YIELD version RETURN version", "version"); !ok {
		t.Fatalf("CALL kv.put u2 yielded no row")
	}
	waitFor(t, "KV-API Get sees Cypher write", 30*time.Second, func() bool {
		resp, err := kvClient(port).Get(ctx, connect.NewRequest(&wavespanv1.GetRequest{Namespace: "profile", Key: []byte("u2")}))
		return err == nil && resp.Msg.GetFound() && string(resp.Msg.GetValue()) == "world"
	})
}
