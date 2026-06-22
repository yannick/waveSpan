//go:build integration

package integration

import (
	"context"
	"net/http"
	"testing"
	"time"

	"connectrpc.com/connect"
	wavespanv1 "github.com/cwire/wavespan/proto/wavespan/v1"
	"github.com/cwire/wavespan/proto/wavespan/v1/wavespanv1connect"
)

func obsClient(adminPort string) wavespanv1connect.ObservabilityServiceClient {
	return wavespanv1connect.NewObservabilityServiceClient(http.DefaultClient, "http://localhost:"+adminPort)
}

func adminCypherClient(adminPort string) wavespanv1connect.CypherClient {
	return wavespanv1connect.NewCypherClient(http.DefaultClient, "http://localhost:"+adminPort)
}

// TestUICypherAndGraphExplore verifies the two UI features end-to-end on the admin port: the Cypher
// console (Cypher service mounted on the admin port) and the visual node explorer (GraphExplore).
func TestUICypherAndGraphExplore(t *testing.T) {
	compose(t, "up", "-d")
	t.Cleanup(func() { compose(t, "down", "-v") })
	waitFor(t, "node up", 60*time.Second, func() bool { return len(membership(t, "7901")) == 3 })

	const dataPort, adminPort = "7811", "7901"
	ctx := context.Background()

	// seed a small graph via the data-port Cypher (CREATE)
	exec := func(q string) {
		stream, err := cypherClient(dataPort).Query(ctx, connect.NewRequest(&wavespanv1.CypherRequest{GraphId: "g", Query: q}))
		if err != nil {
			t.Fatalf("seed %q: %v", q, err)
		}
		for stream.Receive() { //nolint:revive
		}
		if err := stream.Err(); err != nil {
			t.Fatalf("seed stream %q: %v", q, err)
		}
	}
	exec("CREATE (:User {id:'alice', name:'Alice'})")
	exec("CREATE (:User {id:'bob', name:'Bob'})")
	exec("MATCH (a:User {id:'alice'}), (b:User {id:'bob'}) CREATE (a)-[:FOLLOWS]->(b)")

	// (1) Cypher console: the Cypher service is reachable on the ADMIN port (same origin as the UI)
	cs, err := adminCypherClient(adminPort).Query(ctx, connect.NewRequest(&wavespanv1.CypherRequest{GraphId: "g", Query: "MATCH (n:User) RETURN n.name"}))
	if err != nil {
		t.Fatalf("admin-port Cypher query: %v", err)
	}
	rows := 0
	for cs.Receive() {
		if cs.Msg().GetRow() != nil {
			rows++
		}
	}
	if err := cs.Err(); err != nil {
		t.Fatal(err)
	}
	if rows != 2 {
		t.Fatalf("Cypher console should return 2 User rows, got %d", rows)
	}

	// (2) Node explorer: GraphExplore returns the nodes + the FOLLOWS edge
	resp, err := obsClient(adminPort).GraphExplore(ctx, connect.NewRequest(&wavespanv1.GraphExploreRequest{GraphId: "g", IncludeValue: true}))
	if err != nil {
		t.Fatalf("GraphExplore: %v", err)
	}
	if len(resp.Msg.GetNodes()) != 2 {
		t.Fatalf("explorer should return 2 nodes, got %d", len(resp.Msg.GetNodes()))
	}
	if len(resp.Msg.GetEdges()) != 1 || resp.Msg.GetEdges()[0].GetType() != "FOLLOWS" {
		t.Fatalf("explorer should return the FOLLOWS edge: %+v", resp.Msg.GetEdges())
	}
}
