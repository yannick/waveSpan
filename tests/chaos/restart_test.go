//go:build chaos

// Focused probe for the restart catch-up observation: an add-only workload under kills-only faults
// (follower-kill + leader-kill, no partition), then assert every replica converges to exactly the
// acknowledged set. A restarted voter that stays stuck behind the committed tail fails this.
//
// Run:  go test -tags chaos -run TestRestartCatchup ./tests/chaos -v
package chaos

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/yannick/wavespan/internal/collections"
)

func TestRestartCatchup(t *testing.T) {
	parts := newPartitions()
	nodes, start := buildCluster(t, 5, parts.gate)
	for _, n := range nodes {
		start(n)
	}
	defer func() {
		for _, n := range nodes {
			if n.mgr != nil {
				n.mgr.Stop()
			}
		}
	}()
	awaitLeader(t, nodes)
	lt := newLiveTracker()
	ns, coll := []byte("restart"), []byte("set")

	stop := make(chan struct{})
	nemDone := make(chan struct{})
	go runNemesis(nodes, parts, start, lt, false, stop, nemDone) // kills only

	var ackedMu sync.Mutex
	acked := map[string]bool{}
	var wg sync.WaitGroup
	for w := 0; w < 5; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for seq := 0; seq < 80; seq++ {
				select {
				case <-stop:
					return
				default:
				}
				m := []byte(fmt.Sprintf("w%d-%d", w, seq))
				if proposeVia(nodes, lt, func(c *collections.Collections) error {
					ctx, cancel := context.WithTimeout(context.Background(), 1200*time.Millisecond)
					defer cancel()
					_, err := c.SAdd(ctx, ns, coll, m)
					return err
				}) {
					ackedMu.Lock()
					acked[string(m)] = true
					ackedMu.Unlock()
				}
			}
		}(w)
	}
	wg.Wait()
	close(stop)
	<-nemDone

	for _, n := range nodes {
		if !lt.live(n.replicaID) {
			start(n)
			lt.set(n.replicaID, false)
		}
	}
	awaitLeader(t, nodes)

	ackedMu.Lock()
	want := make([]string, 0, len(acked))
	for m := range acked {
		want = append(want, m)
	}
	ackedMu.Unlock()
	sort.Strings(want)

	// Every replica must converge to exactly the acked set (no node stuck behind).
	for _, n := range nodes {
		conv := time.Now().Add(45 * time.Second)
		for {
			got := readMembers(t, n.cols, ns, coll, false)
			if d := setDiff(want, got); d == "" {
				break
			}
			if time.Now().After(conv) {
				lid, _ := n.mgr.LeaderID(shardID)
				t.Fatalf("replica r%d did not converge (stuck behind?): have %d want %d, leader=%d", n.replicaID, len(got), len(want), lid)
			}
			time.Sleep(400 * time.Millisecond)
		}
	}
	t.Logf("OK: all replicas caught up to %d acked adds after kills-only faults", len(want))
}
