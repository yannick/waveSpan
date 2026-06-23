//go:build chaos

// Consensus-tier chaos harness (design/25, extended for design/30). The replicated-collections tier
// is CP — unlike the eventually-consistent KV layer — so this harness asserts the STRONGER invariants
// a linearizable Raft tier owes, in-process (no docker), with the classic consensus nemeses:
//
//	generator : N clients SAdd unique members, retrying across nodes (leader routing) until acked
//	nemeses   : kill a follower + restart, kill the LEADER (forced failover), partition the network
//	checker   : after heal + quiesce, every acknowledged add is durable, all replicas agree exactly,
//	            and the cardinality counter is exact (no lost writes, no phantom writes, no divergence)
//
// Run locally:  go test -tags chaos -run TestConsensus ./tests/chaos -v
package chaos

import (
	"context"
	"fmt"
	"net"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/yannick/wavespan/internal/collections"
	"github.com/yannick/wavespan/internal/storage"
)

const (
	clusterSize = 5
	shardID     = uint64(1)
)

// partitions tracks an isolated side of a network split; gate(src,dst) drops cross-side messages.
type partitions struct {
	mu       sync.RWMutex
	isolated map[string]bool
}

func newPartitions() *partitions { return &partitions{isolated: map[string]bool{}} }

func (p *partitions) gate(src, dst string) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.isolated[src] == p.isolated[dst] // same side ⇒ allowed; cross-side ⇒ dropped
}

func (p *partitions) isolate(addrs ...string) {
	p.mu.Lock()
	for _, a := range addrs {
		p.isolated[a] = true
	}
	p.mu.Unlock()
}

func (p *partitions) heal() {
	p.mu.Lock()
	p.isolated = map[string]bool{}
	p.mu.Unlock()
}

// node is one cluster member; store + dir persist across restarts so a restarted node recovers.
type node struct {
	replicaID uint64
	addr      string
	dir       string
	store     storage.LocalStore
	mgr       *collections.Manager
	cols      *collections.Collections
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("freeAddr: %v", err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().String()
}

func TestConsensusUnderFaults(t *testing.T) {
	parts := newPartitions()
	opts := collections.Options{TransportFactory: &collections.TransportFactory{Gate: parts.gate}}
	members := map[uint64]string{}
	nodes := make([]*node, clusterSize)
	for i := 0; i < clusterSize; i++ {
		rid := uint64(i + 1)
		addr := freeAddr(t)
		members[rid] = addr
		st := storage.NewMemStore()
		t.Cleanup(func() { _ = st.Close() })
		nodes[i] = &node{replicaID: rid, addr: addr, dir: t.TempDir(), store: st}
	}
	start := func(n *node) {
		mgr, err := collections.NewManagerWithOptions(n.dir, n.addr, n.store, opts)
		if err != nil {
			t.Fatalf("NewManager r%d: %v", n.replicaID, err)
		}
		if err := mgr.StartShard(shardID, n.replicaID, members, false); err != nil {
			t.Fatalf("StartShard r%d: %v", n.replicaID, err)
		}
		n.mgr = mgr
		n.cols = collections.New(mgr, collections.SingleShardDirectory(shardID))
	}
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

	ns, coll := []byte("jepsen"), []byte("set")
	awaitLeader(t, nodes)

	var (
		mu       sync.Mutex
		acked    = map[string]bool{} // members confirmed committed
		liveMu   sync.Mutex
		liveDown = map[uint64]bool{} // replicaIDs currently stopped (skip in routing)
	)
	isLive := func(rid uint64) bool { liveMu.Lock(); defer liveMu.Unlock(); return !liveDown[rid] }
	setDown := func(rid uint64, down bool) { liveMu.Lock(); liveDown[rid] = down; liveMu.Unlock() }

	// propose a member through whichever live node is the leader; retry until committed.
	propose := func(member []byte) bool {
		deadline := time.Now().Add(20 * time.Second)
		for time.Now().Before(deadline) {
			for _, n := range nodes {
				if !isLive(n.replicaID) {
					continue
				}
				ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
				_, err := n.cols.SAdd(ctx, ns, coll, member)
				cancel()
				if err == nil {
					return true
				}
			}
			time.Sleep(80 * time.Millisecond)
		}
		return false
	}

	// --- generator: concurrent clients adding unique members ---
	const (
		writers       = 6
		perWriter     = 60
		workloadGrace = 90 * time.Second
	)
	stopWorkload := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for seq := 0; seq < perWriter; seq++ {
				select {
				case <-stopWorkload:
					return
				default:
				}
				m := []byte(fmt.Sprintf("w%d-%d", w, seq))
				if propose(m) {
					mu.Lock()
					acked[string(m)] = true
					mu.Unlock()
				}
			}
		}(w)
	}

	// --- nemesis: cycle faults while the workload runs ---
	nemesisDone := make(chan struct{})
	go func() {
		defer close(nemesisDone)
		faults := []func(){
			// kill a random follower, then restart it (catch-up under load)
			func() {
				rid := pickFollower(nodes)
				if rid == 0 {
					return
				}
				n := nodes[rid-1]
				setDown(rid, true)
				n.mgr.Stop()
				time.Sleep(2 * time.Second)
				start(n)
				setDown(rid, false)
			},
			// kill the leader, forcing a failover
			func() {
				rid := leaderOf(nodes)
				if rid == 0 {
					return
				}
				n := nodes[rid-1]
				setDown(rid, true)
				n.mgr.Stop()
				time.Sleep(3 * time.Second)
				start(n)
				setDown(rid, false)
			},
			// partition a minority (2 of 5) away from the majority, then heal
			func() {
				parts.isolate(nodes[0].addr, nodes[1].addr)
				time.Sleep(4 * time.Second)
				parts.heal()
			},
		}
		for i := 0; ; i++ {
			select {
			case <-stopWorkload:
				return
			default:
			}
			faults[i%len(faults)]()
			time.Sleep(1500 * time.Millisecond)
		}
	}()

	// run the workload to completion (or grace), then stop the nemesis and heal.
	doneCh := make(chan struct{})
	go func() { wg.Wait(); close(doneCh) }()
	select {
	case <-doneCh:
	case <-time.After(workloadGrace):
	}
	close(stopWorkload)
	wg.Wait()
	<-nemesisDone

	// --- heal: every node up, no partitions, all caught up ---
	parts.heal()
	for _, n := range nodes {
		if !isLive(n.replicaID) {
			start(n)
			setDown(n.replicaID, false)
		}
	}
	awaitLeader(t, nodes)
	time.Sleep(5 * time.Second) // quiesce so every replica applies the committed tail

	mu.Lock()
	want := make([]string, 0, len(acked))
	for m := range acked {
		want = append(want, m)
	}
	mu.Unlock()
	sort.Strings(want)
	t.Logf("acknowledged adds: %d", len(want))
	if len(want) == 0 {
		t.Fatal("no writes were acknowledged — workload/nemesis misconfigured")
	}

	// --- checker 1: a linearizable read sees EXACTLY the acked set (no lost / phantom writes) ---
	leader := nodes[leaderIndex(nodes)]
	lin := readMembers(t, leader.cols, ns, coll, true)
	if diff := setDiff(want, lin); diff != "" {
		t.Fatalf("linearizable read != acknowledged set: %s", diff)
	}
	if n := card(t, leader.cols, ns, coll, true); n != uint64(len(want)) {
		t.Fatalf("linearizable SCard = %d, want %d (cardinality counter drifted)", n, len(want))
	}

	// --- checker 2: every replica converged to exactly the acked set (bounded-stale, post-quiesce) ---
	for _, n := range nodes {
		got := readMembers(t, n.cols, ns, coll, false)
		if diff := setDiff(want, got); diff != "" {
			t.Fatalf("replica r%d did not converge: %s", n.replicaID, diff)
		}
		if c := card(t, n.cols, ns, coll, false); c != uint64(len(want)) {
			t.Fatalf("replica r%d SCard = %d, want %d", n.replicaID, c, len(want))
		}
	}
	t.Logf("OK: %d acked adds durable + exactly converged across %d replicas under leader-kill, follower-restart, and partition", len(want), clusterSize)
}

// --- helpers ---

func awaitLeader(t *testing.T, nodes []*node) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if leaderOf(nodes) != 0 {
			return
		}
		time.Sleep(150 * time.Millisecond)
	}
	t.Fatal("no leader elected within deadline")
}

func leaderOf(nodes []*node) uint64 {
	for _, n := range nodes {
		if n.mgr == nil {
			continue
		}
		if id, ok := n.mgr.LeaderID(shardID); ok {
			return id
		}
	}
	return 0
}

func leaderIndex(nodes []*node) int {
	rid := leaderOf(nodes)
	if rid == 0 {
		return 0
	}
	return int(rid - 1)
}

func pickFollower(nodes []*node) uint64 {
	lead := leaderOf(nodes)
	for _, n := range nodes {
		if n.replicaID != lead && n.mgr != nil {
			return n.replicaID
		}
	}
	return 0
}

func readMembers(t *testing.T, c *collections.Collections, ns, coll []byte, lin bool) []string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	rows, err := c.SMembers(ctx, ns, coll, 0, lin)
	if err != nil {
		t.Fatalf("SMembers(lin=%v): %v", lin, err)
	}
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = string(r)
	}
	sort.Strings(out)
	return out
}

func card(t *testing.T, c *collections.Collections, ns, coll []byte, lin bool) uint64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	n, err := c.SCard(ctx, ns, coll, lin)
	if err != nil {
		t.Fatalf("SCard(lin=%v): %v", lin, err)
	}
	return n
}

// setDiff returns "" when want == got, else a description of the first missing / extra member.
func setDiff(want, got []string) string {
	w := map[string]bool{}
	for _, x := range want {
		w[x] = true
	}
	g := map[string]bool{}
	for _, x := range got {
		g[x] = true
	}
	for _, x := range want {
		if !g[x] {
			return fmt.Sprintf("missing acked member %q (lost write); want %d got %d", x, len(want), len(got))
		}
	}
	for _, x := range got {
		if !w[x] {
			return fmt.Sprintf("phantom member %q (never acked); want %d got %d", x, len(want), len(got))
		}
	}
	return ""
}
