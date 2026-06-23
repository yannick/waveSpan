//go:build chaos

// Sharper consensus invariants, inspired by Jepsen's set-full and counter checkers — the ones that
// catch bugs a "read only after heal" check cannot:
//
//   - cardinality invariant: the separately-maintained SCard counter must ALWAYS equal the actual
//     element count (CardCheck reads both from one snapshot), even under concurrent same-key SADD/SREM
//     and faults. This targets the in-batch cardDelta overlay — a classic off-by-one home.
//   - monotonic reads (set-full :lost): for an add-only set, once an element is observed present it
//     must never later be read absent — on a linearizable read (total order) or on any single replica
//     (a replica never un-applies its log). A disappearance is a lost write / divergence / bad
//     snapshot or split.
//
// Run:  go test -tags chaos -run 'TestConsensusCardinality|TestConsensusMonotonic' ./tests/chaos -v
package chaos

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/yannick/wavespan/internal/collections"
	"github.com/yannick/wavespan/internal/storage"
)

func buildCluster(t *testing.T, size int, gate func(src, dst string) bool) ([]*node, func(*node)) {
	opts := collections.Options{TransportFactory: &collections.TransportFactory{Gate: gate}}
	members := map[uint64]string{}
	nodes := make([]*node, size)
	for i := 0; i < size; i++ {
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
	return nodes, start
}

type liveTracker struct {
	mu   sync.Mutex
	down map[uint64]bool
}

func newLiveTracker() *liveTracker { return &liveTracker{down: map[uint64]bool{}} }
func (l *liveTracker) live(rid uint64) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return !l.down[rid]
}
func (l *liveTracker) set(rid uint64, down bool) {
	l.mu.Lock()
	l.down[rid] = down
	l.mu.Unlock()
}

// runNemesis cycles leader-kill, follower-kill+restart, and minority partition until stop is closed.
func runNemesis(nodes []*node, parts *partitions, start func(*node), lt *liveTracker, stop <-chan struct{}, done chan<- struct{}) {
	defer close(done)
	kill := func(rid uint64, pause time.Duration) {
		if rid == 0 {
			return
		}
		n := nodes[rid-1]
		lt.set(rid, true)
		n.mgr.Stop()
		time.Sleep(pause)
		start(n)
		lt.set(rid, false)
	}
	faults := []func(){
		func() { kill(pickFollower(nodes), 2*time.Second) },
		func() { kill(leaderOf(nodes), 3*time.Second) },
		func() {
			parts.isolate(nodes[0].addr, nodes[1].addr)
			time.Sleep(4 * time.Second)
			parts.heal()
		},
	}
	for i := 0; ; i++ {
		select {
		case <-stop:
			return
		default:
		}
		faults[i%len(faults)]()
		time.Sleep(1200 * time.Millisecond)
	}
}

// proposeVia routes one op through whichever live node is the leader, retrying briefly.
func proposeVia(nodes []*node, lt *liveTracker, op func(c *collections.Collections) error) bool {
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if !lt.live(n.replicaID) {
				continue
			}
			if op(n.cols) == nil {
				return true
			}
		}
		time.Sleep(60 * time.Millisecond)
	}
	return false
}

// TestConsensusCardinalityInvariant hammers a small shared keyspace with concurrent SADD/SREM under
// faults and asserts — continuously, on the leader, atomically — that the stored cardinality counter
// equals the actual element count. A drift means the cardDelta overlay miscounts.
func TestConsensusCardinalityInvariant(t *testing.T) {
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
	ns, coll := []byte("card"), []byte("set")
	const keys = 24

	stop := make(chan struct{})
	nemDone := make(chan struct{})
	go runNemesis(nodes, parts, start, lt, stop, nemDone)

	// 8 writers, concurrent SADD/SREM on overlapping keys (high contention on the counter).
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(seed)))
			for {
				select {
				case <-stop:
					return
				default:
				}
				m := []byte(fmt.Sprintf("k%d", rng.Intn(keys)))
				add := rng.Intn(2) == 0
				proposeVia(nodes, lt, func(c *collections.Collections) error {
					ctx, cancel := context.WithTimeout(context.Background(), 1200*time.Millisecond)
					defer cancel()
					var err error
					if add {
						_, err = c.SAdd(ctx, ns, coll, m)
					} else {
						_, err = c.SRem(ctx, ns, coll, m)
					}
					return err
				})
			}
		}(w)
	}

	// Continuous atomic invariant check on the leader.
	var viol struct {
		sync.Mutex
		msgs []string
	}
	checkStop := make(chan struct{})
	checkDone := make(chan struct{})
	go func() {
		defer close(checkDone)
		for {
			select {
			case <-checkStop:
				return
			default:
			}
			if idx := leaderIndex(nodes); idx >= 0 && lt.live(uint64(idx+1)) {
				ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
				cc, err := nodes[idx].cols.CardCheck(ctx, ns, coll, true)
				cancel()
				if err == nil && cc.Stored != cc.Counted {
					viol.Lock()
					viol.msgs = append(viol.msgs, fmt.Sprintf("leader r%d: SCard=%d but elements=%d", idx+1, cc.Stored, cc.Counted))
					viol.Unlock()
				}
			}
			time.Sleep(40 * time.Millisecond)
		}
	}()

	time.Sleep(30 * time.Second)
	close(stop)
	wg.Wait()
	<-nemDone
	close(checkStop)
	<-checkDone

	// Final: heal, quiesce, and check the counter on EVERY replica.
	parts.heal()
	for _, n := range nodes {
		if !lt.live(n.replicaID) {
			start(n)
			lt.set(n.replicaID, false)
		}
	}
	awaitLeader(t, nodes)
	time.Sleep(5 * time.Second)
	for _, n := range nodes {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		cc, err := n.cols.CardCheck(ctx, ns, coll, false)
		cancel()
		if err != nil {
			t.Fatalf("final CardCheck r%d: %v", n.replicaID, err)
		}
		if cc.Stored != cc.Counted {
			t.Fatalf("replica r%d counter drift after quiesce: SCard=%d elements=%d", n.replicaID, cc.Stored, cc.Counted)
		}
	}
	viol.Lock()
	defer viol.Unlock()
	if len(viol.msgs) > 0 {
		t.Fatalf("cardinality invariant violated %d time(s); first: %s", len(viol.msgs), viol.msgs[0])
	}
	t.Logf("OK: SCard == element count held continuously on the leader and on all 5 replicas under faults")
}

// TestConsensusZSetConsistency hammers a sorted set with concurrent ZADD (changing scores) + ZREM
// under faults. A score update must delete the old score-ordered index entry; if it orphans one, the
// member appears twice in ZRange while the counter (distinct members) stays put — so CardCheck's
// Stored != Counted. It also asserts ZRange stays sorted with no duplicate members.
func TestConsensusZSetConsistency(t *testing.T) {
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
	ns, coll := []byte("zcard"), []byte("zset")
	const members = 20

	stop := make(chan struct{})
	nemDone := make(chan struct{})
	go runNemesis(nodes, parts, start, lt, stop, nemDone)

	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(seed) + 100))
			for {
				select {
				case <-stop:
					return
				default:
				}
				m := []byte(fmt.Sprintf("m%d", rng.Intn(members)))
				zadd := rng.Intn(3) != 0 // mostly adds/updates, some removes
				score := float64(rng.Intn(1000))
				proposeVia(nodes, lt, func(c *collections.Collections) error {
					ctx, cancel := context.WithTimeout(context.Background(), 1200*time.Millisecond)
					defer cancel()
					var err error
					if zadd {
						_, err = c.ZAdd(ctx, ns, coll, collections.ScoredMember{Member: m, Score: score})
					} else {
						_, err = c.ZRem(ctx, ns, coll, m)
					}
					return err
				})
			}
		}(w)
	}

	var viol struct {
		sync.Mutex
		msgs []string
	}
	report := func(s string) { viol.Lock(); viol.msgs = append(viol.msgs, s); viol.Unlock() }

	// Continuous, atomic-per-read checks on the leader: counter == index entries, and ZRange is a
	// sorted set of DISTINCT members (one score-index entry per member).
	ckStop := make(chan struct{})
	ckDone := make(chan struct{})
	go func() {
		defer close(ckDone)
		for {
			select {
			case <-ckStop:
				return
			default:
			}
			if idx := leaderIndex(nodes); idx >= 0 && lt.live(uint64(idx+1)) {
				ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
				cc, ccErr := nodes[idx].cols.CardCheck(ctx, ns, coll, true)
				rows, rErr := nodes[idx].cols.ZRange(ctx, ns, coll, 0, true)
				cancel()
				if ccErr == nil && cc.Stored != cc.Counted {
					report(fmt.Sprintf("ZCard=%d but score-index entries=%d (orphaned/duplicate member)", cc.Stored, cc.Counted))
				}
				if rErr == nil {
					seen := map[string]bool{}
					var prev float64
					for i, r := range rows {
						if seen[string(r.Member)] {
							report(fmt.Sprintf("duplicate member %q in ZRange", r.Member))
						}
						seen[string(r.Member)] = true
						if i > 0 && r.Score < prev {
							report("ZRange not sorted by score")
						}
						prev = r.Score
					}
				}
			}
			time.Sleep(40 * time.Millisecond)
		}
	}()

	time.Sleep(30 * time.Second)
	close(stop)
	wg.Wait()
	<-nemDone
	close(ckStop)
	<-ckDone

	parts.heal()
	for _, n := range nodes {
		if !lt.live(n.replicaID) {
			start(n)
			lt.set(n.replicaID, false)
		}
	}
	awaitLeader(t, nodes)
	time.Sleep(5 * time.Second)
	for _, n := range nodes {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		cc, err := n.cols.CardCheck(ctx, ns, coll, false)
		cancel()
		if err != nil {
			t.Fatalf("final CardCheck r%d: %v", n.replicaID, err)
		}
		if cc.Stored != cc.Counted {
			t.Fatalf("replica r%d zset drift: ZCard=%d index entries=%d", n.replicaID, cc.Stored, cc.Counted)
		}
	}
	viol.Lock()
	defer viol.Unlock()
	if len(viol.msgs) > 0 {
		t.Fatalf("zset invariant violated %d time(s); first: %s", len(viol.msgs), viol.msgs[0])
	}
	t.Logf("OK: zset stayed consistent (counter == distinct members, sorted, no orphaned score keys) under faults")
}

// TestConsensusMonotonicReads runs an add-only workload under faults and asserts set-full monotonicity:
// once an element is observed present it is never later read absent — on a linearizable read and on
// each individual replica. A disappearance is a lost write / divergence / bad snapshot.
func TestConsensusMonotonicReads(t *testing.T) {
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
	ns, coll := []byte("mono"), []byte("set")

	stop := make(chan struct{})
	nemDone := make(chan struct{})
	go runNemesis(nodes, parts, start, lt, stop, nemDone)

	var ackedMu sync.Mutex
	acked := map[string]bool{}
	const writers, perWriter = 5, 80
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for seq := 0; seq < perWriter; seq++ {
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

	var viol struct {
		sync.Mutex
		msgs []string
	}
	report := func(s string) { viol.Lock(); viol.msgs = append(viol.msgs, s); viol.Unlock() }

	// Linearizable monotonic reader: the observed set must only grow.
	rdStop := make(chan struct{})
	var rdWg sync.WaitGroup
	rdWg.Add(1)
	go func() {
		defer rdWg.Done()
		seen := map[string]bool{}
		for {
			select {
			case <-rdStop:
				return
			default:
			}
			if idx := leaderIndex(nodes); idx >= 0 && lt.live(uint64(idx+1)) {
				ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
				rows, err := nodes[idx].cols.SMembers(ctx, ns, coll, 0, true)
				cancel()
				if err == nil {
					cur := map[string]bool{}
					for _, r := range rows {
						cur[string(r)] = true
					}
					for m := range seen {
						if !cur[m] {
							report(fmt.Sprintf("linearizable read lost previously-observed member %q", m))
						}
					}
					for m := range cur {
						seen[m] = true
					}
				}
			}
			time.Sleep(50 * time.Millisecond)
		}
	}()

	// Per-replica monotonic readers: a single replica must never un-apply.
	for _, n := range nodes {
		rdWg.Add(1)
		go func(n *node) {
			defer rdWg.Done()
			seen := map[string]bool{}
			for {
				select {
				case <-rdStop:
					return
				default:
				}
				if lt.live(n.replicaID) {
					ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
					rows, err := n.cols.SMembers(ctx, ns, coll, 0, false)
					cancel()
					if err == nil {
						cur := map[string]bool{}
						for _, r := range rows {
							cur[string(r)] = true
						}
						for m := range seen {
							if !cur[m] {
								report(fmt.Sprintf("replica r%d lost previously-observed member %q (un-applied)", n.replicaID, m))
							}
						}
						for m := range cur {
							seen[m] = true
						}
					}
				}
				time.Sleep(60 * time.Millisecond)
			}
		}(n)
	}

	wg.Wait() // workload done
	close(stop)
	<-nemDone

	// Heal + quiesce, then final durability + convergence.
	parts.heal()
	for _, n := range nodes {
		if !lt.live(n.replicaID) {
			start(n)
			lt.set(n.replicaID, false)
		}
	}
	awaitLeader(t, nodes)
	time.Sleep(4 * time.Second)
	close(rdStop)
	rdWg.Wait()

	ackedMu.Lock()
	want := make([]string, 0, len(acked))
	for m := range acked {
		want = append(want, m)
	}
	ackedMu.Unlock()
	sort.Strings(want)
	for _, n := range nodes {
		got := readMembers(t, n.cols, ns, coll, false)
		if d := setDiff(want, got); d != "" {
			t.Fatalf("replica r%d final state wrong: %s", n.replicaID, d)
		}
	}

	viol.Lock()
	defer viol.Unlock()
	if len(viol.msgs) > 0 {
		t.Fatalf("monotonic-read invariant violated %d time(s); first: %s", len(viol.msgs), viol.msgs[0])
	}
	t.Logf("OK: %d acked adds stayed monotonic (never observed-then-lost) on linearizable + all replica reads", len(want))
}
