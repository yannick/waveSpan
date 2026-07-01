package collections

import (
	"context"
	"testing"
	"time"

	"github.com/yannick/wavespan/internal/storage"
)

// Quiescence timing: the shard enters quiesce after ElectionRTT*10 idle ticks (dragonboat quiesce.go),
// i.e. 10*10 = 100 ticks * 50ms RTT ≈ 5s. Tests idle ~9s to be safely quiesced, then observe.
const quiesceIdle = 9 * time.Second

// newQuiesceMgr builds a Manager with quiescence explicitly on/off (default RTT/election tunables).
func newQuiesceMgr(t *testing.T, dir, addr string, store storage.LocalStore, quiesce bool) *Manager {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		m, err := NewManagerWithOptions(dir, addr, store, Options{Tunables: Tunables{Quiesce: &quiesce}})
		if err == nil {
			return m
		}
		if time.Now().After(deadline) {
			t.Fatalf("NewManagerWithOptions: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// startQuiesceCluster brings up a real 3-voter shard with quiescence on/off and returns the managers +
// Collections handles. Cleanup stops the managers.
func startQuiesceCluster(t *testing.T, quiesce bool) (map[uint64]*Manager, map[uint64]*Collections) {
	t.Helper()
	const n = 3
	members := map[uint64]string{}
	mgrs := map[uint64]*Manager{}
	cols := map[uint64]*Collections{}
	for i := uint64(1); i <= n; i++ {
		addr := freeAddr(t)
		members[i] = addr
		store := storage.NewMemStore()
		t.Cleanup(func() { _ = store.Close() })
		mgrs[i] = newQuiesceMgr(t, t.TempDir(), addr, store, quiesce)
	}
	for i := uint64(1); i <= n; i++ {
		if err := mgrs[i].StartShard(1, i, members, false); err != nil {
			t.Fatalf("StartShard %d: %v", i, err)
		}
		cols[i] = New(mgrs[i], SingleShardDirectory(1))
	}
	t.Cleanup(func() {
		for _, m := range mgrs {
			m.Stop()
		}
	})
	return mgrs, cols
}

// clusterLinearMembers performs a LINEARIZABLE SMembers (ReadIndex through the leader), retrying across
// nodes until the leader serves it — so it also exercises waking a quiesced shard on a read.
func clusterLinearMembers(t *testing.T, nodes map[uint64]*Collections, ns, coll []byte) [][]byte {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		for _, c := range nodes {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			got, err := c.SMembers(ctx, ns, coll, 0, true) // linearizable
			cancel()
			if err == nil {
				return got
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("no node served the linearizable read (no stable leader / wake failed?)")
		}
		time.Sleep(150 * time.Millisecond)
	}
}

func hasMember(members [][]byte, want string) bool {
	for _, m := range members {
		if string(m) == want {
			return true
		}
	}
	return false
}

// TestQuiesceNoLeadershipChurnWhenIdle is the top-risk gate: with quiescence ON, an idle shard must NOT
// churn leadership. While quiesced dragonboat runs QuiescedTick (neither leaderTick nor nonLeaderTick), so
// the CheckQuorum step-down timer and the election timers are frozen — the leader stays leader across many
// idle election windows. Observable proof of no re-election storm (quiesce state isn't a public API).
func TestQuiesceNoLeadershipChurnWhenIdle(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node raft test")
	}
	mgrs, cols := startQuiesceCluster(t, true)
	ns, coll := []byte("flags"), []byte("enabled")
	if got := clusterSAdd(t, cols, ns, coll, []byte("a")); got != 1 {
		t.Fatalf("warm-up SAdd = %d want 1", got)
	}
	leader := leaderID(t, mgrs[1], 1)

	time.Sleep(quiesceIdle) // idle well past the quiesce threshold + several election windows

	// Every node must still report the SAME leader — no re-election happened while quiesced.
	for i := uint64(1); i <= 3; i++ {
		id, _, ok, err := mgrs[i].nh.GetLeaderID(1)
		if err != nil || !ok || id != leader {
			t.Fatalf("node %d: leader=%d ok=%v err=%v — want stable leader %d (quiescence caused churn?)", i, id, ok, err, leader)
		}
	}
}

// TestQuiesceWakeOnProposal: an idle→quiesced shard commits a proposal (the write wakes it).
func TestQuiesceWakeOnProposal(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node raft test")
	}
	_, cols := startQuiesceCluster(t, true)
	ns, coll := []byte("flags"), []byte("enabled")
	if got := clusterSAdd(t, cols, ns, coll, []byte("a")); got != 1 {
		t.Fatalf("warm-up SAdd = %d want 1", got)
	}
	time.Sleep(quiesceIdle) // quiesce

	if got := clusterSAdd(t, cols, ns, coll, []byte("b")); got != 1 {
		t.Fatalf("proposal after quiesce = %d want 1 (wake failed?)", got)
	}
	for id, c := range cols {
		if !awaitMember(t, c, ns, coll, []byte("b")) {
			t.Fatalf("node %d never observed the post-quiesce write", id)
		}
	}
}

// TestQuiesceWakeOnLinearizableRead: an idle→quiesced shard serves a linearizable read (ReadIndex through
// the leader wakes it).
func TestQuiesceWakeOnLinearizableRead(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node raft test")
	}
	_, cols := startQuiesceCluster(t, true)
	ns, coll := []byte("flags"), []byte("enabled")
	if got := clusterSAdd(t, cols, ns, coll, []byte("a")); got != 1 {
		t.Fatalf("warm-up SAdd = %d want 1", got)
	}
	time.Sleep(quiesceIdle) // quiesce

	got := clusterLinearMembers(t, cols, ns, coll)
	if !hasMember(got, "a") {
		t.Fatalf("linearizable read after quiesce = %v, want to contain \"a\"", got)
	}
}

// TestQuiesceNormalUnderLoad: with quiescence ON but continuous activity (never idle past the threshold),
// the shard behaves normally — no mid-load quiesce disruption.
func TestQuiesceNormalUnderLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node raft test")
	}
	_, cols := startQuiesceCluster(t, true)
	ns, coll := []byte("flags"), []byte("enabled")
	for i := 0; i < 10; i++ {
		if got := clusterSAdd(t, cols, ns, coll, []byte{byte('a' + i)}); got != 1 {
			t.Fatalf("proposal %d under continuous load = %d want 1", i, got)
		}
		time.Sleep(200 * time.Millisecond) // well under the ~5s quiesce threshold → never quiesces
	}
}

// TestQuiesceDisabledRegression: with quiescence OFF, prior behavior holds — an idle shard still serves a
// later proposal (regression guard for the toggle).
func TestQuiesceDisabledRegression(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node raft test")
	}
	mgrs, cols := startQuiesceCluster(t, false)
	ns, coll := []byte("flags"), []byte("enabled")
	if got := clusterSAdd(t, cols, ns, coll, []byte("a")); got != 1 {
		t.Fatalf("warm-up SAdd = %d want 1", got)
	}
	leader := leaderID(t, mgrs[1], 1)
	time.Sleep(6 * time.Second) // idle — with quiesce off, heartbeats continue; leadership still stable

	id, _, ok, err := mgrs[1].nh.GetLeaderID(1)
	if err != nil || !ok || id != leader {
		t.Fatalf("quiesce-off: leader=%d ok=%v err=%v want stable %d", id, ok, err, leader)
	}
	if got := clusterSAdd(t, cols, ns, coll, []byte("b")); got != 1 {
		t.Fatalf("quiesce-off proposal after idle = %d want 1", got)
	}
}
