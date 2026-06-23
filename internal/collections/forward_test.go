package collections

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/yannick/wavespan/internal/storage"
)

// directForwarder is the in-process Forwarder seam for tests: it applies the write on a peer's engine
// directly (the peer that is the leader accepts). In the node this is the ProposeForward RPC.
type directForwarder struct{ peers []*Collections }

func (f directForwarder) Forward(ctx context.Context, ns, coll, cmd []byte) (uint64, []byte, error) {
	var lastErr error
	for _, p := range f.peers {
		n, data, err := p.ProposeRaw(ctx, ns, coll, cmd)
		if err == nil {
			return n, data, nil
		}
		if errors.Is(err, ErrWrongType) || errors.Is(err, ErrNotNumber) {
			return 0, nil, err
		}
		lastErr = err
	}
	if lastErr == nil {
		lastErr = errors.New("no peer accepted the write")
	}
	return 0, nil, lastErr
}

// TestWriteForwardsToLeader confirms node-side leader routing: a write issued to a FOLLOWER node is
// forwarded to the leader and commits, so a client can call any node (design/30 §13.13).
func TestWriteForwardsToLeader(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node raft test")
	}
	const n = 3
	members := map[uint64]string{}
	mgrs := map[uint64]*Manager{}
	cols := map[uint64]*Collections{}
	for i := uint64(1); i <= n; i++ {
		addr := freeAddr(t)
		members[i] = addr
		store := storage.NewMemStore()
		t.Cleanup(func() { _ = store.Close() })
		mgrs[i] = newMgr(t, t.TempDir(), addr, store)
	}
	for i := uint64(1); i <= n; i++ {
		if err := mgrs[i].StartShard(1, i, members, false); err != nil {
			t.Fatalf("StartShard r%d: %v", i, err)
		}
		cols[i] = New(mgrs[i], SingleShardDirectory(1))
	}
	defer func() {
		for _, m := range mgrs {
			m.Stop()
		}
	}()
	// Each node forwards to the other nodes' engines.
	for i := uint64(1); i <= n; i++ {
		var peers []*Collections
		for j := uint64(1); j <= n; j++ {
			if j != i {
				peers = append(peers, cols[j])
			}
		}
		cols[i].WithForwarder(directForwarder{peers: peers})
	}

	leader := leaderID(t, mgrs[1], 1)
	var follower uint64
	for i := uint64(1); i <= n; i++ {
		if i != leader {
			follower = i
			break
		}
	}

	ns, coll := []byte("app"), []byte("s")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Write via a FOLLOWER — must forward to the leader and commit.
	got, err := cols[follower].SAdd(ctx, ns, coll, []byte("x"), []byte("y"))
	if err != nil {
		t.Fatalf("forwarded write via follower r%d failed: %v", follower, err)
	}
	if got != 2 {
		t.Fatalf("forwarded SAdd = %d want 2", got)
	}
	for i := uint64(1); i <= n; i++ {
		if !awaitMember(t, cols[i], ns, coll, []byte("x")) {
			t.Fatalf("forwarded write not replicated to r%d", i)
		}
	}
	// A direct write to the leader still works (local fast path, no forward).
	if _, err := cols[leader].SAdd(ctx, ns, coll, []byte("z")); err != nil {
		t.Fatalf("leader-local write failed: %v", err)
	}
}
