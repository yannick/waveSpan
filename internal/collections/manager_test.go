package collections

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/cwire/wavespan/internal/storage"
)

// TestM0SingleShardProposeReadRestart is the M-0 spike acceptance test (design/30 §18 / Appendix
// B.12): one dragonboat shard, propose → commit → apply to wavesdb, bounded-stale + linearizable
// reads, and persistence across a NodeHost restart (the on-disk SM resumes from the applied index).
func TestM0SingleShardProposeReadRestart(t *testing.T) {
	mem := storage.NewMemStore()
	t.Cleanup(func() { _ = mem.Close() })
	dir := t.TempDir()
	addr := freeAddr(t)
	members := map[uint64]string{1: addr}

	m, err := NewManager(dir, addr, mem)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	if err := m.StartShard(1, 1, members, false); err != nil {
		t.Fatalf("StartShard: %v", err)
	}

	proposeWithRetry(t, m, 1, command{Op: opPut, Key: []byte("k1"), Val: []byte("v1")})

	// bounded-stale (local) read
	if got := getWithRetry(t, m, 1, []byte("k1"), false); string(got) != "v1" {
		t.Fatalf("stale read = %q, want v1", got)
	}
	// linearizable (leader read-index) read
	if got := getWithRetry(t, m, 1, []byte("k1"), true); string(got) != "v1" {
		t.Fatalf("linearizable read = %q, want v1", got)
	}
	m.Close()

	// restart: same store + dir + addr; the SM's Open resumes the applied index, data persists.
	m2 := newManagerWithRetry(t, dir, addr, mem)
	if err := m2.StartShard(1, 1, members, false); err != nil {
		t.Fatalf("restart StartShard: %v", err)
	}
	defer m2.Close()

	if got := getWithRetry(t, m2, 1, []byte("k1"), true); string(got) != "v1" {
		t.Fatalf("post-restart read = %q, want v1", got)
	}
	// the applied index survived the restart (Open returned it, not 0)
	sm0 := newShardSM(mem, 1)
	if idx, _ := sm0.Open(nil); idx == 0 {
		t.Fatal("applied index did not persist across restart")
	}
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("freeAddr: %v", err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

func newManagerWithRetry(t *testing.T, dir, addr string, store storage.LocalStore) *Manager {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		m, err := NewManager(dir, addr, store)
		if err == nil {
			return m
		}
		if time.Now().After(deadline) {
			t.Fatalf("NewManager (restart) never succeeded: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func proposeWithRetry(t *testing.T, m *Manager, shardID uint64, c command) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, err := m.Propose(ctx, shardID, c)
		cancel()
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("Propose never succeeded (shard not ready?): %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func getWithRetry(t *testing.T, m *Manager, shardID uint64, key []byte, linearizable bool) []byte {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		got, err := m.Get(ctx, shardID, key, linearizable)
		cancel()
		if err == nil && got != nil {
			return got
		}
		if time.Now().After(deadline) {
			t.Fatalf("Get never returned the value: got=%q err=%v", got, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
