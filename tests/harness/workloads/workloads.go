//go:build harness

// Package workloads holds the Jepsen-style op generators ported to Go (design/25, JEPSEN.md). Each
// drives the WaveSpan client (recording the unified history) and is checked by the model-aware
// checkers AFTER faults heal. The eventual-consistency adaptation: convergence/no-loss are asserted
// only post-heal on acked ops.
package workloads

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/yannick/wavespan/tests/harness/client"
)

// Register is the single-key concurrent read/write workload (Jepsen linearizable-register, adapted
// to HLC-LWW): every member overwrites one key; post-heal, all replicas must converge to the
// doc-22-maximal winner (checker: lww-determinism + convergence).
func Register(ctx context.Context, cl *client.Client, members []string, seed int64, perMember int) {
	const ns, key = "default", "reg"
	var wg sync.WaitGroup
	for i, m := range members {
		wg.Add(1)
		go func(member string, base int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed + int64(base)))
			for j := 0; j < perMember && ctx.Err() == nil; j++ {
				cl.Put(ctx, member, ns, key, fmt.Sprintf("%s-%d-%d", member, j, rng.Intn(1000)), "", "")
				time.Sleep(5 * time.Millisecond)
			}
		}(m, i)
	}
	wg.Wait()
}

// Set is the grow-only-set workload (Jepsen g-set / checker.set-full): each add is a distinct key;
// every acked add must be present on every live replica post-heal (checker: durability + convergence
// per key). Returns the set of acked element keys.
func Set(ctx context.Context, cl *client.Client, members []string, seed, perMember int64) []string {
	const ns = "default"
	var mu sync.Mutex
	var acked []string
	var wg sync.WaitGroup
	for i, m := range members {
		wg.Add(1)
		go func(member string, base int) {
			defer wg.Done()
			for j := int64(0); j < perMember && ctx.Err() == nil; j++ {
				k := fmt.Sprintf("set/%s/%d", member, j)
				if cl.Put(ctx, member, ns, k, "present", "", "") {
					mu.Lock()
					acked = append(acked, k)
					mu.Unlock()
				}
				time.Sleep(5 * time.Millisecond)
			}
		}(m, i)
	}
	wg.Wait()
	return acked
}

// WaitConverged polls (without recording) until every member returns the same value for every key,
// or the timeout elapses (design/13: convergence is eventual — give fanout/repair/anti-entropy time
// to settle before the final recorded read). Returns the keys that did NOT converge.
func WaitConverged(ctx context.Context, cl *client.Client, members []string, ns string, keys []string, timeout time.Duration) []string {
	deadline := time.Now().Add(timeout)
	for {
		var pending []string
		for _, key := range keys {
			vals := map[string]bool{}
			for _, m := range members {
				v, _ := cl.Peek(ctx, m, ns, key)
				vals[v] = true
			}
			if len(vals) > 1 {
				pending = append(pending, key)
			}
		}
		if len(pending) == 0 || time.Now().After(deadline) {
			return pending
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// PostHealReadAll reads each key from EVERY member (served_by) so the convergence/lww/durability
// checkers have a per-replica view of the quiescent, post-heal state (Jepsen's final-read phase).
// Call WaitConverged first so the recorded reads reflect the converged state.
func PostHealReadAll(ctx context.Context, cl *client.Client, members []string, ns string, keys []string) {
	for _, key := range keys {
		for _, m := range members {
			cl.Get(ctx, m, ns, key, "")
		}
	}
}

// Idempotency retries the SAME request_id across a fault: a correct dedupe yields exactly one
// logical mutation (checker: idempotency).
func Idempotency(ctx context.Context, cl *client.Client, member, reqID string, attempts int) {
	const ns, key = "default", "idem"
	for i := 0; i < attempts && ctx.Err() == nil; i++ {
		cl.Put(ctx, member, ns, key, "v", reqID, "")
		time.Sleep(10 * time.Millisecond)
	}
}
