package benchengine

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/yannick/wavespan/internal/bench"
)

// seedWith runs addSet(i) for i in [0,n) across `conc` workers, reporting progress periodically.
// It mirrors the channel-fed worker-pool pattern in bench.runPool (race-clean): a jobs channel,
// `conc` workers, a sync.WaitGroup, and an atomic done counter. `conc<1` is clamped to 1. Honors
// ctx cancellation: the first addSet error (or ctx error) aborts and is returned.
func seedWith(ctx context.Context, n, conc int, addSet func(ctx context.Context, i int) error, progress func(done, total int)) error {
	if conc < 1 {
		conc = 1
	}
	if progress == nil {
		progress = func(int, int) {}
	}
	if n <= 0 {
		progress(0, n)
		return nil
	}

	// Report progress every ~1% of the work (at least every job).
	step := n / 100
	if step < 1 {
		step = 1
	}

	jobs := make(chan int, conc*2)
	var wg sync.WaitGroup
	var done atomic.Int64
	var firstErr atomic.Pointer[error]

	setErr := func(e error) {
		if e != nil {
			firstErr.CompareAndSwap(nil, &e)
		}
	}

	for w := 0; w < conc; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				if ctx.Err() != nil {
					setErr(ctx.Err())
					continue
				}
				if firstErr.Load() != nil {
					continue
				}
				if err := addSet(ctx, i); err != nil {
					setErr(err)
					continue
				}
				d := done.Add(1)
				if d%int64(step) == 0 || int(d) == n {
					progress(int(d), n)
				}
			}
		}()
	}

	for i := 0; i < n; i++ {
		if ctx.Err() != nil {
			setErr(ctx.Err())
			break
		}
		jobs <- i
	}
	close(jobs)
	wg.Wait()

	progress(int(done.Load()), n)
	if p := firstErr.Load(); p != nil {
		return *p
	}
	return nil
}

// SeedSets seeds n sets in ns. Each set i = SAdd(set/i, [member, <filler bytes>]) over the data
// addr. If filler <= 0 the filler member is omitted.
func SeedSets(ctx context.Context, dataAddr, ns string, n, filler, conc int, member []byte, progress func(done, total int)) error {
	c := bench.CollectionsClient(dataAddr)
	var fillerBytes [][]byte
	if filler > 0 {
		fb := make([]byte, filler)
		for i := range fb {
			fb[i] = 'f'
		}
		fillerBytes = [][]byte{fb}
	}
	addSet := func(ctx context.Context, i int) error {
		coll := []byte(fmt.Sprintf("set/%d", i))
		members := append([][]byte{member}, fillerBytes...)
		return bench.OpSAdd(ctx, c, ns, coll, members...)
	}
	return seedWith(ctx, n, conc, addSet, progress)
}

// ReAddMember re-adds member to each of colls (for repeat / closed-loop runs).
func ReAddMember(ctx context.Context, dataAddr, ns string, colls [][]byte, member []byte) error {
	c := bench.CollectionsClient(dataAddr)
	for _, coll := range colls {
		if err := bench.OpSAdd(ctx, c, ns, coll, member); err != nil {
			return err
		}
	}
	return nil
}
