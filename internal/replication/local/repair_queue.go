package local

import (
	"container/heap"

	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

// RepairItem is an under-replicated key awaiting repair.
type RepairItem struct {
	Namespace    string
	Key          []byte
	Record       *wavespanv1.StoredRecord // latest winning record (may be nil; read from store)
	Deficit      int                      // how far below target (higher = more urgent)
	EnqueuedAtMs int64
	index        int
}

// repairQueue is a max-heap ordered by deficit, then by age (oldest first). Most
// under-replicated keys drain first (design/23_repair_engine.md priority order).
type repairQueue []*RepairItem

func (q repairQueue) Len() int { return len(q) }
func (q repairQueue) Less(i, j int) bool {
	if q[i].Deficit != q[j].Deficit {
		return q[i].Deficit > q[j].Deficit // higher deficit first
	}
	return q[i].EnqueuedAtMs < q[j].EnqueuedAtMs // older first
}
func (q repairQueue) Swap(i, j int) {
	q[i], q[j] = q[j], q[i]
	q[i].index = i
	q[j].index = j
}
func (q *repairQueue) Push(x any) {
	it := x.(*RepairItem)
	it.index = len(*q)
	*q = append(*q, it)
}
func (q *repairQueue) Pop() any {
	old := *q
	n := len(old)
	it := old[n-1]
	old[n-1] = nil
	*q = old[:n-1]
	return it
}

var _ heap.Interface = (*repairQueue)(nil)
