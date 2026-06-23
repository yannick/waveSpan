package bench

import (
	"context"
	"testing"
	"time"
)

func TestCollectionsExportedSurface(t *testing.T) {
	_ = CollectionsClient
	_ = OpSAdd
	_ = OpSRem
	_ = OpSIsMember
	_ = OpSCard
	_ = OpHSet
	_ = OpHGet
	_ = OpHIncrBy
	_ = OpZAdd
	_ = OpZScore
	_ = OpBulkRemove
}

func TestOpSAddDeadAddr(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	c := CollectionsClient("127.0.0.1:1")
	if err := OpSAdd(ctx, c, "ns", []byte("s"), []byte("m")); err == nil {
		t.Fatal("OpSAdd against a dead address must return a transport error")
	}
}

func TestOpBulkRemoveDeadAddr(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	c := CollectionsClient("127.0.0.1:1")
	if _, _, err := OpBulkRemove(ctx, c, "ns", nil, [][]byte{[]byte("doomed")}); err == nil {
		t.Fatal("OpBulkRemove against a dead address must return a transport error")
	}
}
