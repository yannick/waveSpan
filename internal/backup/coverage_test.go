package backup

import (
	"reflect"
	"testing"

	"github.com/yannick/wavespan/internal/collections"
)

// heldShards is a NodeRecord that exported (Done) holding the given data shards, for coverage tests.
func doneNode(id string, shards ...uint64) NodeRecord {
	return NodeRecord{MemberID: id, Done: true, HeldRanges: formatHeldShards(shards)}
}

func TestCoverageGaps_FullClusterNoGaps(t *testing.T) {
	// 4 data shards (2..5) split across two exporting nodes; both expected members present.
	perNode := []NodeRecord{
		doneNode("m1", collections.FirstDataShard+0, collections.FirstDataShard+1),
		doneNode("m2", collections.FirstDataShard+2, collections.FirstDataShard+3),
	}
	gaps := coverageGaps(perNode, 4, []string{"m1", "m2"})
	if len(gaps) != 0 {
		t.Fatalf("full cluster must have no gaps, got %v", gaps)
	}
}

func TestCoverageGaps_UncoveredDataShard(t *testing.T) {
	// Shard 5 (FirstDataShard+3) is hosted by nobody → a collections gap.
	perNode := []NodeRecord{
		doneNode("m1", collections.FirstDataShard+0, collections.FirstDataShard+1),
		doneNode("m2", collections.FirstDataShard+2),
	}
	gaps := coverageGaps(perNode, 4, []string{"m1", "m2"})
	want := []string{"collections-shard:5"}
	if !reflect.DeepEqual(gaps, want) {
		t.Fatalf("gaps = %v, want %v", gaps, want)
	}
}

func TestCoverageGaps_MissingMember(t *testing.T) {
	// m3 is expected but never exported (down at backup time) → a member gap. Shards fully covered.
	perNode := []NodeRecord{
		doneNode("m1", collections.FirstDataShard+0, collections.FirstDataShard+1),
		doneNode("m2", collections.FirstDataShard+2, collections.FirstDataShard+3),
	}
	gaps := coverageGaps(perNode, 4, []string{"m1", "m2", "m3"})
	want := []string{"member:m3"}
	if !reflect.DeepEqual(gaps, want) {
		t.Fatalf("gaps = %v, want %v", gaps, want)
	}
}

func TestCoverageGaps_ExpectedShardsZeroSkipsCollectionsCheck(t *testing.T) {
	// expectedDataShards == 0 (unknown) must NOT false-PARTIAL even with no held shards.
	perNode := []NodeRecord{doneNode("m1"), doneNode("m2")}
	gaps := coverageGaps(perNode, 0, []string{"m1", "m2"})
	if len(gaps) != 0 {
		t.Fatalf("unknown data-shard count must skip the collections check, got %v", gaps)
	}
}

func TestCoverageGaps_UndoneNodeDoesNotCover(t *testing.T) {
	// A node that reported held shards at prepare but did NOT finish export (Done=false) does not count
	// as covering its shards, and counts as a missing member.
	perNode := []NodeRecord{
		doneNode("m1", collections.FirstDataShard+0, collections.FirstDataShard+1, collections.FirstDataShard+2),
		{MemberID: "m2", Done: false, HeldRanges: formatHeldShards([]uint64{collections.FirstDataShard + 3})},
	}
	gaps := coverageGaps(perNode, 4, []string{"m1", "m2"})
	want := []string{"collections-shard:5", "member:m2"}
	if !reflect.DeepEqual(gaps, want) {
		t.Fatalf("gaps = %v, want %v", gaps, want)
	}
}
