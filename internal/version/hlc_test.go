package version

import "testing"

// fakeWall is a controllable wall clock in milliseconds.
type fakeWall struct{ ms uint64 }

func (f *fakeWall) now() uint64 { return f.ms }

func TestHLCLocalMonotonic(t *testing.T) {
	w := &fakeWall{ms: 100}
	c := NewClock(w.now, 500)

	prev := c.Now()
	for i := 0; i < 5; i++ {
		// same physical millisecond: logical must strictly increase
		got := c.Now()
		if got.Compare(prev) <= 0 {
			t.Fatalf("HLC not strictly increasing within a ms: %+v after %+v", got, prev)
		}
		if got.PhysicalMs != prev.PhysicalMs || got.Logical != prev.Logical+1 {
			t.Fatalf("expected logical+1 within same ms, got %+v after %+v", got, prev)
		}
		prev = got
	}

	// advancing wall resets logical to 0
	w.ms = 101
	got := c.Now()
	if got.PhysicalMs != 101 || got.Logical != 0 {
		t.Fatalf("advancing physical should reset logical to 0, got %+v", got)
	}
	if got.Compare(prev) <= 0 {
		t.Fatalf("HLC regressed across ms boundary: %+v after %+v", got, prev)
	}
}

func TestHLCReceiveMergeBranches(t *testing.T) {
	// phys == lastPhys == msgPhys -> max(lastLogical,msgLogical)+1
	w := &fakeWall{ms: 50}
	c := NewClock(w.now, 500)
	c.Now() // last = (50,0)
	got, err := c.Update(Timestamp{PhysicalMs: 50, Logical: 9})
	if err != nil {
		t.Fatal(err)
	}
	if got.PhysicalMs != 50 || got.Logical != 10 {
		t.Fatalf("equal-phys branch: want (50,10) got %+v", got)
	}

	// phys == msgPhys (msg ahead of wall and last) -> msgLogical+1
	w2 := &fakeWall{ms: 50}
	c2 := NewClock(w2.now, 500)
	c2.Now() // last = (50,0)
	got, err = c2.Update(Timestamp{PhysicalMs: 80, Logical: 3})
	if err != nil {
		t.Fatal(err)
	}
	if got.PhysicalMs != 80 || got.Logical != 4 {
		t.Fatalf("msg-phys branch: want (80,4) got %+v", got)
	}

	// phys == lastPhys (last ahead of msg and wall) -> lastLogical+1
	w3 := &fakeWall{ms: 50}
	c3 := NewClock(w3.now, 500)
	c3.Now()
	c3.Now() // last = (50,1)
	got, err = c3.Update(Timestamp{PhysicalMs: 20, Logical: 99})
	if err != nil {
		t.Fatal(err)
	}
	if got.PhysicalMs != 50 || got.Logical != 2 {
		t.Fatalf("last-phys branch: want (50,2) got %+v", got)
	}

	// else: wall dominates both -> logical 0
	w4 := &fakeWall{ms: 200}
	c4 := NewClock(w4.now, 500)
	c4.Now() // last=(200,0); now move wall forward
	w4.ms = 300
	got, err = c4.Update(Timestamp{PhysicalMs: 100, Logical: 5})
	if err != nil {
		t.Fatal(err)
	}
	if got.PhysicalMs != 300 || got.Logical != 0 {
		t.Fatalf("wall-dominates branch: want (300,0) got %+v", got)
	}
}

func TestHLCReceiveDominatesInputs(t *testing.T) {
	w := &fakeWall{ms: 100}
	c := NewClock(w.now, 1000)
	local := c.Now()
	remote := Timestamp{PhysicalMs: 140, Logical: 2}
	got, err := c.Update(remote)
	if err != nil {
		t.Fatal(err)
	}
	if got.Compare(local) <= 0 || got.Compare(remote) <= 0 {
		t.Fatalf("merged stamp %+v must dominate local %+v and remote %+v", got, local, remote)
	}
}

func TestHLCSkewRejection(t *testing.T) {
	w := &fakeWall{ms: 1000}
	c := NewClock(w.now, 500)
	before := c.Now()

	// remote 600ms ahead of wall (> 500 skew bound) must be rejected
	_, err := c.Update(Timestamp{PhysicalMs: 1600, Logical: 0})
	if err == nil {
		t.Fatal("expected skew rejection for remote beyond maxClockSkewMs")
	}
	if c.SkewRejections() != 1 {
		t.Fatalf("skew metric not incremented: %d", c.SkewRejections())
	}
	// local clock must not have been dragged forward by the rejected stamp
	after := c.Now()
	if after.PhysicalMs > 1000 {
		t.Fatalf("rejected stamp dragged the clock forward to %+v", after)
	}
	_ = before

	// a stamp exactly at the bound is accepted
	if _, err := c.Update(Timestamp{PhysicalMs: 1500, Logical: 0}); err != nil {
		t.Fatalf("stamp at skew bound should be accepted: %v", err)
	}
}

func TestHLCLogicalOverflowAdvancesPhysical(t *testing.T) {
	w := &fakeWall{ms: 7}
	c := NewClock(w.now, 500)
	// force the logical counter near the 16-bit ceiling within one physical ms
	c.forceLast(Timestamp{PhysicalMs: 7, Logical: 0xFFFF})
	got := c.Now()
	if got.PhysicalMs != 8 || got.Logical != 0 {
		t.Fatalf("logical overflow should advance physical and reset logical, got %+v", got)
	}
}
