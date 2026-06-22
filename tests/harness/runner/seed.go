package runner

import "math/rand"

// Schedule is a deterministic plan derived from a seed: the op stream order and the nemesis
// schedule. The same seed reproduces the same run (design/25 "Deterministic"), so any violation
// shrinks to a standalone repro.
type Schedule struct {
	Seed    int64
	rng     *rand.Rand
	Nemeses []ScheduledFault
	OpCount int
}

// ScheduledFault is a planned nemesis activation.
type ScheduledFault struct {
	Kind       string
	Targets    []string
	StartMs    int64
	DurationMs int64
}

// NewSchedule derives a deterministic schedule from a seed.
func NewSchedule(seed int64, opCount int, nemesisKinds []string, members []string) *Schedule {
	rng := rand.New(rand.NewSource(seed))
	s := &Schedule{Seed: seed, rng: rng, OpCount: opCount}
	t := int64(0)
	for _, kind := range nemesisKinds {
		t += int64(500 + rng.Intn(1500))
		target := members[rng.Intn(len(members))]
		s.Nemeses = append(s.Nemeses, ScheduledFault{
			Kind: kind, Targets: []string{target}, StartMs: t, DurationMs: int64(500 + rng.Intn(2000)),
		})
		t += int64(1000 + rng.Intn(1000))
	}
	return s
}

// Rand returns the schedule's deterministic RNG (for op-value generation).
func (s *Schedule) Rand() *rand.Rand { return s.rng }
