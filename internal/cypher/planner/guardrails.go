// Package planner builds logical and physical plans for the Cypher subset and executes them against
// the graph store, enforcing the design/07 guardrails and attaching honest QueryMeta. Unbounded
// traversal is a hard failure, not a warning.
package planner

import "fmt"

// Limits are the design/07 "Guardrails" query limits. They are enforced from the first row, not
// advisory.
type Limits struct {
	MaxRowsReturned     int
	MaxIntermediateRows int
	MaxTraversalDepth   int
	MaxRemoteFragments  int
	QueryTimeoutMs      int
	MaxMemoryBytes      int64
}

// DefaultLimits are the design/07 defaults.
func DefaultLimits() Limits {
	return Limits{
		MaxRowsReturned:     100_000,
		MaxIntermediateRows: 1_000_000,
		MaxTraversalDepth:   8,
		MaxRemoteFragments:  128,
		QueryTimeoutMs:      30_000,
		MaxMemoryBytes:      512 << 20,
	}
}

// GuardrailError is returned when a query exceeds a limit.
type GuardrailError struct {
	Limit string
	Value int
	Max   int
}

func (e *GuardrailError) Error() string {
	return fmt.Sprintf("cypher: guardrail %s exceeded: %d > %d", e.Limit, e.Value, e.Max)
}

func (l Limits) checkIntermediate(n int) error {
	if n > l.MaxIntermediateRows {
		return &GuardrailError{Limit: "maxIntermediateRows", Value: n, Max: l.MaxIntermediateRows}
	}
	return nil
}

func (l Limits) checkDepth(d int) error {
	if d > l.MaxTraversalDepth {
		return &GuardrailError{Limit: "maxTraversalDepth", Value: d, Max: l.MaxTraversalDepth}
	}
	return nil
}
