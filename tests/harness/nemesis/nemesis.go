// Package nemesis injects faults during a harness run and records each as a window on the history
// (design/25). The live fault actions (docker kill/partition/tc latency, the doc-24 runner hooks)
// are supplied as inject/heal functions so the orchestration is unit-testable without a cluster and
// stays pure-Go/no-CGO (design/17).
package nemesis

import "github.com/cwire/wavespan/tests/harness/runner"

// Nemesis starts a fault and fully heals it on Stop, recording both edges on the history.
type Nemesis interface {
	Name() string
	Start(h *runner.History, targets []string, atMs int64)
	Stop(h *runner.History, atMs int64)
}

// New builds a nemesis of the given kind. inject performs the live fault (e.g. `docker kill`); heal
// reverses it. Either may be nil (a pure recording nemesis, for unit tests).
func New(kind string, inject, heal func(targets []string)) Nemesis {
	return &nemesis{kind: kind, inject: inject, heal: heal, faultIdx: -1}
}

type nemesis struct {
	kind         string
	inject, heal func([]string)
	faultIdx     int
	targets      []string
}

func (n *nemesis) Name() string { return n.kind }

// Start records the fault window's opening edge and injects the fault.
func (n *nemesis) Start(h *runner.History, targets []string, atMs int64) {
	n.targets = targets
	n.faultIdx = len(h.Faults)
	h.AppendFault(runner.Fault{Kind: n.kind, Targets: targets, StartMs: atMs})
	if n.inject != nil {
		n.inject(targets)
	}
}

// Stop heals the fault and closes its window.
func (n *nemesis) Stop(h *runner.History, atMs int64) {
	if n.faultIdx >= 0 && n.faultIdx < len(h.Faults) {
		h.Faults[n.faultIdx].EndMs = atMs
	}
	if n.heal != nil {
		n.heal(n.targets)
	}
}

// Compose runs several nemeses as one (start all, stop all) without deadlock.
type Compose struct {
	name     string
	children []Nemesis
}

// NewCompose composes nemeses.
func NewCompose(name string, children ...Nemesis) *Compose {
	return &Compose{name: name, children: children}
}

// Name returns the composite name.
func (c *Compose) Name() string { return c.name }

// Start starts every child.
func (c *Compose) Start(h *runner.History, targets []string, atMs int64) {
	for _, ch := range c.children {
		ch.Start(h, targets, atMs)
	}
}

// Stop stops every child (reverse order).
func (c *Compose) Stop(h *runner.History, atMs int64) {
	for i := len(c.children) - 1; i >= 0; i-- {
		c.children[i].Stop(h, atMs)
	}
}
