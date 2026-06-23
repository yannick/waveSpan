// Package tunables is WaveSpan's single source of truth for performance/behaviour tunables — the
// ~80 knobs that an operator may want to set (and, for the "hot" ones, change at runtime). Each
// tunable is declared exactly once with its key, type, default, documentation, rationale, and
// category (Static = applied at startup / engine-open; Hot = re-readable live). The registry drives
// every consumer of config: the documented reference YAML, koanf file+env loading, the GetNodeConfig
// API/UI, validation, and the gossip-delta runtime-override path.
//
// Node identity & wiring (clusterId, memberId, seeds, ports, TLS material) stays in internal/config —
// those are not tunables and are never gossip-overridden. This package is only the knobs.
package tunables

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Kind is the value type of a tunable.
type Kind uint8

// Kind enumerates the value type of a tunable parameter.
const (
	KindDuration Kind = iota // Go duration string ("30s", "120ms")
	KindInt                  // plain integer
	KindInt64                // 64-bit integer
	KindBytes                // byte size ("64MiB", "100MB", or raw bytes)
	KindFloat                // floating point
	KindBool                 // true/false
	KindString               // free string (enums included)
)

func (k Kind) String() string {
	switch k {
	case KindDuration:
		return "duration"
	case KindInt:
		return "int"
	case KindInt64:
		return "int64"
	case KindBytes:
		return "bytes"
	case KindFloat:
		return "float"
	case KindBool:
		return "bool"
	default:
		return "string"
	}
}

// Category says whether a tunable can change on a live node.
type Category uint8

const (
	// Static tunables are read once at startup (or engine open) and a runtime override only takes
	// effect after a restart — the UI/API surfaces it as "pending restart".
	Static Category = iota
	// Hot tunables are re-read live by their owning worker; a runtime override applies on the next tick.
	Hot
)

func (c Category) String() string {
	if c == Hot {
		return "hot"
	}
	return "static"
}

// Source is the precedence layer a value came from (lowest → highest).
type Source uint8

// Source records where a parameter's current value came from.
const (
	FromDefault Source = iota // built-in default
	FromFile                  // YAML config file (k8s ConfigMap)
	FromEnv                   // WAVESPAN_* environment override
	FromRuntime               // runtime override (gossip-delta)
)

func (s Source) String() string {
	switch s {
	case FromFile:
		return "file"
	case FromEnv:
		return "env"
	case FromRuntime:
		return "runtime"
	default:
		return "default"
	}
}

// value is the immutable, atomically-swapped effective state of a tunable. Storing it behind an
// atomic.Pointer keeps hot reads lock-free.
type value struct {
	raw     string // canonical string form (display / YAML / env / gossip)
	num     int64  // duration ns | int | int64 | bytes
	f       float64
	b       bool
	source  Source
	version uint64 // for runtime LWW (0 for non-runtime)
}

// Param is one declared tunable.
type Param struct {
	Key      string // dotted key, e.g. "storage.engine.writeBufferSize"
	Group    string // subsystem group, e.g. "storage.engine"
	Doc      string // what it controls
	Why      string // why it defaults to its value
	Kind     Kind
	Category Category
	def      string // default, canonical string form

	cur     atomic.Pointer[value]
	applyMu sync.Mutex
	onApply func(*Param) // optional hot-apply hook, fired on Set for Hot params
}

// ---- typed accessors (lock-free) ---------------------------------------------------------------

// Duration returns the current value of a KindDuration param.
func (p *Param) Duration() time.Duration { return time.Duration(p.cur.Load().num) }

// Int returns the current value of a KindInt/KindBytes param.
func (p *Param) Int() int { return int(p.cur.Load().num) }

// Int64 returns the current value of a KindInt64/KindBytes param.
func (p *Param) Int64() int64 { return p.cur.Load().num }

// Float returns the current value of a KindFloat param.
func (p *Param) Float() float64 { return p.cur.Load().f }

// Bool returns the current value of a KindBool param.
func (p *Param) Bool() bool { return p.cur.Load().b }

// String returns the current canonical string value (any kind).
func (p *Param) String() string { return p.cur.Load().raw }

// Source returns where the current value came from.
func (p *Param) Source() Source { return p.cur.Load().source }

// Version returns the runtime override version (0 if not runtime-set).
func (p *Param) Version() uint64 { return p.cur.Load().version }

// Default returns the built-in default in canonical string form.
func (p *Param) Default() string { return p.def }

// OnApply registers a hook fired whenever this (Hot) param's value changes. The worker that owns the
// tunable uses it to re-read live; Static params ignore it.
func (p *Param) OnApply(fn func(*Param)) {
	p.applyMu.Lock()
	p.onApply = fn
	p.applyMu.Unlock()
}

// set parses raw per the param's Kind, swaps in the new value, and (for Hot params) fires onApply.
func (p *Param) set(raw string, src Source, version uint64) error {
	v, err := parse(p.Kind, raw)
	if err != nil {
		return fmt.Errorf("tunable %s: %w", p.Key, err)
	}
	v.source = src
	v.version = version
	p.cur.Store(v)
	if p.Category == Hot {
		p.applyMu.Lock()
		fn := p.onApply
		p.applyMu.Unlock()
		if fn != nil {
			fn(p)
		}
	}
	return nil
}

// ---- Registry ----------------------------------------------------------------------------------

// Registry holds all declared tunables in registration order.
type Registry struct {
	mu     sync.Mutex
	order  []string
	params map[string]*Param
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{params: make(map[string]*Param)}
}

// register declares a tunable, seeding it with its default value. It panics on a duplicate key or an
// invalid default (both are programmer errors caught at startup).
func (r *Registry) register(p *Param) *Param {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, dup := r.params[p.Key]; dup {
		panic("tunables: duplicate key " + p.Key)
	}
	if err := p.set(p.def, FromDefault, 0); err != nil {
		panic("tunables: bad default for " + p.Key + ": " + err.Error())
	}
	r.params[p.Key] = p
	r.order = append(r.order, p.Key)
	return p
}

// Get returns the param for key, or nil.
func (r *Registry) Get(key string) *Param {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.params[key]
}

// All returns every param in registration order.
func (r *Registry) All() []*Param {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]*Param, 0, len(r.order))
	for _, k := range r.order {
		out = append(out, r.params[k])
	}
	return out
}

// Groups returns the distinct group names in first-seen order.
func (r *Registry) Groups() []string {
	seen := map[string]bool{}
	var out []string
	for _, p := range r.All() {
		if !seen[p.Group] {
			seen[p.Group] = true
			out = append(out, p.Group)
		}
	}
	return out
}

// Set applies a value to a key from a given source. Unknown keys are an error so typos in YAML/env
// fail fast rather than being silently ignored.
func (r *Registry) Set(key, raw string, src Source, version uint64) error {
	p := r.Get(key)
	if p == nil {
		return fmt.Errorf("tunables: unknown key %q", key)
	}
	return p.set(raw, src, version)
}

// ---- parsing -----------------------------------------------------------------------------------

func parse(kind Kind, raw string) (*value, error) {
	raw = strings.TrimSpace(raw)
	v := &value{raw: raw}
	switch kind {
	case KindDuration:
		d, err := time.ParseDuration(raw)
		if err != nil {
			return nil, fmt.Errorf("invalid duration %q (want e.g. 30s, 120ms)", raw)
		}
		v.num = int64(d)
		v.raw = d.String()
	case KindBytes:
		n, err := parseBytes(raw)
		if err != nil {
			return nil, err
		}
		v.num = n
		v.raw = formatBytes(n)
	case KindInt, KindInt64:
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid integer %q", raw)
		}
		v.num = n
	case KindFloat:
		f, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid float %q", raw)
		}
		v.f = f
		v.raw = strconv.FormatFloat(f, 'g', -1, 64) // canonical: 0.10 and 0.1 normalize equal
	case KindBool:
		b, err := strconv.ParseBool(raw)
		if err != nil {
			return nil, fmt.Errorf("invalid bool %q (want true/false)", raw)
		}
		v.b = b
		v.raw = strconv.FormatBool(b)
	case KindString:
		// free-form
	}
	return v, nil
}

var byteUnits = []struct {
	suffix string
	mult   int64
}{
	{"GiB", 1 << 30}, {"MiB", 1 << 20}, {"KiB", 1 << 10},
	{"GB", 1_000_000_000}, {"MB", 1_000_000}, {"KB", 1_000},
	{"B", 1},
}

// parseBytes accepts "64MiB", "100MB", "512", etc.
func parseBytes(s string) (int64, error) {
	t := strings.TrimSpace(s)
	for _, u := range byteUnits {
		if strings.HasSuffix(t, u.suffix) {
			num := strings.TrimSpace(strings.TrimSuffix(t, u.suffix))
			f, err := strconv.ParseFloat(num, 64)
			if err != nil {
				return 0, fmt.Errorf("invalid byte size %q", s)
			}
			return int64(f * float64(u.mult)), nil
		}
	}
	n, err := strconv.ParseInt(t, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid byte size %q (want e.g. 64MiB, 100MB, or raw bytes)", s)
	}
	return n, nil
}

// formatBytes renders a byte count using binary units when it divides evenly, else raw bytes.
func formatBytes(n int64) string {
	for _, u := range []struct {
		suffix string
		mult   int64
	}{{"GiB", 1 << 30}, {"MiB", 1 << 20}, {"KiB", 1 << 10}} {
		if n >= u.mult && n%u.mult == 0 {
			return strconv.FormatInt(n/u.mult, 10) + u.suffix
		}
	}
	return strconv.FormatInt(n, 10)
}
