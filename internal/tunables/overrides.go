package tunables

import (
	"sync"
	"time"
)

// Override is a runtime tunable override: a value set at runtime (via the admin API) that takes
// precedence over default/file/env. Overrides gossip cluster-wide and merge last-write-wins by
// (Version, Origin).
type Override struct {
	Key     string `json:"key"`
	Value   string `json:"value"`
	Version uint64 `json:"version"`
	Origin  string `json:"origin"`
}

// newer reports whether a should win over b under LWW (higher version, then higher origin id).
func newer(a, b Override) bool {
	if a.Version != b.Version {
		return a.Version > b.Version
	}
	return a.Origin > b.Origin
}

// Overrides manages the live runtime-override set on a node: it applies overrides to the registry
// (firing Hot params' OnApply hooks), versions local changes monotonically, merges remote deltas by
// LWW, persists a snapshot, and provides the current set for gossip. It is safe for concurrent use.
type Overrides struct {
	reg     *Registry
	origin  string
	now     func() int64 // unix ms
	persist func([]Override)

	mu      sync.Mutex
	set     map[string]Override
	lastVer uint64
}

// NewOverrides builds a manager. origin is this node's member id (LWW tie-break / provenance);
// persist is called with the full set whenever it changes (nil disables persistence).
func NewOverrides(reg *Registry, origin string, persist func([]Override)) *Overrides {
	return &Overrides{
		reg:     reg,
		origin:  origin,
		now:     func() int64 { return time.Now().UnixMilli() },
		persist: persist,
		set:     map[string]Override{},
	}
}

// Set applies a local runtime override for key=value: it validates against the registry, assigns a
// monotonic version, applies it (Hot params take effect live), records it, and persists. It returns
// the assigned version and whether the tunable is Static (so the caller can report requires-restart).
func (o *Overrides) Set(key, value string) (version uint64, requiresRestart bool, err error) {
	p := o.reg.Get(key)
	if p == nil {
		return 0, false, errUnknown(key)
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	v := uint64(o.now())
	if v <= o.lastVer {
		v = o.lastVer + 1
	}
	o.lastVer = v
	od := Override{Key: key, Value: value, Version: v, Origin: o.origin}
	if err := o.reg.Set(key, value, FromRuntime, v); err != nil {
		return 0, false, err
	}
	o.set[key] = od
	o.persistLocked()
	return v, p.Category == Static, nil
}

// ApplyRemote merges gossiped overrides by LWW and returns the ones that actually changed local
// state (for tapping/logging). Unknown or unparseable keys are skipped (forward-compatible).
func (o *Overrides) ApplyRemote(deltas []Override) []Override {
	o.mu.Lock()
	defer o.mu.Unlock()
	var changed []Override
	for _, d := range deltas {
		if cur, ok := o.set[d.Key]; ok && !newer(d, cur) {
			continue
		}
		if o.reg.Get(d.Key) == nil {
			continue
		}
		if err := o.reg.Set(d.Key, d.Value, FromRuntime, d.Version); err != nil {
			continue
		}
		o.set[d.Key] = d
		if d.Version > o.lastVer {
			o.lastVer = d.Version
		}
		changed = append(changed, d)
	}
	if len(changed) > 0 {
		o.persistLocked()
	}
	return changed
}

// LoadSnapshot applies a persisted override set at startup (highest precedence, before workers and
// the engine read the registry). It does not re-persist.
func (o *Overrides) LoadSnapshot(snap []Override) {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, d := range snap {
		if o.reg.Get(d.Key) == nil {
			continue
		}
		if err := o.reg.Set(d.Key, d.Value, FromRuntime, d.Version); err != nil {
			continue
		}
		o.set[d.Key] = d
		if d.Version > o.lastVer {
			o.lastVer = d.Version
		}
	}
}

// List returns the current override set (for gossip and inspection), sorted by key for determinism.
func (o *Overrides) List() []Override {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := make([]Override, 0, len(o.set))
	for _, k := range sortedOverrideKeys(o.set) {
		out = append(out, o.set[k])
	}
	return out
}

func (o *Overrides) persistLocked() {
	if o.persist == nil {
		return
	}
	out := make([]Override, 0, len(o.set))
	for _, k := range sortedOverrideKeys(o.set) {
		out = append(out, o.set[k])
	}
	o.persist(out)
}

func sortedOverrideKeys(m map[string]Override) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	// simple insertion sort keeps deps minimal; the set is small (changed tunables only)
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j-1] > out[j]; j-- {
			out[j-1], out[j] = out[j], out[j-1]
		}
	}
	return out
}

func errUnknown(key string) error { return &unknownKeyError{key} }

type unknownKeyError struct{ key string }

func (e *unknownKeyError) Error() string { return "tunables: unknown key " + e.key }
