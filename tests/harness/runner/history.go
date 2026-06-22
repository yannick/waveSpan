// Package runner is the WaveSpan correctness harness orchestrator (design/25): it records a unified
// op+fault history that the model-aware checkers verify against the DECLARED eventual-consistency
// model (convergence, origin+1 durability, HLC-LWW/keep-siblings, lazy TTL, idempotency, session
// read-your-writes) — never linearizability. Seeded from the testing-waves bank discipline.
package runner

import (
	"encoding/json"

	"github.com/cwire/wavespan/internal/version"
)

// OpKind classifies a recorded operation.
type OpKind string

// Operation kinds.
const (
	OpPut    OpKind = "put"
	OpGet    OpKind = "get"
	OpDelete OpKind = "delete"
	OpScan   OpKind = "scan"
	OpAppend OpKind = "append"
	OpCAS    OpKind = "cas"
)

// Op is one recorded client operation. Reads record ObservedVersion; acked writes record
// WriterVersion. Scans record Completeness + certificate validity. Ack is whether the op returned
// success — convergence/no-loss assertions apply only to acked ops (design/13).
type Op struct {
	Kind            OpKind             `json:"kind"`
	Key             string             `json:"key"`
	Value           string             `json:"value,omitempty"`
	RequestID       string             `json:"request_id,omitempty"`
	Session         string             `json:"session,omitempty"`
	StartMs         int64              `json:"start_ms"`
	EndMs           int64              `json:"end_ms"`
	Ack             bool               `json:"ack"`
	ServedBy        string             `json:"served_by,omitempty"` // replica/member that served a read
	Cluster         string             `json:"cluster,omitempty"`
	Policy          string             `json:"policy,omitempty"` // conflict policy for the key
	ObservedVersion *version.Version   `json:"observed_version,omitempty"`
	WriterVersion   *version.Version   `json:"writer_version,omitempty"`
	Siblings        []*version.Version `json:"siblings,omitempty"` // for keep-siblings reads
	Tombstone       bool               `json:"tombstone,omitempty"`
	ExpiresAtMs     int64              `json:"expires_at_ms,omitempty"`

	// scan fields (property 4)
	Completeness     string `json:"completeness,omitempty"`
	HasCertificate   bool   `json:"has_certificate,omitempty"`
	CertValidUntilMs int64  `json:"cert_valid_until_ms,omitempty"`
}

// Fault is an injected nemesis window.
type Fault struct {
	Kind    string   `json:"kind"`
	Targets []string `json:"targets"`
	StartMs int64    `json:"start_ms"`
	EndMs   int64    `json:"end_ms"` // 0 = still active
}

// History is the unified op+fault timeline of a run.
type History struct {
	Seed   int64   `json:"seed"`
	Ops    []Op    `json:"ops"`
	Faults []Fault `json:"faults"`
}

// Append records an op.
func (h *History) Append(op Op) { h.Ops = append(h.Ops, op) }

// AppendFault records a fault window.
func (h *History) AppendFault(f Fault) { h.Faults = append(h.Faults, f) }

// Serialize encodes the history as JSON.
func (h *History) Serialize() ([]byte, error) { return json.MarshalIndent(h, "", "  ") }

// Parse decodes a serialized history.
func Parse(b []byte) (*History, error) {
	h := &History{}
	if err := json.Unmarshal(b, h); err != nil {
		return nil, err
	}
	return h, nil
}

// HealedAtMs returns the time after which no fault is active (all partitions/kills healed). Ops
// after this with no further faults are the post-heal quiescent window where convergence holds.
func (h *History) HealedAtMs() int64 {
	var latest int64
	for _, f := range h.Faults {
		end := f.EndMs
		if end == 0 {
			return 1<<62 - 1 // a still-active fault means never healed
		}
		if end > latest {
			latest = end
		}
	}
	return latest
}

// AckedWrites returns the acked write ops for a key (puts/deletes/appends/cas that returned success).
func (h *History) AckedWrites(key string) []Op {
	var out []Op
	for _, op := range h.Ops {
		if op.Key != key || !op.Ack {
			continue
		}
		switch op.Kind {
		case OpPut, OpDelete, OpAppend, OpCAS:
			out = append(out, op)
		}
	}
	return out
}

// PostHealReads returns reads of a key that started after the cluster fully healed (the window where
// convergence is asserted).
func (h *History) PostHealReads(key string) []Op {
	healed := h.HealedAtMs()
	var out []Op
	for _, op := range h.Ops {
		if op.Key == key && op.Kind == OpGet && op.Ack && op.StartMs >= healed {
			out = append(out, op)
		}
	}
	return out
}
