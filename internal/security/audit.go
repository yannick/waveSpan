package security

import (
	"log/slog"
	"sync"
	"time"
)

// AuditEvent is a peer-replication audit record (design/15 "Audit logs").
type AuditEvent struct {
	Kind     string // "peer-connect" | "apply-error" | "auth-reject"
	Peer     string
	Detail   string
	AtUnixMs int64
}

// AuditLog records peer connection and apply-error events. It both emits structured logs and
// retains a bounded in-memory ring for the admin diagnostics endpoint.
type AuditLog struct {
	mu        sync.Mutex
	logger    *slog.Logger
	ring      []AuditEvent
	maxEvents int
	now       func() time.Time
}

// NewAuditLog builds an audit log keeping the most recent `maxEvents` events.
func NewAuditLog(logger *slog.Logger, maxEvents int) *AuditLog {
	if logger == nil {
		logger = slog.Default()
	}
	if maxEvents <= 0 {
		maxEvents = 1024
	}
	return &AuditLog{logger: logger, maxEvents: maxEvents, now: time.Now}
}

// Record appends an audit event.
func (a *AuditLog) Record(kind, peer, detail string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	e := AuditEvent{Kind: kind, Peer: peer, Detail: detail, AtUnixMs: a.now().UnixMilli()}
	a.ring = append(a.ring, e)
	if len(a.ring) > a.maxEvents {
		a.ring = a.ring[len(a.ring)-a.maxEvents:]
	}
	a.logger.Info("audit", "kind", kind, "peer", peer, "detail", detail)
}

// Events returns a copy of the retained audit events.
func (a *AuditLog) Events() []AuditEvent {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]AuditEvent(nil), a.ring...)
}
