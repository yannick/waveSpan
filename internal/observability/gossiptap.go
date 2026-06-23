package observability

import (
	"time"

	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

// GossipTap builds redacted GossipRecords from gossip-agent events and feeds them into the ring
// (design/26). It emits DECODED summaries only — never raw record bytes (design/15). It is
// nil-safe so the membership agent can call it unconditionally with no behavior change.
type GossipTap struct {
	ring *GossipRing
	now  func() int64
}

// NewGossipTap wires a tap over a ring.
func NewGossipTap(ring *GossipRing) *GossipTap {
	return &GossipTap{ring: ring, now: func() int64 { return time.Now().UnixMilli() }}
}

func (t *GossipTap) emit(kind wavespanv1.GossipKind, dir wavespanv1.GossipDirection, peer string, sum *wavespanv1.GossipPayloadSummary) {
	if t == nil || t.ring == nil {
		return
	}
	t.ring.Emit(&wavespanv1.GossipRecord{Kind: kind, Direction: dir, Peer: peer, AtUnixMs: t.now(), Summary: sum})
}

// Ping records an outbound or inbound ping/ack with its measured RTT.
func (t *GossipTap) Ping(peer string, dir wavespanv1.GossipDirection, rttMs float64, sizeBytes uint32) {
	t.emit(wavespanv1.GossipKind_GOSSIP_PING, dir, peer, &wavespanv1.GossipPayloadSummary{RttMs: rttMs, PayloadSizeBytes: sizeBytes})
}

// Ack records an ack.
func (t *GossipTap) Ack(peer string, dir wavespanv1.GossipDirection, rttMs float64) {
	t.emit(wavespanv1.GossipKind_GOSSIP_ACK, dir, peer, &wavespanv1.GossipPayloadSummary{RttMs: rttMs})
}

// StateChange records a liveness transition (suspect/alive/unreachable).
func (t *GossipTap) StateChange(peer string, kind wavespanv1.GossipKind, newState string) {
	t.emit(kind, wavespanv1.GossipDirection_GOSSIP_INTERNAL, peer, &wavespanv1.GossipPayloadSummary{NewState: newState})
}

// HolderSummary records a gossiped holder-summary exchange (decoded watermark/approx count only).
func (t *GossipTap) HolderSummary(peer string, dir wavespanv1.GossipDirection, watermark, approxCount uint64, sizeBytes uint32) {
	t.emit(wavespanv1.GossipKind_GOSSIP_HOLDER_SUMMARY, dir, peer, &wavespanv1.GossipPayloadSummary{Watermark: watermark, ApproxCount: approxCount, PayloadSizeBytes: sizeBytes})
}

// LatencyEdge records a latency-graph edge update.
func (t *GossipTap) LatencyEdge(peer string, ewmaMs, p95Ms float64) {
	t.emit(wavespanv1.GossipKind_GOSSIP_LATENCY_EDGE, wavespanv1.GossipDirection_GOSSIP_INTERNAL, peer, &wavespanv1.GossipPayloadSummary{EwmaMs: ewmaMs, P95Ms: p95Ms})
}

// MembershipDelta records added/removed members.
func (t *GossipTap) MembershipDelta(added, removed []string) {
	t.emit(wavespanv1.GossipKind_GOSSIP_MEMBERSHIP_DELTA, wavespanv1.GossipDirection_GOSSIP_INTERNAL, "", &wavespanv1.GossipPayloadSummary{AddedMembers: added, RemovedMembers: removed})
}

// ConfigDelta records a gossiped runtime config-override exchange (key count + payload size only).
func (t *GossipTap) ConfigDelta(peer string, dir wavespanv1.GossipDirection, keyCount, sizeBytes uint32) {
	t.emit(wavespanv1.GossipKind_GOSSIP_CONFIG_DELTA, dir, peer, &wavespanv1.GossipPayloadSummary{ApproxCount: uint64(keyCount), PayloadSizeBytes: sizeBytes})
}
