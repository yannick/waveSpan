package kv

import (
	"context"
	"time"

	"github.com/cwire/wavespan/internal/membership"
	"github.com/cwire/wavespan/internal/recordstore"
	"github.com/cwire/wavespan/internal/version"
	wavespanv1 "github.com/cwire/wavespan/proto/wavespan/v1"
)

// holderScanner scans a remote holder's local store over a subrange (routed-eventual scan).
type holderScanner interface {
	ScanLocal(ctx context.Context, target membership.Member, namespace string, start, end []byte, limit int) ([]*wavespanv1.ScanLocalRow, error)
}

// certValidator reports whether a valid range-coverage certificate covers [start, end) now.
type certValidator interface {
	Covers(namespace string, start, end []byte, nowMs int64) bool
}

// Scanner dispatches range scans across the four modes (design/03 "Range scans"), always declaring
// honest completeness so a partial cache scan is never presented as COMPLETE (property 4).
type Scanner struct {
	store      *recordstore.Store
	self       membership.Member
	cluster    Cluster
	holderScan holderScanner
	certs      certValidator
	nowMs      func() int64
}

// NewScanner wires a scanner. holderScan and certs are optional (nil disables routed scans /
// cache-complete certification).
func NewScanner(store *recordstore.Store, self membership.Member, cluster Cluster, holderScan holderScanner, certs certValidator) *Scanner {
	return &Scanner{store: store, self: self, cluster: cluster, holderScan: holderScan, certs: certs, nowMs: func() int64 { return time.Now().UnixMilli() }}
}

// Scan streams the scan: a header (with the actual completeness), the rows, then a trailer.
func (sc *Scanner) Scan(ctx context.Context, req *wavespanv1.ScanRequest, send func(*wavespanv1.ScanResponse) error) error {
	mode := req.GetMode()
	if mode == wavespanv1.ScanMode_SCAN_MODE_UNSPECIFIED {
		mode = wavespanv1.ScanMode_CACHE_FAST
	}
	ns, start, end, limit := req.GetNamespace(), req.GetStartKey(), req.GetEndKey(), int(req.GetLimit())
	now := sc.nowMs()

	var rows []scanRow
	var completeness wavespanv1.Completeness

	switch mode {
	case wavespanv1.ScanMode_ROUTED_EVENTUAL:
		rows = sc.routed(ctx, ns, start, end, now)
		completeness = wavespanv1.Completeness_PARTIAL // merged holders, but not certified complete
	case wavespanv1.ScanMode_CACHE_COMPLETE:
		rows = sc.local(ns, start, end, limit, now)
		if sc.certs != nil && sc.certs.Covers(ns, start, end, now) {
			completeness = wavespanv1.Completeness_COMPLETE
		} else {
			completeness = wavespanv1.Completeness_BEST_EFFORT // downgrade: no valid certificate
		}
	default: // CACHE_FAST, LOCAL_ONLY
		rows = sc.local(ns, start, end, limit, now)
		completeness = wavespanv1.Completeness_BEST_EFFORT // local cache scan is never COMPLETE
	}
	if limit > 0 && len(rows) > limit {
		rows = rows[:limit]
	}

	header := &wavespanv1.ScanResponse{Msg: &wavespanv1.ScanResponse_Header{Header: &wavespanv1.ScanHeader{
		Meta:         &wavespanv1.ResponseMeta{ServedByClusterId: sc.self.ClusterID, ServedByMemberId: sc.self.MemberID, Completeness: completeness, ObservedAtUnixMs: now},
		Mode:         mode,
		Completeness: completeness,
	}}}
	if err := send(header); err != nil {
		return err
	}
	for _, r := range rows {
		row := &wavespanv1.ScanRow{Key: r.key, Value: r.value, Version: r.version.ToProto()}
		if r.expiresAtMs != nil {
			row.ExpiresAtUnixMs = r.expiresAtMs
		}
		if err := send(&wavespanv1.ScanResponse{Msg: &wavespanv1.ScanResponse_Row{Row: row}}); err != nil {
			return err
		}
	}
	return send(&wavespanv1.ScanResponse{Msg: &wavespanv1.ScanResponse_Trailer{Trailer: &wavespanv1.ScanTrailer{
		RowsReturned: uint64(len(rows)), FinalCompleteness: completeness,
	}}})
}

func (sc *Scanner) local(ns string, start, end []byte, limit int, now int64) []scanRow {
	sr, err := sc.store.ScanRange(ns, start, end, limit, now)
	if err != nil {
		return nil
	}
	out := make([]scanRow, len(sr))
	for i, r := range sr {
		out[i] = scanRow{key: r.Key, value: r.Value, version: r.Version, expiresAtMs: r.ExpiresAtMs}
	}
	return out
}

// routed asks every alive member (including self) for the subrange and merges (latest wins). The
// caller applies the row limit after the merge.
func (sc *Scanner) routed(ctx context.Context, ns string, start, end []byte, now int64) []scanRow {
	m := newRowMerge()
	for _, r := range sc.local(ns, start, end, 0, now) {
		m.add(r)
	}
	if sc.holderScan != nil {
		for _, mv := range sc.cluster.Members() {
			if mv.State != membership.StateAlive || mv.Member.MemberID == sc.self.MemberID {
				continue
			}
			rows, err := sc.holderScan.ScanLocal(ctx, mv.Member, ns, start, end, 0)
			if err != nil {
				continue // a missing holder just reduces completeness (already PARTIAL)
			}
			for _, pr := range rows {
				if now != 0 && pr.ExpiresAtUnixMs != nil && pr.GetExpiresAtUnixMs() <= now {
					continue // hide-expired
				}
				m.add(scanRow{key: pr.GetKey(), value: pr.GetValue(), version: version.FromProto(pr.GetVersion()), expiresAtMs: pr.ExpiresAtUnixMs})
			}
		}
	}
	return m.sorted()
}
