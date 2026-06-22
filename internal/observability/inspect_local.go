package observability

import (
	"context"
	"sort"

	"connectrpc.com/connect"
	"github.com/cwire/wavespan/internal/membership"
	"github.com/cwire/wavespan/internal/security"
	"github.com/cwire/wavespan/internal/version"
	wavespanv1 "github.com/cwire/wavespan/proto/wavespan/v1"
)

const inspectRowCap = 1000

// InspectLocal scans a namespace prefix/range and streams one InspectKey per key (design/26). With
// cluster_wide set (and a cluster scanner wired), it fans the scan out to every alive member and
// merges by key — latest version wins, and each key's Holders list shows which nodes hold it — so
// the Data Browser can see the whole cluster's KV, not just this node's. Otherwise it scans only the
// local store. Values are redacted unless include_value AND the caller is admin; key_hash is always
// present. The trailer reports COMPLETE unless truncated at the limit (then PARTIAL).
func (s *ObsService) InspectLocal(ctx context.Context, req *connect.Request[wavespanv1.InspectLocalRequest], stream *connect.ServerStream[wavespanv1.InspectRow]) error {
	m := req.Msg
	ns := m.GetNamespace()
	start, end := m.GetStartKey(), m.GetEndKey()
	if len(m.GetPrefix()) > 0 {
		start = m.GetPrefix()
		end = prefixEnd(m.GetPrefix())
	}
	limit := int(m.GetLimit())
	if limit <= 0 || limit > inspectRowCap {
		limit = inspectRowCap
	}

	role := security.RoleFrom(ctx)
	reveal := m.GetIncludeValue() && role == security.RoleAdmin

	if err := stream.Send(&wavespanv1.InspectRow{Row: &wavespanv1.InspectRow_Header{Header: &wavespanv1.ResponseMeta{
		ServedByClusterId: s.self.ClusterID, ServedByMemberId: s.self.MemberID, Source: wavespanv1.ReadSource_LOCAL_DURABLE,
	}}}); err != nil {
		return err
	}

	keys, err := s.collectInspectKeys(ctx, m, ns, start, end, reveal)
	if err != nil {
		return connect.NewError(connect.CodeInternal, err)
	}

	truncated := false
	rows := 0
	for _, ik := range keys {
		if rows >= limit {
			truncated = true
			break
		}
		if err := stream.Send(&wavespanv1.InspectRow{Row: &wavespanv1.InspectRow_Key{Key: ik}}); err != nil {
			return err
		}
		rows++
	}

	completeness := wavespanv1.Completeness_COMPLETE
	if truncated {
		completeness = wavespanv1.Completeness_PARTIAL
	}
	return stream.Send(&wavespanv1.InspectRow{Row: &wavespanv1.InspectRow_Trailer{Trailer: &wavespanv1.InspectTrailer{
		RowsReturned: uint64(rows), FinalCompleteness: completeness,
	}}})
}

// collectInspectKeys builds the (key-sorted) InspectKey list, either from the local store only or,
// when cluster_wide is requested and a cluster scanner is wired, merged across all alive members.
func (s *ObsService) collectInspectKeys(ctx context.Context, m *wavespanv1.InspectLocalRequest, ns string, start, end []byte, reveal bool) ([]*wavespanv1.InspectKey, error) {
	recs, err := s.rstore.ScanRecords(ns, start, end)
	if err != nil {
		return nil, err
	}

	merged := map[string]*mergedKey{}
	order := []string{}
	upsert := func(logicalKey []byte, ver *wavespanv1.Version, val []byte, tombstone bool, expires *int64, holder string) {
		k := string(logicalKey)
		mk := merged[k]
		if mk == nil {
			mk = &mergedKey{logicalKey: logicalKey, holders: map[string]*wavespanv1.Version{}}
			merged[k] = mk
			order = append(order, k)
		}
		mk.holders[holder] = ver
		// Latest version wins for the surfaced value/version/tombstone.
		if mk.version == nil || version.FromProto(ver).Compare(version.FromProto(mk.version)) > 0 {
			mk.version, mk.value, mk.tombstone, mk.expires = ver, val, tombstone, expires
		}
	}

	for _, rec := range recs {
		var val []byte
		if reveal && !rec.GetTombstone() {
			val = rec.GetValue().GetInline()
		}
		upsert(rec.GetLogicalKey(), rec.GetVersion(), val, rec.GetTombstone(), rec.ExpiresAtUnixMs, s.self.MemberID)
	}

	if m.GetClusterWide() && s.clusterScan != nil {
		for _, mv := range s.cluster.Members() {
			if mv.Member.MemberID == s.self.MemberID || mv.State != membership.StateAlive {
				continue
			}
			rowsP, scanErr := s.clusterScan.ScanLocal(ctx, mv.Member, ns, start, end, 0)
			if scanErr != nil {
				continue // best-effort: a missing peer must not fail the whole browse
			}
			for _, row := range rowsP {
				var val []byte
				if reveal {
					val = row.GetValue()
				}
				// ScanLocalRow carries no tombstone flag; a peer-sourced winner is shown as live.
				upsert(row.GetKey(), row.GetVersion(), val, false, row.ExpiresAtUnixMs, mv.Member.MemberID)
			}
		}
	}

	sort.Strings(order)
	out := make([]*wavespanv1.InspectKey, 0, len(order))
	for _, k := range order {
		out = append(out, merged[k].toInspectKey(ns))
	}
	return out, nil
}

// mergedKey accumulates one logical key seen on one or more members.
type mergedKey struct {
	logicalKey []byte
	version    *wavespanv1.Version
	value      []byte
	tombstone  bool
	expires    *int64
	holders    map[string]*wavespanv1.Version // memberID -> the version that member holds
}

func (mk *mergedKey) toInspectKey(ns string) *wavespanv1.InspectKey {
	ik := &wavespanv1.InspectKey{
		LogicalPath: ns + "/" + string(mk.logicalKey),
		KeyHash:     security.KeyHash(ns, mk.logicalKey),
		LogicalKey:  mk.logicalKey,
		Version:     mk.version,
		Tombstone:   mk.tombstone,
	}
	if mk.expires != nil {
		ik.ExpiresAtUnixMs = mk.expires
	}
	if len(mk.value) > 0 {
		ik.Value = mk.value
	}
	// Stable holder order so the UI rows don't shuffle between requests.
	ids := make([]string, 0, len(mk.holders))
	for id := range mk.holders {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		ik.Holders = append(ik.Holders, &wavespanv1.InspectHolder{
			MemberId: id, HolderClass: wavespanv1.HolderClass_HOLDER_DURABLE, Version: mk.holders[id],
		})
	}
	return ik
}

func prefixEnd(p []byte) []byte {
	end := append([]byte(nil), p...)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] != 0xff {
			end[i]++
			return end[:i+1]
		}
	}
	return nil
}
