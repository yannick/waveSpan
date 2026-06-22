package observability

import (
	"context"

	"connectrpc.com/connect"
	"github.com/cwire/wavespan/internal/security"
	wavespanv1 "github.com/cwire/wavespan/proto/wavespan/v1"
)

const inspectRowCap = 1000

// InspectLocal scans the local store over a namespace prefix/range and streams one InspectKey per
// key (design/26). Values are redacted unless include_value AND the caller is admin; key_hash is
// always present. The trailer reports COMPLETE unless truncated at the limit (then PARTIAL).
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

	recs, err := s.rstore.ScanRecords(ns, start, end)
	if err != nil {
		return connect.NewError(connect.CodeInternal, err)
	}
	truncated := false
	rows := 0
	for _, rec := range recs {
		if rows >= limit {
			truncated = true
			break
		}
		ik := &wavespanv1.InspectKey{
			LogicalPath: ns + "/" + string(rec.GetLogicalKey()),
			KeyHash:     security.KeyHash(ns, rec.GetLogicalKey()),
			Version:     rec.GetVersion(),
			Tombstone:   rec.GetTombstone(),
			Holders: []*wavespanv1.InspectHolder{{
				MemberId: s.self.MemberID, HolderClass: wavespanv1.HolderClass_HOLDER_DURABLE, Version: rec.GetVersion(),
			}},
		}
		if rec.ExpiresAtUnixMs != nil {
			ik.ExpiresAtUnixMs = rec.ExpiresAtUnixMs
		}
		if reveal && !rec.GetTombstone() {
			ik.Value = rec.GetValue().GetInline()
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
