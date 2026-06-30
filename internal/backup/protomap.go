package backup

import wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"

// This file maps between the backup engine's domain types and the BackupService proto. Keeping the
// mapping in the backup package (which already depends on the dependency-free proto package) lets the
// collections service layer stay a thin transport adapter and avoids an import cycle.

func setFromSlice(ss []string) map[string]struct{} {
	if len(ss) == 0 {
		return nil
	}
	return Set(ss...)
}

func selectorFromProto(s *wavespanv1.Selection) Selector {
	if s == nil {
		return Selector{}
	}
	return Selector{
		Namespaces:        setFromSlice(s.GetNamespaces()),
		Graphs:            setFromSlice(s.GetGraphs()),
		VectorCollections: setFromSlice(s.GetVectorCollections()),
	}
}

func planesFromProto(ps []wavespanv1.BackupPlane) []Plane {
	if len(ps) == 0 {
		return nil
	}
	out := make([]Plane, 0, len(ps))
	for _, p := range ps {
		switch p {
		case wavespanv1.BackupPlane_BACKUP_PLANE_PHYSICAL:
			out = append(out, PlanePhysical)
		default:
			out = append(out, PlaneLogical)
		}
	}
	return out
}

func sliceFromSet(set map[string]struct{}) []string {
	if len(set) == 0 {
		return nil
	}
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	return out
}

func selectionToProto(s Selector) *wavespanv1.Selection {
	if s.IsEmpty() {
		return nil
	}
	return &wavespanv1.Selection{
		Namespaces:        sliceFromSet(s.Namespaces),
		Graphs:            sliceFromSet(s.Graphs),
		VectorCollections: sliceFromSet(s.VectorCollections),
	}
}

func planesToProto(ps []Plane) []wavespanv1.BackupPlane {
	out := make([]wavespanv1.BackupPlane, 0, len(ps))
	for _, p := range ps {
		if p == PlanePhysical {
			out = append(out, wavespanv1.BackupPlane_BACKUP_PLANE_PHYSICAL)
		} else {
			out = append(out, wavespanv1.BackupPlane_BACKUP_PLANE_LOGICAL)
		}
	}
	return out
}

func planesToStrings(ps []Plane) []string {
	out := make([]string, 0, len(ps))
	for _, p := range ps {
		if p == PlanePhysical {
			out = append(out, "physical")
		} else {
			out = append(out, "logical")
		}
	}
	return out
}

func descriptorToProto(d Descriptor) *wavespanv1.Destination {
	dst := &wavespanv1.Destination{
		Name:         d.Name,
		Bucket:       d.Bucket,
		Prefix:       d.Prefix,
		Region:       d.Region,
		Endpoint:     d.Endpoint,
		UseSsl:       d.UseSSL,
		UsePathStyle: d.UsePathStyle,
	}
	if d.SecretName != "" {
		dst.Credential = &wavespanv1.CredentialRef{SecretName: d.SecretName}
	}
	return dst
}

func statusToProto(s Status) wavespanv1.BackupStatus {
	switch s {
	case StatusRunning:
		return wavespanv1.BackupStatus_BACKUP_RUNNING
	case StatusComplete:
		return wavespanv1.BackupStatus_BACKUP_COMPLETE
	case StatusPartial:
		return wavespanv1.BackupStatus_BACKUP_PARTIAL
	case StatusFailed:
		return wavespanv1.BackupStatus_BACKUP_FAILED
	default:
		return wavespanv1.BackupStatus_BACKUP_STATUS_UNSPECIFIED
	}
}

func statusString(s Status) string {
	switch s {
	case StatusRunning:
		return "RUNNING"
	case StatusComplete:
		return "COMPLETE"
	case StatusPartial:
		return "PARTIAL"
	case StatusFailed:
		return "FAILED"
	default:
		return "UNSPECIFIED"
	}
}

func phaseToProto(p Phase) wavespanv1.BackupPhase {
	switch p {
	case PhaseAssign:
		return wavespanv1.BackupPhase_BACKUP_PHASE_ASSIGN
	case PhasePrepare:
		return wavespanv1.BackupPhase_BACKUP_PHASE_PREPARE
	case PhaseExport:
		return wavespanv1.BackupPhase_BACKUP_PHASE_EXPORT
	case PhaseCommit:
		return wavespanv1.BackupPhase_BACKUP_PHASE_COMMIT
	default:
		return wavespanv1.BackupPhase_BACKUP_PHASE_UNSPECIFIED
	}
}

func intentToState(in *Intent) *wavespanv1.BackupState {
	perNode := make([]*wavespanv1.NodeProgress, 0, len(in.PerNode))
	done := 0
	for _, n := range in.PerNode {
		if n.Done {
			done++
		}
		perNode = append(perNode, &wavespanv1.NodeProgress{
			MemberId: n.MemberID,
			Phase:    phaseToProto(n.Phase),
			Objects:  n.Objects,
			Bytes:    n.Bytes,
			Done:     n.Done,
		})
	}
	var pct float64
	switch {
	case in.Status == StatusComplete || in.Status == StatusPartial:
		pct = 100
	case len(in.PerNode) > 0:
		pct = float64(done) / float64(len(in.PerNode)) * 100
	}
	return &wavespanv1.BackupState{
		BackupId:    in.BackupID,
		Status:      statusToProto(in.Status),
		Phase:       phaseToProto(in.Phase),
		PerNode:     perNode,
		OverallPct:  pct,
		Gaps:        in.Gaps,
		StartedMs:   in.StartedMs,
		FinishedMs:  in.FinishedMs,
		Parent:      in.Parent,
		Destination: descriptorToProto(in.Destination),
	}
}
