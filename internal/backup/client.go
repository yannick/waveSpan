package backup

import (
	"context"
	"path"
	"sync"

	"github.com/yannick/wavespan/internal/rpcopts"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

// AllExportAssigner is the 3a full-backup assignment policy: every live node exports its entire local
// state (the "all-export fallback"). For a full logical backup this covers the keyspace — each node
// holds its shards' authoritative data — so the union is complete and there are no gaps as long as at
// least one node is live. (Owner-narrowed assignment from the range directory is a later refinement;
// the coordinator's gap/PARTIAL machinery is exercised by narrower assigners.)
type AllExportAssigner struct{}

// Assign returns a full-export assignment for every live member; an empty live set is itself a gap.
func (AllExportAssigner) Assign(live []string) (map[string]Selector, []string) {
	if len(live) == 0 {
		return map[string]Selector{}, []string{"no live members to export"}
	}
	m := make(map[string]Selector, len(live))
	for _, id := range live {
		m[id] = Selector{}
	}
	return m, nil
}

// grpcNodeClient is a NodeClient that calls a peer's BackupService over the gRPC data port.
type grpcNodeClient struct{ c wavespanv1.BackupServiceClient }

func (g grpcNodeClient) Prepare(ctx context.Context, backupID string, frontierT int64) (PrepareResult, error) {
	res, err := g.c.PrepareBackup(ctx, &wavespanv1.PrepareBackupRequest{BackupId: backupID, FrontierT: frontierT})
	if err != nil {
		return PrepareResult{}, err
	}
	return PrepareResult{GlobalSeq: res.GetGlobalSeq(), HeldRanges: res.GetHeldRanges()}, nil
}

func (g grpcNodeClient) Export(ctx context.Context, req ExportRequest) (ExportResult, error) {
	res, err := g.c.ExportBackup(ctx, &wavespanv1.ExportBackupRequest{
		BackupId:   req.BackupID,
		FrontierT:  req.FrontierT,
		Assignment: selectionToProto(req.Assignment),
		Planes:     planesToProto(req.Planes),
		KeyPrefix:  path.Join(req.BackupID, "nodes", req.MemberID),
	})
	if err != nil {
		return ExportResult{}, err
	}
	return ExportResult{Objects: res.GetObjects(), Bytes: res.GetBytes(), SubManifestKey: res.GetSubManifestKey()}, nil
}

// NewGRPCClientFactory builds a ClientFactory that dials each member's BackupService over gRPC. addrFor
// resolves a member id to its data-port address (e.g. from gossip membership); dialed clients are cached
// per member. Dialing reuses the shared rpcopts pooled, mTLS-secured connections (as the write
// forwarder does), so the coordinator fans prepare/export out over the same secured data plane.
func NewGRPCClientFactory(addrFor func(memberID string) (string, error)) ClientFactory {
	var mu sync.Mutex
	clients := map[string]NodeClient{}
	return func(memberID string) (NodeClient, error) {
		mu.Lock()
		defer mu.Unlock()
		if c, ok := clients[memberID]; ok {
			return c, nil
		}
		addr, err := addrFor(memberID)
		if err != nil {
			return nil, err
		}
		conn, err := rpcopts.GRPCConn(addr)
		if err != nil {
			return nil, err
		}
		c := grpcNodeClient{c: wavespanv1.NewBackupServiceClient(conn)}
		clients[memberID] = c
		return c, nil
	}
}
