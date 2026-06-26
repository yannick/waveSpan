package global

import (
	"context"
	"fmt"
	"sort"

	"github.com/yannick/wavespan/internal/config"
	"github.com/yannick/wavespan/internal/rpcopts"
	"github.com/yannick/wavespan/internal/version"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
	"google.golang.org/grpc"
)

// inspectKeyClient is the slice of GlobalReplicationClient that PeerInspector uses (one method),
// so tests can fake it without a real connection.
type inspectKeyClient interface {
	InspectKey(ctx context.Context, in *wavespanv1.InspectKeyRequest, opts ...grpc.CallOption) (*wavespanv1.InspectKeyResponse, error)
}

// PeerInspector resolves a key across configured peer clusters (Layer 2, design/06+26). For each
// peer it calls GlobalReplication.InspectKey; the peer runs its own within-cluster resolution and
// returns holders tagged with its cluster_id. Best-effort: an unreachable peer yields a warning
// and PARTIAL, never an error.
type PeerInspector struct {
	selfCluster string
	peers       []config.ClusterPeer
	dial        func(endpoint string) (inspectKeyClient, error)
}

// NewPeerInspector builds the inspector dialling peers over the pooled gRPC connections (mirrors
// Sender.client).
func NewPeerInspector(selfCluster string, peers []config.ClusterPeer) *PeerInspector {
	return newPeerInspectorWithDial(selfCluster, peers, func(endpoint string) (inspectKeyClient, error) {
		conn, err := rpcopts.GRPCConn(endpoint)
		if err != nil {
			return nil, err
		}
		return wavespanv1.NewGlobalReplicationClient(conn), nil
	})
}

func newPeerInspectorWithDial(selfCluster string, peers []config.ClusterPeer, dial func(string) (inspectKeyClient, error)) *PeerInspector {
	return &PeerInspector{selfCluster: selfCluster, peers: peers, dial: dial}
}

// InspectPeers returns aggregated peer holders, the latest peer record, completeness, and warnings.
func (p *PeerInspector) InspectPeers(ctx context.Context, ns string, key []byte, reveal bool) (holders []*wavespanv1.InspectHolder, best *wavespanv1.StoredRecord, complete bool, warnings []string) {
	complete = true
	for _, peer := range p.peers {
		if peer.ReplEndpoint == "" || peer.ClusterID == p.selfCluster {
			continue
		}
		cl, err := p.dial(peer.ReplEndpoint)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("peer %s (%s) dial failed: %v", peer.ClusterID, peer.ReplEndpoint, err))
			complete = false
			continue
		}
		resp, err := cl.InspectKey(ctx, &wavespanv1.InspectKeyRequest{Namespace: ns, Key: key, IncludeValue: reveal})
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("peer %s (%s) unreachable: %v", peer.ClusterID, peer.ReplEndpoint, err))
			complete = false
			continue
		}
		holders = append(holders, resp.GetHolders()...)
		warnings = append(warnings, resp.GetWarnings()...)
		if !resp.GetComplete() {
			complete = false
		}
		if rec := resp.GetBest(); rec != nil {
			if best == nil || version.FromProto(rec.GetVersion()).Compare(version.FromProto(best.GetVersion())) > 0 {
				best = rec
			}
		}
	}
	sort.Slice(holders, func(i, j int) bool {
		if holders[i].GetPeerClusterId() != holders[j].GetPeerClusterId() {
			return holders[i].GetPeerClusterId() < holders[j].GetPeerClusterId()
		}
		return holders[i].GetMemberId() < holders[j].GetMemberId()
	})
	return holders, best, complete, warnings
}

// BuildPeerResponse stamps each holder with this cluster's id and wraps a Layer 1 result as the
// InspectKey RPC response. Used by the gRPC GlobalReplication adapter's InspectKey handler.
func BuildPeerResponse(selfCluster string, holders []*wavespanv1.InspectHolder, best *wavespanv1.StoredRecord, complete bool, warnings []string) *wavespanv1.InspectKeyResponse {
	for _, h := range holders {
		h.PeerClusterId = selfCluster
	}
	return &wavespanv1.InspectKeyResponse{Holders: holders, Best: best, Complete: complete, Warnings: warnings}
}
