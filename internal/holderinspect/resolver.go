// Package holderinspect resolves a single key's holders across the alive members of one cluster.
// It is the Layer 1 building block of the Global Data Browser (design/26): a point fan-out that
// mirrors the cluster-wide InspectLocal merge, specialized to an exact key. It owns the serving
// node's own holder (read from the local record store), so callers must NOT add self separately.
package holderinspect

import (
	"context"
	"fmt"
	"sort"

	"github.com/yannick/wavespan/internal/membership"
	"github.com/yannick/wavespan/internal/version"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
	"google.golang.org/protobuf/proto"
)

// MemberSource yields the current membership roster (satisfied by *membership.Service).
type MemberSource interface{ Members() []membership.MemberView }

// ReplicaFetcher fetches one key's local winning record from a member (satisfied by
// *local.ConnectReplicator via its FetchReplica client method).
type ReplicaFetcher interface {
	FetchReplica(ctx context.Context, target membership.Member, namespace string, key []byte) (*wavespanv1.FetchReplicaResponse, error)
}

// LocalRecordFn reads the serving node's own winning record for a key (satisfied by
// recordstore.Store.GetRecord).
type LocalRecordFn func(namespace string, key []byte) (*wavespanv1.StoredRecord, bool, error)

// ClusterResolver resolves a key across this cluster's alive members.
type ClusterResolver struct {
	self    membership.Member
	members MemberSource
	fetch   ReplicaFetcher
	local   LocalRecordFn
}

// New builds a ClusterResolver.
func New(self membership.Member, members MemberSource, fetch ReplicaFetcher, local LocalRecordFn) *ClusterResolver {
	return &ClusterResolver{self: self, members: members, fetch: fetch, local: local}
}

// ResolveKey returns the holders within this cluster, the latest record observed (nil if none),
// whether every alive member answered (complete), and warnings for unreachable members. reveal
// gates whether the returned record carries its inline value. Best-effort: an unreachable member
// flips complete=false and adds a warning, never an error.
func (r *ClusterResolver) ResolveKey(ctx context.Context, ns string, key []byte, reveal bool) (holders []*wavespanv1.InspectHolder, best *wavespanv1.StoredRecord, complete bool, warnings []string) {
	complete = true
	consider := func(memberID string, rec *wavespanv1.StoredRecord) {
		holders = append(holders, &wavespanv1.InspectHolder{
			MemberId:    memberID,
			HolderClass: wavespanv1.HolderClass_HOLDER_DURABLE,
			Version:     rec.GetVersion(),
		})
		if best == nil || version.FromProto(rec.GetVersion()).Compare(version.FromProto(best.GetVersion())) > 0 {
			best = rec
		}
	}

	// Self via the local store (Layer 1 owns the self-holder).
	if rec, found, err := r.local(ns, key); err == nil && found {
		consider(r.self.MemberID, rec)
	}

	// Other alive members via a point FetchReplica.
	for _, mv := range r.members.Members() {
		if mv.Member.MemberID == r.self.MemberID || mv.State != membership.StateAlive {
			continue
		}
		resp, err := r.fetch.FetchReplica(ctx, mv.Member, ns, key)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("member %s unreachable: %v", mv.Member.MemberID, err))
			complete = false
			continue
		}
		if resp.GetFound() {
			consider(mv.Member.MemberID, resp.GetRecord())
		}
	}

	if best != nil {
		best = redact(best, reveal)
	}
	sortHolders(holders)
	return holders, best, complete, warnings
}

// redact returns rec with its inline value stripped unless reveal is set (and it is not a
// tombstone). It deep-copies via proto.Clone so the caller's store record is never mutated.
// NOTE: never shallow-copy a proto message (`clone := *rec`) — StoredRecord embeds a
// protoimpl.MessageState containing a sync.Mutex, which trips go vet's copylocks check.
func redact(rec *wavespanv1.StoredRecord, reveal bool) *wavespanv1.StoredRecord {
	if reveal && !rec.GetTombstone() {
		return rec
	}
	clone := proto.Clone(rec).(*wavespanv1.StoredRecord)
	clone.Value = nil
	return clone
}

// sortHolders orders by (peer_cluster_id, member_id) so identical requests yield identical lists.
func sortHolders(hs []*wavespanv1.InspectHolder) {
	sort.Slice(hs, func(i, j int) bool {
		if hs[i].GetPeerClusterId() != hs[j].GetPeerClusterId() {
			return hs[i].GetPeerClusterId() < hs[j].GetPeerClusterId()
		}
		return hs[i].GetMemberId() < hs[j].GetMemberId()
	})
}
