package backup

import (
	"bytes"
	"encoding/json"
	"io"
)

// clusterManifestFormatVersion is the current cluster.manifest schema version.
const clusterManifestFormatVersion = 1

// TopologyEntry records one source node's identity at capture time (member id + storage uuid), so a
// restore can map exported state back to (or away from) the original topology.
type TopologyEntry struct {
	MemberID    string `json:"member_id"`
	StorageUUID string `json:"storage_uuid,omitempty"`
}

// PerNodeRef points the cluster manifest at one node's sub-manifest(s) and records its export counts.
// PhysicalManifest/PhysicalGlobalSeq are set when the physical plane ran (3b) — restore and incremental
// chaining read them to find each node's checkpoint and parent watermark.
type PerNodeRef struct {
	MemberID          string `json:"member_id"`
	Ref               string `json:"ref"` // object key of the node's node.manifest.json (logical)
	Objects           int64  `json:"objects"`
	Bytes             int64  `json:"bytes"`
	PhysicalManifest  string `json:"physical_manifest,omitempty"` // object key of the node's physical.manifest.json
	PhysicalGlobalSeq uint64 `json:"physical_global_seq,omitempty"`
}

// ClusterManifest is the authoritative top-level record of a cluster backup: the chosen frontier, the
// capture topology, the per-node sub-manifest references, and the committed status (COMPLETE or PARTIAL
// with enumerated gaps). It is written once at commit and is the entry point a restore reads.
type ClusterManifest struct {
	FormatVersion      int             `json:"format_version"`
	BackupID           string          `json:"backup_id"`
	FrontierT          int64           `json:"frontier_t"`
	CaptureWallClockMs int64           `json:"capture_wall_clock_ms"`
	Planes             []string        `json:"planes"`
	Parent             string          `json:"parent,omitempty"`
	SourceTopology     []TopologyEntry `json:"source_topology"`
	NamespaceInventory []string        `json:"namespace_inventory,omitempty"`
	PerNode            []PerNodeRef    `json:"per_node"`
	Status             string          `json:"status"`
	Gaps               []string        `json:"gaps,omitempty"`
	// PerObjectSha256 maps object key -> content hash. Content addressing/verification is a later-phase
	// refinement; it is present (and empty) in 3a so the schema is stable.
	PerObjectSha256 map[string]string `json:"per_object_sha256,omitempty"`
}

// ClusterManifestKey is the object key the cluster manifest is written under for a backup.
func ClusterManifestKey(backupID string) string { return backupID + "/cluster.manifest.json" }

// WriteClusterManifest serializes m and writes it to store under its backup's manifest key.
func WriteClusterManifest(store ObjectStore, m *ClusterManifest) error {
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	return store.Put(ClusterManifestKey(m.BackupID), bytes.NewReader(b), int64(len(b)))
}

// ReadClusterManifest reads and decodes the cluster manifest for backupID.
func ReadClusterManifest(store ObjectStore, backupID string) (*ClusterManifest, error) {
	rc, err := store.Get(ClusterManifestKey(backupID))
	if err != nil {
		return nil, err
	}
	defer func() { _ = rc.Close() }()
	b, err := io.ReadAll(rc)
	if err != nil {
		return nil, err
	}
	var m ClusterManifest
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	return &m, nil
}
