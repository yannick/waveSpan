package backup

import (
	"bytes"
	"encoding/json"
	"io"
	"path"

	"wavesdb"
)

// physicalManifestFormatVersion is the per-node physical sub-manifest schema version.
const physicalManifestFormatVersion = 1

// PhysicalTable mirrors wavesdb.CheckpointTable in a JSON-stable form (one exported SSTable).
type PhysicalTable struct {
	CF       string `json:"cf"`
	ID       uint64 `json:"id"`
	MaxSeq   uint64 `json:"max_seq"`
	KlogSize int64  `json:"klog_size"`
	VlogSize int64  `json:"vlog_size"`
}

// PhysicalManifest is a node's per-backup physical checkpoint record, persisted alongside the logical
// node manifest. It captures the full cumulative table set as of this backup (so a restore can
// reconstitute the node) plus the parent watermark for chain bookkeeping. An incremental records the
// SAME full table set as a full would — only the uploaded objects differ (parent ids are skipped).
type PhysicalManifest struct {
	FormatVersion   int             `json:"format_version"`
	GlobalSeq       uint64          `json:"global_seq"`
	NextFileID      uint64          `json:"next_file_id"`
	ParentGlobalSeq uint64          `json:"parent_global_seq,omitempty"` // 0 for a full
	Tables          []PhysicalTable `json:"tables"`
}

// physicalManifestFromCheckpoint converts a wavesdb checkpoint result into the persisted form, recording
// the parent watermark (0 for a full).
func physicalManifestFromCheckpoint(cm *wavesdb.CheckpointManifest, parentGlobalSeq uint64) *PhysicalManifest {
	pm := &PhysicalManifest{
		FormatVersion:   physicalManifestFormatVersion,
		GlobalSeq:       cm.GlobalSeq,
		NextFileID:      cm.NextFileID,
		ParentGlobalSeq: parentGlobalSeq,
	}
	for _, t := range cm.Tables {
		pm.Tables = append(pm.Tables, PhysicalTable{CF: t.CF, ID: t.ID, MaxSeq: t.MaxSeq, KlogSize: t.KlogSize, VlogSize: t.VlogSize})
	}
	return pm
}

// ToCheckpointManifest rebuilds the wavesdb checkpoint manifest so this backup can be used as the parent
// of an incremental (the engine diffs by table id against it).
func (pm *PhysicalManifest) ToCheckpointManifest() *wavesdb.CheckpointManifest {
	cm := &wavesdb.CheckpointManifest{GlobalSeq: pm.GlobalSeq, NextFileID: pm.NextFileID}
	for _, t := range pm.Tables {
		cm.Tables = append(cm.Tables, wavesdb.CheckpointTable{CF: t.CF, ID: t.ID, MaxSeq: t.MaxSeq, KlogSize: t.KlogSize, VlogSize: t.VlogSize})
	}
	return cm
}

// PhysicalManifestKey is the object key of a node's physical sub-manifest for a backup.
func PhysicalManifestKey(backupID, memberID string) string {
	return path.Join(backupID, "nodes", memberID, "physical.manifest.json")
}

// WritePhysicalManifest serializes pm and writes it to store under key.
func WritePhysicalManifest(store ObjectStore, key string, pm *PhysicalManifest) error {
	b, err := json.MarshalIndent(pm, "", "  ")
	if err != nil {
		return err
	}
	return store.Put(key, bytes.NewReader(b), int64(len(b)))
}

// ReadPhysicalManifest reads and decodes a physical sub-manifest from store.
func ReadPhysicalManifest(store ObjectStore, key string) (*PhysicalManifest, error) {
	rc, err := store.Get(key)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rc.Close() }()
	b, err := io.ReadAll(rc)
	if err != nil {
		return nil, err
	}
	var pm PhysicalManifest
	if err := json.Unmarshal(b, &pm); err != nil {
		return nil, err
	}
	return &pm, nil
}
