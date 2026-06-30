package backup

import (
	"bytes"
	"context"
	"encoding/gob"
	"sort"
)

// intentSchemaVersion is the current BackupIntent schema version, stamped into every persisted intent
// (the durable meta-shard blob). A reader can branch on it if the layout ever changes; gob already
// tolerates added fields, this guards against an incompatible reinterpretation of existing ones.
const intentSchemaVersion = 1

// Status is a backup's lifecycle state (mirrors proto BackupStatus).
// These enum values are gob-persisted positionally in BackupIntent — append-only; never reorder.
type Status int32

const (
	StatusUnspecified Status = iota
	StatusRunning
	StatusComplete
	StatusPartial // some ranges had no live holder; gaps enumerated
	StatusFailed
)

// Phase is the coordinator's current phase (mirrors proto BackupPhase).
// These enum values are gob-persisted positionally in BackupIntent — append-only; never reorder.
type Phase int32

const (
	PhaseUnspecified Phase = iota
	PhaseAssign
	PhasePrepare
	PhaseExport
	PhaseCommit
)

// Plane selects the logical and/or physical export plane (mirrors proto BackupPlane).
// These enum values are gob-persisted positionally in BackupIntent — append-only; never reorder.
type Plane int32

const (
	PlaneUnspecified Plane = iota
	PlaneLogical
	PlanePhysical
)

// Descriptor describes an object-store target without ever carrying a raw secret: SecretName is a
// reference (e.g. a k8s secret name) resolved out of band at run time. An empty descriptor means the
// node's configured default store. DefaultFS marks the local FS fallback (dev) so it is distinguished
// from a real destination without relying on a magic Name (a named destination could legitimately be
// called anything).
type Descriptor struct {
	Name         string
	Bucket       string
	Prefix       string
	Region       string
	Endpoint     string
	UseSSL       bool
	UsePathStyle bool
	SecretName   string
	DefaultFS    bool
}

// NodeRecord is one node's recorded participation in a backup: its phase, export counts, the key of the
// per-node sub-manifest it wrote, the ranges it reported holding at prepare time, and its stable storage
// identity (needed by 3c physical restore to match exported state back to a node).
type NodeRecord struct {
	MemberID            string
	Phase               Phase
	Objects             int64
	Bytes               int64
	Done                bool
	SubManifestKey      string
	HeldRanges          []string
	StorageUUID         string
	PhysicalManifestKey string // physical plane only — key of this node's physical.manifest.json
	PhysicalGlobalSeq   uint64 // physical plane only — the checkpoint cut sequence
}

// BackupIntent is the durable catalog record of a backup, stored as a blob in the meta shard. It is the
// single source of truth a coordinator (or a new coordinator resuming after failure) reads to know what
// to do next: the chosen frontier, the assignment plan, per-node progress, and the current phase/status.
// It never holds raw secrets (the destination carries only a credential reference).
type BackupIntent struct {
	SchemaVersion      int
	BackupID           string
	FrontierT          int64
	CaptureWallClockMs int64
	Selection          Selector
	Planes             []Plane
	Parent             string
	Destination        Descriptor
	Status             Status
	Phase              Phase
	LeaseDeadlineMs    int64
	RetainUntilMs      int64
	PerNode            []NodeRecord
	Gaps               []string
	StartedMs          int64
	FinishedMs         int64
	Assignments        map[string]Selector // member id -> the ranges/namespaces it was asked to export
}

// MarshalIntent serializes a BackupIntent to a self-describing gob blob. gob is forward-compatible:
// fields added by a newer writer are ignored by an older reader, and fields a newer reader expects but
// an older blob lacks decode to their zero value.
func MarshalIntent(in *BackupIntent) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(in); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// UnmarshalIntent decodes a gob blob produced by MarshalIntent.
func UnmarshalIntent(b []byte) (*BackupIntent, error) {
	var in BackupIntent
	if err := gob.NewDecoder(bytes.NewReader(b)).Decode(&in); err != nil {
		return nil, err
	}
	return &in, nil
}

// MetaStore is the durable blob catalog the intent helpers persist to: the meta-shard backup sub-space
// (design/backup phase 3a). It is an interface so the coordinator unit tests can substitute an in-memory
// fake; the production implementation proposes opBackupPut/opBackupDelete to the meta Raft group and
// reads via its Lookup (internal/collections).
type MetaStore interface {
	PutBlob(ctx context.Context, key string, blob []byte) error
	GetBlob(ctx context.Context, key string) ([]byte, bool, error)
	ListBlobs(ctx context.Context) (map[string][]byte, error)
	DeleteBlob(ctx context.Context, key string) error
}

// PutIntent serializes and durably stores an intent under its BackupID.
func PutIntent(ctx context.Context, store MetaStore, in *BackupIntent) error {
	blob, err := MarshalIntent(in)
	if err != nil {
		return err
	}
	return store.PutBlob(ctx, in.BackupID, blob)
}

// GetIntent loads the intent for id; found is false when no such intent exists.
func GetIntent(ctx context.Context, store MetaStore, id string) (*BackupIntent, bool, error) {
	blob, found, err := store.GetBlob(ctx, id)
	if err != nil || !found {
		return nil, found, err
	}
	in, err := UnmarshalIntent(blob)
	if err != nil {
		return nil, false, err
	}
	return in, true, nil
}

// ListIntents loads every intent from the catalog, ordered by BackupID for a stable listing.
func ListIntents(ctx context.Context, store MetaStore) ([]*BackupIntent, error) {
	blobs, err := store.ListBlobs(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]*BackupIntent, 0, len(blobs))
	for _, b := range blobs {
		in, err := UnmarshalIntent(b)
		if err != nil {
			return nil, err
		}
		out = append(out, in)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].BackupID < out[j].BackupID })
	return out, nil
}

// DeleteIntent removes the intent for id from the catalog (object GC is Phase 3d).
func DeleteIntent(ctx context.Context, store MetaStore, id string) error {
	return store.DeleteBlob(ctx, id)
}
