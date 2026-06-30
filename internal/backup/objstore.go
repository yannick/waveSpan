package backup

import (
	"io"
	"time"
)

// ObjectStore is the minimal object-storage surface the backup engine needs.
// wavesdb/objstore.FS and the S3 backend satisfy it structurally.
type ObjectStore interface {
	Put(key string, r io.Reader, size int64) error
	Get(key string) (io.ReadCloser, error)
	List(prefix string) ([]string, error)
	Exists(key string) (bool, error)
	// Delete removes key (used by lifecycle GC, Phase 3d). Deleting a missing key is not an error.
	Delete(key string) error
	// ModTime returns key's last-modified time. Orphan reconciliation uses it to apply an age grace
	// period so a just-written object is never reaped by a racing sweep (Phase 3c hardening).
	ModTime(key string) (time.Time, error)
}
