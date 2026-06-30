package backup

import "io"

// ObjectStore is the minimal object-storage surface the backup engine needs.
// wavesdb/objstore.FS and the S3 backend satisfy it structurally.
type ObjectStore interface {
	Put(key string, r io.Reader, size int64) error
	Get(key string) (io.ReadCloser, error)
	List(prefix string) ([]string, error)
	Exists(key string) (bool, error)
	// Delete removes key (used by lifecycle GC, Phase 3d). Deleting a missing key is not an error.
	Delete(key string) error
}
