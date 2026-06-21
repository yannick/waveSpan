package storage

import "errors"

var (
	// ErrClosed is returned when operating on a closed store.
	ErrClosed = errors.New("storage: closed")
	// ErrConflict is returned when a transaction aborts due to a write-write conflict.
	ErrConflict = errors.New("storage: write-write conflict")
	// ErrUnknownColumnFamily is returned for a ColumnFamily not in the registry.
	ErrUnknownColumnFamily = errors.New("storage: unknown column family")
)
