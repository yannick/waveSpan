package storage

import (
	"crypto/rand"
	"fmt"
)

// storageUUIDKey is where durable storage identity lives in column family sys.
const storageUUIDKey = "/sys/storage_uuid"

// EnsureStorageUUID returns the store's durable storage UUID, generating and persisting a
// new one on first open (design/02_storage_wavesdb.md "Crash recovery" steps 1-3). This
// identity is distinct from the runtime memberId: a pod rescheduled onto an empty volume
// gets a new storage UUID and is treated as a new storage member
// (design/04_membership_latency_gossip.md, design/00 "Spot-node assumption").
func EnsureStorageUUID(s LocalStore) (string, error) {
	v, found, err := s.Get(CFSys, []byte(storageUUIDKey))
	if err != nil {
		return "", err
	}
	if found {
		return string(v), nil
	}
	id, err := newUUIDv4()
	if err != nil {
		return "", err
	}
	if err := s.Put(CFSys, []byte(storageUUIDKey), []byte(id)); err != nil {
		return "", err
	}
	return id, nil
}

// newUUIDv4 generates a random RFC 4122 version-4 UUID string without external deps.
func newUUIDv4() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant 10
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}
