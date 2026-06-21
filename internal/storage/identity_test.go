package storage

import (
	"bytes"
	"testing"
)

func TestStorageUUIDStableWithinStore(t *testing.T) {
	s := NewMemStore()
	id1, err := EnsureStorageUUID(s)
	if err != nil || id1 == "" {
		t.Fatalf("ensure uuid: %q %v", id1, err)
	}
	id2, err := EnsureStorageUUID(s)
	if err != nil || id2 != id1 {
		t.Fatalf("uuid changed on second call: %q != %q (%v)", id2, id1, err)
	}
}

func TestStorageUUIDPersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()

	s1, err := OpenWavesdb(dir)
	if err != nil {
		t.Fatal(err)
	}
	id1, err := EnsureStorageUUID(s1)
	if err != nil {
		t.Fatal(err)
	}
	// also write some user data to prove restart durability
	if err := s1.Put(CFKVData, []byte("durable"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := s1.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := OpenWavesdb(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s2.Close() }()

	id2, err := EnsureStorageUUID(s2)
	if err != nil {
		t.Fatal(err)
	}
	if id2 != id1 {
		t.Fatalf("storage UUID regenerated across restart: %q != %q", id2, id1)
	}
	got, found, err := s2.Get(CFKVData, []byte("durable"))
	if err != nil || !found || !bytes.Equal(got, []byte("value")) {
		t.Fatalf("data not durable across restart: (%q,%v,%v)", got, found, err)
	}
}

func TestUUIDv4Format(t *testing.T) {
	id, err := newUUIDv4()
	if err != nil {
		t.Fatal(err)
	}
	// 8-4-4-4-12 hex with version 4 and variant 10xx
	if len(id) != 36 || id[14] != '4' {
		t.Fatalf("not a v4 uuid: %q", id)
	}
}
