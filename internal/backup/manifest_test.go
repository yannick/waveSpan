package backup

import (
	"bytes"
	"testing"
)

func TestNodeManifestRoundTrip(t *testing.T) {
	m := NodeManifest{
		FormatVersion:      1,
		CaptureWallClockMs: 1719000000000,
		StorageUUID:        "uuid-123",
		CFs: []CFEntry{
			{CF: "kv_data", Entries: 42, Bytes: 1000},
			{CF: "repl_data", Entries: 7, Bytes: 256},
		},
	}
	var buf bytes.Buffer
	if err := m.WriteTo(&buf); err != nil {
		t.Fatal(err)
	}
	got, err := ReadNodeManifest(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if got.FormatVersion != 1 || got.StorageUUID != "uuid-123" || len(got.CFs) != 2 {
		t.Fatalf("round-trip mismatch: %+v", got)
	}
}

func TestNodeManifestForwardCompat(t *testing.T) {
	// A manifest written by a newer version with an extra field must still parse.
	raw := []byte(`{"format_version":1,"capture_wall_clock_ms":1,"cfs":[],"future_field":{"x":1}}`)
	m, err := ReadNodeManifest(bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("forward-compat parse failed: %v", err)
	}
	if m.FormatVersion != 1 {
		t.Fatalf("want format_version 1, got %d", m.FormatVersion)
	}
}
