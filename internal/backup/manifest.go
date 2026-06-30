package backup

import (
	"encoding/json"
	"io"
)

// manifestFormatVersion is the current NodeManifest schema version. A reader
// rejects manifests whose FormatVersion exceeds this (major-version guard);
// unknown JSON fields are ignored, giving additive forward-compatibility.
const manifestFormatVersion = 1

// CFEntry summarizes one exported column family. CF is the wavesdb cf name
// string (e.g. "kv_data"), matching cf.Name().
type CFEntry struct {
	CF      string `json:"cf"`
	Entries int64  `json:"entries"`
	Bytes   int64  `json:"bytes"`
}

// NodeManifest is the versioned, self-describing record of a single node's
// logical backup: the format version, the capture cut time, the node's storage
// identity, and per-CF entry/byte counts.
type NodeManifest struct {
	FormatVersion      int       `json:"format_version"`
	CaptureWallClockMs int64     `json:"capture_wall_clock_ms"`
	StorageUUID        string    `json:"storage_uuid,omitempty"`
	CFs                []CFEntry `json:"cfs"`
}

// Encode marshals the manifest as indented JSON to w. (Named Encode rather than
// WriteTo to avoid colliding with the io.WriterTo convention, which expects an
// (int64, error) return — a single-error return is clearer for callers here.)
func (m *NodeManifest) Encode(w io.Writer) error {
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	_, err = w.Write(b)
	return err
}

// CFEntryCount returns the recorded entry count for the named CF, or 0 if the CF
// is absent from the manifest.
func (m *NodeManifest) CFEntryCount(name string) int64 {
	for _, e := range m.CFs {
		if e.CF == name {
			return e.Entries
		}
	}
	return 0
}

// ReadNodeManifest decodes a NodeManifest from r. Unknown fields are ignored
// (forward-compatible).
func ReadNodeManifest(r io.Reader) (*NodeManifest, error) {
	var m NodeManifest
	if err := json.NewDecoder(r).Decode(&m); err != nil {
		return nil, err
	}
	return &m, nil
}
