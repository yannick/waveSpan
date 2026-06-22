package tunables

import (
	"encoding/json"
	"os"
	"path/filepath"
)

// LoadOverridesFile reads a persisted override snapshot (JSON array of Override). A missing file is
// not an error — it returns an empty set so a fresh node starts clean.
func LoadOverridesFile(path string) ([]Override, error) {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []Override
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// SaveOverridesFile writes the override snapshot atomically (write-temp-then-rename) so a crash mid
// write can't corrupt it. It is the persist callback wired into NewOverrides.
func SaveOverridesFile(path string, set []Override) error {
	data, err := json.MarshalIndent(set, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// OverridesPath returns the snapshot path beside the storage directory. It lives outside the engine
// (not in a column family) so it can be read before the engine opens — letting Static engine
// overrides take effect on restart.
func OverridesPath(storageDir string) string {
	return filepath.Join(storageDir, "config_overrides.json")
}
