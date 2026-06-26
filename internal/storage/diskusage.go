package storage

// DiskUsage reports the capacity and free space of the filesystem backing a path.
//
// It exists so the disk-pressure admission monitor (internal/health, design/36) can watch the volume
// that holds the collections-raft LogDB (pebble) and the wavesdb store, and shed writes BEFORE the
// volume fills — pebble panics on "no space left on device", which crash-loops every voter on replay.
// We only Statfs the path here; the gating lives at the consensus/admission layer, never inside the
// storage engine.
type DiskUsage struct {
	// TotalBytes is the filesystem capacity in bytes (excluding root-reserved blocks).
	TotalBytes uint64
	// FreeBytes is the space available to an unprivileged writer in bytes.
	FreeBytes uint64
}

// FreeFraction is FreeBytes/TotalBytes in [0,1]. A zero-capacity filesystem reports 1 (no pressure)
// so a Statfs that returns nonsense never trips the shed and takes writes down on its own.
func (u DiskUsage) FreeFraction() float64 {
	if u.TotalBytes == 0 {
		return 1
	}
	return float64(u.FreeBytes) / float64(u.TotalBytes)
}

// Statfs returns the disk usage of the filesystem backing path. On platforms without a Statfs syscall
// (the build-tagged fallback) it returns a zero-capacity DiskUsage and a nil error, which FreeFraction
// reports as no pressure — the monitor then never sheds, matching the "fail open, never self-DoS" rule.
func Statfs(path string) (DiskUsage, error) {
	return statfs(path)
}
