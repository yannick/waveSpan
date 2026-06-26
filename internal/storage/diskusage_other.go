//go:build !unix

package storage

// statfs is the no-op fallback on platforms without a Statfs syscall (e.g. plan9, js/wasm). It reports
// zero capacity, which FreeFraction reads as no pressure, so the disk-pressure monitor never sheds on
// such a platform — it fails open rather than blocking writes it cannot reason about.
func statfs(string) (DiskUsage, error) {
	return DiskUsage{}, nil
}
