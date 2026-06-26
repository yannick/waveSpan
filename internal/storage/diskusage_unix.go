//go:build unix

package storage

import "golang.org/x/sys/unix"

// statfs reads filesystem stats via the unix Statfs(2) syscall. Bavail (blocks free to an unprivileged
// writer) is used for FreeBytes — not Bfree — so root-reserved space is not counted as available, which
// matches what the process can actually write before ENOSPC.
func statfs(path string) (DiskUsage, error) {
	var st unix.Statfs_t
	if err := unix.Statfs(path, &st); err != nil {
		return DiskUsage{}, err
	}
	bsize := uint64(st.Bsize) //nolint:unconvert,gosec // Bsize is int64 on darwin, int32 on linux
	return DiskUsage{
		TotalBytes: st.Blocks * bsize,
		FreeBytes:  st.Bavail * bsize,
	}, nil
}
