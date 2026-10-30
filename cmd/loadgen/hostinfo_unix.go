//go:build unix

package main

import "syscall"

// diskUsage reports the filesystem's total and available GiB for path.
//
// §16's machine block asks for disk, and a delivery ceiling is a write-path claim:
// the same box with 2% free is a different machine. Available (not free) is the
// figure reported, because that is what a process may actually write.
func diskUsage(path string) (totalGiB, freeGiB float64) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return 0, 0
	}
	const gib = 1024 * 1024 * 1024
	return float64(st.Blocks) * float64(st.Bsize) / gib,
		float64(st.Bavail) * float64(st.Bsize) / gib
}
