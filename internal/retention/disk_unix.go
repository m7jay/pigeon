//go:build unix

package retention

import "syscall"

// diskUsage reports how much of the filesystem holding path is in use.
//
// "Used" counts blocks that are not available to an unprivileged writer
// (total - bavail), which is the same reading df's Use% shows: a filesystem with
// 5% reserved for root is at 95% when an application can no longer write, not at
// 100%. That is the number the disk budget has to be measured against, because
// SB-3's "data directory exceeds max_disk_pct" means "writes are about to fail".
func diskUsage(path string) (used, total uint64, err error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return 0, 0, err
	}
	bs := uint64(st.Bsize)
	total = st.Blocks * bs
	avail := st.Bavail * bs
	if avail > total {
		avail = total
	}
	return total - avail, total, nil
}
