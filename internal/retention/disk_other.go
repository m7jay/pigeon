//go:build !unix

package retention

import "errors"

// diskUsage is unavailable off unix. The deployment target is Linux (the image
// and the compose file are linux/amd64), so this exists only so the package still
// builds elsewhere; a FilesystemGauge with Bytes set falls back to the database's
// own size against the configured budget.
func diskUsage(string) (used, total uint64, err error) {
	return 0, 0, errors.New("retention: filesystem usage is only implemented on unix")
}
