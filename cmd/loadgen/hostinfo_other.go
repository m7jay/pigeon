//go:build !unix

package main

// diskUsage is unavailable on this platform; the machine block simply omits disk
// rather than failing the run. The harness is developed and graded on Linux (§16's
// protocol), and the two fields are informational.
func diskUsage(string) (totalGiB, freeGiB float64) { return 0, 0 }
