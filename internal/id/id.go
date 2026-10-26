// Package id mints the prefixed identifiers the tables use.
//
// The prefix is part of the contract: it makes an id self-describing in a log
// line or a support ticket (`prd_…` cannot be confused with `key_…`), and it
// costs nothing. Ids are 96 bits of crypto/rand, lowercase hex, so they sort
// stable enough for cursors without depending on a clock (a ULID's ordering
// guarantee would need the clock, and the clock is not a source of uniqueness
// here).
package id

import (
	"crypto/rand"
	"encoding/hex"
)

// New returns `<prefix>_<24 hex chars>`. The prefix is not validated: every
// caller is a package constant.
func New(prefix string) string {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand failing means the kernel entropy source is gone. Every
		// remaining code path in this process mints credentials, so continuing
		// would be minting guessable ones.
		panic("id: crypto/rand unavailable: " + err.Error())
	}
	return prefix + "_" + hex.EncodeToString(b[:])
}
