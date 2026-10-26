// Package id mints the prefixed identifiers the tables use.
//
// Two shapes, chosen per resource:
//
//   - New returns `<prefix>_<24 hex chars>`: 96 bits of crypto/rand, no clock.
//     Used for ids whose order does not matter (producers, keys, consumers). It
//     is deliberately not time-sortable, because a guessable time prefix on a
//     secret-bearing id would be a weakness.
//   - NewULID returns a ULID: a 48-bit millisecond timestamp followed by 80 bits
//     of crypto/rand, Crockford base32 (26 uppercase chars). Events and delivery
//     ids use it (FR-901) because they are read back in time order for cursors
//     and the outbox lease, and a ULID gives that ordering from the id alone
//     without trusting a wall clock for uniqueness.
package id

import (
	"crypto/rand"
	"encoding/hex"
	"time"
)

// crockford is Crockford's base32 alphabet, which ULIDs use. It omits the
// confusables I, L, O, U.
const crockford = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

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

// NewULID mints a time-ordered ULID with the current millisecond time.
func NewULID(prefix string) string {
	return NewULIDAt(prefix, time.Now().UnixMilli())
}

// NewULIDAt mints a ULID at a fixed millisecond epoch. Exported so tests can
// assert ordering and stability against a known clock.
func NewULIDAt(prefix string, ms int64) string {
	var b [16]byte
	if _, err := rand.Read(b[6:]); err != nil {
		panic("id: crypto/rand unavailable: " + err.Error())
	}
	// The timestamp is the low 48 bits, big-endian into the first six bytes.
	b[0] = byte(ms >> 40)
	b[1] = byte(ms >> 32)
	b[2] = byte(ms >> 24)
	b[3] = byte(ms >> 16)
	b[4] = byte(ms >> 8)
	b[5] = byte(ms)
	return prefix + "_" + encodeULID(b[:])
}

// encodeULID is the reference ULID encoder: 16 bytes (128 bits, top four zero)
// to 26 Crockford base32 characters. It is inlined from the canonical
// implementation so the dependency surface stays at stdlib.
func encodeULID(b []byte) string {
	out := make([]byte, 26)
	out[0] = crockford[(b[0]>>3)&31]
	out[1] = crockford[((b[0]<<2)|(b[1]>>6))&31]
	out[2] = crockford[(b[1]>>1)&31]
	out[3] = crockford[((b[1]<<4)|(b[2]>>4))&31]
	out[4] = crockford[((b[2]<<1)|(b[3]>>7))&31]
	out[5] = crockford[(b[3]>>2)&31]
	out[6] = crockford[((b[3]<<3)|(b[4]>>5))&31]
	out[7] = crockford[b[4]&31]
	out[8] = crockford[(b[5]>>3)&31]
	out[9] = crockford[((b[5]<<2)|(b[6]>>6))&31]
	out[10] = crockford[(b[6]>>1)&31]
	out[11] = crockford[((b[6]<<4)|(b[7]>>4))&31]
	out[12] = crockford[((b[7]<<1)|(b[8]>>7))&31]
	out[13] = crockford[(b[8]>>2)&31]
	out[14] = crockford[((b[8]<<3)|(b[9]>>5))&31]
	out[15] = crockford[b[9]&31]
	out[16] = crockford[(b[10]>>3)&31]
	out[17] = crockford[((b[10]<<2)|(b[11]>>6))&31]
	out[18] = crockford[(b[11]>>1)&31]
	out[19] = crockford[((b[11]<<4)|(b[12]>>4))&31]
	out[20] = crockford[((b[12]<<1)|(b[13]>>7))&31]
	out[21] = crockford[(b[13]>>2)&31]
	out[22] = crockford[((b[13]<<3)|(b[14]>>5))&31]
	out[23] = crockford[b[14]&31]
	out[24] = crockford[(b[15]>>3)&31]
	out[25] = crockford[(b[15]<<2)&31]
	return string(out)
}
