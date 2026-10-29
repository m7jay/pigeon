// Package atrest is pigeon's at-rest encryption primitive (FR-1006).
//
// It is deliberately small: AES-256-GCM under a 32-byte key the deployment
// supplies as base64 (PIGEON_HEADERS_KEY), a fresh random nonce per value, and a
// caller-supplied additional-authenticated-data string. The first user is the
// endpoint custom header values (FR-405, FR-1006); endpoint signing secrets are
// stored plaintext because they must be usable at sign time, so there was no
// earlier primitive to reuse and this one is the pattern the rest of the tree
// should follow:
//
//   - one deployment key, validated at startup (config.Validate), never a
//     per-row or per-request key;
//   - one AAD string that names where the ciphertext belongs - the table, the
//     row, and the field when one row holds several encrypted values - so a
//     value cannot be moved to another row or onto another field by anyone who
//     can write the column but does not hold the key. The primitive cannot
//     enforce that: the AAD is the caller's string. It is therefore a rule in
//     docs/at-rest-encryption.md, and the store builds it next to the SQL that
//     names the row;
//   - the ciphertext is stored as base64(nonce || ciphertext), so a column holds
//     one self-contained value and no separate nonce column can drift from it;
//   - plaintext never travels further than the write and the delivery path, and
//     an error from here never quotes the plaintext or the ciphertext
//     (FR-1008).
//
// What this does not do, and deliberately: key rotation, and per-producer key
// derivation. Rotating the key would require re-sealing every stored value in the
// same transaction that swaps the key, which is a migration and not a helper; a
// deployment that must rotate re-seals through a migration. And the key is one
// deployment key: the AAD keeps a ciphertext from being moved between rows or
// fields, but every producer's value opens under that one key, so a holder of it
// reads all of them - which is what §15.1's MT-7 asks the tree not to do. That
// derivation is deferred to v1.1, with the cost stated in REQUIREMENTS.md §17.1.
// docs/at-rest-encryption.md states both limitations rather than leaving them to
// be discovered.
package atrest

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
)

// KeyEnv is the environment variable that carries the key. It is named here so
// the startup refusal and the documentation cannot name two different variables.
const KeyEnv = "PIGEON_HEADERS_KEY"

// KeyBytes is the AES-256 key length. Anything else is refused rather than
// stretched: a deployment that supplies a password instead of a key has a
// configuration bug, and silently deriving a weaker key from it would hide it.
const KeyBytes = 32

var (
	// ErrNoKey means no key was configured at all. The zero Key is usable as a
	// value (so a struct can hold one) but seals and opens nothing.
	ErrNoKey = errors.New("at-rest key is not configured")
	// ErrInvalidKey means the supplied encoding cannot be a key: not base64, or
	// the wrong number of bytes.
	ErrInvalidKey = errors.New("at-rest key is not 32 bytes of base64")
	// ErrNotAuthentic means the ciphertext did not authenticate: it was tampered
	// with, it was sealed under a different key, or its AAD names another table,
	// row or field.
	// The three are one error on purpose - telling them apart is something only
	// an attacker benefits from.
	ErrNotAuthentic = errors.New("at-rest ciphertext did not authenticate")
)

// Key is a parsed at-rest key. The zero value is a valid, unusable Key: every
// operation on it answers ErrNoKey, so a caller that never configured one gets a
// clear refusal instead of a panic.
type Key struct {
	aead cipher.AEAD
}

// ParseKey reads the base64 form of a key. Both the padded standard alphabet
// (what `openssl rand -base64 32` emits) and the unpadded form are accepted, so
// a key pasted from a helper that trims the padding is not a boot failure. The
// URL-safe alphabet is deliberately NOT accepted: two encodings for one key is
// how a "the key looks right but does not decrypt" incident starts.
func ParseKey(encoded string) (Key, error) {
	encoded = strings.TrimSpace(encoded)
	if encoded == "" {
		return Key{}, fmt.Errorf("%w: %s is empty", ErrNoKey, KeyEnv)
	}
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		raw, err = base64.RawStdEncoding.DecodeString(encoded)
	}
	if err != nil {
		return Key{}, fmt.Errorf("%w: %s is not base64", ErrInvalidKey, KeyEnv)
	}
	if len(raw) != KeyBytes {
		return Key{}, fmt.Errorf("%w: %s decodes to %d bytes, want %d",
			ErrInvalidKey, KeyEnv, len(raw), KeyBytes)
	}
	block, err := aes.NewCipher(raw)
	if err != nil {
		return Key{}, fmt.Errorf("%w: %s: %v", ErrInvalidKey, KeyEnv, err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return Key{}, fmt.Errorf("%w: %s: %v", ErrInvalidKey, KeyEnv, err)
	}
	return Key{aead: aead}, nil
}

// GenerateKey mints a fresh key and the encoding to put in the environment. It
// is what a test and an operator's `openssl rand -base64 32` both produce; it
// lives here so the documented answer and the tested behaviour cannot drift.
func GenerateKey() (Key, string, error) {
	raw := make([]byte, KeyBytes)
	if _, err := rand.Read(raw); err != nil {
		return Key{}, "", fmt.Errorf("at-rest key entropy: %w", err)
	}
	encoded := base64.StdEncoding.EncodeToString(raw)
	k, err := ParseKey(encoded)
	if err != nil {
		return Key{}, "", err
	}
	return k, encoded, nil
}

// Configured reports whether the key can be used.
func (k Key) Configured() bool { return k.aead != nil }

// Seal encrypts plaintext and returns base64(nonce || ciphertext). aad names
// where the value belongs - the table, the row, and the field when one row holds
// several encrypted values (see the package comment); a caller that hands it
// another row's aad, or another field's inside the same row, has just given up
// that protection, which is why the AAD is built next to the SQL that names the
// row rather than passed around by callers.
func (k Key) Seal(plaintext []byte, aad string) (string, error) {
	if k.aead == nil {
		return "", fmt.Errorf("%w: %s", ErrNoKey, KeyEnv)
	}
	nonce := make([]byte, k.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return "", fmt.Errorf("at-rest nonce: %w", err)
	}
	return base64.StdEncoding.EncodeToString(k.aead.Seal(nonce, nonce, plaintext, []byte(aad))), nil
}

// Open decrypts a value Seal produced for the same aad. A wrong key, a tampered
// value and a mismatched aad are all ErrNotAuthentic. The error never quotes the
// ciphertext or the plaintext.
func (k Key) Open(sealed, aad string) ([]byte, error) {
	if k.aead == nil {
		return nil, fmt.Errorf("%w: %s", ErrNoKey, KeyEnv)
	}
	raw, err := base64.StdEncoding.DecodeString(sealed)
	if err != nil {
		return nil, fmt.Errorf("%w: value is not base64", ErrNotAuthentic)
	}
	n := k.aead.NonceSize()
	if len(raw) < n+k.aead.Overhead() {
		return nil, fmt.Errorf("%w: value is too short to hold a nonce and a tag", ErrNotAuthentic)
	}
	out, err := k.aead.Open(nil, raw[:n], raw[n:], []byte(aad))
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrNotAuthentic, err)
	}
	return out, nil
}
