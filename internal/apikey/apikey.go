// Package apikey issues and verifies producer bearer keys.
//
// Contract: the plaintext is shown once and never stored (FR-202); the database
// holds only a SHA-256 of it. A key is scoped to one producer and one
// environment (FR-203), so a leaked test key cannot publish into live.
package apikey

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
)

// KeyPrefix is the fixed namespace for Pigeon keys. It exists so a leaked key in
// a public repository can be found by secret scanners, which is the only cheap
// defence against that failure.
const KeyPrefix = "pgn_"

// HintLen is how much of the key is safe to store for the UI (FR-206). Twelve
// characters identify which row a key is without revealing usable entropy.
const HintLen = 12

// EntropyBytes is 32 bytes = 256 bits, base64url-encoded to 43 characters.
const EntropyBytes = 32

// ErrMalformed is returned for a key that cannot be a Pigeon key at all. Callers
// map it to 401 without distinguishing it from a wrong key.
var ErrMalformed = errors.New("malformed api key")

// Key is a freshly minted credential. Plaintext is the only copy that will ever
// exist outside the caller's memory.
type Key struct {
	Plaintext string
	Hash      []byte
	Hint      string
	Env       string
}

// Generate mints a key for one environment: "test" or "live" (FR-103).
func Generate(env string) (Key, error) {
	if err := validEnv(env); err != nil {
		return Key{}, err
	}
	raw := make([]byte, EntropyBytes)
	if _, err := rand.Read(raw); err != nil {
		return Key{}, fmt.Errorf("read entropy: %w", err)
	}
	plain := KeyPrefix + env + "_" + base64.RawURLEncoding.EncodeToString(raw)
	return Key{
		Plaintext: plain,
		Hash:      Hash(plain),
		Hint:      hint(plain),
		Env:       env,
	}, nil
}

// Hash is what the database stores. SHA-256 rather than bcrypt/argon2 is
// deliberate: the input is 256 bits of uniform randomness, so key stretching
// defends against nothing and would put ~40 ms of CPU on the hot auth path of
// every ingest request.
func Hash(plaintext string) []byte {
	sum := sha256.Sum256([]byte(plaintext))
	return sum[:]
}

// Env extracts the environment segment of a well-formed key. It must be called
// before the hash lookup so the query targets the right data plane.
func Env(plaintext string) (string, error) {
	rest, ok := strings.CutPrefix(plaintext, KeyPrefix)
	if !ok {
		return "", ErrMalformed
	}
	env, payload, ok := strings.Cut(rest, "_")
	if !ok || payload == "" {
		return "", ErrMalformed
	}
	if err := validEnv(env); err != nil {
		return "", err
	}
	if _, err := base64.RawURLEncoding.DecodeString(payload); err != nil {
		return "", ErrMalformed
	}
	return env, nil
}

// Verify is a constant-time comparison between a presented plaintext and the
// stored hash. Callers fetch the row by hash index and then call this, so that
// an index probe cannot become a timing oracle for a neighbouring key.
func Verify(plaintext string, storedHash []byte) bool {
	got := Hash(plaintext)
	return subtle.ConstantTimeCompare(got, storedHash) == 1
}

func hint(plain string) string {
	if len(plain) <= HintLen {
		return plain
	}
	return plain[:HintLen]
}

func validEnv(env string) error {
	if env != "test" && env != "live" {
		return fmt.Errorf("env %q: want test|live", env)
	}
	return nil
}
