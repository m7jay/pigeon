package portal

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
)

// SecureCookie encodes an opaque value (the core session token) into an
// encrypted, authenticated cookie value with AES-256-GCM. The value the portal
// holds is therefore never in cleartext in a log, a browser-storage dump or a
// leaked cookie jar; the browser still sends the cookie over the wire, which is
// why it is set httpOnly, SameSite=Lax and Secure. GCM's authentication tag also
// makes tampering detectable, so a forged cookie value fails to decode rather
// than silently naming the wrong session.
type SecureCookie struct {
	name string
	key  []byte
	gcm  cipher.AEAD
}

// NewSecureCookie derives a 32-byte AES key from secret (a SHA-256 of the
// operator-supplied PORTAL_SESSION_SECRET). An empty secret is refused: a cookie
// with no key is a cookie anyone can forge.
func NewSecureCookie(name, secret string) (*SecureCookie, error) {
	if secret == "" {
		return nil, fmt.Errorf("session cookie secret is empty")
	}
	var sum [32]byte
	h := sha256.Sum256([]byte(secret))
	copy(sum[:], h[:])
	block, err := aes.NewCipher(sum[:])
	if err != nil {
		return nil, fmt.Errorf("new cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("new gcm: %w", err)
	}
	return &SecureCookie{name: name, key: sum[:], gcm: gcm}, nil
}

// Name is the cookie name.
func (c *SecureCookie) Name() string { return c.name }

// Encode encrypts value into a single base64url cookie value (nonce || sealed
// box). It is idempotent only in the sense that any decode of it yields value;
// each call uses a fresh random nonce, so two encodings of the same value differ.
func (c *SecureCookie) Encode(value string) (string, error) {
	nonce := make([]byte, c.gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", fmt.Errorf("nonce: %w", err)
	}
	sealed := c.gcm.Seal(nonce, nonce, []byte(value), nil)
	return base64.RawURLEncoding.EncodeToString(sealed), nil
}

// Decode reverses Encode. A malformed, truncated or tampered value is an error,
// which the caller treats as "no valid session".
func (c *SecureCookie) Decode(s string) (string, error) {
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return "", fmt.Errorf("decode cookie: %w", err)
	}
	ns := c.gcm.NonceSize()
	if len(raw) < ns {
		return "", fmt.Errorf("cookie too short")
	}
	nonce, ct := raw[:ns], raw[ns:]
	open, err := c.gcm.Open(nil, nonce, ct, nil)
	if err != nil {
		return "", fmt.Errorf("authenticate cookie: %w", err)
	}
	return string(open), nil
}
