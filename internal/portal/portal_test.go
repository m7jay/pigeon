package portal

import "testing"

// TestSecureCookieRoundTripAndTamper: the session cookie must round-trip the
// core token, and any tamper or wrong-secret decode must fail authentication
// rather than yield a forged or garbage session (internal/portal/securecookie.go).
func TestSecureCookieRoundTripAndTamper(t *testing.T) {
	c, err := NewSecureCookie("consumer_session", "a-long-enough-secret-value-for-sha256")
	if err != nil {
		t.Fatalf("NewSecureCookie(): %v", err)
	}
	enc, err := c.Encode("token-123")
	if err != nil {
		t.Fatalf("Encode(): %v", err)
	}
	dec, err := c.Decode(enc)
	if err != nil {
		t.Fatalf("Decode(): %v", err)
	}
	if dec != "token-123" {
		t.Fatalf("Decode() = %q, want token-123", dec)
	}

	// Each Encode uses a fresh random nonce, so two encodings of the same value
	// differ; both decode back to it.
	enc2, _ := c.Encode("token-123")
	if enc2 == enc {
		t.Fatal("two encodings with the same secret are identical; the nonce is not random")
	}
	if d, err := c.Decode(enc2); err != nil || d != "token-123" {
		t.Fatalf("second Decode = (%q, %v), want token-123 nil", d, err)
	}

	// Tampering with the ciphertext must fail authentication.
	if _, err := c.Decode(enc + "aa"); err == nil {
		t.Fatal("Decode() of a tampered value should error")
	}

	// A value sealed under a different secret must not decode under this one.
	other, _ := NewSecureCookie("consumer_session", "a-completely-different-secret-value")
	encOther, _ := other.Encode("token-123")
	if _, err := c.Decode(encOther); err == nil {
		t.Fatal("Decode() under the wrong secret should error")
	}
}

// TestNewSecureCookieRefusesEmptySecret: an unsigned cookie is a forgery waiting
// to happen, so construction is refused.
func TestNewSecureCookieRefusesEmptySecret(t *testing.T) {
	if _, err := NewSecureCookie("x", ""); err == nil {
		t.Fatal("NewSecureCookie() with an empty secret should error")
	}
}
