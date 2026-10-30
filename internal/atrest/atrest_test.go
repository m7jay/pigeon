package atrest

import (
	"bytes"
	"encoding/base64"
	"errors"
	"strings"
	"testing"
)

func testKey(t *testing.T) Key {
	t.Helper()
	k, _, err := GenerateKey()
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	return k
}

// TestRoundTrip is the whole point of the primitive: a sealed value comes back
// byte for byte under the same key and the same AAD.
func TestRoundTrip(t *testing.T) {
	k := testKey(t)
	for _, plaintext := range []string{"", "s3cr3t", "Bearer sk-live-abc123", "héllo — 世界"} {
		sealed, err := k.Seal([]byte(plaintext), "endpoint:end_1")
		if err != nil {
			t.Fatalf("Seal(%q): %v", plaintext, err)
		}
		if plaintext != "" && strings.Contains(sealed, plaintext) {
			t.Fatalf("the ciphertext contains the plaintext: %q", sealed)
		}
		got, err := k.Open(sealed, "endpoint:end_1")
		if err != nil {
			t.Fatalf("Open(%q): %v", plaintext, err)
		}
		if string(got) != plaintext {
			t.Fatalf("round trip = %q, want %q", got, plaintext)
		}
	}
}

// TestNonceIsFreshEveryTime: two seals of the same value must differ, and both
// must decrypt. A repeated nonce under one key is the classic GCM break.
func TestNonceIsFreshEveryTime(t *testing.T) {
	k := testKey(t)
	first, err := k.Seal([]byte("same"), "endpoint:end_1")
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	second, err := k.Seal([]byte("same"), "endpoint:end_1")
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	if first == second {
		t.Fatal("two seals of the same value produced identical ciphertext: the nonce is not fresh")
	}
	for _, sealed := range []string{first, second} {
		got, err := k.Open(sealed, "endpoint:end_1")
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		if string(got) != "same" {
			t.Fatalf("round trip = %q, want %q", got, "same")
		}
	}
}

// TestWrongAADFailsToDecrypt is FR-1006's move-the-row evidence: a ciphertext
// sealed for one endpoint id must not authenticate for another.
func TestWrongAADFailsToDecrypt(t *testing.T) {
	k := testKey(t)
	sealed, err := k.Seal([]byte("sk-live-abc123"), "endpoint:end_1")
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	if _, err := k.Open(sealed, "endpoint:end_2"); !errors.Is(err, ErrNotAuthentic) {
		t.Fatalf("Open with another row's AAD: err = %v, want ErrNotAuthentic", err)
	}
}

func TestTamperedCiphertextFailsToDecrypt(t *testing.T) {
	k := testKey(t)
	sealed, err := k.Seal([]byte("sk-live-abc123"), "endpoint:end_1")
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	raw, err := base64.StdEncoding.DecodeString(sealed)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	raw[len(raw)-1] ^= 0x01
	tampered := base64.StdEncoding.EncodeToString(raw)
	if _, err := k.Open(tampered, "endpoint:end_1"); !errors.Is(err, ErrNotAuthentic) {
		t.Fatalf("Open of a tampered value: err = %v, want ErrNotAuthentic", err)
	}
}

func TestAnotherKeyFailsToDecrypt(t *testing.T) {
	k := testKey(t)
	other := testKey(t)
	sealed, err := k.Seal([]byte("sk-live-abc123"), "endpoint:end_1")
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	if _, err := other.Open(sealed, "endpoint:end_1"); !errors.Is(err, ErrNotAuthentic) {
		t.Fatalf("Open under another key: err = %v, want ErrNotAuthentic", err)
	}
}

// TestOpenRejectsGarbage keeps a hand-edited column from panicking the worker:
// not-base64 and too-short values are ErrNotAuthentic, never a slice bounds
// error.
func TestOpenRejectsGarbage(t *testing.T) {
	k := testKey(t)
	for _, sealed := range []string{"", "not base64!!", base64.StdEncoding.EncodeToString([]byte("short"))} {
		if _, err := k.Open(sealed, "endpoint:end_1"); !errors.Is(err, ErrNotAuthentic) {
			t.Errorf("Open(%q): err = %v, want ErrNotAuthentic", sealed, err)
		}
	}
}

func TestParseKey(t *testing.T) {
	_, encoded, err := GenerateKey()
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	k, err := ParseKey(encoded)
	if err != nil {
		t.Fatalf("ParseKey of a generated key: %v", err)
	}
	if !k.Configured() {
		t.Fatal("a parsed key is not configured")
	}
	// The unpadded form is accepted; the URL-safe alphabet is not.
	unpadded := strings.TrimRight(encoded, "=")
	if _, err := ParseKey(unpadded); err != nil {
		t.Errorf("ParseKey of the unpadded form: %v", err)
	}
	// Whitespace from a shell or an env file is tolerated.
	if _, err := ParseKey("  " + encoded + "\n"); err != nil {
		t.Errorf("ParseKey with surrounding whitespace: %v", err)
	}
}

func TestParseKeyRefusals(t *testing.T) {
	full, _, err := GenerateKey()
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	_ = full

	cases := map[string]struct {
		encoded string
		want    error
	}{
		"empty":             {"", ErrNoKey},
		"whitespace only":   {"   ", ErrNoKey},
		"not base64":        {"not-a-key!!", ErrInvalidKey},
		"too short":         {base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{7}, 16)), ErrInvalidKey},
		"one byte short":    {base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{7}, 31)), ErrInvalidKey},
		"one byte long":     {base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{7}, 33)), ErrInvalidKey},
		"a password":        {base64.StdEncoding.EncodeToString([]byte("correct-horse-battery-staple")), ErrInvalidKey},
		"url-safe alphabet": {base64.URLEncoding.EncodeToString(bytes.Repeat([]byte{0xfb}, 32)), ErrInvalidKey},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := ParseKey(tc.encoded)
			if !errors.Is(err, tc.want) {
				t.Fatalf("ParseKey(%q): err = %v, want %v", tc.encoded, err, tc.want)
			}
			if err != nil && strings.Contains(err.Error(), tc.encoded) && tc.encoded != "" {
				t.Errorf("the refusal quotes the key material: %v", err)
			}
		})
	}
}

// TestZeroKeyIsInert: a Store (or a Worker) that never got a key must refuse to
// seal rather than write plaintext, and must refuse to open rather than return
// an empty value that a caller might happily send.
func TestZeroKeyIsInert(t *testing.T) {
	var k Key
	if k.Configured() {
		t.Fatal("the zero Key reports itself configured")
	}
	if _, err := k.Seal([]byte("x"), "endpoint:end_1"); !errors.Is(err, ErrNoKey) {
		t.Fatalf("zero Key Seal: err = %v, want ErrNoKey", err)
	}
	if _, err := k.Open("AAAA", "endpoint:end_1"); !errors.Is(err, ErrNoKey) {
		t.Fatalf("zero Key Open: err = %v, want ErrNoKey", err)
	}
}

// TestKeyIsCopyable: the Key travels by value (a struct field), so a copy must
// still work. It holds an interface, so this pins that a copy is not a
// half-initialised value.
func TestKeyIsCopyable(t *testing.T) {
	k := testKey(t)
	copied := k
	sealed, err := copied.Seal([]byte("value"), "endpoint:end_1")
	if err != nil {
		t.Fatalf("Seal from a copy: %v", err)
	}
	got, err := k.Open(sealed, "endpoint:end_1")
	if err != nil {
		t.Fatalf("Open with the original: %v", err)
	}
	if string(got) != "value" {
		t.Fatalf("round trip = %q, want %q", got, "value")
	}
}
