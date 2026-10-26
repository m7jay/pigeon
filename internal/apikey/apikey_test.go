package apikey

import (
	"bytes"
	"strings"
	"testing"
)

func TestGenerateProducesEnvScopedKey(t *testing.T) {
	k, err := Generate("live")
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if !strings.HasPrefix(k.Plaintext, KeyPrefix+"live_") {
		t.Fatalf("plaintext %q lacks the env prefix", k.Plaintext)
	}
	if env, err := Env(k.Plaintext); err != nil || env != "live" {
		t.Fatalf("Env = %q, %v; want live, nil", env, err)
	}
	if len(k.Hash) != 32 {
		t.Fatalf("hash is %d bytes, want 32", len(k.Hash))
	}
	if len(k.Hint) != HintLen {
		t.Fatalf("hint %q is %d chars, want %d", k.Hint, len(k.Hint), HintLen)
	}
	if strings.Contains(string(k.Hash), k.Plaintext) {
		t.Fatal("hash contains the plaintext")
	}
}

func TestGenerateRejectsUnknownEnv(t *testing.T) {
	if _, err := Generate("staging"); err == nil {
		t.Fatal("want error for env staging")
	}
}

func TestGenerateIsUnique(t *testing.T) {
	seen := map[string]struct{}{}
	for i := 0; i < 500; i++ {
		k, err := Generate("test")
		if err != nil {
			t.Fatalf("Generate: %v", err)
		}
		if _, dup := seen[k.Plaintext]; dup {
			t.Fatalf("duplicate key at iteration %d", i)
		}
		seen[k.Plaintext] = struct{}{}
	}
}

func TestVerify(t *testing.T) {
	k, err := Generate("live")
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if !Verify(k.Plaintext, k.Hash) {
		t.Fatal("Verify rejected the key that was just minted")
	}
	if Verify(k.Plaintext+"x", k.Hash) {
		t.Fatal("Verify accepted a modified key")
	}
	other, _ := Generate("live")
	if Verify(other.Plaintext, k.Hash) {
		t.Fatal("Verify accepted a different key with the same env")
	}
}

func TestEnvRejectsMalformed(t *testing.T) {
	for _, bad := range []string{
		"",
		"abc",
		"pgn_live",
		"pgn_live_",
		"pgn_staging_AAAA",
		"pgn_live_!!!notbase64!!!",
		"svix_live_AAAA",
	} {
		if _, err := Env(bad); err == nil {
			t.Errorf("Env(%q) accepted a malformed key", bad)
		}
	}
}

func TestHashIsDeterministic(t *testing.T) {
	k, _ := Generate("live")
	if !bytes.Equal(Hash(k.Plaintext), Hash(k.Plaintext)) {
		t.Fatal("Hash is not deterministic")
	}
}
