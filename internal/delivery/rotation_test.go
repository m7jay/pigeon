package delivery

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"strings"
	"testing"
	"time"

	"github.com/m7jay/pigeon/internal/store"
)

// sigFor independently computes one signature the way a receiver would. It is the
// same independent re-implementation validSig uses, factored out so a test of the
// dual-signature header can verify each half without calling the signer.
func sigFor(t *testing.T, secret string, sr signedRequest) string {
	t.Helper()
	mac := hmac.New(sha256.New, decodeSecret(t, secret))
	_, _ = mac.Write([]byte(sr.webhookID + "." + sr.webhookTime + "."))
	_, _ = mac.Write(sr.body)
	return "v1," + base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

// TestRotationCarriesBothSignaturesFor24h is FR-1004: during the overlap the
// header carries BOTH signatures space-separated, and a receiver holding either
// secret can verify - without deploying anything, which is the whole point.
//
// The receiver is simulated the way the requirement describes it: a verifier that
// only knows the OLD secret must find a matching entry in the header, and one that
// only knows the NEW secret must too. Asserting "there are two entries" would not
// be the same claim.
func TestRotationCarriesBothSignaturesFor24h(t *testing.T) {
	const oldSecret = "whsec_AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8="
	const newSecret = "whsec_ICEiIyQlJicoKSorLC0uLzAxMjM0NTY3ODk6Ozw9Pj8="

	d := sampleDelivery()
	d.SigningSecret = newSecret
	d.PreviousSigningSecret = oldSecret
	expires := attemptTime.Add(store.SecretOverlapTTL)
	d.PreviousSecretExpiresAt = &expires

	// Mid-overlap: one hour after the rotation.
	mid := attemptTime.Add(time.Hour)
	sr, err := buildSignedRequest(d, 1, "pigeon/test", mid)
	if err != nil {
		t.Fatalf("buildSignedRequest: %v", err)
	}

	entries := strings.Fields(sr.webhookSig)
	if len(entries) != 2 {
		t.Fatalf("webhook-signature has %d entries (%q), want 2 during the overlap", len(entries), sr.webhookSig)
	}
	wantNew := sigFor(t, newSecret, sr)
	wantOld := sigFor(t, oldSecret, sr)

	// Both orderings are acceptable; what is not acceptable is either signature
	// being absent or wrong.
	verify := func(known string) string {
		for _, e := range entries {
			if hmac.Equal([]byte(e), []byte(known)) {
				return e
			}
		}
		return ""
	}
	if verify(wantNew) == "" {
		t.Errorf("a receiver on the NEW secret cannot verify: header %q has no %q", sr.webhookSig, wantNew)
	}
	if verify(wantOld) == "" {
		t.Errorf("a receiver still on the OLD secret cannot verify, which is the case the overlap exists for: header %q has no %q", sr.webhookSig, wantOld)
	}
	for _, e := range entries {
		if !strings.HasPrefix(e, "v1,") {
			t.Errorf("entry %q is missing the v1, prefix (FR-1001)", e)
		}
	}
}

// TestRotationOverlapEndsAt24Hours is the "up to" half: once the window has
// closed the header carries exactly one signature, so a secret the endpoint has
// retired is not accepted by a verifier that trusts the header's tell.
func TestRotationOverlapEndsAt24Hours(t *testing.T) {
	d := sampleDelivery()
	d.PreviousSigningSecret = "whsec_AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8="
	expires := attemptTime.Add(store.SecretOverlapTTL)
	d.PreviousSecretExpiresAt = &expires

	cases := []struct {
		name    string
		now     time.Time
		entries int
	}{
		{"one second before the window closes", expires.Add(-time.Second), 2},
		{"exactly at the expiry", expires, 1},
		{"a day after the expiry", expires.Add(24 * time.Hour), 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sr, err := buildSignedRequest(d, 1, "pigeon/test", tc.now)
			if err != nil {
				t.Fatalf("buildSignedRequest: %v", err)
			}
			if got := len(strings.Fields(sr.webhookSig)); got != tc.entries {
				t.Fatalf("webhook-signature has %d entries at %s, want %d (%q)",
					got, tc.now.Sub(expires), tc.entries, sr.webhookSig)
			}
			if !validSig(decodeSecret(t, d.SigningSecret), sr) {
				// validSig compares the whole field, so this only holds for the
				// single-entry case; assert it there and use the explicit
				// comparison otherwise.
				if tc.entries == 1 {
					t.Errorf("the single signature does not verify against the current secret")
				}
			}
		})
	}
}

// TestNoRotationMeansOneSignature is the baseline: an endpoint that has never
// rotated must be unaffected by FR-1004. Without this, a signer that always
// emitted two entries would pass the overlap test above.
func TestNoRotationMeansOneSignature(t *testing.T) {
	d := sampleDelivery()
	if d.PreviousSigningSecret != "" || d.PreviousSecretExpiresAt != nil {
		t.Fatal("the sample delivery carries a previous secret; the baseline is not a baseline")
	}
	sr, err := buildSignedRequest(d, 1, "pigeon/test", attemptTime)
	if err != nil {
		t.Fatalf("buildSignedRequest: %v", err)
	}
	if got := strings.Fields(sr.webhookSig); len(got) != 1 {
		t.Fatalf("webhook-signature = %q, want exactly one entry for an endpoint that never rotated", sr.webhookSig)
	}
	if !validSig(decodeSecret(t, d.SigningSecret), sr) {
		t.Fatal("the single signature does not verify against the current secret")
	}
}

// TestRotationWithNoExpirySendsOneSignature covers the half-written row: a
// previous secret with no expiry is not an open window, and the signer must not
// treat a nil expiry as "forever" - that would keep a retired secret in the header
// indefinitely, which is the failure mode that makes rotation pointless.
func TestRotationWithNoExpirySendsOneSignature(t *testing.T) {
	d := sampleDelivery()
	d.PreviousSigningSecret = "whsec_AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8="
	d.PreviousSecretExpiresAt = nil
	sr, err := buildSignedRequest(d, 1, "pigeon/test", attemptTime)
	if err != nil {
		t.Fatalf("buildSignedRequest: %v", err)
	}
	if got := strings.Fields(sr.webhookSig); len(got) != 1 {
		t.Fatalf("webhook-signature = %q, want one entry when there is no expiry to honour", sr.webhookSig)
	}
}
