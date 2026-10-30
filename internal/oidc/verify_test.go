package oidc

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/asn1"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Every test in this file is written from the side of the identity provider: it
// mints a token that violates exactly one of the rules verify.go enforces, and
// asserts the verifier refuses it. A rule that is deleted from verify.go takes
// one of these tests down with it, which is the point.

func TestVerifyIDTokenHappyRS256(t *testing.T) {
	i := newIdP(t)
	p := i.provider(t)
	raw := i.mint(t, "RS256", i.rsaKid, i.validClaims(nil))

	claims, err := p.VerifyIDToken(context.Background(), raw)
	require.NoError(t, err)
	require.Equal(t, "user-1", claims.Subject)
	require.Equal(t, "user@example.com", claims.Email)
	require.True(t, claims.EmailVerified)
	require.Equal(t, "User One", claims.Name)
	require.Equal(t, i.srv.URL, claims.Issuer)
	require.Equal(t, []string{"client-1"}, claims.Audience)
	require.Equal(t, i.clock.Now().Add(600*time.Second), claims.ExpiresAt)
	require.Equal(t, i.clock.Now(), claims.IssuedAt)
	require.Equal(t, "nonce-1", claims.Nonce)
}

// TestVerifyIDTokenHappyES256RawSignature is the only test that can catch the
// encoding mistake ES* invites: JWS carries the raw r||s pair, crypto/ecdsa
// produces and consumes a DER SEQUENCE. The signature here is produced by the
// harness with ecdsa.Sign and re-encoded the way an IdP does, and the test then
// proves both halves - that the raw pair is what verifies, and that the DER
// encoding of the same pair is refused as a token signature.
func TestVerifyIDTokenHappyES256RawSignature(t *testing.T) {
	i := newIdP(t)
	p := i.provider(t)
	raw := i.mint(t, "ES256", i.ecKid, i.validClaims(nil))

	claims, err := p.VerifyIDToken(context.Background(), raw)
	require.NoError(t, err)
	require.Equal(t, "user-1", claims.Subject)
	require.True(t, claims.EmailVerified)

	parts := strings.Split(raw, ".")
	require.Len(t, parts, 3)
	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	require.NoError(t, err)
	// Two 32-byte integers for P-256, not the ~70-byte DER form.
	require.Len(t, signature, 64)

	r := new(big.Int).SetBytes(signature[:32])
	s := new(big.Int).SetBytes(signature[32:])
	digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	require.True(t, ecdsa.Verify(&i.ecKey.PublicKey, digest[:], r, s), "the harness signature must be a valid ECDSA signature")

	der, err := asn1.Marshal(struct{ R, S *big.Int }{r, s})
	require.NoError(t, err)
	require.True(t, ecdsa.VerifyASN1(&i.ecKey.PublicKey, digest[:], der))
	// The raw pair is not a DER SEQUENCE, so a verifier that forwarded it to
	// VerifyASN1 would reject every valid token:
	require.False(t, ecdsa.VerifyASN1(&i.ecKey.PublicKey, digest[:], signature))

	// ...and the package's own conversion is exactly that DER, which is what
	// makes the raw wire form verify:
	gotR, gotS, err := decodeRawECDSASignature(signature, 32)
	require.NoError(t, err)
	require.Equal(t, r, gotR)
	require.Equal(t, s, gotS)
	converted, err := encodeECDSASignature(gotR, gotS)
	require.NoError(t, err)
	require.Equal(t, der, converted)
	require.True(t, ecdsa.VerifyASN1(&i.ecKey.PublicKey, digest[:], converted))

	// A signature of any other length is not a raw pair and is refused with
	// ErrInvalidToken rather than by panicking on the slice arithmetic.
	_, _, err = decodeRawECDSASignature(signature[:63], 32)
	require.ErrorIs(t, err, ErrInvalidToken)

	// And a token that carries the DER form of this very signature must be
	// rejected: one signature, one encoding.
	derToken := parts[0] + "." + parts[1] + "." + b64(der)
	_, err = p.VerifyIDToken(context.Background(), derToken)
	require.ErrorIs(t, err, ErrInvalidToken)
}

func TestVerifyIDTokenRejections(t *testing.T) {
	i := newIdP(t)
	p := i.provider(t)
	ctx := context.Background()

	rawHeader := func(t *testing.T, header map[string]any, payload []byte, key jwsSigner) string {
		t.Helper()
		hb, err := json.Marshal(header)
		require.NoError(t, err)
		input := b64(hb) + "." + b64(payload)
		signature, err := key.sign(header["alg"].(string), []byte(input))
		require.NoError(t, err)
		return input + "." + b64(signature)
	}

	cases := []struct {
		name string
		raw  func(t *testing.T) string
	}{
		{
			name: "alg none is rejected",
			raw: func(t *testing.T) string {
				return i.mint(t, "none", i.rsaKid, i.validClaims(nil))
			},
		},
		{
			name: "HS256 is rejected as key confusion",
			raw: func(t *testing.T) string {
				return i.mint(t, "HS256", i.rsaKid, i.validClaims(nil))
			},
		},
		{
			name: "HS512 is rejected as key confusion",
			raw: func(t *testing.T) string {
				return i.mint(t, "HS512", i.rsaKid, i.validClaims(nil))
			},
		},
		{
			name: "an algorithm this package does not implement is rejected",
			raw: func(t *testing.T) string {
				claims, err := json.Marshal(i.validClaims(nil))
				require.NoError(t, err)
				return rawHeader(t, map[string]any{"alg": "PS256", "kid": i.rsaKid}, claims, i.signer())
			},
		},
		{
			name: "an empty alg is rejected",
			raw: func(t *testing.T) string {
				return i.mint(t, "", i.rsaKid, i.validClaims(nil))
			},
		},
		{
			name: "a header without a kid is rejected",
			raw: func(t *testing.T) string {
				return i.mint(t, "RS256", "", i.validClaims(nil))
			},
		},
		{
			name: "an unpublished kid is rejected",
			raw: func(t *testing.T) string {
				return i.mint(t, "RS256", "rotated-away", i.validClaims(nil))
			},
		},
		{
			name: "RS256 signed by a different RSA key is rejected",
			raw: func(t *testing.T) string {
				other, err := rsa.GenerateKey(rand.Reader, 2048)
				require.NoError(t, err)
				return jwsSigner{rsa: other, ec: i.ecKey}.mint(t, "RS256", i.rsaKid, i.validClaims(nil))
			},
		},
		{
			name: "ES256 naming the RSA kid is rejected",
			raw: func(t *testing.T) string {
				return i.mint(t, "ES256", i.rsaKid, i.validClaims(nil))
			},
		},
		{
			name: "ES384 over the P-256 key is rejected",
			raw: func(t *testing.T) string {
				return i.mint(t, "ES384", i.ecKid, i.validClaims(nil))
			},
		},
		{
			name: "an algorithm the key is not published for is rejected",
			raw: func(t *testing.T) string {
				return i.mint(t, "RS512", i.rsaKid, i.validClaims(nil))
			},
		},
		{
			name: "a tampered payload is rejected",
			raw: func(t *testing.T) string {
				return tamperPayload(t, i.mint(t, "RS256", i.rsaKid, i.validClaims(nil)), func(c map[string]any) {
					c["sub"] = "admin"
				})
			},
		},
		{
			name: "a tampered signature is rejected",
			raw: func(t *testing.T) string {
				return tamperSignature(t, i.mint(t, "RS256", i.rsaKid, i.validClaims(nil)))
			},
		},
		{
			name: "a token with two parts is rejected",
			raw: func(t *testing.T) string {
				return "aGVhZGVy.cGF5bG9hZA"
			},
		},
		{
			name: "a token with four parts is rejected",
			raw: func(t *testing.T) string {
				return "aGVhZGVy.cGF5bG9hZA.c2ln.eA"
			},
		},
		{
			name: "a token with an empty signature part is rejected",
			raw: func(t *testing.T) string {
				parts := strings.Split(i.mint(t, "RS256", i.rsaKid, i.validClaims(nil)), ".")
				return parts[0] + "." + parts[1] + "."
			},
		},
		{
			name: "a payload that is not JSON is rejected",
			raw: func(t *testing.T) string {
				return rawHeader(t, map[string]any{"alg": "RS256", "kid": i.rsaKid}, []byte("not json at all"), i.signer())
			},
		},
		{
			name: "an iss that is not the configured issuer is rejected",
			raw: func(t *testing.T) string {
				return i.mint(t, "RS256", i.rsaKid, i.validClaims(func(c map[string]any) {
					c["iss"] = "https://idp.attacker.example"
				}))
			},
		},
		{
			name: "an iss that is a prefix of the configured issuer is rejected",
			raw: func(t *testing.T) string {
				return i.mint(t, "RS256", i.rsaKid, i.validClaims(func(c map[string]any) {
					c["iss"] = i.srv.URL + ".attacker.example"
				}))
			},
		},
		{
			name: "an aud string for another client is rejected",
			raw: func(t *testing.T) string {
				return i.mint(t, "RS256", i.rsaKid, i.validClaims(func(c map[string]any) {
					c["aud"] = "another-client"
				}))
			},
		},
		{
			name: "an aud array without this client is rejected",
			raw: func(t *testing.T) string {
				return i.mint(t, "RS256", i.rsaKid, i.validClaims(func(c map[string]any) {
					c["aud"] = []string{"another-client", "a-third"}
				}))
			},
		},
		{
			name: "a missing aud is rejected",
			raw: func(t *testing.T) string {
				return i.mint(t, "RS256", i.rsaKid, i.validClaims(func(c map[string]any) {
					delete(c, "aud")
				}))
			},
		},
		{
			name: "a missing sub is rejected",
			raw: func(t *testing.T) string {
				return i.mint(t, "RS256", i.rsaKid, i.validClaims(func(c map[string]any) {
					delete(c, "sub")
				}))
			},
		},
		{
			name: "an expired exp is rejected",
			raw: func(t *testing.T) string {
				return i.mint(t, "RS256", i.rsaKid, i.validClaims(func(c map[string]any) {
					c["exp"] = i.clock.Now().Add(-time.Hour).Unix()
				}))
			},
		},
		{
			name: "a missing exp is rejected",
			raw: func(t *testing.T) string {
				return i.mint(t, "RS256", i.rsaKid, i.validClaims(func(c map[string]any) {
					delete(c, "exp")
				}))
			},
		},
		{
			name: "an iat far in the future is rejected",
			raw: func(t *testing.T) string {
				return i.mint(t, "RS256", i.rsaKid, i.validClaims(func(c map[string]any) {
					c["iat"] = i.clock.Now().Add(time.Hour).Unix()
				}))
			},
		},
		{
			name: "an empty email is rejected",
			raw: func(t *testing.T) string {
				return i.mint(t, "RS256", i.rsaKid, i.validClaims(func(c map[string]any) {
					c["email"] = ""
				}))
			},
		},
		{
			name: "a missing email is rejected",
			raw: func(t *testing.T) string {
				return i.mint(t, "RS256", i.rsaKid, i.validClaims(func(c map[string]any) {
					delete(c, "email")
				}))
			},
		},
		{
			name: "email_verified false is rejected",
			raw: func(t *testing.T) string {
				return i.mint(t, "RS256", i.rsaKid, i.validClaims(func(c map[string]any) {
					c["email_verified"] = false
				}))
			},
		},
		{
			name: "the string \"false\" for email_verified is rejected",
			raw: func(t *testing.T) string {
				return i.mint(t, "RS256", i.rsaKid, i.validClaims(func(c map[string]any) {
					c["email_verified"] = "false"
				}))
			},
		},
		{
			name: "a missing email_verified is rejected",
			raw: func(t *testing.T) string {
				return i.mint(t, "RS256", i.rsaKid, i.validClaims(func(c map[string]any) {
					delete(c, "email_verified")
				}))
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw := tc.raw(t)
			claims, err := p.VerifyIDToken(ctx, raw)
			require.Error(t, err)
			require.ErrorIs(t, err, ErrInvalidToken, "every rejection must wrap ErrInvalidToken, got: %v", err)
			require.Zero(t, claims)
		})
	}
}

func TestVerifyIDTokenAcceptsBothAudienceForms(t *testing.T) {
	i := newIdP(t)
	cases := []struct {
		name     string
		audience any
		accepted bool
	}{
		{name: "string", audience: "client-1", accepted: true},
		{name: "array of one", audience: []string{"client-1"}, accepted: true},
		{name: "array with others", audience: []string{"another-client", "client-1"}, accepted: true},
		{name: "string for another client", audience: "another-client", accepted: false},
		{name: "array without this client", audience: []string{"another-client"}, accepted: false},
		{name: "empty array", audience: []string{}, accepted: false},
		{name: "a number", audience: 42, accepted: false},
		{name: "null", audience: nil, accepted: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// A provider per case: a rejected token is not worth a refetch, but
			// keeping the request counts separate keeps the cases independent.
			p := i.provider(t)
			raw := i.mint(t, "RS256", i.rsaKid, i.validClaims(func(c map[string]any) {
				c["aud"] = tc.audience
			}))
			_, err := p.VerifyIDToken(context.Background(), raw)
			if tc.accepted {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, ErrInvalidToken)
		})
	}
}

func TestVerifyIDTokenAcceptsTheStringTrueForEmailVerified(t *testing.T) {
	i := newIdP(t)
	p := i.provider(t)
	// Some IdPs send email_verified as the string "true" while the specification
	// says boolean; that spelling is accepted, and only that spelling.
	raw := i.mint(t, "RS256", i.rsaKid, i.validClaims(func(c map[string]any) {
		c["email_verified"] = "true"
	}))
	claims, err := p.VerifyIDToken(context.Background(), raw)
	require.NoError(t, err)
	require.True(t, claims.EmailVerified)
}

// TestVerifyIDTokenClockSkewWindow pins the leeway to a number, because "a 60
// second leeway" that is silently 0 or an hour is the difference between a
// deployment that works and one that does not.
func TestVerifyIDTokenClockSkewWindow(t *testing.T) {
	i := newIdP(t)
	p := i.provider(t)
	ctx := context.Background()

	raw := i.mint(t, "RS256", i.rsaKid, i.validClaims(func(c map[string]any) {
		c["exp"] = i.clock.Now().Add(-30 * time.Second).Unix()
	}))
	_, err := p.VerifyIDToken(ctx, raw)
	require.NoError(t, err, "exp 30s in the past is inside the leeway")

	raw = i.mint(t, "RS256", i.rsaKid, i.validClaims(func(c map[string]any) {
		c["exp"] = i.clock.Now().Add(-90 * time.Second).Unix()
	}))
	_, err = p.VerifyIDToken(ctx, raw)
	require.ErrorIs(t, err, ErrInvalidToken, "exp 90s in the past is outside the leeway")

	raw = i.mint(t, "RS256", i.rsaKid, i.validClaims(func(c map[string]any) {
		c["iat"] = i.clock.Now().Add(4 * time.Minute).Unix()
	}))
	_, err = p.VerifyIDToken(ctx, raw)
	require.NoError(t, err, "iat 4 minutes ahead is inside maxIATFuture plus the leeway")

	raw = i.mint(t, "RS256", i.rsaKid, i.validClaims(func(c map[string]any) {
		c["iat"] = i.clock.Now().Add(10 * time.Minute).Unix()
	}))
	_, err = p.VerifyIDToken(ctx, raw)
	require.ErrorIs(t, err, ErrInvalidToken, "iat 10 minutes ahead is not")
}

func TestVerifyIDTokenCachesTheJWKS(t *testing.T) {
	i := newIdP(t)
	p := i.provider(t)
	ctx := context.Background()

	for n := 0; n < 3; n++ {
		raw := i.mint(t, "RS256", i.rsaKid, i.validClaims(nil))
		_, err := p.VerifyIDToken(ctx, raw)
		require.NoError(t, err)
	}
	require.Equal(t, 1, i.jwksRequests(), "three verifies must cost the IdP one key set fetch")

	// The harness publishes Cache-Control: max-age=300. One second inside that
	// window is still the cached document...
	i.clock.advance(299 * time.Second)
	raw := i.mint(t, "RS256", i.rsaKid, i.validClaims(nil))
	_, err := p.VerifyIDToken(ctx, raw)
	require.NoError(t, err)
	require.Equal(t, 1, i.jwksRequests())

	// ...and past it the document is fetched again, so a rotation reaches us
	// without a restart.
	i.clock.advance(2 * time.Second)
	raw = i.mint(t, "RS256", i.rsaKid, i.validClaims(nil))
	_, err = p.VerifyIDToken(ctx, raw)
	require.NoError(t, err)
	require.Equal(t, 2, i.jwksRequests())
}

func TestVerifyIDTokenRefetchesOnceForAnUnknownKid(t *testing.T) {
	i := newIdP(t)
	p := i.provider(t)
	ctx := context.Background()

	_, err := p.VerifyIDToken(ctx, i.mint(t, "RS256", i.rsaKid, i.validClaims(nil)))
	require.NoError(t, err)
	require.Equal(t, 1, i.jwksRequests())

	// A rotation the cache predates: the first unknown kid buys one refetch, and
	// the new key then verifies.
	rotated := i.addRSAKey(t, "rsa-2")
	rotatedSigner := jwsSigner{rsa: rotated, ec: i.ecKey}
	claims, err := p.VerifyIDToken(ctx, rotatedSigner.mint(t, "RS256", "rsa-2", i.validClaims(nil)))
	require.NoError(t, err)
	require.Equal(t, "user-1", claims.Subject)
	require.Equal(t, 2, i.jwksRequests())

	// The refetched document is cached like any other.
	_, err = p.VerifyIDToken(ctx, rotatedSigner.mint(t, "RS256", "rsa-2", i.validClaims(nil)))
	require.NoError(t, err)
	require.Equal(t, 2, i.jwksRequests())

	// A kid nobody ever published is rejected, and inside the refetch interval it
	// does not reach the IdP at all: an unauthenticated attacker who can present
	// tokens must not be able to use this path to hammer the IdP.
	_, err = p.VerifyIDToken(ctx, i.mint(t, "RS256", "never-published", i.validClaims(nil)))
	require.ErrorIs(t, err, ErrInvalidToken)
	require.Equal(t, 2, i.jwksRequests())

	// After the interval the refetch is allowed again, so a rotation that happens
	// while forged tokens are arriving is still seen.
	i.clock.advance(jwksRefetchMinInterval + time.Second)
	_, err = p.VerifyIDToken(ctx, i.mint(t, "RS256", "never-published", i.validClaims(nil)))
	require.ErrorIs(t, err, ErrInvalidToken)
	require.Equal(t, 3, i.jwksRequests())
}

// TestVerifyIDTokenRejectsWhenTheJWKSIsUnusable: a key set the IdP will not
// serve, or serves broken, must reject the token that needed it - never fail
// open with an empty key set, and never be reported as something other than an
// invalid token.
func TestVerifyIDTokenRejectsWhenTheJWKSIsUnusable(t *testing.T) {
	cases := []struct {
		name  string
		prime bool
		apply func(i *idp)
	}{
		{
			name: "the endpoint returns 500",
			apply: func(i *idp) {
				i.jwksStatus = 500
			},
		},
		{
			name: "the document is not JSON",
			apply: func(i *idp) {
				i.jwksRaw = "<html>nope</html>"
			},
		},
		{
			name:  "a refetch after a good fetch is broken",
			prime: true,
			apply: func(i *idp) {
				i.jwksStatus = 500
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			i := newIdP(t)
			p := i.provider(t)
			ctx := context.Background()
			if tc.prime {
				// Prime the cache with a good document, then break the endpoint
				// and force the refetch path with an unknown kid.
				_, err := p.VerifyIDToken(ctx, i.mint(t, "RS256", i.rsaKid, i.validClaims(nil)))
				require.NoError(t, err)
				tc.apply(i)
				_, err = p.VerifyIDToken(ctx, i.mint(t, "RS256", "unknown-kid", i.validClaims(nil)))
				require.ErrorIs(t, err, ErrInvalidToken)
				return
			}
			tc.apply(i)
			_, err := p.VerifyIDToken(ctx, i.mint(t, "RS256", i.rsaKid, i.validClaims(nil)))
			require.ErrorIs(t, err, ErrInvalidToken)
		})
	}
}

func TestVerifyIDTokenOnAProviderNotBuiltByDiscover(t *testing.T) {
	p := &Provider{}
	_, err := p.VerifyIDToken(context.Background(), "a.b.c")
	require.ErrorIs(t, err, ErrInvalidToken)
}

// TestVerifyIDTokenRefusesAKidlessToken: some providers publish a single key
// with no kid at all. A token that names no key must still be refused - matching
// it to whatever key happens to be unnamed is how a verifier ends up trusting a
// key it never selected, which is why the kid is required on both sides.
func TestVerifyIDTokenRefusesAKidlessToken(t *testing.T) {
	i := newIdP(t)
	i.mu.Lock()
	// The same key, published a second time with no kid.
	i.extraKeys = append(i.extraKeys, rsaJWK(&i.rsaKey.PublicKey, "", "RS256"))
	i.mu.Unlock()
	p := i.provider(t)

	// The signature really is valid under that key; only the missing kid makes
	// this token unacceptable.
	raw := i.mint(t, "RS256", "", i.validClaims(nil))
	_, err := p.VerifyIDToken(context.Background(), raw)
	require.ErrorIs(t, err, ErrInvalidToken)
	require.Contains(t, err.Error(), "no kid")
}

// TestVerifySignatureRequiresTheKeyTypeToMatchTheAlgorithm reaches past the
// token path to the rule itself, because that is the only place it can be shown
// to be load-bearing: the JWKS entry here is valid RSA material under the right
// kid, and only its declared kty is wrong. A verifier that let the token's alg
// header pick the verification routine would verify this token.
func TestVerifySignatureRequiresTheKeyTypeToMatchTheAlgorithm(t *testing.T) {
	i := newIdP(t)
	signingInput := []byte("aGVhZGVy.cGF5bG9hZA")
	digest := crypto.SHA256.New()
	digest.Write(signingInput)
	signature, err := rsa.SignPKCS1v15(rand.Reader, i.rsaKey, crypto.SHA256, digest.Sum(nil))
	require.NoError(t, err)

	published := rsaJWK(&i.rsaKey.PublicKey, "rsa-1", "RS256")
	key := jsonWebKey{
		Kty: "EC",
		Kid: "rsa-1",
		Use: "sig",
		Alg: "RS256",
		N:   published["n"].(string),
		E:   published["e"].(string),
	}
	err = verifySignature("RS256", key, signingInput, signature)
	require.ErrorIs(t, err, ErrInvalidToken)
	require.Contains(t, err.Error(), "needs an RSA key")
}

// TestVerifyIDTokenRefusesAKeyPublishedForAnotherAlgorithm: the alg header may
// name a key, not an algorithm the key was not published for. Without this check
// an RS512 token would verify against a key the IdP published only as an RS256
// key, which is a signature the IdP never intended to make.
func TestVerifyIDTokenRefusesAKeyPublishedForAnotherAlgorithm(t *testing.T) {
	i := newIdP(t)
	p := i.provider(t)

	// The harness publishes rsa-1 with alg "RS256"; the same key signs this
	// token as RS512.
	raw := i.mint(t, "RS512", i.rsaKid, i.validClaims(nil))
	_, err := p.VerifyIDToken(context.Background(), raw)
	require.ErrorIs(t, err, ErrInvalidToken)
	require.Contains(t, err.Error(), "published for RS256")
}
