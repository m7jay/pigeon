package oidc

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/hmac"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	// Registered for its side effect: crypto.SHA384.New needs SHA-512's
	// implementation linked in, and the harness signs HS384 with it.
	_ "crypto/sha512"
	"encoding/base64"
	"encoding/json"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// This file is the harness the rest of the tests are written against: an
// in-process identity provider (discovery, JWKS and token endpoint over
// httptest), two keys it signs with, and the ability to mint a token under any
// algorithm and kid. Everything the package does that could be faked - time, the
// HTTP client, the IdP's answers - is controlled here, so no test needs the
// network and every test can count what was asked of the IdP.

// fakeClock is the clock the tests give Config.Now. time is a parameter of the
// cache, the rate limiter and the claim checks, so it has to be a parameter of
// the tests too: a test that waited fifteen real minutes for a cache to expire
// would not run.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock(t time.Time) *fakeClock { return &fakeClock{t: t} }

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// idp is the in-process identity provider.
type idp struct {
	t   *testing.T
	srv *httptest.Server

	rsaKey *rsa.PrivateKey
	rsaKid string
	ecKey  *ecdsa.PrivateKey
	ecKid  string

	clock *fakeClock

	mu        sync.Mutex
	jwksHits  int
	tokenHits int
	tokenForm url.Values

	// extraKeys are added to the JWKS beyond the two the harness signs with, so
	// a test can simulate a key rotation.
	extraKeys []map[string]any
	// cacheControl is the Cache-Control the JWKS endpoint sends.
	cacheControl string
	// jwksStatus, when non-zero, replaces the JWKS status.
	jwksStatus int
	// jwksRaw, when set, is served instead of the real key set.
	jwksRaw string
	// tokenStatus and tokenBody drive the token endpoint.
	tokenStatus int
	tokenBody   string
	// discoveryDoc, when set, replaces the discovery document.
	discoveryDoc func(base string) map[string]any
	// discoveryStatus, when non-zero, replaces the discovery status.
	discoveryStatus int
	// discoveryRaw, when set, is served verbatim.
	discoveryRaw string
}

func newIdP(t *testing.T) *idp {
	t.Helper()
	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	ecKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	i := &idp{
		t:            t,
		rsaKey:       rsaKey,
		rsaKid:       "rsa-1",
		ecKey:        ecKey,
		ecKid:        "ec-1",
		clock:        newFakeClock(time.Date(2026, time.March, 1, 12, 0, 0, 0, time.UTC)),
		cacheControl: "max-age=300",
		tokenStatus:  http.StatusOK,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/openid-configuration", i.serveDiscovery)
	mux.HandleFunc("/jwks", i.serveJWKS)
	mux.HandleFunc("/token", i.serveToken)
	i.srv = httptest.NewServer(mux)
	t.Cleanup(i.srv.Close)
	return i
}

func (i *idp) serveDiscovery(w http.ResponseWriter, r *http.Request) {
	if i.discoveryRaw != "" {
		w.Header().Set("Content-Type", "application/json")
		if i.discoveryStatus != 0 {
			w.WriteHeader(i.discoveryStatus)
		}
		_, _ = io.WriteString(w, i.discoveryRaw)
		return
	}
	doc := defaultDiscovery(i.srv.URL)
	if i.discoveryDoc != nil {
		doc = i.discoveryDoc(i.srv.URL)
	}
	w.Header().Set("Content-Type", "application/json")
	if i.discoveryStatus != 0 {
		w.WriteHeader(i.discoveryStatus)
	}
	_ = json.NewEncoder(w).Encode(doc)
}

func (i *idp) serveJWKS(w http.ResponseWriter, r *http.Request) {
	i.mu.Lock()
	i.jwksHits++
	keys := i.jwksDocument()
	cacheControl := i.cacheControl
	status := i.jwksStatus
	raw := i.jwksRaw
	i.mu.Unlock()

	if cacheControl != "" {
		w.Header().Set("Cache-Control", cacheControl)
	}
	w.Header().Set("Content-Type", "application/json")
	if status != 0 {
		w.WriteHeader(status)
	}
	if raw != "" {
		_, _ = io.WriteString(w, raw)
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"keys": keys})
}

func (i *idp) serveToken(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	i.mu.Lock()
	i.tokenHits++
	i.tokenForm = r.PostForm
	status := i.tokenStatus
	body := i.tokenBody
	i.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if body == "" {
		body = `{"id_token":"hdr.payload.sig","access_token":"access-1","refresh_token":"refresh-1","token_type":"Bearer","expires_in":3600}`
	}
	_, _ = io.WriteString(w, body)
}

// jwksDocument is the key set the IdP publishes: the RSA key, the P-256 key, any
// keys a test added, plus an encryption key that must be ignored.
func (i *idp) jwksDocument() []map[string]any {
	keys := []map[string]any{
		rsaJWK(&i.rsaKey.PublicKey, i.rsaKid, "RS256"),
		ecJWK(&i.ecKey.PublicKey, i.ecKid, "ES256"),
		{"kty": "RSA", "kid": "enc-1", "use": "enc", "alg": "RSA-OAEP", "n": rsaJWK(&i.rsaKey.PublicKey, i.rsaKid, "RS256")["n"], "e": "AQAB"},
	}
	return append(keys, i.extraKeys...)
}

func (i *idp) jwksRequests() int {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.jwksHits
}

func (i *idp) tokenRequests() int {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.tokenHits
}

func (i *idp) lastTokenForm() url.Values {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.tokenForm
}

// addRSAKey publishes another RSA key, the shape a key rotation takes.
func (i *idp) addRSAKey(t *testing.T, kid string) *rsa.PrivateKey {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	i.mu.Lock()
	i.extraKeys = append(i.extraKeys, rsaJWK(&key.PublicKey, kid, "RS256"))
	i.mu.Unlock()
	return key
}

func (i *idp) config() Config {
	return Config{
		Issuer:       i.srv.URL,
		ClientID:     "client-1",
		ClientSecret: "secret-1",
		RedirectURL:  i.srv.URL + "/callback",
		HTTPClient:   i.srv.Client(),
		Now:          i.clock.Now,
	}
}

func (i *idp) provider(t *testing.T) *Provider {
	t.Helper()
	p, err := Discover(context.Background(), i.config())
	require.NoError(t, err)
	return p
}

// signer is the harness's hand as the IdP.
func (i *idp) signer() jwsSigner {
	return jwsSigner{rsa: i.rsaKey, ec: i.ecKey}
}

// validClaims is a claim set that passes every rule, which each test then breaks
// in exactly one way.
func (i *idp) validClaims(mutate func(map[string]any)) map[string]any {
	now := i.clock.Now().Unix()
	claims := map[string]any{
		"iss":            i.srv.URL,
		"sub":            "user-1",
		"aud":            "client-1",
		"exp":            now + 600,
		"iat":            now,
		"email":          "user@example.com",
		"email_verified": true,
		"name":           "User One",
		"nonce":          "nonce-1",
	}
	if mutate != nil {
		mutate(claims)
	}
	return claims
}

func (i *idp) mint(t *testing.T, alg, kid string, claims map[string]any) string {
	t.Helper()
	return i.signer().mint(t, alg, kid, claims)
}

// defaultDiscovery is the document a well-behaved provider publishes.
func defaultDiscovery(base string) map[string]any {
	return map[string]any{
		"issuer":                                base,
		"authorization_endpoint":                base + "/authorize",
		"token_endpoint":                        base + "/token",
		"jwks_uri":                              base + "/jwks",
		"userinfo_endpoint":                     base + "/userinfo",
		"end_session_endpoint":                  base + "/logout",
		"id_token_signing_alg_values_supported": []string{"RS256", "ES256"},
		"code_challenge_methods_supported":      []string{"S256"},
	}
}

func rsaJWK(pub *rsa.PublicKey, kid, alg string) map[string]any {
	return map[string]any{
		"kty": "RSA",
		"kid": kid,
		"use": "sig",
		"alg": alg,
		"n":   b64(pub.N.Bytes()),
		"e":   b64(big.NewInt(int64(pub.E)).Bytes()),
	}
}

func ecJWK(pub *ecdsa.PublicKey, kid, alg string) map[string]any {
	x, y := ecCoordinates(pub)
	return map[string]any{
		"kty": "EC",
		"kid": kid,
		"use": "sig",
		"alg": alg,
		"crv": "P-256",
		"x":   x,
		"y":   y,
	}
}

func ecCoordinates(pub *ecdsa.PublicKey) (string, string) {
	size := (pub.Curve.Params().BitSize + 7) / 8
	x := make([]byte, size)
	y := make([]byte, size)
	pub.X.FillBytes(x)
	pub.Y.FillBytes(y)
	return b64(x), b64(y)
}

// jwsSigner mints tokens the way an IdP does, which is the only way to test the
// verifier honestly: every signature in these tests is produced by code that is
// not the code under test.
type jwsSigner struct {
	rsa *rsa.PrivateKey
	ec  *ecdsa.PrivateKey
}

func (s jwsSigner) mint(t *testing.T, alg, kid string, claims map[string]any) string {
	t.Helper()
	header := map[string]any{"alg": alg, "typ": "JWT"}
	if kid != "" {
		header["kid"] = kid
	}
	hb, err := json.Marshal(header)
	require.NoError(t, err)
	pb, err := json.Marshal(claims)
	require.NoError(t, err)
	input := b64(hb) + "." + b64(pb)
	sig, err := s.sign(alg, []byte(input))
	require.NoError(t, err)
	return input + "." + b64(sig)
}

func (s jwsSigner) sign(alg string, input []byte) ([]byte, error) {
	switch alg {
	case "none":
		return nil, nil
	case "HS256", "HS384", "HS512":
		// Key confusion in the shape it is actually attempted: the attacker
		// signs with the IdP's *public* modulus, because the modulus is the only
		// key material they have seen. A verifier that picked its routine from
		// the token's alg header, and its key from the kid, would accept this
		// very token. The package must refuse the algorithm before it chooses a
		// key at all, which is what the HS* cases assert.
		mac := hmac.New(testHMACHash(alg).New, s.rsa.PublicKey.N.Bytes())
		mac.Write(input)
		return mac.Sum(nil), nil
	case "RS256", "RS384", "RS512":
		hash := testHash(alg)
		h := hash.New()
		h.Write(input)
		return rsa.SignPKCS1v15(rand.Reader, s.rsa, hash, h.Sum(nil))
	case "ES256":
		h := sha256.Sum256(input)
		r, ss, err := ecdsa.Sign(rand.Reader, s.ec, h[:])
		if err != nil {
			return nil, err
		}
		size := (s.ec.Curve.Params().BitSize + 7) / 8
		out := make([]byte, 2*size)
		r.FillBytes(out[:size])
		ss.FillBytes(out[size:])
		return out, nil
	case "ES384":
		// Signing ES384 over the P-256 key on purpose: the harness has no P-384
		// key, and the verifier must refuse an ES384 header before it can be
		// reached with one.
		hash := crypto.SHA384
		h := hash.New()
		h.Write(input)
		r, ss, err := ecdsa.Sign(rand.Reader, s.ec, h.Sum(nil))
		if err != nil {
			return nil, err
		}
		size := (s.ec.Curve.Params().BitSize + 7) / 8
		out := make([]byte, 2*size)
		r.FillBytes(out[:size])
		ss.FillBytes(out[size:])
		return out, nil
	default:
		// An algorithm the package must refuse before it ever looks at a
		// signature. A placeholder keeps these tests about the refusal rather
		// than about the harness being able to sign.
		return []byte("not-a-real-signature"), nil
	}
}

func testHash(alg string) crypto.Hash {
	switch alg {
	case "RS256":
		return crypto.SHA256
	case "RS384":
		return crypto.SHA384
	default:
		return crypto.SHA512
	}
}

func testHMACHash(alg string) crypto.Hash {
	switch alg {
	case "HS384":
		return crypto.SHA384
	case "HS512":
		return crypto.SHA512
	default:
		return crypto.SHA256
	}
}

func b64(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }

// tamperPayload rewrites one claim after signing, which is what "the payload was
// edited in flight" looks like on the wire.
func tamperPayload(t *testing.T, raw string, mutate func(map[string]any)) string {
	t.Helper()
	parts := strings.Split(raw, ".")
	require.Len(t, parts, 3)
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	require.NoError(t, err)
	var claims map[string]any
	require.NoError(t, json.Unmarshal(payload, &claims))
	mutate(claims)
	edited, err := json.Marshal(claims)
	require.NoError(t, err)
	parts[1] = b64(edited)
	return strings.Join(parts, ".")
}

// tamperSignature rewrites one byte of the signature.
func tamperSignature(t *testing.T, raw string) string {
	t.Helper()
	parts := strings.Split(raw, ".")
	require.Len(t, parts, 3)
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	require.NoError(t, err)
	require.NotEmpty(t, sig)
	sig[len(sig)-1] ^= 0x01
	parts[2] = b64(sig)
	return strings.Join(parts, ".")
}

func TestConfigEnabled(t *testing.T) {
	cases := []struct {
		name           string
		cfg            Config
		server, client bool
	}{
		{name: "empty", cfg: Config{}, server: false, client: false},
		{name: "issuer only", cfg: Config{Issuer: "https://idp.example.com"}, server: false, client: false},
		{name: "issuer and client id verifies tokens", cfg: Config{Issuer: "https://idp.example.com", ClientID: "c"}, server: true, client: false},
		{name: "secret without redirect does not log in", cfg: Config{Issuer: "https://idp.example.com", ClientID: "c", ClientSecret: "s"}, server: true, client: false},
		{name: "redirect without secret does not log in", cfg: Config{Issuer: "https://idp.example.com", ClientID: "c", RedirectURL: "https://app.example.com/cb"}, server: true, client: false},
		{name: "full", cfg: Config{Issuer: "https://idp.example.com", ClientID: "c", ClientSecret: "s", RedirectURL: "https://app.example.com/cb"}, server: true, client: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.server, tc.cfg.ServerEnabled())
			require.Equal(t, tc.client, tc.cfg.ClientEnabled())
			require.Equal(t, tc.server, tc.cfg.Enabled())
		})
	}
}

func TestDiscover(t *testing.T) {
	i := newIdP(t)
	p, err := Discover(context.Background(), i.config())
	require.NoError(t, err)
	require.Equal(t, i.srv.URL+"/authorize", p.AuthorizationEndpoint)
	require.Equal(t, i.srv.URL+"/token", p.TokenEndpoint)
	require.Equal(t, i.srv.URL+"/jwks", p.JWKSURI)
	require.Equal(t, i.srv.URL+"/userinfo", p.UserinfoEndpoint)
	require.Equal(t, i.srv.URL+"/logout", p.EndSessionEndpoint)
	require.Equal(t, []string{"RS256", "ES256"}, p.IDTokenSigningAlgValuesSupported)
	require.True(t, p.ClientEnabled())
	require.Equal(t, i.config().Issuer, p.Config.Issuer)
}

func TestDiscoverAcceptsATrailingSlashOnTheIssuer(t *testing.T) {
	i := newIdP(t)
	cfg := i.config()
	cfg.Issuer = i.srv.URL + "/"
	p, err := Discover(context.Background(), cfg)
	require.NoError(t, err)

	// The trimming must hold on both sides: a token whose iss is the same issuer
	// spelled with the slash verifies against a configuration written without it.
	raw := i.signer().mint(t, "RS256", i.rsaKid, i.validClaims(func(c map[string]any) {
		c["iss"] = i.srv.URL + "/"
	}))
	_, err = p.VerifyIDToken(context.Background(), raw)
	require.NoError(t, err)
}

func TestDiscoverRejects(t *testing.T) {
	cases := []struct {
		name  string
		setup func(i *idp, cfg *Config)
	}{
		{
			name: "no issuer configured",
			setup: func(i *idp, cfg *Config) {
				cfg.Issuer = ""
			},
		},
		{
			name: "document without issuer",
			setup: func(i *idp, cfg *Config) {
				i.discoveryDoc = func(base string) map[string]any {
					doc := defaultDiscovery(base)
					delete(doc, "issuer")
					return doc
				}
			},
		},
		{
			name: "document without authorization endpoint",
			setup: func(i *idp, cfg *Config) {
				i.discoveryDoc = func(base string) map[string]any {
					doc := defaultDiscovery(base)
					delete(doc, "authorization_endpoint")
					return doc
				}
			},
		},
		{
			name: "document without token endpoint",
			setup: func(i *idp, cfg *Config) {
				i.discoveryDoc = func(base string) map[string]any {
					doc := defaultDiscovery(base)
					delete(doc, "token_endpoint")
					return doc
				}
			},
		},
		{
			name: "document without jwks uri",
			setup: func(i *idp, cfg *Config) {
				i.discoveryDoc = func(base string) map[string]any {
					doc := defaultDiscovery(base)
					delete(doc, "jwks_uri")
					return doc
				}
			},
		},
		{
			name: "document for a different issuer",
			setup: func(i *idp, cfg *Config) {
				i.discoveryDoc = func(base string) map[string]any {
					doc := defaultDiscovery(base)
					doc["issuer"] = "https://idp.attacker.example"
					return doc
				}
			},
		},
		{
			name: "non-200 document",
			setup: func(i *idp, cfg *Config) {
				i.discoveryStatus = http.StatusNotFound
			},
		},
		{
			name: "document that is not JSON",
			setup: func(i *idp, cfg *Config) {
				i.discoveryRaw = "<html>not json</html>"
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			i := newIdP(t)
			cfg := i.config()
			tc.setup(i, &cfg)
			_, err := Discover(context.Background(), cfg)
			require.Error(t, err)
			require.ErrorIs(t, err, ErrDiscoveryFailed)
		})
	}
}

func TestCacheLifetime(t *testing.T) {
	cases := []struct {
		cacheControl string
		want         time.Duration
	}{
		{cacheControl: "", want: defaultJWKSMaxAge},
		{cacheControl: "max-age=300", want: 5 * time.Minute},
		{cacheControl: "public, max-age=60", want: time.Minute},
		{cacheControl: "MAX-AGE=120", want: 2 * time.Minute},
		{cacheControl: "no-store", want: defaultJWKSMaxAge},
		{cacheControl: "max-age=soon", want: defaultJWKSMaxAge},
		{cacheControl: "max-age=-5", want: defaultJWKSMaxAge},
	}
	for _, tc := range cases {
		t.Run(tc.cacheControl, func(t *testing.T) {
			require.Equal(t, tc.want, cacheLifetime(tc.cacheControl))
		})
	}
}

func TestSelectKey(t *testing.T) {
	keys := []jsonWebKey{
		{Kid: "rsa-1", Kty: "RSA", Use: "sig"},
		{Kid: "enc-1", Kty: "RSA", Use: "enc"},
		{Kid: "oct-1", Kty: "oct", Use: "sig"},
		{Kid: "ec-1", Kty: "EC", Use: "", Crv: "P-256"},
	}
	got, ok := selectKey(keys, "rsa-1")
	require.True(t, ok)
	require.Equal(t, "RSA", got.Kty)

	// An encryption key published under the kid a token names must not be used
	// as a signing key.
	_, ok = selectKey(keys, "enc-1")
	require.False(t, ok)

	// A key type this package cannot verify with is skipped, whatever its use.
	_, ok = selectKey(keys, "oct-1")
	require.False(t, ok)

	_, ok = selectKey(keys, "ec-1")
	require.True(t, ok)

	_, ok = selectKey(keys, "")
	require.False(t, ok)

	_, ok = selectKey(keys, "missing")
	require.False(t, ok)
}
