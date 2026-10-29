// Package oidc is pigeon's optional OpenID Connect integration (FR-106).
//
// Why this package exists, and why it has the two halves it has: FR-106 asks for
// "optional OIDC login, configurable by env vars, with password auth remaining
// available", and that sentence hides two deliveries that run in two different
// places and carry two different amounts of trust.
//
//   - The core API server has to answer "who is the bearer of this id_token?" for
//     tokens an external identity provider minted. That half - VerifyIDToken -
//     never holds a client secret and never talks to the IdP except to read the
//     public key set it publishes. Nothing in it takes the token's word for
//     anything: a token becomes Claims only after its signature verifies against
//     a key fetched from the IdP's JWKS and after every claim rule in
//     verify.go holds.
//   - The relying party has to start a login (AuthCodeURL, authorization code +
//     PKCE) and finish one (Exchange). That half holds the client secret and the
//     redirect URL and belongs to the portal, not to every process that serves a
//     request.
//
// Both live in one package because they share one Config, one discovery document
// and one cache of the IdP's keys, and because "where is OIDC implemented?" should
// have exactly one answer. They are separated by method, and by the two predicates
// a deployment checks at startup: Config.ServerEnabled (we can verify id_tokens)
// and Config.ClientEnabled (we can also log users in). A deployment that only
// wants OIDC login on the web portal and only token verification in the API sets
// neither, one or both, and the predicates name which construction is legal.
//
// Why it is written rather than pulled in: the tree already takes golang.org/x/crypto
// and the standard library, and a JOSE dependency is a large, security-critical
// surface for what is two algorithms (RSASSA-PKCS1-v1_5 and ECDSA over NIST
// curves) and one JSON document. The parts a library would have hidden are
// exactly the parts that need a test here - the raw r||s encoding JWS uses for
// ES* signatures, the key-confusion rule that must reject HMAC algorithms
// outright, and the claim rules that decide whether a token is a login.
//
// Password authentication remains the default; SPEC.md defers FR-106 to S9, and
// this package is deliberately inert unless the deployment supplies an issuer.
// docs/oidc.md is the operator-facing half of the same story.
//
// Error handling is sentinel-style: every rejection of a token wraps
// ErrInvalidToken, so a caller can answer 401 for "this token is not valid" and
// tell it apart from a transport failure or a programming mistake. Errors never
// quote the token, the client secret or the client secret's hash.
package oidc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const (
	// defaultHTTPTimeout bounds every outbound call the package makes. Both the
	// discovery fetch and the token exchange happen on a user's login path, so a
	// hung IdP must fail the request rather than hold a connection forever.
	defaultHTTPTimeout = 10 * time.Second

	// defaultJWKSMaxAge is how long a key set is cached when the IdP sends no
	// Cache-Control: max-age. Key rotation is a rare event and a fetch per token
	// would put the IdP on the critical path of every request.
	defaultJWKSMaxAge = 15 * time.Minute

	// jwksRefetchMinInterval rate-limits the "unknown kid" refetch. An attacker
	// who can present tokens with random kids must not be able to turn the API
	// server into a request amplifier against the IdP.
	jwksRefetchMinInterval = 30 * time.Second

	// clockSkew is the leeway applied to exp and iat. The API server and the IdP
	// are different machines with different clocks; 60 seconds is the tolerance
	// this package documents rather than an accident of arithmetic.
	clockSkew = 60 * time.Second

	// maxIATFuture is how far a token's iat may run ahead of our clock (plus
	// clockSkew). A token issued "five minutes from now" is a forged or badly
	// clocked token, not a fast IdP.
	maxIATFuture = 5 * time.Minute

	// maxBodyBytes caps every response body this package reads. Discovery
	// documents, JWKS documents and token responses are all small JSON; a byte
	// count is what keeps a hostile or broken IdP from exhausting memory.
	maxBodyBytes = 1 << 20
)

// defaultHTTPClient is used when Config.HTTPClient is nil. It is shared so the
// connection pool is shared too; http.Client is safe for concurrent use.
var defaultHTTPClient = &http.Client{Timeout: defaultHTTPTimeout}

// ErrInvalidToken is the sentinel every rejection of an id_token wraps. A caller
// that sees errors.Is(err, ErrInvalidToken) may answer 401; anything else is a
// transport failure or a misuse of this package, which are not the user's fault.
var ErrInvalidToken = errors.New("oidc: invalid id_token")

// ErrExchangeFailed is the sentinel every failure of the token exchange wraps.
// It says the code could not be turned into tokens - never that the resulting
// id_token is invalid, which is VerifyIDToken's question.
var ErrExchangeFailed = errors.New("oidc: token exchange failed")

// ErrDiscoveryFailed is the sentinel every failure to load a provider's discovery
// document wraps.
var ErrDiscoveryFailed = errors.New("oidc: discovery failed")

// Config is the deployment's OIDC settings, as assembled from environment
// variables (FR-106). One Config describes both halves; the predicates below say
// which halves it is complete enough for.
type Config struct {
	// Issuer is the discovery base, e.g. https://accounts.example.com. A single
	// trailing slash is tolerated and ignored everywhere it is compared.
	Issuer string
	// ClientID is this deployment's client id at the IdP. Required for both halves.
	ClientID string
	// ClientSecret is used only by the relying-party half (Exchange). The
	// verifier never sends it and never needs it.
	ClientSecret string
	// RedirectURL is used only by the relying-party half.
	RedirectURL string
	// Scopes are the scopes the client asks for. Empty means openid, email and
	// profile; "openid" is always included whatever this holds.
	Scopes []string
	// HTTPClient is optional. Nil means a client with a 10 second timeout.
	HTTPClient *http.Client
	// Now is optional and exists so tests can drive time. Nil means time.Now.
	// It is used for the JWKS cache clock and for exp/iat checks alike.
	Now func() time.Time
}

// ServerEnabled reports whether the configuration can verify id_tokens: that is
// the core API server half. It needs an issuer to discover the key set from and a
// client id to check the audience against. It deliberately does not need the
// client secret or the redirect URL, because the process that verifies tokens
// receives them from the IdP, it does not ask for them.
func (c Config) ServerEnabled() bool {
	return c.Issuer != "" && c.ClientID != ""
}

// ClientEnabled reports whether the configuration can also act as the relying
// party: start a login and exchange a code. That is the portal half, and it is
// the only half allowed to hold a client secret and a redirect URL.
func (c Config) ClientEnabled() bool {
	return c.ServerEnabled() && c.ClientSecret != "" && c.RedirectURL != ""
}

// Enabled is ServerEnabled. It exists because early sketches of FR-106 asked one
// question of one boolean; keeping the spelling means a caller that only ever
// wants the verifier does not have to know which half it is asking about.
func (c Config) Enabled() bool { return c.ServerEnabled() }

// now returns the configured clock, or the wall clock when none is set.
func (c Config) now() time.Time {
	if c.Now != nil {
		return c.Now()
	}
	return time.Now()
}

// httpClient returns the configured client, or the shared default.
func (c Config) httpClient() *http.Client {
	if c.HTTPClient != nil {
		return c.HTTPClient
	}
	return defaultHTTPClient
}

// scopes returns the scopes to request: "openid" is always present (without it
// the response is not an OpenID Connect response and carries no id_token), and an
// empty Config.Scopes means the conventional openid, email, profile.
func (c Config) scopes() []string {
	out := []string{"openid"}
	for _, s := range c.Scopes {
		if s != "openid" {
			out = append(out, s)
		}
	}
	if len(c.Scopes) == 0 {
		out = append(out, "email", "profile")
	}
	return out
}

// issuer trims a single trailing slash. Issuers are written both ways in the
// wild and in configuration files; the one place the two spellings must agree is
// every comparison, so the trim happens once, here.
func (c Config) issuer() string { return trimSlash(c.Issuer) }

func trimSlash(s string) string { return strings.TrimSuffix(s, "/") }

// discoveryDocument is the subset of the OpenID Connect discovery document this
// package reads. Fields it does not need are ignored rather than modelled.
type discoveryDocument struct {
	Issuer                           string   `json:"issuer"`
	AuthorizationEndpoint            string   `json:"authorization_endpoint"`
	TokenEndpoint                    string   `json:"token_endpoint"`
	JWKSURI                          string   `json:"jwks_uri"`
	UserinfoEndpoint                 string   `json:"userinfo_endpoint"`
	EndSessionEndpoint               string   `json:"end_session_endpoint"`
	IDTokenSigningAlgValuesSupported []string `json:"id_token_signing_alg_values_supported"`
	CodeChallengeMethodsSupported    []string `json:"code_challenge_methods_supported"`
}

// Provider is a discovered OpenID Connect provider plus the Config it was
// discovered from. The key set is cached behind it and refreshed on its own; a
// Provider is safe for concurrent use.
type Provider struct {
	Config Config

	// AuthorizationEndpoint is where the browser is sent to authenticate.
	AuthorizationEndpoint string
	// TokenEndpoint is where an authorization code is exchanged.
	TokenEndpoint string
	// JWKSURI is where the IdP publishes its signing keys.
	JWKSURI string
	// UserinfoEndpoint is optional; not used by this package, kept because the
	// configuration surface FR-106 describes lists it.
	UserinfoEndpoint string
	// EndSessionEndpoint is optional; likewise kept for the caller.
	EndSessionEndpoint string
	// IDTokenSigningAlgValuesSupported is what the IdP advertises. It is advisory
	// only: VerifyIDToken accepts the algorithms this package implements
	// regardless, because an IdP that advertises nothing is common and a token
	// that verifies is the real test.
	IDTokenSigningAlgValuesSupported []string

	jwks *jwksCache
}

// ClientEnabled reports whether this provider's configuration can act as the
// relying party. It is a method as well as a Config predicate because the caller
// usually holds a *Provider, not a Config.
func (p *Provider) ClientEnabled() bool { return p.Config.ClientEnabled() }

// Discover fetches {issuer}/.well-known/openid-configuration and returns a
// Provider built from it.
//
// It fails when the document cannot be fetched or decoded, when it omits the
// issuer, the authorization endpoint, the token endpoint or the JWKS URI, or when
// its issuer is not the issuer we asked about - a provider that answers for a
// different iss cannot be the trust anchor for tokens that say that iss, and
// accepting it would let a redirect or a proxy rewrite the identity of the IdP.
func Discover(ctx context.Context, cfg Config) (*Provider, error) {
	if trimSlash(cfg.Issuer) == "" {
		return nil, fmt.Errorf("%w: issuer is required", ErrDiscoveryFailed)
	}
	docURL := cfg.issuer() + "/.well-known/openid-configuration"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, docURL, nil)
	if err != nil {
		return nil, fmt.Errorf("%w: %s: %v", ErrDiscoveryFailed, docURL, err)
	}
	req.Header.Set("Accept", "application/json")
	resp, err := cfg.httpClient().Do(req)
	if err != nil {
		return nil, fmt.Errorf("%w: fetching %s: %v", ErrDiscoveryFailed, docURL, err)
	}
	defer resp.Body.Close() //nolint:errcheck // nothing useful to do with a close error on a response body
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes))
	if err != nil {
		return nil, fmt.Errorf("%w: reading %s: %v", ErrDiscoveryFailed, docURL, err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%w: %s returned %s", ErrDiscoveryFailed, docURL, resp.Status)
	}
	var doc discoveryDocument
	if err := json.Unmarshal(body, &doc); err != nil {
		return nil, fmt.Errorf("%w: decoding %s: %v", ErrDiscoveryFailed, docURL, err)
	}
	var missing []string
	if doc.Issuer == "" {
		missing = append(missing, "issuer")
	}
	if doc.AuthorizationEndpoint == "" {
		missing = append(missing, "authorization_endpoint")
	}
	if doc.TokenEndpoint == "" {
		missing = append(missing, "token_endpoint")
	}
	if doc.JWKSURI == "" {
		missing = append(missing, "jwks_uri")
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("%w: %s has no %s", ErrDiscoveryFailed, docURL, strings.Join(missing, ", "))
	}
	if trimSlash(doc.Issuer) != cfg.issuer() {
		return nil, fmt.Errorf("%w: %s answered with issuer %q, not %q", ErrDiscoveryFailed, docURL, doc.Issuer, cfg.Issuer)
	}

	p := &Provider{
		Config:                           cfg,
		AuthorizationEndpoint:            doc.AuthorizationEndpoint,
		TokenEndpoint:                    doc.TokenEndpoint,
		JWKSURI:                          doc.JWKSURI,
		UserinfoEndpoint:                 doc.UserinfoEndpoint,
		EndSessionEndpoint:               doc.EndSessionEndpoint,
		IDTokenSigningAlgValuesSupported: doc.IDTokenSigningAlgValuesSupported,
	}
	p.jwks = &jwksCache{provider: p}
	return p, nil
}

// Claims is the identity an id_token asserts, after verification. Every field
// here has been checked against the rules in verify.go; a zero-valued field means
// the token did not carry it, not that it was trusted unchecked.
type Claims struct {
	// Subject is the IdP's stable identifier for the person (sub).
	Subject string
	// Email is the address the IdP asserts (email); verification rules require
	// it to be non-empty.
	Email string
	// Name is the display name (name), when the IdP sends one.
	Name string
	// Issuer is the issuing authority as the token spelled it (iss). It has
	// already been shown to match Config.Issuer.
	Issuer string
	// Audience is the token's audience (aud), in either of its two wire forms.
	Audience []string
	// EmailVerified is the IdP's assertion that it has verified Email. It is
	// always true for a Claims that VerifyIDToken returned.
	EmailVerified bool
	// ExpiresAt is exp as an instant.
	ExpiresAt time.Time
	// IssuedAt is iat as an instant, zero when the token carried none.
	IssuedAt time.Time
	// Nonce is the login nonce (nonce), when the IdP echoes one. VerifyIDToken
	// cannot check it - it never saw the request - so the relying party must
	// compare it against the nonce it generated in AuthCodeURL.
	Nonce string
}

// Tokens is the result of exchanging an authorization code.
type Tokens struct {
	// IDToken is the signed token that VerifyIDToken turns into Claims.
	IDToken string
	// AccessToken is the bearer token for the IdP's own APIs.
	AccessToken string
	// RefreshToken is present only when the IdP chose to issue one.
	RefreshToken string
	// ExpiresAt is when the access token expires, derived from the response's
	// expires_in. It is the zero time when the IdP sent none.
	ExpiresAt time.Time
}
