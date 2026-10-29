package oidc

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// This file is the trust anchor: the IdP's published signing keys, and the cache
// that keeps the IdP off the critical path of every request without ever letting
// a stale document hide a key rotation. Two rules shape it:
//
//   - a document is cached for as long as the IdP says (Cache-Control: max-age),
//     and for fifteen minutes when it says nothing, because a fetch per token
//     would put an external service in the way of every authenticated request;
//   - a kid we have never seen may mean "rotated", so we refetch once - but only
//     once per thirty seconds, because an unauthenticated attacker who can send
//     tokens with made-up kids must not be able to make us hammer the IdP.

// jsonWebKeySet is the document served at jwks_uri.
type jsonWebKeySet struct {
	Keys []jsonWebKey `json:"keys"`
}

// jsonWebKey is the subset of a JWK this package uses. Members it does not need
// (x5c, key_ops, ...) are ignored, and unknown kty/crv values are rejected at use
// time rather than at parse time so that one unsupported key in a set does not
// invalidate the keys beside it.
type jsonWebKey struct {
	// Kty is the key type: "RSA" or "EC" here.
	Kty string `json:"kty"`
	// Kid is the key id the token header names.
	Kid string `json:"kid"`
	// Use is "sig" for signing keys; empty is tolerated as "sig".
	Use string `json:"use"`
	// Alg is the algorithm the key is for, when the IdP names one.
	Alg string `json:"alg"`
	// Crv is the curve for EC keys: "P-256", "P-384" or "P-521".
	Crv string `json:"crv"`
	// N and E are the RSA modulus and public exponent, base64url.
	N string `json:"n"`
	E string `json:"e"`
	// X and Y are the EC point coordinates, base64url.
	X string `json:"x"`
	Y string `json:"y"`
}

// jwksCache holds one provider's key set. The mutex is held across the network
// fetch, which serialises concurrent verifies onto one fetch - a stampede of
// first requests after a restart should cost the IdP one request, not one per
// request in flight.
type jwksCache struct {
	provider *Provider

	mu   sync.Mutex
	keys []jsonWebKey
	// expiresAt is when the cached document stops being usable.
	expiresAt time.Time
	// loaded records that keys were fetched at least once.
	loaded bool
	// lastRefetchAt is when the unknown-kid refetch last ran. Only that path
	// touches it, so an ordinary cache expiry is never rate limited.
	lastRefetchAt time.Time
}

// get returns the cached keys, fetching the document when the cache is empty or
// has expired.
func (c *jwksCache) get(ctx context.Context) ([]jsonWebKey, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.loaded && c.provider.Config.now().Before(c.expiresAt) {
		return c.keys, nil
	}
	return c.load(ctx)
}

// keyByID returns the key with the given kid, or an error wrapping
// ErrInvalidToken. An unknown kid triggers at most one refetch, rate limited by
// jwksRefetchMinInterval: the usual cause is a key rotation we have not seen yet,
// and the unusual cause is a forged token.
func (c *jwksCache) keyByID(ctx context.Context, kid string) (jsonWebKey, error) {
	keys, err := c.get(ctx)
	if err != nil {
		return jsonWebKey{}, err
	}
	if k, ok := selectKey(keys, kid); ok {
		return k, nil
	}
	keys, err = c.refetch(ctx)
	if err != nil {
		return jsonWebKey{}, err
	}
	if k, ok := selectKey(keys, kid); ok {
		return k, nil
	}
	return jsonWebKey{}, fmt.Errorf("%w: the identity provider publishes no usable signing key with kid %q", ErrInvalidToken, kid)
}

// refetch reloads the document for an unknown kid, at most once per
// jwksRefetchMinInterval. When the interval has not elapsed it returns whatever
// is cached without a network call; the caller then reports the unknown kid.
func (c *jwksCache) refetch(ctx context.Context) ([]jsonWebKey, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.provider.Config.now()
	if now.Sub(c.lastRefetchAt) < jwksRefetchMinInterval {
		return c.keys, nil
	}
	c.lastRefetchAt = now
	return c.load(ctx)
}

// load fetches and parses the document. The caller must hold c.mu.
func (c *jwksCache) load(ctx context.Context) ([]jsonWebKey, error) {
	cfg := c.provider.Config
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.provider.JWKSURI, nil)
	if err != nil {
		return nil, fmt.Errorf("%w: building the JWKS request: %v", ErrInvalidToken, err)
	}
	req.Header.Set("Accept", "application/json")
	resp, err := cfg.httpClient().Do(req)
	if err != nil {
		return nil, fmt.Errorf("%w: fetching the JWKS from %s: %v", ErrInvalidToken, c.provider.JWKSURI, err)
	}
	defer resp.Body.Close() //nolint:errcheck // nothing useful to do with a close error on a response body
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes))
	if err != nil {
		return nil, fmt.Errorf("%w: reading the JWKS from %s: %v", ErrInvalidToken, c.provider.JWKSURI, err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%w: the JWKS endpoint %s returned %s", ErrInvalidToken, c.provider.JWKSURI, resp.Status)
	}
	var doc jsonWebKeySet
	if err := json.Unmarshal(body, &doc); err != nil {
		return nil, fmt.Errorf("%w: decoding the JWKS from %s: %v", ErrInvalidToken, c.provider.JWKSURI, err)
	}
	c.keys = doc.Keys
	c.loaded = true
	c.expiresAt = cfg.now().Add(cacheLifetime(resp.Header.Get("Cache-Control")))
	return c.keys, nil
}

// cacheLifetime turns a Cache-Control header into a cache lifetime, falling back
// to defaultJWKSMaxAge when the IdP says nothing or says something unusable.
func cacheLifetime(cacheControl string) time.Duration {
	for _, directive := range strings.Split(cacheControl, ",") {
		name, value, ok := strings.Cut(strings.TrimSpace(directive), "=")
		if !ok || !strings.EqualFold(name, "max-age") {
			continue
		}
		seconds, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
		if err != nil || seconds < 0 {
			continue
		}
		return time.Duration(seconds) * time.Second
	}
	return defaultJWKSMaxAge
}

// selectKey picks the signing key with this kid. Keys that are not for signatures
// (use is something other than "sig") and keys whose type this package cannot
// verify with are skipped rather than returned, so a set that carries an
// encryption key with the same kid as a signing key still resolves correctly.
func selectKey(keys []jsonWebKey, kid string) (jsonWebKey, bool) {
	if kid == "" {
		return jsonWebKey{}, false
	}
	for _, k := range keys {
		if k.Kid != kid {
			continue
		}
		if k.Use != "" && k.Use != "sig" {
			continue
		}
		switch k.Kty {
		case "RSA", "EC":
			return k, true
		}
	}
	return jsonWebKey{}, false
}

// decodeBase64URL decodes a JOSE base64url member. JOSE is unpadded, so a padded
// value is a different encoding and is rejected rather than quietly accepted.
func decodeBase64URL(s string) ([]byte, error) {
	return base64.RawURLEncoding.DecodeString(s)
}

// trimJSON strips the whitespace around a raw JSON value so a caller can switch
// on its first byte.
func trimJSON(raw json.RawMessage) []byte { return bytes.TrimSpace(raw) }
