package oidc

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestLazyVerifierConstructsWithoutIO is the property the core server depends on:
// building the verifier must not touch the network, because an instance whose IdP
// is down has to boot (FR-106 is optional).
func TestLazyVerifierConstructsWithoutIO(t *testing.T) {
	i := newIdP(t)
	lazy := NewLazyVerifier(i.config())
	require.Zero(t, i.jwksRequests()+i.tokenRequests(), "construction must not make any request")

	// The issuer is never contacted, so a request against a server that has been
	// shut down still fails as a token error rather than a panic or a hang.
	res := make(chan error, 1)
	go func() {
		_, err := lazy.VerifyIDToken(context.Background(), "not-a-token")
		res <- err
	}()
	select {
	case err := <-res:
		require.Error(t, err)
		require.ErrorIs(t, err, ErrInvalidToken)
	case <-time.After(5 * time.Second):
		t.Fatal("VerifyIDToken did not return")
	}
}

// TestLazyVerifierDiscoversOnceAndVerifies covers the happy path: the first call
// discovers, the token verifies, and the second call reuses the provider.
func TestLazyVerifierDiscoversOnceAndVerifies(t *testing.T) {
	i := newIdP(t)
	lazy := NewLazyVerifier(i.config())
	token := i.mint(t, "RS256", i.rsaKid, i.validClaims(nil))

	claims, err := lazy.VerifyIDToken(context.Background(), token)
	require.NoError(t, err)
	require.Equal(t, "user@example.com", claims.Email)

	hits := i.jwksRequests()
	_, err = lazy.VerifyIDToken(context.Background(), token)
	require.NoError(t, err)
	require.Equal(t, hits, i.jwksRequests(), "the second verify must reuse the discovered provider")
}

// TestLazyVerifierRetriesAfterAFailure is the recovery half: a discovery that
// failed is retried once the interval elapses, and refused in between.
func TestLazyVerifierRetriesAfterAFailure(t *testing.T) {
	i := newIdP(t)
	cfg := i.config()
	clock := i.clock
	cfg.Now = clock.Now
	i.discoveryStatus = 500

	lazy := NewLazyVerifier(cfg)
	_, err := lazy.VerifyIDToken(context.Background(), "x")
	require.ErrorIs(t, err, ErrDiscoveryFailed)

	// Within the interval: refused from the recorded error, without a request.
	i.discoveryStatus = 0
	_, err = lazy.VerifyIDToken(context.Background(), "x")
	require.ErrorIs(t, err, ErrDiscoveryFailed)
	require.Contains(t, err.Error(), "retrying in")

	// After the interval: discovered, then the token verifies.
	clock.advance(discoverRetryInterval + time.Second)
	token := i.mint(t, "RS256", i.rsaKid, i.validClaims(nil))
	claims, err := lazy.VerifyIDToken(context.Background(), token)
	require.NoError(t, err)
	require.Equal(t, "user@example.com", claims.Email)
}

// TestLazyVerifierRefusesWithoutAnIssuer keeps the misconfiguration distinct: a
// process with no issuer configured never reports a token failure as an issuer
// outage, and never makes a request.
func TestLazyVerifierRefusesWithoutAnIssuer(t *testing.T) {
	lazy := NewLazyVerifier(Config{ClientID: "client-1"})
	_, err := lazy.VerifyIDToken(context.Background(), "x")
	require.ErrorIs(t, err, ErrInvalidToken)
	require.Contains(t, err.Error(), "no issuer configured")
}
