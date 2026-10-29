package oidc

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAuthCodeURL(t *testing.T) {
	i := newIdP(t)
	p := i.provider(t)

	raw := p.AuthCodeURL("state-1", "nonce-1", "verifier-1")
	u, err := url.Parse(raw)
	require.NoError(t, err)
	require.Equal(t, i.srv.URL+"/authorize", u.Scheme+"://"+u.Host+u.Path)

	q := u.Query()
	require.Equal(t, "code", q.Get("response_type"))
	require.Equal(t, "client-1", q.Get("client_id"))
	require.Equal(t, i.srv.URL+"/callback", q.Get("redirect_uri"))
	require.Equal(t, "openid email profile", q.Get("scope"))
	require.Equal(t, "state-1", q.Get("state"))
	require.Equal(t, "nonce-1", q.Get("nonce"))
	require.Equal(t, "S256", q.Get("code_challenge_method"))
	require.Equal(t, CodeChallengeS256("verifier-1"), q.Get("code_challenge"))
	// The challenge is a SHA-256 digest in base64url, 43 characters and unpadded.
	require.Len(t, q.Get("code_challenge"), 43)
	require.NotContains(t, q.Get("code_challenge"), "=")
	require.NotContains(t, raw, "verifier-1", "the PKCE verifier must never appear in the URL")
}

func TestAuthCodeURLScopes(t *testing.T) {
	i := newIdP(t)
	cfg := i.config()
	cfg.Scopes = []string{"groups"}
	p, err := Discover(context.Background(), cfg)
	require.NoError(t, err)

	u, err := url.Parse(p.AuthCodeURL("s", "n", "v"))
	require.NoError(t, err)
	// openid is always present - without it the response carries no id_token -
	// and the configured scope is asked for in addition.
	require.Equal(t, "openid groups", u.Query().Get("scope"))
}

// TestCodeChallengeS256 uses the worked example from RFC 7636 appendix B, which
// is the only check of the transform that does not come from this package.
func TestCodeChallengeS256(t *testing.T) {
	require.Equal(t,
		"E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM",
		CodeChallengeS256("dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"))
}

func TestExchangeSuccess(t *testing.T) {
	i := newIdP(t)
	p := i.provider(t)

	tokens, err := p.Exchange(context.Background(), "code-1", "verifier-1")
	require.NoError(t, err)
	require.Equal(t, "hdr.payload.sig", tokens.IDToken)
	require.Equal(t, "access-1", tokens.AccessToken)
	require.Equal(t, "refresh-1", tokens.RefreshToken)
	require.Equal(t, i.clock.Now().Add(time.Hour), tokens.ExpiresAt)
	require.Equal(t, 1, i.tokenRequests())

	// The request itself: a form POST carrying the code, the PKCE verifier and
	// the client's credentials, with the redirect URI it was issued for.
	form := i.lastTokenForm()
	require.Equal(t, "authorization_code", form.Get("grant_type"))
	require.Equal(t, "code-1", form.Get("code"))
	require.Equal(t, "verifier-1", form.Get("code_verifier"))
	require.Equal(t, "client-1", form.Get("client_id"))
	require.Equal(t, "secret-1", form.Get("client_secret"))
	require.Equal(t, i.srv.URL+"/callback", form.Get("redirect_uri"))
}

func TestExchangeWithoutAnExpiry(t *testing.T) {
	i := newIdP(t)
	i.tokenBody = `{"id_token":"a.b.c","access_token":"access-1","token_type":"Bearer"}`
	p := i.provider(t)

	tokens, err := p.Exchange(context.Background(), "code-1", "verifier-1")
	require.NoError(t, err)
	require.True(t, tokens.ExpiresAt.IsZero(), "an IdP that sends no expires_in leaves the field zero")
}

func TestExchangeFailures(t *testing.T) {
	cases := []struct {
		name   string
		status int
		body   string
		// want is a substring the error must carry, so a failure is not just
		// "something went wrong".
		want string
	}{
		{
			name:   "non-200 with an oauth error body",
			status: http.StatusBadRequest,
			body:   `{"error":"invalid_grant","error_description":"the code expired"}`,
			want:   "invalid_grant",
		},
		{
			name:   "non-200 with a body that is not JSON",
			status: http.StatusBadGateway,
			body:   "<html>gateway</html>",
			want:   "502",
		},
		{
			name:   "malformed JSON on a 200",
			status: http.StatusOK,
			body:   `{"id_token":`,
			want:   "not JSON",
		},
		{
			name:   "a 200 that carries an error member",
			status: http.StatusOK,
			body:   `{"error":"invalid_client"}`,
			want:   "invalid_client",
		},
		{
			name:   "a 200 without an id_token",
			status: http.StatusOK,
			body:   `{"access_token":"access-1","token_type":"Bearer"}`,
			want:   "no id_token",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			i := newIdP(t)
			i.tokenStatus = tc.status
			i.tokenBody = tc.body
			p := i.provider(t)

			_, err := p.Exchange(context.Background(), "code-1", "verifier-1")
			require.Error(t, err)
			require.ErrorIs(t, err, ErrExchangeFailed)
			require.Contains(t, err.Error(), tc.want)
			// The secret must not be echoed back in an error, whatever the IdP
			// answered.
			require.NotContains(t, err.Error(), "secret-1")
		})
	}
}

func TestExchangeWhenTheTokenEndpointIsUnreachable(t *testing.T) {
	i := newIdP(t)
	p := i.provider(t)
	// A port nothing listens on: the transport failure must be an exchange
	// failure, not a panic and not a silent zero Tokens.
	p.TokenEndpoint = "http://127.0.0.1:1/token"

	_, err := p.Exchange(context.Background(), "code-1", "verifier-1")
	require.Error(t, err)
	require.ErrorIs(t, err, ErrExchangeFailed)
}

func TestExchangeRespondsToContextCancellation(t *testing.T) {
	i := newIdP(t)
	p := i.provider(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := p.Exchange(ctx, "code-1", "verifier-1")
	require.Error(t, err)
	require.ErrorIs(t, err, ErrExchangeFailed)
}

// TestTokensRoundTripThroughVerify checks the caller's path rather than either
// half: exchange a code, then verify the id_token that came back. It is what a
// portal does, and it is the one place the two halves meet.
func TestTokensRoundTripThroughVerify(t *testing.T) {
	i := newIdP(t)
	idToken := i.mint(t, "RS256", i.rsaKid, i.validClaims(nil))
	body, err := json.Marshal(map[string]any{
		"id_token":     idToken,
		"access_token": "access-1",
		"token_type":   "Bearer",
		"expires_in":   3600,
	})
	require.NoError(t, err)
	i.tokenBody = string(body)

	p := i.provider(t)
	tokens, err := p.Exchange(context.Background(), "code-1", "verifier-1")
	require.NoError(t, err)

	claims, err := p.VerifyIDToken(context.Background(), tokens.IDToken)
	require.NoError(t, err)
	require.Equal(t, "user-1", claims.Subject)
	// The nonce the caller put in the request is what ties the id_token to this
	// login; VerifyIDToken cannot check it, so the caller must.
	require.Equal(t, "nonce-1", claims.Nonce)
}
