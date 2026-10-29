package oidc

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// This file is the relying-party half: the two calls the portal makes, one to
// send a browser to the IdP and one to turn the code it comes back with into
// tokens. It is the only half that holds a client secret, and the two functions
// are separate from VerifyIDToken exactly so that the process serving API
// requests does not have to hold one.
//
// The flow is authorization code with PKCE (RFC 7636), not the plain code flow
// and not the implicit flow: PKCE is what makes a code that leaks out of a
// redirect URI useless to whoever takes it, and it is done here in its S256 form
// because the "plain" form protects against nothing on the wire.

// AuthCodeURL builds the URL the browser is sent to in order to log in.
//
// The caller generates state (an unguessable value it stores to compare against
// the state on the callback, which is what stops CSRF on the callback), nonce
// (an unguessable value it compares against Claims.Nonce after verifying the
// id_token, which is what stops a token minted for another login being replayed)
// and codeVerifier (the PKCE secret it keeps for Exchange). This package cannot
// generate them for the caller because it cannot keep them: state and nonce only
// mean anything when checked against what the same caller stored, and the code
// verifier only protects the exchange when it never left the caller's process.
func (p *Provider) AuthCodeURL(state, nonce, codeVerifier string) string {
	params := url.Values{}
	params.Set("response_type", "code")
	params.Set("client_id", p.Config.ClientID)
	params.Set("redirect_uri", p.Config.RedirectURL)
	params.Set("scope", strings.Join(p.Config.scopes(), " "))
	params.Set("state", state)
	params.Set("nonce", nonce)
	params.Set("code_challenge", CodeChallengeS256(codeVerifier))
	params.Set("code_challenge_method", "S256")

	endpoint, err := url.Parse(p.AuthorizationEndpoint)
	if err != nil {
		// Discover accepted this endpoint without parsing it, so the only way
		// here is a document that was edited after discovery. Fall back to the
		// string form rather than returning an error, because an error here
		// would be an error on a string join, and the fields are still the
		// caller's to inspect.
		return p.AuthorizationEndpoint + "?" + params.Encode()
	}
	// Preserve any query the IdP's endpoint already carries; parameters of ours
	// win, since they are the ones that describe this request.
	existing := endpoint.Query()
	for key, values := range params {
		existing[key] = values
	}
	endpoint.RawQuery = existing.Encode()
	return endpoint.String()
}

// CodeChallengeS256 is the RFC 7636 S256 transform: base64url of the SHA-256 of
// the verifier, without padding. It is exported because the caller needs the same
// value to build a verifier it can test against, and because a second
// implementation of it somewhere else in the tree is exactly how the challenge
// and the verifier come to disagree.
func CodeChallengeS256(codeVerifier string) string {
	sum := sha256.Sum256([]byte(codeVerifier))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

// Exchange trades an authorization code for tokens at the provider's token
// endpoint.
//
// The client authenticates with its secret in the form body (client_secret_post).
// The alternative, HTTP Basic (client_secret_basic), is what RFC 6749 puts first,
// but it is not universal: IdPs that accept only the form are common, and a
// client that sends both is rejected by the ones that treat the two as mutually
// exclusive. One scheme, chosen and documented, is worth more here than
// negotiation a portal cannot perform without a human.
//
// The returned Tokens.IDToken is unchecked; the caller must call VerifyIDToken
// before believing anything in it, and must compare Claims.Nonce against the
// nonce it passed to AuthCodeURL.
func (p *Provider) Exchange(ctx context.Context, code, codeVerifier string) (Tokens, error) {
	form := url.Values{}
	form.Set("grant_type", "authorization_code")
	form.Set("code", code)
	form.Set("redirect_uri", p.Config.RedirectURL)
	form.Set("client_id", p.Config.ClientID)
	form.Set("client_secret", p.Config.ClientSecret)
	form.Set("code_verifier", codeVerifier)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.TokenEndpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return Tokens{}, fmt.Errorf("%w: building the token request: %v", ErrExchangeFailed, err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")

	resp, err := p.Config.httpClient().Do(req)
	if err != nil {
		return Tokens{}, fmt.Errorf("%w: calling the token endpoint: %v", ErrExchangeFailed, err)
	}
	defer resp.Body.Close() //nolint:errcheck // nothing useful to do with a close error on a response body
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes))
	if err != nil {
		return Tokens{}, fmt.Errorf("%w: reading the token response: %v", ErrExchangeFailed, err)
	}

	if resp.StatusCode != http.StatusOK {
		// An error response is itself JSON by RFC 6749 §5.2. Quote the error code
		// when there is one and the status when there is not - never the body,
		// which can echo the client secret back.
		var oauthErr tokenError
		if err := json.Unmarshal(body, &oauthErr); err == nil && oauthErr.Error != "" {
			return Tokens{}, fmt.Errorf("%w: the token endpoint returned %s: %s %s", ErrExchangeFailed, resp.Status, oauthErr.Error, oauthErr.ErrorDescription)
		}
		return Tokens{}, fmt.Errorf("%w: the token endpoint returned %s", ErrExchangeFailed, resp.Status)
	}

	var parsed tokenResponse
	if err := json.Unmarshal(body, &parsed); err != nil {
		return Tokens{}, fmt.Errorf("%w: the token response is not JSON: %v", ErrExchangeFailed, err)
	}
	if parsed.Error != "" {
		return Tokens{}, fmt.Errorf("%w: the token endpoint returned %s %s", ErrExchangeFailed, parsed.Error, parsed.ErrorDescription)
	}
	tokens := Tokens{
		IDToken:      parsed.IDToken,
		AccessToken:  parsed.AccessToken,
		RefreshToken: parsed.RefreshToken,
	}
	if parsed.IDToken == "" {
		return Tokens{}, fmt.Errorf("%w: the token response carries no id_token", ErrExchangeFailed)
	}
	if parsed.ExpiresIn > 0 {
		tokens.ExpiresAt = p.Config.now().Add(time.Duration(parsed.ExpiresIn) * time.Second)
	}
	return tokens, nil
}

// tokenResponse is the successful shape of RFC 6749 §5.1 with the OIDC members,
// plus the §5.2 error members, because a provider may answer 200 with an error
// body and that must not be read as success.
type tokenResponse struct {
	IDToken          string  `json:"id_token"`
	AccessToken      string  `json:"access_token"`
	RefreshToken     string  `json:"refresh_token"`
	TokenType        string  `json:"token_type"`
	ExpiresIn        float64 `json:"expires_in"`
	Error            string  `json:"error"`
	ErrorDescription string  `json:"error_description"`
}

// tokenError is the RFC 6749 §5.2 error response.
type tokenError struct {
	Error            string `json:"error"`
	ErrorDescription string `json:"error_description"`
}
