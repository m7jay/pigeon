package httpapi

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/m7jay/pigeon/internal/oidc"
	"github.com/m7jay/pigeon/internal/store"
)

// FR-106's optional OIDC login (S9a). It is one route, and it is deliberately the
// *second half* of the flow rather than the whole of it:
//
//	the portal is the relying party  - it holds the client id/secret, redirects
//	                                   the browser to the issuer with PKCE and a
//	                                   nonce, and exchanges the code;
//	the core is the trust anchor     - it verifies the resulting id_token against
//	                                   the issuer's published keys and mints a
//	                                   session.
//
// The split is what keeps §14 honest. A portal is a thin API client, so the thing
// that decides whether an identity is real must be the API, not the portal; and
// the client secret - the one value that lets a process speak for the portal at
// the issuer - never has to be given to the API server. The core needs only the
// issuer and the client id, which are public.
//
// Three properties are enforced here rather than assumed:
//
//   - **Optional means absent.** With no provider configured the route is a 404
//     and no portal renders an OIDC control. An instance that never configured an
//     IdP has no OIDC surface at all, which is the only useful reading of
//     "optional" (a disabled-looking button that 500s is not).
//   - **A verified address maps to an EXISTING account.** The route never creates
//     a user: an address with no account is a 401, and an account with no claim
//     gets no session (store.CreateSessionForVerifiedIdentity). FR-111's
//     "MUST NOT be able to self-register into a producer" therefore holds for
//     OIDC exactly as it does for a password.
//   - **A nonce is required.** The core has no browser cookie, so the browser-held
//     nonce comes in the request and must match the token's `nonce` claim. That
//     binds the token to the flow the browser actually started: an id_token
//     captured from a redirect is useless without the matching cookie.

// OIDCVerifier is the trust anchor *oidc.Provider implements: it turns an
// id_token into verified claims or refuses it. It is an interface so the handler
// tests can drive every rejection without a live issuer.
type OIDCVerifier interface {
	VerifyIDToken(ctx context.Context, raw string) (oidc.Claims, error)
}

// oidcLoginRequest is the POST /v1/session/oidc body. Nonce is mandatory: it is
// the value the portal put in the browser's cookie before the redirect, and the
// match is what makes a stolen id_token insufficient on its own.
type oidcLoginRequest struct {
	IDToken string `json:"id_token"`
	Nonce   string `json:"nonce"`
}

// handleOIDCLogin is POST /v1/session/oidc: exchange a verified id_token for a
// portal session.
func (s *Server) handleOIDCLogin(w http.ResponseWriter, r *http.Request) {
	if s.deps.OIDC == nil {
		// FR-106 is optional, so an unconfigured instance does not have this route
		// in any meaningful sense. A 404 says that; a 501 would imply it is coming.
		writeError(w, http.StatusNotFound, "not_found",
			"this instance has no OIDC provider configured (FR-106 is optional)")
		return
	}
	var in oidcLoginRequest
	if !s.decodeJSON(w, r, &in) {
		return
	}
	if strings.TrimSpace(in.IDToken) == "" || strings.TrimSpace(in.Nonce) == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "id_token and nonce are both required")
		return
	}
	claims, err := s.deps.OIDC.VerifyIDToken(r.Context(), in.IDToken)
	if err != nil {
		// One answer for every rejection (bad signature, wrong issuer, wrong
		// audience, expired, unverified address, unknown key): the caller is
		// anonymous and must not be able to probe which rule it failed. The reason
		// goes to the log with the request id instead.
		s.log.Warn("oidc login refused", "request_id", requestIDFrom(r.Context()), "reason", err.Error())
		writeError(w, http.StatusUnauthorized, "invalid_token", "the id_token could not be verified")
		return
	}
	if claims.Nonce != in.Nonce {
		s.log.Warn("oidc login refused: nonce mismatch", "request_id", requestIDFrom(r.Context()))
		writeError(w, http.StatusUnauthorized, "invalid_token", "the id_token could not be verified")
		return
	}
	sess, token, err := s.deps.Data.CreateSessionForVerifiedIdentity(r.Context(), claims.Email, s.deps.SessionTTL)
	switch {
	case errors.Is(err, store.ErrNotFound):
		// Either no account for that address, or an account with no claim. Both are
		// "an OIDC login cannot get you in", and neither may be told apart from the
		// other: the second case would leak that the address exists.
		s.log.Warn("oidc login refused: no account with a claim for the verified address",
			"request_id", requestIDFrom(r.Context()))
		writeError(w, http.StatusUnauthorized, "unauthenticated",
			"no account on this instance can sign in with that verified address")
		return
	case err != nil:
		s.storeError(w, r, err)
		return
	}
	s.log.Info("oidc login", "user_id", sess.UserID, "request_id", requestIDFrom(r.Context()))
	writeJSON(w, http.StatusCreated, newSessionResponse(sess, token))
}
