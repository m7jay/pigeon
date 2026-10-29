package httpapi

import (
	"context"
	"net/http"
)

// FR-111's password reset (S9a). The tree already ships the invitation half of
// FR-111; this is the half a user reaches for when a working invitation is not the
// problem - they know their address and not their password.
//
// Two properties are the whole design, and both are the reason this is a pair of
// anonymous routes rather than a member one:
//
//   - **The request endpoint cannot be used to enumerate accounts.** It answers
//     202 with the same body whether or not the address has an account, and the
//     only place the token appears is the notifier. A public form that answered
//     "no such user" would be a list of every address the instance knows.
//   - **Accepting grants a credential, not a claim.** It rewrites one account's
//     password, revokes that account's sessions and burns the token; it creates no
//     membership, attaches no consumer and redeems no invitation. That is what
//     keeps FR-111's "MUST NOT be able to self-register into a producer" true by
//     construction, and it is the same boundary SPEC.md's gap list item 7 records
//     from the other side: an account whose last binding was revoked still has no
//     claim after a reset, and re-inviting it is still the only way back in.
//
// The reset link's transport is the deployment's. v1.0 ships no mailer (SPEC.md
// lines 148 and 1314), so the token goes to Deps.NotifyPasswordReset: a mailer in
// production, and (when PIGEON_PASSWORD_RESET_LOG_TOKENS is on) the operator's own
// log on a single-operator instance. Nothing is logged by default, because a reset
// link in a shared log is a credential in a shared log.

// passwordResetRequest is the POST /v1/password-resets body.
type passwordResetRequest struct {
	Email string `json:"email"`
}

// handleRequestPasswordReset is POST /v1/password-resets. It always answers 202:
// the body says what will happen ("if that address has an account, a link has been
// sent"), never what did, so the response cannot be used to ask whether an address
// is registered.
func (s *Server) handleRequestPasswordReset(w http.ResponseWriter, r *http.Request) {
	var in passwordResetRequest
	if !s.decodeJSON(w, r, &in) {
		return
	}
	_, token, found, err := s.deps.Data.CreatePasswordReset(
		r.Context(), in.Email, s.deps.PasswordResetTTL)
	if err != nil {
		// A malformed address is the caller's error and is told apart from a
		// missing account: syntax is not a secret, existence is.
		s.storeError(w, r, err)
		return
	}
	if found {
		s.deliverPasswordReset(r, in.Email, token)
	} else {
		// Recorded with the request id and NOT the address: the operator needs to
		// know resets are being asked for, not who asked.
		s.log.Info("password reset requested for an address with no account",
			"request_id", requestIDFrom(r.Context()))
	}
	writeJSON(w, http.StatusAccepted, map[string]any{
		"status": "accepted",
		"detail": "if that address has an account, a single-use reset link has been sent to it; " +
			"it expires and does not by itself grant access to any producer or consumer",
	})
}

// deliverPasswordReset hands the token to the configured notifier. A notifier
// failure is logged and never changes the response: the reset row exists and is
// valid, so telling the caller "we could not deliver it" would both leak that the
// account exists and be untrue about the state we just wrote.
func (s *Server) deliverPasswordReset(r *http.Request, email, token string) {
	if s.deps.NotifyPasswordReset == nil {
		s.log.Warn("password reset minted but no notifier is configured; "+
			"set PIGEON_PASSWORD_RESET_LOG_TOKENS for a single-operator instance or wire a mailer",
			"request_id", requestIDFrom(r.Context()))
		return
	}
	if err := s.deps.NotifyPasswordReset(r.Context(), email, token); err != nil {
		s.log.Error("password reset delivery failed",
			"request_id", requestIDFrom(r.Context()), "error", err.Error())
	}
}

// passwordResetAcceptRequest is the POST /v1/password-resets/accept body.
type passwordResetAcceptRequest struct {
	Token       string `json:"token"`
	NewPassword string `json:"new_password"`
}

// handleAcceptPasswordReset is POST /v1/password-resets/accept: set a new password
// with a valid reset token. It answers 204 and mints no session - the user proves
// the new password at the login form, which is also where a forced change
// (FR-101) would otherwise be cleared without anyone typing the old one.
//
// One answer covers every way a token can be unusable (unknown, expired, spent,
// superseded, malformed): 404, never 410 or "expired", so a caller learns only
// that the value it holds does not work.
func (s *Server) handleAcceptPasswordReset(w http.ResponseWriter, r *http.Request) {
	var in passwordResetAcceptRequest
	if !s.decodeJSON(w, r, &in) {
		return
	}
	accepted, err := s.deps.Data.AcceptPasswordReset(r.Context(), in.Token, in.NewPassword)
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	// The audit line MT-5 wants: which account's password moved, never the password
	// or the token.
	s.log.Info("password reset accepted",
		"user_id", accepted.UserID, "reset_id", accepted.ID,
		"request_id", requestIDFrom(r.Context()))
	w.WriteHeader(http.StatusNoContent)
}

// PasswordResetNotifier is the seam FR-111's delivery runs through, named here so
// the wiring in cmd/pigeon and the tests inject the same shape.
type PasswordResetNotifier func(ctx context.Context, email, token string) error
