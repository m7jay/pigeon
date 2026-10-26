package httpapi

import (
	"errors"
	"net/http"
	"strings"

	"github.com/m7jay/pigeon/internal/store"
)

// loginRequest is POST /v1/session. producer_slug is only needed when the user
// belongs to more than one producer (FR-105).
type loginRequest struct {
	Email        string `json:"email"`
	Password     string `json:"password"`
	ProducerSlug string `json:"producer_slug"`
}

// sessionResponse is what a caller needs to act: the token, when it dies, and the
// claims the server will enforce. The claims are echoed, not trusted — every
// request re-resolves them from the database.
type sessionResponse struct {
	Token     string        `json:"token"`
	ExpiresAt string        `json:"expires_at"`
	User      sessionUser   `json:"user"`
	Claims    sessionClaims `json:"claims"`
}

type sessionUser struct {
	ID                 string `json:"id"`
	Email              string `json:"email"`
	MustChangePassword bool   `json:"must_change_password"`
}

type sessionClaims struct {
	InstanceAdmin bool   `json:"instance_admin"`
	ProducerID    string `json:"producer_id,omitempty"`
	Role          string `json:"role,omitempty"`
	ConsumerID    string `json:"consumer_id,omitempty"`
	Env           string `json:"env,omitempty"`
}

func newSessionResponse(sess store.Session, token string) sessionResponse {
	return sessionResponse{
		Token:     token,
		ExpiresAt: sess.ExpiresAt.UTC().Format("2006-01-02T15:04:05Z07:00"),
		User: sessionUser{
			ID:                 sess.UserID,
			Email:              sess.Email,
			MustChangePassword: sess.MustChangePassword,
		},
		Claims: sessionClaims{
			InstanceAdmin: sess.InstanceAdmin,
			ProducerID:    sess.ProducerID,
			Role:          sess.Role,
			ConsumerID:    sess.ConsumerID,
			Env:           sess.Env,
		},
	}
}

// handleLogin exchanges a password for a session token.
//
// Every credential failure is one 401 with one message: "no such user" and "wrong
// password" must not be distinguishable, or the login form becomes a user
// enumeration oracle.
func (s *Server) handleLogin(w http.ResponseWriter, r *http.Request) {
	var req loginRequest
	if !s.decodeJSON(w, r, &req) {
		return
	}
	if strings.TrimSpace(req.Email) == "" || req.Password == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "email and password are required")
		return
	}

	sess, token, err := s.deps.Data.CreateSession(r.Context(), store.LoginInput{
		Email:        req.Email,
		Password:     req.Password,
		ProducerSlug: req.ProducerSlug,
	}, s.deps.SessionTTL)
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeError(w, http.StatusUnauthorized, "unauthenticated", "invalid email or password")
		return
	case err != nil:
		s.storeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, newSessionResponse(sess, token))
}

// handleSession answers "who am I": the claims the server will enforce, which is
// also the fastest way for a client to discover that its forced password change
// blocks it.
func (s *Server) handleSession(w http.ResponseWriter, r *http.Request) {
	sess, ok := sessionFrom(r.Context())
	if !ok {
		writeError(w, http.StatusUnauthorized, "unauthenticated", "no session on request")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"user": sessionUser{
			ID:                 sess.UserID,
			Email:              sess.Email,
			MustChangePassword: sess.MustChangePassword,
		},
		"claims": sessionClaims{
			InstanceAdmin: sess.InstanceAdmin,
			ProducerID:    sess.ProducerID,
			Role:          sess.Role,
			ConsumerID:    sess.ConsumerID,
			Env:           sess.Env,
		},
		"expires_at": sess.ExpiresAt.UTC().Format("2006-01-02T15:04:05Z07:00"),
	})
}

// handleLogout revokes the presented session. It is idempotent so a client
// retrying a logout is not shown an error.
func (s *Server) handleLogout(w http.ResponseWriter, r *http.Request) {
	token, ok := bearerToken(r.Header.Get("Authorization"))
	if !ok {
		writeError(w, http.StatusUnauthorized, "unauthenticated", "no session on request")
		return
	}
	if err := s.deps.Data.RevokeSession(r.Context(), token); err != nil {
		s.storeError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// passwordRequest is both FR-101's forced change and the ordinary one.
type passwordRequest struct {
	OldPassword string `json:"old_password"`
	NewPassword string `json:"new_password"`
}

// handleChangePassword is reachable while a forced change is pending, which is
// the only route that is (see kindPasswordChange).
func (s *Server) handleChangePassword(w http.ResponseWriter, r *http.Request) {
	sess, ok := sessionFrom(r.Context())
	if !ok {
		writeError(w, http.StatusUnauthorized, "unauthenticated", "no session on request")
		return
	}
	var req passwordRequest
	if !s.decodeJSON(w, r, &req) {
		return
	}
	if len(req.NewPassword) < 8 {
		writeError(w, http.StatusUnprocessableEntity, "invalid", "new_password must be at least 8 characters")
		return
	}
	if req.OldPassword == req.NewPassword {
		writeError(w, http.StatusUnprocessableEntity, "invalid", "new_password must differ from old_password")
		return
	}
	if err := s.deps.Data.ChangePassword(r.Context(), sess.UserID, req.OldPassword, req.NewPassword); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			// A stale password is the same information as a stale session: it
			// must not confirm that the account exists to someone holding only a
			// token.
			writeError(w, http.StatusUnauthorized, "unauthenticated", "old_password does not match")
			return
		}
		s.storeError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// inviteAcceptRequest consumes an invitation (FR-104, FR-109, FR-111). It is the
// only route that can create a membership from an email address: there is no
// registration form that takes a producer id.
type inviteAcceptRequest struct {
	Token    string `json:"token"`
	Password string `json:"password"`
}

func (s *Server) handleAcceptInvite(w http.ResponseWriter, r *http.Request) {
	var req inviteAcceptRequest
	if !s.decodeJSON(w, r, &req) {
		return
	}
	if strings.TrimSpace(req.Token) == "" || len(req.Password) < 8 {
		writeError(w, http.StatusBadRequest, "invalid_request",
			"token and a password of at least 8 characters are required")
		return
	}
	acc, err := s.deps.Data.AcceptInvite(r.Context(), req.Token, req.Password)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "invalid_invite",
				"this invitation is unknown, expired, revoked or already used")
			return
		}
		s.storeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"email":       acc.Email,
		"role":        acc.Role,
		"producer_id": acc.ProducerID,
		"consumer_id": acc.ConsumerID,
		"env":         acc.Env,
	})
}
