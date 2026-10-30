package httpapi

import (
	"net/http"

	"github.com/m7jay/pigeon/internal/store"
)

// memberJSON is FR-104's view of a producer membership.
type memberJSON struct {
	UserID    string `json:"user_id"`
	Email     string `json:"email"`
	Role      string `json:"role"`
	CreatedAt string `json:"created_at"`
}

// inviteJSON is an invitation as returned at creation: the only time the token
// exists outside the invitee's inbox.
type inviteJSON struct {
	ID         string  `json:"id"`
	Email      string  `json:"email"`
	Role       string  `json:"role"`
	ConsumerID string  `json:"consumer_id,omitempty"`
	Env        string  `json:"env,omitempty"`
	ExpiresAt  string  `json:"expires_at"`
	Token      *string `json:"token,omitempty"`
}

// inviteMetaJSON is an invitation without its token, for listing.
type inviteMetaJSON struct {
	ID         string `json:"id"`
	Email      string `json:"email"`
	Role       string `json:"role"`
	ConsumerID string `json:"consumer_id,omitempty"`
	Env        string `json:"env,omitempty"`
	CreatedAt  string `json:"created_at"`
	ExpiresAt  string `json:"expires_at"`
}

// consumerJSON is a producer's own customer (§7.5a).
type consumerJSON struct {
	ID        string `json:"id"`
	Ref       string `json:"ref"`
	Name      string `json:"name"`
	Env       string `json:"env"`
	CreatedAt string `json:"created_at"`
}

// consumerUserJSON is FR-109's binding.
type consumerUserJSON struct {
	UserID     string `json:"user_id"`
	Email      string `json:"email"`
	ConsumerID string `json:"consumer_id"`
	CreatedAt  string `json:"created_at"`
}

// handleListMembers is FR-105's read side: one human may appear in several
// producers, and each producer sees only its own memberships.
func (s *Server) handleListMembers(w http.ResponseWriter, r *http.Request) {
	sc, err := s.producerScope(r)
	if err != nil {
		s.scopeError(w, r, err)
		return
	}
	members, err := s.deps.Data.ListMembers(r.Context(), sc)
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	out := make([]memberJSON, 0, len(members))
	for _, m := range members {
		out = append(out, memberJSON{UserID: m.UserID, Email: m.Email, Role: m.Role, CreatedAt: ts(m.CreatedAt)})
	}
	writeJSON(w, http.StatusOK, map[string]any{"members": out})
}

type inviteMemberRequest struct {
	Email string `json:"email"`
	Role  string `json:"role"`
}

// handleInviteMember is FR-104. Only an owner may invite, and only an owner or
// developer may be invited: there is no route that makes a consumer user out of
// a producer invitation.
func (s *Server) handleInviteMember(w http.ResponseWriter, r *http.Request) {
	sc, err := s.producerScope(r)
	if err != nil {
		s.scopeError(w, r, err)
		return
	}
	var req inviteMemberRequest
	if !s.decodeJSON(w, r, &req) {
		return
	}
	if req.Role == "" {
		// Defaulting to owner would be the most dangerous possible default;
		// defaulting to developer at least cannot escalate.
		req.Role = "developer"
	}
	inv, err := s.deps.Data.InviteMember(r.Context(), sc, actorFrom(r), req.Email, req.Role)
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	token := inv.Token
	writeJSON(w, http.StatusCreated, inviteJSON{
		ID: inv.ID, Email: inv.Email, Role: inv.Role, ExpiresAt: ts(inv.ExpiresAt), Token: &token,
	})
}

func (s *Server) handleListInvites(w http.ResponseWriter, r *http.Request) {
	// Producer-wide invitations carry no environment; consumer invitations do. A
	// member without ?env= therefore sees the producer-wide ones only, which is
	// the honest answer rather than a merged view across planes.
	sc, err := s.producerScope(r)
	if err != nil {
		s.scopeError(w, r, err)
		return
	}
	if env := r.URL.Query().Get("env"); env != "" {
		if !store.ValidEnv(env) {
			writeError(w, http.StatusBadRequest, "invalid_env", "env must be test or live")
			return
		}
		sc.Env = env
	}
	invites, err := s.deps.Data.ListInvites(r.Context(), sc)
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	out := make([]inviteMetaJSON, 0, len(invites))
	for _, i := range invites {
		out = append(out, inviteMetaJSON{
			ID: i.ID, Email: i.Email, Role: i.Role, ConsumerID: i.ConsumerID,
			Env: i.Env, CreatedAt: ts(i.CreatedAt), ExpiresAt: ts(i.ExpiresAt),
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"invites": out})
}

func (s *Server) handleRevokeInvite(w http.ResponseWriter, r *http.Request) {
	sc, err := s.memberScope(r)
	if err != nil {
		s.scopeError(w, r, err)
		return
	}
	if err := s.deps.Data.RevokeInvite(r.Context(), sc, r.PathValue("inviteID")); err != nil {
		s.storeError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleListConsumers(w http.ResponseWriter, r *http.Request) {
	sc, err := s.memberScope(r)
	if err != nil {
		s.scopeError(w, r, err)
		return
	}
	consumers, err := s.deps.Data.ListConsumers(r.Context(), sc)
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	out := make([]consumerJSON, 0, len(consumers))
	for _, c := range consumers {
		out = append(out, consumerJSON{ID: c.ID, Ref: c.Ref, Name: c.Name, Env: c.Env, CreatedAt: ts(c.CreatedAt)})
	}
	writeJSON(w, http.StatusOK, map[string]any{"consumers": out})
}

type createConsumerRequest struct {
	Ref  string `json:"ref"`
	Name string `json:"name"`
}

// handleCreateConsumer registers a consumer in the caller's environment. The
// environment comes from ?env= (member session), never from the body: a body
// field would let a test-plane request create live-plane data (FR-103).
func (s *Server) handleCreateConsumer(w http.ResponseWriter, r *http.Request) {
	sc, err := s.memberScope(r)
	if err != nil {
		s.scopeError(w, r, err)
		return
	}
	var req createConsumerRequest
	if !s.decodeJSON(w, r, &req) {
		return
	}
	c, err := s.deps.Data.CreateConsumer(r.Context(), sc, req.Ref, req.Name)
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, consumerJSON{
		ID: c.ID, Ref: c.Ref, Name: c.Name, Env: c.Env, CreatedAt: ts(c.CreatedAt),
	})
}

func (s *Server) handleGetConsumer(w http.ResponseWriter, r *http.Request) {
	sc, err := s.memberScope(r)
	if err != nil {
		s.scopeError(w, r, err)
		return
	}
	c, err := s.deps.Data.GetConsumer(r.Context(), sc, r.PathValue("consumerID"))
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, consumerJSON{
		ID: c.ID, Ref: c.Ref, Name: c.Name, Env: c.Env, CreatedAt: ts(c.CreatedAt),
	})
}

type inviteConsumerUserRequest struct {
	Email string `json:"email"`
}

// handleInviteConsumerUser is FR-109: bind a consumer user to exactly one
// consumer, in that consumer's environment.
func (s *Server) handleInviteConsumerUser(w http.ResponseWriter, r *http.Request) {
	sc, err := s.memberScope(r)
	if err != nil {
		s.scopeError(w, r, err)
		return
	}
	var req inviteConsumerUserRequest
	if !s.decodeJSON(w, r, &req) {
		return
	}
	inv, err := s.deps.Data.InviteConsumerUser(r.Context(), sc, actorFrom(r), r.PathValue("consumerID"), req.Email)
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	token := inv.Token
	writeJSON(w, http.StatusCreated, inviteJSON{
		ID: inv.ID, Email: inv.Email, Role: inv.Role, ConsumerID: inv.ConsumerID,
		Env: inv.Env, ExpiresAt: ts(inv.ExpiresAt), Token: &token,
	})
}

func (s *Server) handleListConsumerUsers(w http.ResponseWriter, r *http.Request) {
	sc, err := s.memberScope(r)
	if err != nil {
		s.scopeError(w, r, err)
		return
	}
	consumerID := r.PathValue("consumerID")
	// Listing users of a consumer that does not exist in this scope is a 404,
	// not an empty list: an empty list would confirm the consumer id was
	// understood, which is the enumeration FR-108 forbids.
	if _, err := s.deps.Data.GetConsumer(r.Context(), sc, consumerID); err != nil {
		s.storeError(w, r, err)
		return
	}
	users, err := s.deps.Data.ListConsumerUsers(r.Context(), sc, consumerID)
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	out := make([]consumerUserJSON, 0, len(users))
	for _, u := range users {
		out = append(out, consumerUserJSON{UserID: u.UserID, Email: u.Email, ConsumerID: u.ConsumerID, CreatedAt: ts(u.CreatedAt)})
	}
	writeJSON(w, http.StatusOK, map[string]any{"users": out})
}

// handleRevokeConsumerUser is the "independently of the consumer record" half of
// FR-109. The consumer keeps its identity, endpoints and history; only the
// human's access goes away, and their session stops resolving on its next
// request (0004's lookup joins the binding).
func (s *Server) handleRevokeConsumerUser(w http.ResponseWriter, r *http.Request) {
	sc, err := s.memberScope(r)
	if err != nil {
		s.scopeError(w, r, err)
		return
	}
	if err := s.deps.Data.RevokeConsumerUser(r.Context(), sc, r.PathValue("consumerID"), r.PathValue("userID")); err != nil {
		s.storeError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleConsumerMe is FR-110's proof at the API surface: the consumer id comes
// from the session claim, and the read is scoped by it server-side. A consumer
// user cannot ask for another consumer's row because no route takes a consumer
// id from a consumer session.
func (s *Server) handleConsumerMe(w http.ResponseWriter, r *http.Request) {
	sess, ok := sessionFrom(r.Context())
	if !ok || !sess.IsConsumer() {
		s.writeNotFound(w, r)
		return
	}
	sc, err := consumerScope(r)
	if err != nil {
		s.writeNotFound(w, r)
		return
	}
	c, err := s.deps.Data.GetConsumer(r.Context(), sc, sess.ConsumerID)
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, consumerJSON{
		ID: c.ID, Ref: c.Ref, Name: c.Name, Env: c.Env, CreatedAt: ts(c.CreatedAt),
	})
}
