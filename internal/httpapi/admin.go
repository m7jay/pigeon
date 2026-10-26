package httpapi

import (
	"net/http"
	"time"

	"github.com/m7jay/pigeon/internal/store"
)

// producerJSON is the admin view of a tenant.
type producerJSON struct {
	ID          string  `json:"id"`
	Slug        string  `json:"slug"`
	Name        string  `json:"name"`
	Suspended   bool    `json:"suspended"`
	SuspendedAt *string `json:"suspended_at,omitempty"`
	CreatedAt   string  `json:"created_at"`
}

func newProducerJSON(p store.Producer) producerJSON {
	out := producerJSON{
		ID:        p.ID,
		Slug:      p.Slug,
		Name:      p.Name,
		Suspended: p.Suspended(),
		CreatedAt: p.CreatedAt.UTC().Format(rfc3339),
	}
	if p.SuspendedAt != nil {
		s := p.SuspendedAt.UTC().Format(rfc3339)
		out.SuspendedAt = &s
	}
	return out
}

// rfc3339 is the one timestamp layout the API emits. RFC 3339 with seconds is
// what every client library parses without a format string.
const rfc3339 = "2006-01-02T15:04:05Z07:00"

func ts(t time.Time) string { return t.UTC().Format(rfc3339) }

func tsPtr(t *time.Time) *string {
	if t == nil {
		return nil
	}
	s := ts(*t)
	return &s
}

type createProducerRequest struct {
	Slug       string `json:"slug"`
	Name       string `json:"name"`
	OwnerEmail string `json:"owner_email"`
}

// handleCreateProducer is FR-102. It writes a `producer.created` audit row with
// the acting admin in the same transaction (MT-5), so a tenant that exists always
// has an answer to "who made it". The response carries the first owner's
// invitation token exactly once, because a producer nobody owns is unmanageable.
func (s *Server) handleCreateProducer(w http.ResponseWriter, r *http.Request) {
	var req createProducerRequest
	if !s.decodeJSON(w, r, &req) {
		return
	}
	p, inv, err := s.deps.Data.CreateProducer(r.Context(), actorFrom(r), store.NewProducer{
		Slug:       req.Slug,
		Name:       req.Name,
		OwnerEmail: req.OwnerEmail,
	})
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	token := inv.Token
	out := map[string]any{
		"producer": newProducerJSON(p),
		"owner_invite": inviteJSON{
			ID: inv.ID, Email: inv.Email, Role: inv.Role, ExpiresAt: ts(inv.ExpiresAt), Token: &token,
		},
	}
	writeJSON(w, http.StatusCreated, out)
}

func (s *Server) handleListProducers(w http.ResponseWriter, r *http.Request) {
	ps, err := s.deps.Data.ListProducers(r.Context())
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	out := make([]producerJSON, 0, len(ps))
	for _, p := range ps {
		out = append(out, newProducerJSON(p))
	}
	writeJSON(w, http.StatusOK, map[string]any{"producers": out})
}

func (s *Server) handleGetProducer(w http.ResponseWriter, r *http.Request) {
	p, err := s.deps.Data.GetProducer(r.Context(), r.PathValue("producerID"))
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, newProducerJSON(p))
}

// suspensionRequest carries FR-107's reason. It is optional in the schema and
// recorded verbatim in the audit row: "why" is the only part of a suspension that
// a later incident review cannot reconstruct from state.
type suspensionRequest struct {
	Reason string `json:"reason"`
}

func (s *Server) handleSuspendProducer(w http.ResponseWriter, r *http.Request) {
	s.setSuspended(w, r, true)
}

func (s *Server) handleResumeProducer(w http.ResponseWriter, r *http.Request) {
	s.setSuspended(w, r, false)
}

// setSuspended is the shared body of suspend and resume. An empty body is taken
// as "no reason given" rather than a 400: the two routes are operator buttons,
// and refusing them over a missing sentence would push the operator to curl.
func (s *Server) setSuspended(w http.ResponseWriter, r *http.Request, suspended bool) {
	producerID := r.PathValue("producerID")
	var req suspensionRequest
	if r.ContentLength != 0 {
		if !s.decodeJSON(w, r, &req) {
			return
		}
	}
	p, err := s.deps.Data.SetProducerSuspended(r.Context(), actorFrom(r), producerID, suspended, req.Reason)
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, newProducerJSON(p))
}

// auditJSON is one MT-5 row.
type auditJSON struct {
	ID          string  `json:"id"`
	ActorUserID string  `json:"actor_user_id"`
	Action      string  `json:"action"`
	Reason      *string `json:"reason,omitempty"`
	RequestID   *string `json:"request_id,omitempty"`
	CreatedAt   string  `json:"created_at"`
}

// handleProducerAudit exposes the audit trail for one producer. It exists so the
// requirement is checkable through the same surface that writes it, instead of
// only by psql: an audit log nobody can read is a table, not a control.
func (s *Server) handleProducerAudit(w http.ResponseWriter, r *http.Request) {
	entries, err := s.deps.Data.ListAudit(r.Context(), r.PathValue("producerID"))
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	out := make([]auditJSON, 0, len(entries))
	for _, e := range entries {
		out = append(out, auditJSON{
			ID:          e.ID,
			ActorUserID: e.ActorUserID,
			Action:      e.Action,
			Reason:      e.Reason,
			RequestID:   e.RequestID,
			CreatedAt:   ts(e.CreatedAt),
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"audit": out})
}

// handleSelfRegister is FR-102's opt-in path: POST /v1/producers creates a tenant
// and its first owner in one step. The route only exists when
// PIGEON_SELF_REGISTRATION is on (see routes()).
func (s *Server) handleSelfRegister(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Slug     string `json:"slug"`
		Name     string `json:"name"`
		Email    string `json:"email"`
		Password string `json:"password"`
	}
	if !s.decodeJSON(w, r, &req) {
		return
	}
	p, err := s.deps.Data.SelfRegisterProducer(r.Context(), req.Slug, req.Name, req.Email, req.Password)
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, newProducerJSON(p))
}
