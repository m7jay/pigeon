package httpapi

import (
	"net/http"

	"github.com/m7jay/pigeon/internal/store"
)

// keyJSON is a key's metadata. `key` is present only in the response to the
// request that minted it (FR-202); every later read is metadata alone.
type keyJSON struct {
	ID          string  `json:"id"`
	Env         string  `json:"env"`
	Hint        string  `json:"hint"`
	ReplayScope bool    `json:"replay_scope"`
	LastUsedAt  *string `json:"last_used_at"`
	RevokedAt   *string `json:"revoked_at"`
	CreatedAt   string  `json:"created_at"`
	Key         *string `json:"key,omitempty"`
}

func newKeyJSON(k store.KeyMeta) keyJSON {
	return keyJSON{
		ID:          k.ID,
		Env:         k.Env,
		Hint:        k.Hint,
		ReplayScope: k.ReplayScope,
		LastUsedAt:  tsPtr(k.LastUsedAt),
		RevokedAt:   tsPtr(k.RevokedAt),
		CreatedAt:   ts(k.CreatedAt),
	}
}

// handleIssueKey is FR-201..FR-204. The key is minted for the environment in
// ?env=, so a test-plane session cannot accidentally hand out a live credential,
// and rotation stays additive: existing keys are untouched.
func (s *Server) handleIssueKey(w http.ResponseWriter, r *http.Request) {
	sc, err := s.memberScope(r)
	if err != nil {
		s.scopeError(w, r, err)
		return
	}
	issued, err := s.deps.Data.IssueKey(r.Context(), sc, actorFrom(r))
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	out := newKeyJSON(issued.KeyMeta)
	plaintext := issued.Plaintext
	out.Key = &plaintext
	writeJSON(w, http.StatusCreated, out)
}

func (s *Server) handleListKeys(w http.ResponseWriter, r *http.Request) {
	sc, err := s.memberScope(r)
	if err != nil {
		s.scopeError(w, r, err)
		return
	}
	keys, err := s.deps.Data.ListKeys(r.Context(), sc)
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	out := make([]keyJSON, 0, len(keys))
	for _, k := range keys {
		out = append(out, newKeyJSON(k))
	}
	writeJSON(w, http.StatusOK, map[string]any{"keys": out})
}

func (s *Server) handleGetKey(w http.ResponseWriter, r *http.Request) {
	sc, err := s.memberScope(r)
	if err != nil {
		s.scopeError(w, r, err)
		return
	}
	k, err := s.deps.Data.GetKey(r.Context(), sc, r.PathValue("keyID"))
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, newKeyJSON(k))
}

// handleRevokeKey is FR-205. Revocation is a database fact the next request
// reads, so the 60 s budget is met by construction rather than by a background
// job; the test asserts the next request is refused immediately.
func (s *Server) handleRevokeKey(w http.ResponseWriter, r *http.Request) {
	sc, err := s.memberScope(r)
	if err != nil {
		s.scopeError(w, r, err)
		return
	}
	if err := s.deps.Data.RevokeKey(r.Context(), sc, r.PathValue("keyID")); err != nil {
		s.storeError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
