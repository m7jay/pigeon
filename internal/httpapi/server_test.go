package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/m7jay/pigeon/internal/store"
)

func newTestServer(lookup Lookup, ready func(context.Context) error) http.Handler {
	return New(Deps{Lookup: lookup, Ready: ready, Version: "test"},
		slog.New(slog.NewTextHandler(nopWriter{}, nil))).Handler()
}

type nopWriter struct{}

func (nopWriter) Write(p []byte) (int, error) { return len(p), nil }

func TestLivenessDoesNotTouchDependencies(t *testing.T) {
	h := newTestServer(nil, func(context.Context) error {
		t.Fatal("liveness probe called the readiness check")
		return nil
	})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
}

func TestReadinessReflectsDependencyFailure(t *testing.T) {
	h := newTestServer(nil, func(context.Context) error { return errors.New("postgres unreachable") })
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health/ready", nil))

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
	var body struct {
		Status string `json:"status"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Status != "not_ready" {
		t.Fatalf("status field = %q, want not_ready", body.Status)
	}
}

func TestWhoamiRequiresBearerToken(t *testing.T) {
	called := false
	h := newTestServer(func(context.Context, string) (store.Principal, error) {
		called = true
		return store.Principal{}, nil
	}, nil)

	for _, header := range []string{"", "Bearer", "Basic abc", "Bearer "} {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/v1/whoami", nil)
		if header != "" {
			req.Header.Set("Authorization", header)
		}
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("header %q: status = %d, want 401", header, rec.Code)
		}
	}
	if called {
		t.Fatal("lookup was called without a bearer token")
	}
}

func TestWhoamiReturnsPrincipal(t *testing.T) {
	h := newTestServer(func(_ context.Context, token string) (store.Principal, error) {
		if token != "pgn_live_good" {
			return store.Principal{}, store.ErrNotFound
		}
		return store.Principal{KeyID: "key_1", ProducerID: "prd_1", Env: "live"}, nil
	}, nil)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/whoami", nil)
	req.Header.Set("Authorization", "Bearer pgn_live_good")
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", rec.Code, rec.Body.String())
	}
	var body map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body["producer_id"] != "prd_1" || body["env"] != "live" {
		t.Fatalf("body = %v, want producer prd_1 env live", body)
	}
}

func TestUnknownKeyIs401AndIndistinguishable(t *testing.T) {
	h := newTestServer(func(context.Context, string) (store.Principal, error) {
		return store.Principal{}, store.ErrNotFound
	}, nil)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/whoami", nil)
	req.Header.Set("Authorization", "Bearer pgn_live_nope")
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
	var body struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Error.Code != "unauthenticated" {
		t.Fatalf("code = %q, want unauthenticated", body.Error.Code)
	}
	if body.Error.Message != "invalid api key" {
		t.Fatalf("message = %q; must not distinguish unknown from revoked", body.Error.Message)
	}
}

func TestRequestIDIsEchoedOrGenerated(t *testing.T) {
	h := newTestServer(nil, nil)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	req.Header.Set("X-Request-Id", "abc-123")
	h.ServeHTTP(rec, req)
	if got := rec.Header().Get("X-Request-Id"); got != "abc-123" {
		t.Fatalf("X-Request-Id = %q, want abc-123", got)
	}

	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))
	if got := rec.Header().Get("X-Request-Id"); got == "" {
		t.Fatal("no X-Request-Id generated")
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/health", nil)
	req.Header.Set("X-Request-Id", "bad id with spaces")
	h.ServeHTTP(rec, req)
	if got := rec.Header().Get("X-Request-Id"); got == "bad id with spaces" {
		t.Fatal("accepted an unsafe X-Request-Id")
	}
}
