package delivery

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/m7jay/pigeon/internal/atrest"
	"github.com/m7jay/pigeon/internal/store"
)

// FR-405 at the end of the pipe: the headers a producer registered are on the
// wire, they are the ones that were sealed, and the value is nowhere in the log
// (FR-1006). The registration half is in internal/store and internal/httpapi;
// this is the one place a header plaintext is produced after storage.

// canaryHeaderValue is a distinctive stand-in for a credential. Every assertion
// about it is "this string is absent" or "this string is exactly what arrived".
const canaryHeaderValue = "sk-live-fr1006-never-log-this"

// TestDeliverySendsTheRegisteredCustomHeaders: the endpoint's headers arrive on
// the request, alongside - not instead of - the platform's own, and the signature
// the consumer verifies is unaffected by them.
func TestDeliverySendsTheRegisteredCustomHeaders(t *testing.T) {
	key, _, err := atrest.GenerateKey()
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	admin := testPool(t)
	st := store.New(admin, store.WithHeadersKey(key))

	var (
		gotHeaders http.Header
		gotBody    []byte
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHeaders = r.Header.Clone()
		gotBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	eventID, secret, deliveryID := seedHarnessWithHeaders(t, st, admin, srv.URL, []store.CustomHeader{
		{Name: "X-Api-Key", Value: canaryHeaderValue},
		{Name: "X-Tenant", Value: "acme"},
		{Name: "X-Empty", Value: ""},
	})

	// The store is the same one the registered endpoint's value was sealed under,
	// and the log captures every level so "the value is absent" covers debug too.
	var logged bytes.Buffer
	opts := testOptions(5*time.Second, 30*time.Second, time.Minute)
	opts.Logger = slog.New(slog.NewTextHandler(&logged, &slog.HandlerOptions{Level: slog.LevelDebug}))
	w := New(st, opts)

	if did, err := w.ProcessOne(context.Background()); err != nil || !did {
		t.Fatalf("ProcessOne = %v, %v; want a claimed delivery", did, err)
	}
	if status, _, _, _, _ := deliveryState(t, admin, deliveryID); status != "delivered" {
		t.Fatalf("delivery status = %q, want delivered", status)
	}

	// FR-405: the registered headers are on the request.
	if got := gotHeaders.Get("X-Api-Key"); got != canaryHeaderValue {
		t.Fatalf("X-Api-Key = %q, want the registered value", got)
	}
	if got := gotHeaders.Get("X-Tenant"); got != "acme" {
		t.Fatalf("X-Tenant = %q, want acme", got)
	}
	// An empty value is still a header: the receiver must be able to tell it was
	// configured, which is what the empty entry is for.
	if _, present := gotHeaders["X-Empty"]; !present {
		t.Fatalf("X-Empty was not sent; want the header present with an empty value (%v)", gotHeaders)
	}
	// ...and the platform's own are untouched.
	if got := gotHeaders.Get("Content-Type"); got != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json (FR-801)", got)
	}
	if got := gotHeaders.Get("User-Agent"); got != "pigeon/test" {
		t.Fatalf("User-Agent = %q, want pigeon/test (FR-810)", got)
	}
	if !verifySignature(secret, gotHeaders.Get("Webhook-Id"), gotHeaders.Get("Webhook-Timestamp"),
		string(gotBody), gotHeaders.Get("Webhook-Signature")) {
		t.Fatalf("the signature does not verify over <id>.<ts>.<body>; the custom headers changed it (FR-1002)")
	}
	if got := gotHeaders.Get("Webhook-Id"); got != eventID {
		t.Fatalf("Webhook-Id = %q, want %q", got, eventID)
	}

	// FR-1006: not in the log, at any level.
	if strings.Contains(logged.String(), canaryHeaderValue) {
		t.Fatalf("the header value reached the delivery log: %s", logged.String())
	}
}

// TestDeliveryDropsACiphertextThatDidNotAuthenticate: a stored entry whose AAD
// does not match the row it sits on is not delivered - the request goes out
// without it, the log names the header (never the value), and the delivery is
// still delivered rather than wedged (FR-1006).
func TestDeliveryDropsACiphertextThatDidNotAuthenticate(t *testing.T) {
	key, _, err := atrest.GenerateKey()
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	admin := testPool(t)
	st := store.New(admin, store.WithHeadersKey(key))
	ctx := context.Background()

	var gotHeaders http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHeaders = r.Header.Clone()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	_, _, deliveryID := seedHarnessWithHeaders(t, st, admin, srv.URL, []store.CustomHeader{
		{Name: "X-Api-Key", Value: canaryHeaderValue},
	})

	// A second endpoint seals its own value, and then the two rows are swapped:
	// the target row now holds a ciphertext sealed under another endpoint's id.
	sc := store.Scope{ProducerID: "prd_delac", Env: "test"}
	other, err := st.CreateEndpoint(ctx, sc, store.NewEndpoint{
		ConsumerID: mustConsumerID(t, admin), Name: "e2", URL: "https://8.8.8.8/hook", Internal: true,
		CustomHeaders: []store.CustomHeader{{Name: "X-Api-Key", Value: "sk-live-someone-else"}},
	})
	if err != nil {
		t.Fatalf("CreateEndpoint(e2): %v", err)
	}
	var targetID string
	if err := admin.QueryRow(ctx,
		`select id from endpoints where name = 'e1' and producer_id = $1`, sc.ProducerID).Scan(&targetID); err != nil {
		t.Fatalf("resolve the seeded endpoint: %v", err)
	}
	if _, err := admin.Exec(ctx,
		`update endpoints set custom_headers = (select custom_headers from endpoints where id = $1)
		 where id = $2`, other.Endpoint.ID, targetID); err != nil {
		t.Fatalf("swap the sealed column: %v", err)
	}

	var logged bytes.Buffer
	opts := testOptions(5*time.Second, 30*time.Second, time.Minute)
	opts.Logger = slog.New(slog.NewTextHandler(&logged, &slog.HandlerOptions{Level: slog.LevelDebug}))
	w := New(st, opts)

	did, err := w.ProcessOne(ctx)
	if err != nil {
		t.Fatalf("ProcessOne: %v", err)
	}
	if !did {
		t.Fatal("ProcessOne did no work; the delivery was not claimed")
	}
	if status, _, _, _, _ := deliveryState(t, admin, deliveryID); status != "delivered" {
		t.Fatalf("delivery status = %q, want delivered: an unreadable header must not fail the delivery", status)
	}
	if got, present := gotHeaders["X-Api-Key"]; present {
		t.Fatalf("the unauthenticated value was sent: %v", got)
	}
	if strings.Contains(logged.String(), "sk-live-someone-else") || strings.Contains(logged.String(), canaryHeaderValue) {
		t.Fatalf("a header value reached the log: %s", logged.String())
	}
	if !strings.Contains(logged.String(), "X-Api-Key") {
		t.Fatalf("the drop was not reported by header name, so an operator cannot tell what was refused: %s", logged.String())
	}
}

// mustConsumerID reads the consumer the seed created, by slug.
func mustConsumerID(t *testing.T, admin *pgxpool.Pool) string {
	t.Helper()
	var id string
	if err := admin.QueryRow(context.Background(),
		`select id from consumers where ref = 'school_1'`).Scan(&id); err != nil {
		t.Fatalf("resolve consumer: %v", err)
	}
	return id
}
