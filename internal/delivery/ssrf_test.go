package delivery

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/m7jay/pigeon/internal/ssrf"
	"github.com/m7jay/pigeon/internal/store"
)

// TestRequestTimeRefusalIsTerminal is FR-1005's request-time half as the worker
// sees it: a URL that is blocked now is refused before the socket, classified
// terminal, and carries no response status (no peer answered, so there is none to
// record).
//
// Terminal is the load-bearing part. A retryable classification would make an
// endpoint pointed at the metadata service a 15-attempt, 72-hour campaign against
// it - the opposite of what an SSRF check is for.
func TestRequestTimeRefusalIsTerminal(t *testing.T) {
	cases := []struct {
		name         string
		d            store.LeasedDelivery
		allowPrivate bool
		blocked      bool
	}{
		{
			name: "the instance metadata service",
			d:    store.LeasedDelivery{EndpointURL: "http://169.254.169.254/latest/meta-data/", Env: "test"},
			// even in the test env, where http is allowed: the address is the
			// problem, not the scheme
			blocked: true,
		},
		{
			name:    "loopback",
			d:       store.LeasedDelivery{EndpointURL: "http://127.0.0.1:9000/hook", Env: "test"},
			blocked: true,
		},
		{
			name:    "RFC 1918",
			d:       store.LeasedDelivery{EndpointURL: "https://10.0.0.7/hook", Env: "live"},
			blocked: true,
		},
		{
			name: "an internal endpoint keeps its escape hatch",
			d:    store.LeasedDelivery{EndpointURL: "http://10.0.0.7/hook", Env: "test"},
			// FR-402: the operator opted this endpoint in, so the worker dials.
			allowPrivate: true,
		},
		{
			name: "a public endpoint is delivered",
			d:    store.LeasedDelivery{EndpointURL: "https://93.184.216.34/hook", Env: "live"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res, blocked := ssrfRefusal(tc.d, tc.allowPrivate)
			if blocked != tc.blocked {
				t.Fatalf("blocked = %v, want %v (res %+v)", blocked, tc.blocked, res)
			}
			if !blocked {
				return
			}
			if res.outcome != outcomeTerminal {
				t.Errorf("outcome = %v, want terminal: a blocked address does not get better with retries", res.outcome)
			}
			if res.status != 0 {
				t.Errorf("status = %d, want 0: nothing answered, so there is no status to record", res.status)
			}
			if !errors.Is(res.sendErr, ssrf.ErrBlocked) {
				t.Errorf("sendErr = %v, want it to wrap ssrf.ErrBlocked so the attempt row says why", res.sendErr)
			}
			if res.errorClass != classDialError {
				t.Errorf("errorClass = %q, want %q (the DB's CHECK constraint allows no new value)",
					res.errorClass, classDialError)
			}
		})
	}
}

// TestTransportRefusesToDialABlockedAddress is the backstop, exercised through a
// real http.Client: the transport's dial hook refuses the connection, so the
// request fails without a packet leaving the process. This is the check a
// rebinding name cannot get past, because it judges the address the kernel is
// about to connect to.
func TestTransportRefusesToDialABlockedAddress(t *testing.T) {
	cache := newTransportCache(1)
	client := cache.clientFor("end-blocked", 1, false)

	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, "http://169.254.169.254/latest/meta-data/", nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	start := time.Now()
	resp, err := client.Do(req)
	if err == nil {
		resp.Body.Close()
		t.Fatalf("the transport connected to 169.254.169.254 (status %d); FR-1005's dial hook did not run", resp.StatusCode)
	}
	if !errors.Is(err, ssrf.ErrBlocked) {
		t.Fatalf("err = %v, want it to wrap ssrf.ErrBlocked", err)
	}
	// The refusal must be local: a dial hook that had already resolved and
	// connected would have taken longer and could have reached the service.
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("the refusal took %s; it should be decided before any connection attempt", elapsed)
	}
}

// TestTransportCacheSeparatesTheDialPolicy pins FR-402's escape hatch at the layer
// that actually connects: the client built for an endpoint flagged internal dials a
// loopback address, the client built for the same endpoint without the flag does
// not, and the cache never hands one policy's client to the other.
//
// It matters because the flag is now read afresh with every claim (0022), while a
// transport outlives the attempt that built it. Keyed by endpoint alone, the cache
// would answer with the client carrying the previous policy, so the dial hook could
// refuse an attempt its own claim had just admitted - FR-1005's two request-time
// halves disagreeing about one attempt.
func TestTransportCacheSeparatesTheDialPolicy(t *testing.T) {
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cache := newTransportCache(1)
	optIn := cache.clientFor("end-hatch", 1, true)
	public := cache.clientFor("end-hatch", 1, false)
	if optIn == public {
		t.Fatal("one client serves both dial policies; a loopback attempt is then judged by whichever policy was built first")
	}
	if again := cache.clientFor("end-hatch", 1, true); again != optIn {
		t.Error("the client for one policy is not reused; every attempt would rebuild the endpoint's pool")
	}

	// The endpoint the operator opted in reaches the loopback address it was opted
	// into.
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, srv.URL, nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	resp, err := optIn.Do(req)
	if err != nil {
		t.Fatalf("an endpoint flagged internal could not reach a loopback address: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	// The same endpoint without the flag is refused at the dial hook, so nothing
	// reaches the sink - the same failure the pre-attempt check produces.
	req, err = http.NewRequestWithContext(context.Background(), http.MethodPost, srv.URL, nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	if resp, err = public.Do(req); err == nil {
		resp.Body.Close()
		t.Fatal("a client without the escape hatch connected to a loopback address; FR-1005's dial hook did not run")
	}
	if !errors.Is(err, ssrf.ErrBlocked) {
		t.Fatalf("dial err = %v, want it to wrap ssrf.ErrBlocked", err)
	}
	if got := hits.Load(); got != 1 {
		t.Errorf("the sink saw %d requests, want 1: the refused dial reached it", got)
	}
}
