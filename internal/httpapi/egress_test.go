package httpapi

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/m7jay/pigeon/internal/ratelimit"
)

// withEgress builds a second API over the same database with a chosen FR-1007
// answer. The shared harness is built once per package, so this is how a test
// drives the knob without mutating it for every other test - and the discovery
// function is injected so the assertion does not depend on the interfaces of the
// machine the suite happens to run on.
func (e *env) withEgress(t *testing.T, ips []string, discover func() []string) *env {
	t.Helper()
	api := New(Deps{
		Lookup:          e.st.LookupAPIKey,
		Sessions:        e.st.LookupSession,
		Data:            e.st,
		Version:         "test-egress",
		SessionTTL:      time.Hour,
		KeyLimiter:      ratelimit.New(1000, 1000),
		ProducerLimiter: ratelimit.New(1000, 1000),
		EgressIPs:       ips,
		DiscoverEgress:  discover,
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	out := &env{admin: e.admin, app: e.app, st: e.st, api: api, adminToken: e.adminToken}
	out.srv = httptest.NewServer(api.Handler())
	t.Cleanup(out.srv.Close)
	return out
}

// TestMetaEgressIPsReportsTheInstancesAddresses is FR-1007. The criterion is
// "GET /v1/meta/egress-ips MUST report the instance's egress addresses", and the
// reason it exists is that a consumer has to allowlist them - so the test asserts
// the three things that make the answer usable: the configured list is what is
// served (sorted, so two reads agree), the discovered fallback says it is a
// guess rather than presenting interface addresses as the truth behind NAT, and
// the route needs a credential like every other one.
func TestMetaEgressIPsReportsTheInstancesAddresses(t *testing.T) {
	e := setup(t)

	t.Run("configured", func(t *testing.T) {
		api := e.withEgress(t, []string{"203.0.113.7", "203.0.113.0/24", "2001:db8::/32"}, nil)
		code, raw := api.do(http.MethodGet, "/v1/meta/egress-ips", api.adminToken, nil)
		mustStatus(t, code, raw, http.StatusOK)

		got := decode[egressPayload](t, raw)
		if got.Source != egressSourceConfigured {
			t.Errorf("source = %q, want %q: the operator declared the list, so the answer is not a discovery",
				got.Source, egressSourceConfigured)
		}
		want := []string{"2001:db8::/32", "203.0.113.0/24", "203.0.113.7"}
		if len(got.IPs) != len(want) {
			t.Fatalf("ips = %v, want %v", got.IPs, want)
		}
		for i := range want {
			if got.IPs[i] != want[i] {
				t.Fatalf("ips = %v, want %v (sorted, so the same instance answers the same way twice)", got.IPs, want)
			}
		}
		if got.Note != "" {
			t.Errorf("a configured list carried the discovery caveat (%q); the caveat is about not knowing the answer", got.Note)
		}
	})

	t.Run("discovered", func(t *testing.T) {
		api := e.withEgress(t, nil, func() []string { return []string{"10.1.2.3", "fd00::5"} })
		code, raw := api.do(http.MethodGet, "/v1/meta/egress-ips", api.adminToken, nil)
		mustStatus(t, code, raw, http.StatusOK)

		got := decode[egressPayload](t, raw)
		if got.Source != egressSourceDiscovered {
			t.Errorf("source = %q, want %q when PIGEON_EGRESS_IPS is unset",
				got.Source, egressSourceDiscovered)
		}
		if len(got.IPs) != 2 || got.IPs[0] != "10.1.2.3" || got.IPs[1] != "fd00::5" {
			t.Errorf("ips = %v, want the host's two addresses", got.IPs)
		}
		// AC-22's standard applied to an API answer: a value that is a guess must
		// say so. A consumer that allowlists a container's private address has
		// been told something true and useless.
		if got.Note == "" {
			t.Error("the discovered answer carried no note; behind NAT these are not the addresses a receiver sees")
		}
		if !strings.Contains(got.Note, "PIGEON_EGRESS_IPS") {
			t.Errorf("the discovery note does not name the knob that fixes it: %q", got.Note)
		}
	})

	t.Run("nothing discovered is an empty list", func(t *testing.T) {
		api := e.withEgress(t, nil, func() []string { return nil })
		code, raw := api.do(http.MethodGet, "/v1/meta/egress-ips", api.adminToken, nil)
		mustStatus(t, code, raw, http.StatusOK)
		if !strings.Contains(string(raw), `"ips":[]`) {
			t.Errorf("an empty discovery should encode as [], not null, so allowlist tooling cannot confuse it with a bug: %s", raw)
		}
	})
}
