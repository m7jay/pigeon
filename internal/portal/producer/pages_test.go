package producer

import (
	"net/http"
	"net/url"
	"strings"
	"testing"
)

// The producer portal's page tests, one per UI row of REQUIREMENTS.md:910-920.
// Each drives the portal's own handler against a fakeCore and asserts two things
// at once - the rendered page, and the exact core API calls (method, path, and
// the session's bearer) the page was built from. The portal decides nothing by
// itself, so "what it asked the core and what it drew" is the whole of its
// behaviour, and a page that rendered the right panel from the wrong call would
// fail here.

const (
	acmeConsumerJSON   = `{"id":"con_1","ref":"acme","name":"Acme Inc.","env":"test","created_at":"2026-01-01T00:00:00Z"}`
	globexConsumerJSON = `{"id":"con_2","ref":"globex","name":"Globex","env":"test","created_at":"2026-01-02T00:00:00Z"}`

	orderShippedTypeJSON = `{"name":"order.shipped","routing_mode":"consumer","event_class":"normal",` +
		`"payload_schema":{"type":"object","properties":{"order_id":{"type":"string"}}},"payload_version":2,"enabled":true}`
)

// mustBeSessionCall asserts one recorded core call is the documented method+path
// and carried the signed-in session's bearer. The producer id is never named in a
// member route: it rides in this bearer, so a call with the wrong (or missing)
// Authorization is a call the portal could not have made on this human's behalf.
func mustBeSessionCall(t *testing.T, core *fakeCore, method, path string) coreCall {
	t.Helper()
	got := core.lastRequest(method, path)
	if got.Path != path {
		t.Fatalf("the portal never made %s %s (calls: %v)", method, path, core.calls())
	}
	if got.Auth != "Bearer "+portalSessionToken {
		t.Fatalf("%s %s carried Authorization %q, want the session's bearer - the identity must come from the session, not the request", method, path, got.Auth)
	}
	if !strings.Contains(got.URI, "env=test") {
		t.Fatalf("%s %s did not carry the environment the request acts in: %q", method, path, got.URI)
	}
	return got
}

// --- UI-1 ----------------------------------------------------------------

func TestOnboardingChecklistIsDerivedFromTheAPI(t *testing.T) {
	t.Run("every step's data present marks every step done", func(t *testing.T) {
		core := newFakeCore(t)
		seedMemberSession(core, "owner")
		core.on(http.MethodGet, "/v1/producer/event-types", `[`+orderShippedTypeJSON+`]`, http.StatusOK)
		core.on(http.MethodGet, "/v1/producer/consumers", `{"consumers":[`+acmeConsumerJSON+`]}`, http.StatusOK)
		core.on(http.MethodGet, "/v1/producer/consumers/con_1/endpoints", `{"endpoints":[`+
			`{"id":"end_1","consumer_id":"con_1","name":"prod","url":"https://acme.example/hook",`+
			`"status":"active","verified":true,"created_at":"2026-01-01T00:00:00Z",`+
			`"health":{"failure_streak":0,"last_status":"200"}}]}`, http.StatusOK)
		core.on(http.MethodGet, "/v1/producer/deliveries", `{"deliveries":[`+
			`{"id":42,"event_id":"evt_1","event_type":"order.shipped","endpoint_id":"end_1","consumer_id":"con_1",`+
			`"status":"delivered","attempts":1,"created_at":"2026-01-01T00:00:00Z","is_test":true}],"next_cursor":""}`, http.StatusOK)
		h := newPortalAt(t, core.srv.URL)

		// A crafted producer_id in the request must not reach the API: the
		// producer is the session's, and the portal adds no path for naming one.
		rec := signedIn(t, h, http.MethodGet, "/onboarding?producer_id=prd_evil", nil)
		if rec.Code != http.StatusOK {
			t.Fatalf("GET /onboarding status = %d, want 200; body: %s", rec.Code, rec.Body.String())
		}
		for _, path := range []string{
			"/v1/producer/event-types",
			"/v1/producer/consumers",
			"/v1/producer/consumers/con_1/endpoints",
			"/v1/producer/deliveries",
		} {
			mustBeSessionCall(t, core, http.MethodGet, path)
		}
		for _, call := range core.all() {
			if strings.Contains(call.URI, "prd_evil") || strings.Contains(call.Body, "prd_evil") {
				t.Fatalf("the portal forwarded the request's producer_id to the core: %s %s body=%s", call.Method, call.URI, call.Body)
			}
		}

		states := checklistStates(rec.Body.String())
		if len(states) != 6 {
			t.Fatalf("the checklist rendered %d steps, want 6:\n%s", len(states), rec.Body.String())
		}
		for i, done := range states {
			if !done {
				t.Fatalf("step %d is incomplete although the API carries its data:\n%s", i+1, rec.Body.String())
			}
		}
	})

	t.Run("a missing endpoint leaves its step incomplete", func(t *testing.T) {
		core := newFakeCore(t)
		seedMemberSession(core, "owner")
		core.on(http.MethodGet, "/v1/producer/event-types", `[`+orderShippedTypeJSON+`]`, http.StatusOK)
		core.on(http.MethodGet, "/v1/producer/consumers", `{"consumers":[`+acmeConsumerJSON+`]}`, http.StatusOK)
		core.on(http.MethodGet, "/v1/producer/consumers/con_1/endpoints", `{"endpoints":[]}`, http.StatusOK)
		core.on(http.MethodGet, "/v1/producer/deliveries", `{"deliveries":[],"next_cursor":""}`, http.StatusOK)
		core.on(http.MethodGet, "/v1/producer/consumers/con_1/events", `{"events":[]}`, http.StatusOK)
		h := newPortalAt(t, core.srv.URL)

		rec := signedIn(t, h, http.MethodGet, "/onboarding", nil)
		if rec.Code != http.StatusOK {
			t.Fatalf("GET /onboarding status = %d, want 200; body: %s", rec.Code, rec.Body.String())
		}
		states := checklistStates(rec.Body.String())
		want := []bool{true, true, false, false, false, false}
		if len(states) != len(want) {
			t.Fatalf("the checklist rendered %d steps, want %d", len(states), len(want))
		}
		for i := range want {
			if states[i] != want[i] {
				t.Fatalf("step %d done = %v, want %v (no endpoint data in this scenario):\n%s",
					i+1, states[i], want[i], rec.Body.String())
			}
		}
		mustContain(t, "checklist", rec.Body.String(), "Register a consumer and an endpoint")
	})
}

// --- UI-2 ----------------------------------------------------------------

func TestEndpointListShowsEveryConsumerWithHealth(t *testing.T) {
	core := newFakeCore(t)
	seedMemberSession(core, "owner")
	core.on(http.MethodGet, "/v1/producer/consumers", `{"consumers":[`+acmeConsumerJSON+`,`+globexConsumerJSON+`]}`, http.StatusOK)
	core.on(http.MethodGet, "/v1/producer/consumers/con_1/endpoints", `{"endpoints":[`+
		`{"id":"end_a","consumer_id":"con_1","name":"acme-legacy","url":"https://acme.example/hook",`+
		`"status":"active","verified":true,"created_at":"2026-01-01T00:00:00Z",`+
		`"health":{"failure_streak":4,"last_status":"502","last_delivery_at":"2026-01-02T00:00:00Z"}},`+
		`{"id":"end_b","consumer_id":"con_1","name":"acme-second","url":"https://acme.example/second",`+
		`"status":"active","verified":true,"created_at":"2026-01-01T00:00:00Z",`+
		`"health":{"failure_streak":0,"last_status":"200","last_delivery_at":"2026-01-02T00:00:00Z"}}]}`, http.StatusOK)
	core.on(http.MethodGet, "/v1/producer/consumers/con_2/endpoints", `{"endpoints":[`+
		`{"id":"end_1","consumer_id":"con_2","name":"globex-hook","url":"https://globex.example/hook",`+
		`"status":"active","verified":true,"created_at":"2026-01-02T00:00:00Z",`+
		`"health":{"failure_streak":3,"last_status":"503","last_delivery_at":"2026-01-02T00:00:00Z"}}]}`, http.StatusOK)
	h := newPortalAt(t, core.srv.URL)

	// The consumers page asks the API for every consumer and for each consumer's
	// endpoint list - the API lists endpoints per consumer, so "health across all
	// consumers" is a join the portal performs.
	rec := signedIn(t, h, http.MethodGet, "/consumers", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /consumers status = %d, want 200; body: %s", rec.Code, rec.Body.String())
	}
	for _, path := range []string{
		"/v1/producer/consumers",
		"/v1/producer/consumers/con_1/endpoints",
		"/v1/producer/consumers/con_2/endpoints",
	} {
		mustBeSessionCall(t, core, http.MethodGet, path)
	}
	body := rec.Body.String()
	mustContain(t, "consumers page", body, "acme", "Acme Inc.", "globex", "Globex")
	// Endpoint counts come from the API's per-consumer endpoint lists.
	mustContain(t, "consumers page", body, "<td>2</td>", "<td>1</td>")
	// UI-2 asks for health ACROSS ALL consumers on this page, so every endpoint of
	// every consumer appears here with the streak and last status the API sent -
	// not only on the per-consumer page it links to.
	mustContain(t, "consumers page", body,
		"acme-legacy", "acme-second", "globex-hook", "https://globex.example/hook", "503")
	if !strings.Contains(body, "<strong>4</strong>") || !strings.Contains(body, "<strong>3</strong>") {
		t.Errorf("consumers page did not render the API's failure streaks (4 for acme-legacy, 3 for globex-hook); body: %s", body)
	}

	// The endpoint page finds its endpoint by walking the producer's consumers, and
	// renders the health block the API sent for it: failure streak and last status.
	ep := signedIn(t, h, http.MethodGet, "/endpoints/end_1", nil)
	if ep.Code != http.StatusOK {
		t.Fatalf("GET /endpoints/end_1 status = %d, want 200; body: %s", ep.Code, ep.Body.String())
	}
	// The walk asks each consumer's endpoint list, in order, until it finds one.
	mustBeSessionCall(t, core, http.MethodGet, "/v1/producer/consumers/con_1/endpoints")
	mustBeSessionCall(t, core, http.MethodGet, "/v1/producer/consumers/con_2/endpoints")
	ebody := ep.Body.String()
	mustContain(t, "endpoint page", ebody, "globex-hook", "<dd>3</dd>", "503")
	// It rendered the endpoint it was asked for, not another consumer's health.
	mustNotContain(t, "endpoint page", ebody, "acme-legacy")
}

// --- UI-3 ----------------------------------------------------------------

func TestDeliveryLogShowsAttemptsAndFiltersByConsumer(t *testing.T) {
	core := newFakeCore(t)
	seedMemberSession(core, "owner")
	core.on(http.MethodGet, "/v1/producer/deliveries", `{"deliveries":[`+
		`{"id":42,"event_id":"evt_1","event_type":"order.shipped","endpoint_id":"end_1","consumer_id":"con_1",`+
		`"status":"failed_terminal","attempts":3,"created_at":"2026-01-01T00:00:00Z","is_test":false}],"next_cursor":""}`, http.StatusOK)
	core.on(http.MethodGet, "/v1/producer/deliveries/42", `{"id":42,"event_id":"evt_1","event_type":"order.shipped",`+
		`"endpoint_id":"end_1","consumer_id":"con_1","status":"pending","attempts":2,"created_at":"2026-01-01T00:00:00Z",`+
		`"duration_ms":120,"is_test":false,`+
		`"attempts_rows":[{"attempt":1,"occurred_at":"2026-01-01T00:00:00Z","duration_ms":10,"response_status":500,`+
		`"response_body":"boom","error_class":"http_5xx","outcome":"retry","next_retry_at":"2026-01-01T00:01:00Z"}],`+
		`"effective_attempt":{"attempt":1,"occurred_at":"2026-01-01T00:00:00Z","duration_ms":10,"status":"200","outcome":"delivered"}}`, http.StatusOK)
	core.on(http.MethodGet, "/v1/producer/consumers", `{"consumers":[`+acmeConsumerJSON+`]}`, http.StatusOK)
	core.on(http.MethodGet, "/v1/producer/consumers/con_1/endpoints", `{"endpoints":[`+
		`{"id":"end_1","consumer_id":"con_1","name":"prod","url":"https://acme.example/hook","status":"active",`+
		`"verified":true,"created_at":"2026-01-01T00:00:00Z"}]}`, http.StatusOK)
	h := newPortalAt(t, core.srv.URL)

	log := signedIn(t, h, http.MethodGet, "/deliveries?consumer_id=con_1", nil)
	if log.Code != http.StatusOK {
		t.Fatalf("GET /deliveries status = %d, want 200; body: %s", log.Code, log.Body.String())
	}
	// The consumer filter is forwarded to the API, unchanged, as a query parameter.
	got := mustBeSessionCall(t, core, http.MethodGet, "/v1/producer/deliveries")
	q, err := url.ParseQuery(strings.TrimPrefix(got.URI, got.Path+"?"))
	if err != nil {
		t.Fatalf("cannot parse the delivery query %q: %v", got.URI, err)
	}
	if q.Get("consumer_id") != "con_1" {
		t.Fatalf("the log did not forward consumer_id=con_1 to the API (query %q)", got.URI)
	}
	if q.Get("producer_id") != "" {
		t.Fatalf("the log forwarded a producer_id the API never asked for: %q", got.URI)
	}
	mustContain(t, "delivery log", log.Body.String(), "failed_terminal", "<td>3</td>", `href="/deliveries/42"`)

	// The detail page renders each attempt row and FR-1110's synthesized first
	// attempt, which is why a successful delivery never renders an empty trail.
	det := signedIn(t, h, http.MethodGet, "/deliveries/42", nil)
	if det.Code != http.StatusOK {
		t.Fatalf("GET /deliveries/42 status = %d, want 200; body: %s", det.Code, det.Body.String())
	}
	mustBeSessionCall(t, core, http.MethodGet, "/v1/producer/deliveries/42")
	mustContain(t, "delivery detail", det.Body.String(), "500", "boom", "http_5xx", "no attempt row (FR-1110)", "200")
}

// --- UI-4 ----------------------------------------------------------------

func TestTestEventIsSubmittedWithTheSyntheticMarker(t *testing.T) {
	core := newFakeCore(t)
	seedMemberSession(core, "owner")
	core.on(http.MethodGet, "/v1/producer/event-types", `[`+orderShippedTypeJSON+`]`, http.StatusOK)
	core.on(http.MethodGet, "/v1/producer/consumers", `{"consumers":[`+acmeConsumerJSON+`]}`, http.StatusOK)
	core.on(http.MethodPost, "/v1/producer/events",
		`{"id":"evt_test","event_type":"order.shipped","is_test":true}`, http.StatusAccepted)
	h := newPortalAt(t, core.srv.URL)

	form := signedIn(t, h, http.MethodGet, "/test-event", nil)
	if form.Code != http.StatusOK {
		t.Fatalf("GET /test-event status = %d, want 200; body: %s", form.Code, form.Body.String())
	}
	mustContain(t, "test-event form", form.Body.String(), "synthetic", `action="/test-event"`)

	rec := signedIn(t, h, http.MethodPost, "/test-event", url.Values{
		"event_type":   {"order.shipped"},
		"consumer_ref": {"acme"},
		"payload":      {`{"id":"1"}`},
	})
	if rec.Code != http.StatusSeeOther {
		t.Fatalf("POST /test-event status = %d, want 303; body: %s", rec.Code, rec.Body.String())
	}
	if loc := rec.Header().Get("Location"); loc != "/events/evt_test" {
		t.Fatalf("POST /test-event redirected to %q, want the new event's page", loc)
	}
	got := mustBeSessionCall(t, core, http.MethodPost, "/v1/producer/events")
	mustContain(t, "event publish", got.Body, `"test":true`, `"event_type":"order.shipped"`,
		`"consumer_ref":"acme"`, `"payload":{"id":"1"}`)
	// The page says the event is synthetic, before and after the submit.
	if c := sessionCookie(rec, flashCookie); c == nil || !strings.Contains(c.Value, "synthetic") {
		t.Fatalf("the accepted test event did not say it was synthetic (flash: %v)", c)
	}
}

// --- UI-5 ----------------------------------------------------------------

func TestReplayNeedsConfirmationStatingTheCountAndTheRate(t *testing.T) {
	core := newFakeCore(t)
	seedMemberSession(core, "owner")
	core.on(http.MethodGet, "/v1/producer/deliveries", `{"deliveries":[`+
		`{"id":41,"event_id":"evt_1","event_type":"order.shipped","endpoint_id":"end_1","consumer_id":"con_1",`+
		`"status":"failed_terminal","attempts":2,"created_at":"2026-01-01T00:00:00Z","is_test":false}],"next_cursor":""}`, http.StatusOK)
	core.on(http.MethodGet, "/v1/producer/consumers", `{"consumers":[`+acmeConsumerJSON+`]}`, http.StatusOK)
	core.on(http.MethodGet, "/v1/producer/consumers/con_1/endpoints", `{"endpoints":[]}`, http.StatusOK)
	core.on(http.MethodPost, "/v1/producer/deliveries/replay",
		`{"matched":3,"created":0,"delivery_ids":[],"repeated":false,"release_per_second":100,"dry_run":true}`, http.StatusOK)
	h := newPortalAt(t, core.srv.URL)

	// Step 1: the confirmation is the API's own dry run.
	confirm := signedIn(t, h, http.MethodPost, "/deliveries/replay/confirm", url.Values{"consumer_id": {"con_1"}})
	if confirm.Code != http.StatusOK {
		t.Fatalf("POST /deliveries/replay/confirm status = %d, want 200; body: %s", confirm.Code, confirm.Body.String())
	}
	confirmReq := mustBeSessionCall(t, core, http.MethodPost, "/v1/producer/deliveries/replay")
	mustContain(t, "replay dry run", confirmReq.Body, `"dry_run":true`, `"consumer_id":"con_1"`)
	if confirmReq.Idem == "" {
		t.Fatalf("the confirmation carried no Idempotency-Key (FR-1107)")
	}
	// The count and the rate are the API's own numbers, quoted into the page.
	body := confirm.Body.String()
	mustContain(t, "replay confirmation", body, "matched <strong>3</strong>", "<strong>100/s</strong>",
		"Nothing has been created yet")
	renderedKey := attrValue(t, body, `name="idempotency_key" value="`)
	if renderedKey != confirmReq.Idem {
		t.Fatalf("the confirmation rendered key %q but sent %q", renderedKey, confirmReq.Idem)
	}
	// The confirm step created nothing: exactly one replay call, and it was a dry run.
	if n := core.count(http.MethodPost, "/v1/producer/deliveries/replay"); n != 1 {
		t.Fatalf("the confirm step made %d replay calls, want 1 (a dry run creates nothing)", n)
	}

	// Step 2: the commit is a separate POST carrying the SAME request key.
	core.on(http.MethodPost, "/v1/producer/deliveries/replay",
		`{"matched":3,"created":3,"delivery_ids":[1,2,3],"repeated":false,"release_per_second":100,"dry_run":false}`, http.StatusOK)
	commit := signedIn(t, h, http.MethodPost, "/deliveries/replay",
		url.Values{"consumer_id": {"con_1"}, "idempotency_key": {renderedKey}})
	if commit.Code != http.StatusSeeOther {
		t.Fatalf("POST /deliveries/replay status = %d, want 303; body: %s", commit.Code, commit.Body.String())
	}
	commitReq := mustBeSessionCall(t, core, http.MethodPost, "/v1/producer/deliveries/replay")
	mustContain(t, "replay commit", commitReq.Body, `"dry_run":false`, `"consumer_id":"con_1"`)
	if commitReq.Idem == "" || commitReq.Idem != renderedKey {
		t.Fatalf("the commit step's key %q is not the confirmation's %q", commitReq.Idem, renderedKey)
	}
}

// --- UI-6 ----------------------------------------------------------------

func TestRotationExplainsTheOverlapWindow(t *testing.T) {
	core := newFakeCore(t)
	seedMemberSession(core, "owner")
	core.on(http.MethodGet, "/v1/producer/consumers", `{"consumers":[`+acmeConsumerJSON+`]}`, http.StatusOK)
	core.on(http.MethodGet, "/v1/producer/consumers/con_1/endpoints", `{"endpoints":[`+
		`{"id":"end_1","consumer_id":"con_1","name":"prod","url":"https://acme.example/hook","status":"active",`+
		`"verified":true,"created_at":"2026-01-01T00:00:00Z"}]}`, http.StatusOK)
	core.on(http.MethodPost, "/v1/producer/consumers/con_1/endpoints/end_1/rotate",
		`{"signing_secret":"whsec_NEWSECRET","signing_secret_hint":"whsec_NE",`+
			`"previous_secret_expires_at":"2026-01-03T00:00:00Z","overlap_seconds":86400}`, http.StatusCreated)
	h := newPortalAt(t, core.srv.URL)

	rec := signedIn(t, h, http.MethodPost, "/endpoints/end_1/rotate", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("POST /endpoints/end_1/rotate status = %d, want 200; body: %s", rec.Code, rec.Body.String())
	}
	mustBeSessionCall(t, core, http.MethodPost, "/v1/producer/consumers/con_1/endpoints/end_1/rotate")
	body := rec.Body.String()
	// The new secret, the overlap and the instant the old secret stops verifying,
	// in the API's own numbers: 86400 s renders as "24 h" and the deadline is the
	// previous_secret_expires_at the API reported.
	mustContain(t, "endpoint page after rotation", body,
		"whsec_NEWSECRET", "whsec_NE", "24 h", "2026-01-03T00:00:00Z")
	// The control posts the documented rotate route.
	mustContain(t, "rotation control", body, `action="/endpoints/end_1/rotate"`)
}

// --- UI-7 ----------------------------------------------------------------

func TestSnippetRendersFourLanguagesWithoutSendingTheSecret(t *testing.T) {
	core := newFakeCore(t)
	seedMemberSession(core, "owner")
	h := newPortalAt(t, core.srv.URL)
	const secret = "whsec_ONLYINTHESNIPPET"

	rec := signedIn(t, h, http.MethodPost, "/snippet", url.Values{
		"endpoint_id": {"end_1"},
		"whsec":       {secret},
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("POST /snippet status = %d, want 200; body: %s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	mustContain(t, "snippet page", body, "Go", "Node", "Python", "curl / shell",
		"standardwebhooks", secret, "never sent to the core API", `action="/snippet"`)

	// The secret never left this process: no recorded core request carries it, in
	// the body, the path or the query string.
	for _, call := range core.all() {
		if strings.Contains(call.URI, secret) || strings.Contains(call.Path, secret) || strings.Contains(call.Body, secret) {
			t.Fatalf("the portal sent the signing secret to the core API: %s %s body=%s", call.Method, call.URI, call.Body)
		}
	}
	// It is not a query parameter of the portal's own answer either: the snippet is
	// rendered from the POSTed body, so it cannot land in a URL, a log or a history.
	mustNotContain(t, "snippet page", body, "whsec=", "?whsec")
}

// --- UI-8 ----------------------------------------------------------------

func TestRetentionPanelSaysItCannotTellRatherThanZero(t *testing.T) {
	t.Run("a 503 renders the cannot-tell message and no gauge", func(t *testing.T) {
		core := newFakeCore(t)
		seedMemberSession(core, "owner")
		core.on(http.MethodGet, "/v1/producer/usage",
			`{"error":{"code":"unavailable","message":"retention telemetry is not configured"}}`, http.StatusServiceUnavailable)
		h := newPortalAt(t, core.srv.URL)

		rec := signedIn(t, h, http.MethodGet, "/usage", nil)
		if rec.Code != http.StatusOK {
			t.Fatalf("GET /usage status = %d, want 200; body: %s", rec.Code, rec.Body.String())
		}
		// The usage route is a producer member route; the bearer carries the scope.
		got := core.lastRequest(http.MethodGet, "/v1/producer/usage")
		if got.Path != "/v1/producer/usage" {
			t.Fatalf("the usage page never asked the API (calls: %v)", core.calls())
		}
		if got.Auth != "Bearer "+portalSessionToken {
			t.Fatalf("/v1/producer/usage carried Authorization %q, want the session's bearer", got.Auth)
		}
		body := rec.Body.String()
		mustContain(t, "usage page", body, "cannot tell how full the disk is", "retention telemetry is not configured")
		// A reading that could not be taken is not a reading of zero: the gauge and
		// its labels are absent, not drawn at 0%.
		mustNotContain(t, "usage page", body, "Used (percent)", "Partitions dropped", ">0 bytes<", "0.0%")
	})

	t.Run("a 200 renders the numbers", func(t *testing.T) {
		core := newFakeCore(t)
		seedMemberSession(core, "owner")
		core.on(http.MethodGet, "/v1/producer/usage",
			`{"disk":{"used_bytes":12000000,"total_bytes":100000000,"used_pct":12.5,"source":"statfs","path":"/var/lib/pigeon"},`+
				`"max_disk_pct":80,"over_budget":true,"retention_days":{"deliveries":30,"events":7},`+
				`"dropped_partitions_total":3,"dedupe_archive_bytes":4096,"checked_at":"2026-01-01T00:00:00Z"}`, http.StatusOK)
		h := newPortalAt(t, core.srv.URL)

		rec := signedIn(t, h, http.MethodGet, "/usage", nil)
		if rec.Code != http.StatusOK {
			t.Fatalf("GET /usage status = %d, want 200; body: %s", rec.Code, rec.Body.String())
		}
		mustContain(t, "usage page", rec.Body.String(),
			"Used (percent)", "12.5", "80", "12000000", "100000000", "deliveries", "30", "3")
		mustNotContain(t, "usage page", rec.Body.String(), "cannot tell how full the disk is")
	})
}

// --- UI-9 ----------------------------------------------------------------

func TestIntegrationGuideRendersTheLiveSchemas(t *testing.T) {
	core := newFakeCore(t)
	seedMemberSession(core, "owner")
	core.on(http.MethodGet, "/v1/producer/event-types", `[`+orderShippedTypeJSON+`]`, http.StatusOK)
	h := newPortalAt(t, core.srv.URL)

	rec := signedIn(t, h, http.MethodGet, "/guide", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /guide status = %d, want 200; body: %s", rec.Code, rec.Body.String())
	}
	mustBeSessionCall(t, core, http.MethodGet, "/v1/producer/event-types")
	body := rec.Body.String()
	// The live schema from GET /v1/producer/event-types is what the guide renders.
	mustContain(t, "guide page", body, "order.shipped", "order_id", "Payload schema", "Payload version",
		"consumer", "Publishing and fan-out")

	// One source, no other: the guide read the event-type table and nothing else.
	for _, call := range core.all() {
		if strings.HasPrefix(call.Path, "/v1/producer/") && call.Path != "/v1/producer/event-types" {
			t.Fatalf("the guide read %s, but UI-9 renders only the event-type table", call.Path)
		}
	}
}

// --- UI-10 ---------------------------------------------------------------

// seedConsumerDetail registers every read GET /consumers/{id} makes.
func seedConsumerDetail(core *fakeCore) {
	core.on(http.MethodGet, "/v1/producer/consumers", `{"consumers":[`+acmeConsumerJSON+`]}`, http.StatusOK)
	core.on(http.MethodGet, "/v1/producer/consumers/con_1/endpoints", `{"endpoints":[`+
		`{"id":"end_1","consumer_id":"con_1","name":"prod","url":"https://acme.example/hook","status":"active",`+
		`"verified":true,"created_at":"2026-01-01T00:00:00Z"}]}`, http.StatusOK)
	core.on(http.MethodGet, "/v1/producer/consumers/con_1/users", `{"users":[`+
		`{"user_id":"usr_1","email":"consumer@acme.test","consumer_id":"con_1","created_at":"2026-01-01T00:00:00Z"}]}`, http.StatusOK)
	core.on(http.MethodGet, "/v1/producer/consumers/con_1/events", `{"events":[]}`, http.StatusOK)
}

func TestConsumerInviteAndRevokeAreOwnerOnly(t *testing.T) {
	const (
		inviteAction = `action="/consumers/con_1/invite"`
		revokeAction = `action="/consumers/con_1/users/usr_1/revoke"`
	)

	t.Run("an owner is offered the controls and they post the documented routes", func(t *testing.T) {
		core := newFakeCore(t)
		seedMemberSession(core, "owner")
		seedConsumerDetail(core)
		core.on(http.MethodPost, "/v1/producer/consumers/con_1/invites",
			`{"id":"inv_1","email":"new@acme.test","role":"viewer","consumer_id":"con_1",`+
				`"expires_at":"2026-02-01T00:00:00Z","token":"pgni_onetimetoken"}`, http.StatusCreated)
		core.on(http.MethodDelete, "/v1/producer/consumers/con_1/users/usr_1", "", http.StatusNoContent)
		h := newPortalAt(t, core.srv.URL)

		page := signedIn(t, h, http.MethodGet, "/consumers/con_1", nil)
		if page.Code != http.StatusOK {
			t.Fatalf("GET /consumers/con_1 status = %d, want 200; body: %s", page.Code, page.Body.String())
		}
		mustContain(t, "consumer page (owner)", page.Body.String(), inviteAction, revokeAction)

		// The invite form's action is the API's documented route, and submitting it
		// reaches exactly that method and path. The response is the consumer page
		// itself, carrying the one-time token the API returned: the token is
		// returned exactly once, so it is shown on the page that created it rather
		// than round-tripped through a cookie (a cookie value cannot hold every
		// byte an opaque token may contain).
		inv := signedIn(t, h, http.MethodPost, "/consumers/con_1/invite", url.Values{"email": {"new@acme.test"}})
		if inv.Code != http.StatusCreated {
			t.Fatalf("POST /consumers/con_1/invite status = %d, want 201; body: %s", inv.Code, inv.Body.String())
		}
		gotInv := mustBeSessionCall(t, core, http.MethodPost, "/v1/producer/consumers/con_1/invites")
		mustContain(t, "invite call", gotInv.Body, `"email":"new@acme.test"`)
		mustContain(t, "invite page", inv.Body.String(), "pgni_onetimetoken", "new@acme.test",
			"shown once", "no mailer")

		rev := signedIn(t, h, http.MethodPost, "/consumers/con_1/users/usr_1/revoke", nil)
		if rev.Code != http.StatusSeeOther {
			t.Fatalf("POST /consumers/con_1/users/usr_1/revoke status = %d, want 303; body: %s", rev.Code, rev.Body.String())
		}
		mustBeSessionCall(t, core, http.MethodDelete, "/v1/producer/consumers/con_1/users/usr_1")
	})

	t.Run("a member is offered neither control", func(t *testing.T) {
		core := newFakeCore(t)
		seedMemberSession(core, "member")
		seedConsumerDetail(core)
		h := newPortalAt(t, core.srv.URL)

		page := signedIn(t, h, http.MethodGet, "/consumers/con_1", nil)
		if page.Code != http.StatusOK {
			t.Fatalf("GET /consumers/con_1 status = %d, want 200; body: %s", page.Code, page.Body.String())
		}
		body := page.Body.String()
		// Hiding the control is the courtesy; the API refuses the call for a member
		// either way. What this asserts is that no such form is rendered at all.
		mustNotContain(t, "consumer page (member)", body, inviteAction, revokeAction, "/invite", "/revoke")
		mustContain(t, "consumer page (member)", body, "Only an owner")
	})
}

// --- UI-11 ---------------------------------------------------------------

func TestConsumerViewShowsFailureStreakAndUnroutableEvents(t *testing.T) {
	t.Run("the failures the API reports are rendered", func(t *testing.T) {
		core := newFakeCore(t)
		seedMemberSession(core, "owner")
		seedConsumerDetail(core)
		core.on(http.MethodGet, "/v1/producer/consumers/con_1/endpoints", `{"endpoints":[`+
			`{"id":"end_1","consumer_id":"con_1","name":"prod","url":"https://acme.example/hook","status":"active",`+
			`"verified":true,"created_at":"2026-01-01T00:00:00Z",`+
			`"health":{"failure_streak":7,"last_status":"503","last_delivery_at":"2026-01-02T00:00:00Z"}}]}`, http.StatusOK)
		core.on(http.MethodGet, "/v1/producer/consumers/con_1/events", `{"events":[`+
			`{"id":"evt_9","event_type":"order.shipped","routing_state":"unroutable",`+
			`"non_delivery_reason":"no matching endpoint","occurred_at":"2026-01-02T00:00:00Z",`+
			`"created_at":"2026-01-02T00:00:00Z","is_test":false}]}`, http.StatusOK)
		h := newPortalAt(t, core.srv.URL)

		rec := signedIn(t, h, http.MethodGet, "/consumers/con_1", nil)
		if rec.Code != http.StatusOK {
			t.Fatalf("GET /consumers/con_1 status = %d, want 200; body: %s", rec.Code, rec.Body.String())
		}
		for _, path := range []string{
			"/v1/producer/consumers",
			"/v1/producer/consumers/con_1/endpoints",
			"/v1/producer/consumers/con_1/users",
			"/v1/producer/consumers/con_1/events",
		} {
			mustBeSessionCall(t, core, http.MethodGet, path)
		}
		mustContain(t, "consumer view", rec.Body.String(),
			"<td>7</td>", "503", "Last delivery", "unroutable", "no matching endpoint", "evt_9")
	})

	t.Run("empty lists are said plainly", func(t *testing.T) {
		core := newFakeCore(t)
		seedMemberSession(core, "owner")
		seedConsumerDetail(core)
		// seedConsumerDetail registered one endpoint and no events; empty them both.
		core.on(http.MethodGet, "/v1/producer/consumers/con_1/endpoints", `{"endpoints":[]}`, http.StatusOK)
		h := newPortalAt(t, core.srv.URL)

		rec := signedIn(t, h, http.MethodGet, "/consumers/con_1", nil)
		if rec.Code != http.StatusOK {
			t.Fatalf("GET /consumers/con_1 status = %d, want 200; body: %s", rec.Code, rec.Body.String())
		}
		mustContain(t, "empty consumer view", rec.Body.String(),
			"This consumer has no endpoints yet",
			"The API reported no unroutable or undelivered events for this consumer: nothing has been refused.")
	})
}

// --- FR-1009 by inheritance ----------------------------------------------

// seedEverything registers every read the portal's GET pages make, so each can be
// rendered in one pass for the form-token sweep.
func seedEverything(core *fakeCore) {
	seedMemberSession(core, "owner")
	core.on(http.MethodGet, "/v1/producer/event-types", `[`+orderShippedTypeJSON+`]`, http.StatusOK)
	core.on(http.MethodGet, "/v1/producer/consumers", `{"consumers":[`+acmeConsumerJSON+`]}`, http.StatusOK)
	core.on(http.MethodGet, "/v1/producer/consumers/con_1/endpoints", `{"endpoints":[`+
		`{"id":"end_1","consumer_id":"con_1","name":"prod","url":"https://acme.example/hook","status":"active",`+
		`"verified":true,"created_at":"2026-01-01T00:00:00Z","health":{"failure_streak":1,"last_status":"500"}}]}`, http.StatusOK)
	core.on(http.MethodGet, "/v1/producer/consumers/con_1/users", `{"users":[`+
		`{"user_id":"usr_1","email":"consumer@acme.test","consumer_id":"con_1","created_at":"2026-01-01T00:00:00Z"}]}`, http.StatusOK)
	core.on(http.MethodGet, "/v1/producer/consumers/con_1/events", `{"events":[]}`, http.StatusOK)
	core.on(http.MethodGet, "/v1/producer/deliveries", `{"deliveries":[`+
		`{"id":42,"event_id":"evt_1","event_type":"order.shipped","endpoint_id":"end_1","consumer_id":"con_1",`+
		`"status":"delivered","attempts":1,"created_at":"2026-01-01T00:00:00Z","is_test":false}],"next_cursor":""}`, http.StatusOK)
	core.on(http.MethodGet, "/v1/producer/deliveries/42", `{"id":42,"event_id":"evt_1","event_type":"order.shipped",`+
		`"endpoint_id":"end_1","consumer_id":"con_1","status":"delivered","attempts":1,"created_at":"2026-01-01T00:00:00Z",`+
		`"duration_ms":20,"is_test":false,`+
		`"attempts_rows":[{"attempt":1,"occurred_at":"2026-01-01T00:00:00Z","duration_ms":20,"response_status":200,`+
		`"response_body":"ok","error_class":"","outcome":"delivered","next_retry_at":null}]}`, http.StatusOK)
	core.on(http.MethodGet, "/v1/producer/events/evt_1", `{"event":{"id":"evt_1","event_type":"order.shipped",`+
		`"routing_mode":"consumer","consumer_ref":"acme","payload_version":2,`+
		`"occurred_at":"2026-01-01T00:00:00Z","created_at":"2026-01-01T00:00:00Z","is_test":false},`+
		`"rollup":"1 delivered, 0 unroutable","deliveries":[]}`, http.StatusOK)
	core.on(http.MethodGet, "/v1/producer/usage", `{"disk":{"used_bytes":1000,"total_bytes":10000,"used_pct":10,`+
		`"source":"statfs","path":"/var/lib/pigeon"},"max_disk_pct":80,"retention_days":{},"checked_at":"2026-01-01T00:00:00Z"}`, http.StatusOK)
}

// TestEveryFormCarriesTheCSRFToken is FR-1009 by inheritance: the portal sets no
// form of its own up without the shared csrf_field, and the middleware refuses a
// state-changing POST that arrives without it (AC-50). A form that rendered
// without a token would be a control the user cannot use, so this sweeps every
// GET page - the anonymous ones included, because the nav's environment switch is
// a POST and a cross-site login is a real attack.
func TestEveryFormCarriesTheCSRFToken(t *testing.T) {
	core := newFakeCore(t)
	seedEverything(core)
	h := newPortalAt(t, core.srv.URL)

	signedTargets := []string{
		"/onboarding", "/consumers", "/consumers/con_1", "/endpoints/end_1",
		"/deliveries", "/deliveries/42", "/events/evt_1", "/test-event",
		"/snippet", "/guide", "/usage", "/account",
	}
	for _, target := range signedTargets {
		rec := signedIn(t, h, http.MethodGet, target, nil)
		if rec.Code != http.StatusOK {
			t.Fatalf("GET %s status = %d, want 200; body: %s", target, rec.Code, rec.Body.String())
		}
		assertFormsCarryCSRF(t, "GET "+target, rec.Body.String())
	}
	for _, target := range []string{"/login", "/forgot", "/reset"} {
		rec := getAnonymous(h, target)
		if rec.Code != http.StatusOK {
			t.Fatalf("GET %s status = %d, want 200; body: %s", target, rec.Code, rec.Body.String())
		}
		assertFormsCarryCSRF(t, "GET "+target, rec.Body.String())
	}

	// A POST without the token is refused by the portal's own middleware, before
	// any handler runs - and the core API never sees it.
	enc, err := h.cookie.Encode(portalSessionToken)
	if err != nil {
		t.Fatalf("encode session cookie: %v", err)
	}
	if rec := postRaw(h, "/consumers", url.Values{"ref": {"x"}, "name": {"y"}}, enc); rec.Code != http.StatusForbidden {
		t.Fatalf("POST /consumers without a CSRF token: status = %d, want 403", rec.Code)
	}
	if rec := postRaw(h, "/login", url.Values{"email": {"a@b.c"}, "password": {"x"}}, ""); rec.Code != http.StatusForbidden {
		t.Fatalf("anonymous POST /login without a CSRF token: status = %d, want 403", rec.Code)
	}
	if core.lastRequest(http.MethodPost, "/v1/producer/consumers").Path != "" {
		t.Fatalf("a refused POST still reached the core API: %v", core.calls())
	}
	if core.lastRequest(http.MethodPost, "/v1/session").Path != "" {
		t.Fatalf("a refused login still reached the core API: %v", core.calls())
	}
}
