package producer

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/m7jay/pigeon/internal/portalui"
)

// The producer portal's template set: 18 files, one page per row of pageNames
// plus a nav override and the fragments. These tests render every page from the
// real embedded tree (the same buildRenderer() the handler uses) and require:
//
//   - the page renders at all, with the layout, and with no execution error;
//   - no template action is left unexpanded in the output (i.e. every component,
//     including csrf_field, compiled away);
//   - every POST form carries a non-empty csrf_token, which is AC-50's structural
//     half and is what the portal's own middleware refuses a request for.
//
// A zero-valued render is the important one: it is what a page renders when the
// API answered with empty lists, and a template that reached for a field the view
// struct does not have would fail here rather than in a browser.
//
// testToken is a stand-in for the token CSRFMiddleware mints. Every populated
// render sets it on the Page, so a form that rendered the token (rather than an
// empty paste of it) is distinguishable from one that did not.

const testToken = "test-csrf-token"

// renderPage renders one page from the shared renderer and returns the body,
// failing the test on any of the structural problems listed in the file comment.
func renderPage(t *testing.T, r *portalui.Renderer, name string, page portalui.Page) string {
	t.Helper()
	rec := httptest.NewRecorder()
	r.Page(rec, http.StatusOK, name, page)
	if rec.Code != http.StatusOK {
		t.Fatalf("%s: Page() answered %d; the renderer refused the page", name, rec.Code)
	}
	body := rec.Body.String()
	// Renderer.Page writes this marker instead of returning the execute error, so
	// it is the only signal that a template action failed at execution time (a
	// missing field, a bad index, a malformed call).
	if strings.Contains(body, "<!-- render error -->") {
		t.Fatalf("%s: the template failed to execute:\n%s", name, body)
	}
	if !strings.Contains(body, "<!doctype html>") {
		t.Fatalf("%s: the layout did not render:\n%s", name, body)
	}
	if strings.Contains(body, "{{") || strings.Contains(body, "}}") {
		t.Fatalf("%s: an unexpanded template action reached the output:\n%s", name, body)
	}
	return body
}

func mustRenderer(t *testing.T) *portalui.Renderer {
	t.Helper()
	r, err := buildRenderer()
	if err != nil {
		t.Fatalf("buildRenderer(): %v", err)
	}
	return r
}

// TestPageNamesMatchTheTemplateFiles pins the embedded tree to pageNames: the set
// of pages this portal can render is the set of files buildRenderer reads, so a
// page added to one and not the other fails here.
func TestPageNamesMatchTheTemplateFiles(t *testing.T) {
	for _, name := range pageNames {
		if _, err := templatesFS.ReadFile("templates/pages/" + name + ".html"); err != nil {
			t.Fatalf("pageNames lists %q but templates/pages/%s.html is missing: %v", name, name, err)
		}
	}
	for _, shared := range []string{"templates/nav.html", "templates/frags.html"} {
		if _, err := templatesFS.ReadFile(shared); err != nil {
			t.Fatalf("%s is missing from the embedded tree: %v", shared, err)
		}
	}
}

// zeroViews is the zero value of each page's own view type. A page's .Data is
// always a concrete view struct (the handler builds one and the API's empty
// answer fills it with zeroes), so "zero-valued data" means this - not a nil Data,
// which no handler ever passes and which the layout would report as a nil
// interface, not as a template fault.
func zeroViews() map[string]any {
	return map[string]any{
		"login":      loginView{},
		"forgot":     forgotView{},
		"reset":      resetView{},
		"onboarding": onboardingView{},
		"consumers":  consumersView{},
		"consumer":   consumerView{},
		"endpoint":   endpointView{},
		"deliveries": deliveriesView{},
		"delivery":   deliveryView{},
		"event":      eventView{},
		"testevent":  testEventView{},
		"snippet":    snippetView{},
		"guide":      guideView{},
		"usage":      usageView{},
		"account":    accountView{},
		"problem":    problemView{},
	}
}

// TestEveryPageRendersWithZeroData is the smoke test the task asks for: every
// name in pageNames renders with zero-valued view data. This is what a page
// renders when the API answered with empty lists, and a template that reached for
// a field its view struct does not carry fails here instead of in a browser.
func TestEveryPageRendersWithZeroData(t *testing.T) {
	r := mustRenderer(t)
	zeros := zeroViews()
	for _, name := range pageNames {
		name := name
		t.Run(name, func(t *testing.T) {
			data, ok := zeros[name]
			if !ok {
				t.Fatalf("pageNames lists %q but zeroViews() has no view type for it", name)
			}
			body := renderPage(t, r, name, portalui.Page{Brand: portalui.NeutralFallback, Title: name, Data: data})
			// An empty render is still a full page with the shell around it.
			assertContainsAll(t, name, body, []string{`class="content"`, `class="footer"`})
		})
	}
}

// pageCase is one page's populated render: the view it is given, the strings that
// prove the interesting parts of it rendered, and the strings that must not
// appear. also/alsoWants/alsoAbsent render the same page in its other state (for
// example a replay panel before and after the API's dry run), because several of
// these pages have two faces and a template can get one of them right by accident.
type pageCase struct {
	data       any
	wants      []string
	absent     []string
	also       any
	alsoWants  []string
	alsoAbsent []string
}

// populatedPages is a realistic payload per page, taken from the fields the view
// structs actually carry.
func populatedPages() map[string]pageCase {
	inviteToken := "pgni_onetimetoken"

	return map[string]pageCase{
		"login": {
			data: loginView{
				Next: "/deliveries", Error: "Email and password are both required.",
				ForgotOK: true, OIDCEnable: true, Env: "test",
			},
			wants: []string{
				`action="/login"`, `name="next" value="/deliveries"`, "Email and password are both required.",
				"Sign in with single sign-on", "Forgot your password?",
				"If that address has an account, a link has been sent.",
			},
			also:      loginView{Env: "live"},
			alsoWants: []string{"Forgot your password?", `name="password"`},
			alsoAbsent: []string{
				"Sign in with single sign-on",
			},
		},
		"forgot": {
			data:  forgotView{Error: "The request could not be sent: the API could not be reached."},
			wants: []string{`action="/forgot"`, "The request could not be sent"},
			also:  forgotView{Sent: true},
			alsoWants: []string{
				"If that address has an account, a link has been sent.",
			},
			alsoAbsent: []string{`action="/forgot"`},
		},
		"reset": {
			data:      resetView{Token: "prt_token"},
			wants:     []string{`action="/reset"`, `name="token" value="prt_token"`, `name="new_password"`},
			also:      resetView{Done: true},
			alsoWants: []string{"Your password has changed", `href="/login"`},
		},
		"onboarding": {
			data: onboardingView{
				Env: "test",
				Steps: []stepView{
					{N: 1, Title: "Producer", Detail: "You are signed in.", Done: true},
					{N: 2, Title: "Register an event type", Detail: "The contract.", Done: false},
				},
				EventTypes: []eventTypeRow{{Name: "order.shipped", RoutingMode: "consumer", Enabled: true}},
				Consumers:  []consumerRow{{ID: "con_1", Ref: "acme", Name: "Acme Inc.", Env: "test", EndpointCnt: 2}},
				Endpoints: []endpointRow{{
					ID: "end_1", Name: "prod webhook", URL: "https://acme.example.com/hooks", Verified: true, FailureStreak: 1,
				}},
				EventTypesCount: 1,
				EndpointsCount:  1,
				TestEventBody:   "{}",
			},
			wants: []string{
				`action="/onboarding/event-type"`, `action="/onboarding/endpoint"`,
				`name="routing_mode"`, `name="payload_schema"`, `name="internal"`,
				"order.shipped", "Acme Inc.", "prod webhook", "Register an event type",
				"done", "todo", "/test-event?event_type=order.shipped",
			},
		},
		"consumers": {
			data: consumersView{
				Env:       "test",
				Consumers: []consumerRow{{ID: "con_1", Ref: "acme", Name: "Acme Inc.", Env: "test", CreatedAt: "2026-01-01", EndpointCnt: 3}},
				Created:   struct{ Ref, Name string }{Ref: "acme", Name: "Acme Inc."},
			},
			wants: []string{
				`action="/consumers"`, `href="/consumers/con_1"`, "acme", "Acme Inc.", "2026-01-01", ">3<",
			},
		},
		"consumer": {
			data: consumerView{
				Env:      "test",
				IsOwner:  true,
				Consumer: consumerRow{ID: "con_1", Ref: "acme", Name: "Acme Inc.", Env: "test", CreatedAt: "2026-01-01"},
				Endpoints: []endpointRow{{
					ID: "end_1", Name: "prod webhook", URL: "https://acme.example.com/hooks", Status: "active",
					Verified: true, FailureStreak: 4, LastStatus: "500", LastDeliveryAt: "2026-01-02T00:00:00Z",
				}},
				Users:  []consumerUserJSON{{UserID: "usr_1", Email: "consumer@acme.test", ConsumerID: "con_1", CreatedAt: "2026-01-01"}},
				Events: []consumerEventJSON{{ID: "evt_1", EventType: "order.shipped", RoutingState: "unroutable", NonDeliveryReason: "no matching endpoint", OccurredAt: "2026-01-02T00:00:00Z", IsTest: true}},
				// NewInviteToken is what consumerInvite shows once; the handler in
				// this tree redirects instead, so the field is exercised here.
				NewInviteToken: inviteToken,
				NewInviteEmail: "consumer@acme.test",
			},
			wants: []string{
				`action="/consumers/con_1/invite"`, `/consumers/con_1/users/usr_1/revoke`,
				"consumer@acme.test", "unroutable", "no matching endpoint",
				`href="/deliveries?consumer_id=con_1"`, inviteToken, "prod webhook", "Failure streak",
			},
			also: consumerView{
				Env:      "test",
				IsOwner:  false,
				Consumer: consumerRow{ID: "con_1", Ref: "acme", Name: "Acme Inc.", Env: "test"},
			},
			alsoWants: []string{
				"Only an owner", "no invitation or revoke control is shown",
				"The API reported no unroutable or undelivered events for this consumer: nothing has been refused.",
			},
			alsoAbsent: []string{`action="/consumers/con_1/invite"`, "/users/usr_1/revoke"},
		},
		"endpoint": {
			data: endpointView{
				Env:      "test",
				Consumer: consumerRow{ID: "con_1", Ref: "acme", Name: "Acme Inc."},
				Endpoint: endpointRow{
					ID: "end_1", ConsumerID: "con_1", ConsumerName: "Acme Inc.", ConsumerRef: "acme",
					Name: "prod webhook", URL: "https://acme.example.com/hooks", Status: "active",
					Verified: false, NotificationEmail: "ops@acme.test", FailureStreak: 2,
					LastStatus: "503", LastDeliveryAt: "2026-01-02T00:00:00Z",
					RotatedSecret: "whsec_newsecret", RotatedHint: "whsec_ne",
					RotatedOverlap: "24 h", RotatedUntil: "2026-01-03T00:00:00Z",
					VerificationToken: "vrf_onetime",
				},
			},
			wants: []string{
				`action="/endpoints/end_1/verify"`, `action="/endpoints/end_1/pause"`,
				`action="/endpoints/end_1/resume"`, `action="/endpoints/end_1/rotate"`,
				"whsec_newsecret", "whsec_ne", "24 h", "2026-01-03T00:00:00Z", "vrf_onetime",
				"overlap", `href="/snippet?endpoint=end_1"`, "ops@acme.test",
			},
		},
		"deliveries": {
			data: deliveriesView{
				Env:    "test",
				Filter: deliveryFilterView{ConsumerID: "con_1", EndpointID: "end_1", Status: "failed_terminal", Limit: "50"},
				Deliveries: []deliveryRow{{
					ID: 42, EventID: "evt_1", EventType: "order.shipped", EndpointID: "end_1", ConsumerID: "con_1",
					Status: "failed_terminal", Attempts: 3, CreatedAt: "2026-01-01T00:00:00Z",
					DurationMs: "120 ms", NonDeliveryReason: "connection refused", IsTest: true, ReplayOfDeliveryID: 41,
				}},
				NextCursor: "cur_2",
				Consumers:  []consumerRow{{ID: "con_1", Ref: "acme", Name: "Acme Inc."}},
				Endpoints:  []endpointRow{{ID: "end_1", Name: "prod webhook"}},
				Statuses:   []string{"pending", "delivered", "failed_terminal", "skipped"},
				Replay: replayView{
					Selection: "consumer con_1", Key: "portal-replay-abc",
					Hidden: []hiddenField{{Name: "idempotency_key", Value: "portal-replay-abc"}, {Name: "consumer_id", Value: "con_1"}},
				},
			},
			wants: []string{
				`id="replay-panel"`, `action="/deliveries/replay/confirm"`,
				"failed_terminal", "connection refused", "idempotency_key", "portal-replay-abc",
				"cursor=cur_2", "Next page", "test", "replay of 41", "Review this replay",
			},
			absent: []string{`action="/deliveries/replay" `},
			also: deliveriesView{
				Env:      "test",
				Statuses: []string{"pending"},
				Replay: replayView{
					Selection: "consumer con_1", Key: "portal-replay-abc", Confirmed: true, Matched: 3, Rate: 100,
					Hidden: []hiddenField{{Name: "idempotency_key", Value: "portal-replay-abc"}},
				},
			},
			alsoWants:  []string{`action="/deliveries/replay"`, "committing is the step that writes", "Replay 3"},
			alsoAbsent: []string{`action="/deliveries/replay/confirm"`},
		},
		"delivery": {
			data: deliveryView{
				Env: "test",
				Delivery: deliveryRow{
					ID: 42, EventID: "evt_1", EventType: "order.shipped", EndpointID: "end_1", ConsumerID: "con_1",
					Status: "pending", Attempts: 2, CreatedAt: "2026-01-01T00:00:00Z", DurationMs: "120 ms",
					HeldBackReason: "the endpoint is paused", IsTest: true,
					RollupNote: "Not delivered yet: in flight, held back, or awaiting a retry.",
					Trail: []trailRow{
						{Attempt: 1, OccurredAt: "2026-01-01T00:00:00Z", DurationMs: "10 ms", ResponseStatus: "500", ResponseBody: "boom", ErrorClass: "http_5xx", Outcome: "retry", NextRetryAt: "2026-01-01T00:01:00Z"},
						{Attempt: 1, OccurredAt: "2026-01-01T00:00:00Z", ResponseStatus: "200", Outcome: "delivered", Synthesized: true},
					},
				},
			},
			wants: []string{
				"no attempt row (FR-1110)", "synthetic test delivery", "boom", "http_5xx",
				"the endpoint is paused", "Not delivered yet", `href="/deliveries"`,
			},
		},
		"event": {
			data: eventView{
				Env: "test",
				Event: eventRowView{
					ID: "evt_1", EventType: "order.shipped", RoutingMode: "consumer", ConsumerRef: "acme",
					OccurredAt: "2026-01-01T00:00:00Z", CreatedAt: "2026-01-01T00:00:00Z",
					RoutingState: "unroutable", NonDeliveryReason: "no matching endpoint", IsTest: true,
					Rollup: "0 delivered, 1 unroutable",
					Deliveries: []deliveryRow{{
						ID: 42, EndpointID: "end_1", Status: "skipped", Attempts: 0, IsTest: true,
					}},
				},
			},
			wants: []string{
				"synthetic test event", "unroutable", "no matching endpoint", "0 delivered, 1 unroutable",
				`href="/deliveries/42"`, `href="/endpoints/end_1"`, "acme",
			},
		},
		"testevent": {
			data: testEventView{
				Env:        "test",
				EventTypes: []eventTypeRow{{Name: "order.shipped"}, {Name: "order.cancelled"}},
				Consumers:  []consumerRow{{ID: "con_1", Ref: "acme", Name: "Acme Inc."}},
				EventType:  "order.shipped",
				Body:       "{\"id\":\"1\"}",
			},
			wants: []string{
				"marked as a test everywhere", "travels the production path", `action="/test-event"`,
				`value="order.shipped"`, "order.cancelled", "Not addressed to one consumer", `name="payload"`,
			},
			also:      testEventView{Env: "test"},
			alsoWants: []string{"You have no event types yet"},
		},
		"snippet": {
			data: snippetView{
				EndpointID: "end_1", Whsec: "whsec_x", Note: "the portal renders this, the API never sees it",
				Snippet: []snippetRow{
					{Language: "go", Label: "Go", Source: "verifySomething()", Note: "needs the webhook header"},
					{Language: "python", Label: "Python", Source: "verify_something()"},
				},
			},
			wants: []string{
				`action="/snippet"`, `name="whsec"`, "Go", "Python", "verifySomething()",
				"verify_something()", "needs the webhook header", "never forwarded to the API",
			},
			also:      snippetView{Note: "no secret submitted"},
			alsoWants: []string{"Submit an endpoint id and its signing secret"},
		},
		"guide": {
			data: guideView{
				Env:     "test",
				BaseURL: "http://core:8080",
				EventTypes: []eventTypeRow{
					{Name: "order.shipped", RoutingMode: "consumer", EventClass: "normal", PayloadVersion: 2, Enabled: true, Schema: "{\n  \"type\": \"object\"\n}"},
					{Name: "ping", RoutingMode: "broadcast", PayloadVersion: 1, Enabled: false},
				},
			},
			wants: []string{
				"order.shipped", "ping", "broadcast", "consumer", "Payload schema",
				"Publishing and fan-out", "FR-505", "http://core:8080", "Payload version",
			},
		},
		"usage": {
			data: func() usageView {
				var u usageResponse
				u.Disk.UsedBytes = 12_000_000
				u.Disk.TotalBytes = 100_000_000
				u.Disk.UsedPct = 12.5
				u.Disk.Source = "statfs"
				u.Disk.Path = "/var/lib/pigeon"
				u.MaxDiskPct = 80
				u.OverBudget = true
				u.RetentionDays = map[string]int{"deliveries": 30, "events": 7}
				u.DroppedPartitions = 3
				u.DedupeArchiveBytes = 4096
				u.CheckedAt = "2026-01-01T00:00:00Z"
				u.ReadError = "the last read failed"
				u.Note = "the partition catalogue is operator telemetry"
				return usageView{Env: "test", Usage: u, Available: true}
			}(),
			wants: []string{
				"12000000", "100000000", "12.5", "80", "over budget",
				"deliveries", "30", "events", "7", "3", "4096",
				"2026-01-01T00:00:00Z", "the partition catalogue is operator telemetry",
				"the last read failed", "/var/lib/pigeon",
			},
			also:      usageView{Env: "test", Error: "retention telemetry is not configured"},
			alsoWants: []string{"cannot tell how full the disk is", "retention telemetry is not configured"},
			alsoAbsent: []string{
				"Used (percent)", "Partitions dropped",
			},
		},
		"account": {
			data: accountView{
				Email: "producer@acme.test", Role: "owner", ProducerID: "prd_1",
				MustChangePassword: true, Changed: true, ResetSent: true,
			},
			wants: []string{
				"producer@acme.test", "owner", "prd_1", `action="/account/password"`,
				`name="old_password"`, `name="new_password"`, `href="/forgot"`,
				"has been asked to change its password", "Your password has changed.",
			},
			also:      accountView{Email: "member@acme.test", Role: "member", ProducerID: "prd_1", Error: "the old password did not match"},
			alsoWants: []string{"the old password did not match", "member@acme.test"},
		},
		"problem": {
			data:  problemView{Title: "No such delivery", Message: "This delivery is not one of yours, or no longer exists.", Detail: "The core API is the source of every panel here."},
			wants: []string{"No such delivery", "no longer exists", "The core API is the source of every panel here."},
		},
	}
}

// TestEveryPageRendersWithPopulatedData renders each page with a view that has
// something in every panel, and checks the strings that prove the panel is the
// one the contract asks for.
func TestEveryPageRendersWithPopulatedData(t *testing.T) {
	r := mustRenderer(t)
	pages := populatedPages()
	// Every page in pageNames has a populated case: a page added without one
	// fails here rather than silently skipping the assertions.
	for _, name := range pageNames {
		if _, ok := pages[name]; !ok {
			t.Fatalf("pageNames lists %q but populatedPages() has no case for it", name)
		}
	}
	for _, name := range pageNames {
		name := name
		t.Run(name, func(t *testing.T) {
			c := pages[name]
			body := renderPage(t, r, name, populatedPage(name, c.data))
			assertContainsAll(t, name, body, c.wants)
			assertContainsNone(t, name, body, c.absent)
			if c.also == nil {
				return
			}
			alt := renderPage(t, r, name, populatedPage(name, c.also))
			assertContainsAll(t, name+" (second state)", alt, c.alsoWants)
			assertContainsNone(t, name+" (second state)", alt, c.alsoAbsent)
		})
	}
}

// populatedPage is a Page that looks like one a handler would pass: a title, the
// nav item for the page where there is one, and the request's csrf token.
func populatedPage(name string, data any) portalui.Page {
	active := ""
	switch name {
	case "onboarding", "testevent":
		active = "onboarding"
	case "consumers", "consumer", "endpoint":
		active = "consumers"
	case "deliveries", "delivery":
		active = "deliveries"
	case "guide":
		active = "guide"
	case "snippet":
		active = "snippet"
	case "usage":
		active = "usage"
	case "account":
		active = "account"
	}
	return portalui.Page{
		Brand: portalui.NeutralFallback, Title: name, ActiveNav: active,
		Email: "producer@acme.test", Data: data, CSRFToken: testToken,
	}
}

func assertContainsAll(t *testing.T, label, body string, wants []string) {
	t.Helper()
	for _, want := range wants {
		if !strings.Contains(body, want) {
			t.Fatalf("%s: rendered page is missing %q:\n%s", label, want, body)
		}
	}
}

func assertContainsNone(t *testing.T, label, body string, absent []string) {
	t.Helper()
	for _, no := range absent {
		if strings.Contains(body, no) {
			t.Fatalf("%s: rendered page must not contain %q:\n%s", label, no, body)
		}
	}
}

// TestEveryRenderedPostFormCarriesTheCSRFToken is AC-50's structural half, the
// same check the consumer portal makes. The middleware refuses a POST without the
// token, so a form that rendered without one is a control the user cannot use -
// and a handler-level CSRF test would still pass, because it supplies the token
// itself. This walks every page in both of its states and requires each POST form
// to carry a non-empty token, including the forms inside the replay fragment and
// the environment switch in the nav.
func TestEveryRenderedPostFormCarriesTheCSRFToken(t *testing.T) {
	r := mustRenderer(t)
	pages := populatedPages()
	for _, name := range pageNames {
		name := name
		t.Run(name, func(t *testing.T) {
			c := pages[name]
			assertPostFormsCarryTokens(t, name, renderPage(t, r, name, populatedPage(name, c.data)))
			if c.also != nil {
				assertPostFormsCarryTokens(t, name+" (second state)", renderPage(t, r, name, populatedPage(name, c.also)))
			}
		})
	}
}

// assertPostFormsCarryTokens scans rendered HTML for POST forms and requires each
// to contain a non-empty csrf_token field before its closing tag. A GET form (the
// delivery log's filter) needs no token: it changes nothing and its query string
// is the point of it.
func assertPostFormsCarryTokens(t *testing.T, label, body string) {
	t.Helper()
	rest := body
	posts := 0
	for {
		i := strings.Index(rest, "<form")
		if i < 0 {
			break
		}
		rest = rest[i:]
		end := strings.Index(rest, "</form>")
		if end < 0 {
			t.Fatalf("%s: an unterminated <form> in the rendered page", label)
		}
		form := rest[:end]
		rest = rest[end:]
		head := form
		if j := strings.IndexByte(form, '>'); j >= 0 {
			head = form[:j]
		}
		if !strings.Contains(head, `method="post"`) {
			continue
		}
		posts++
		const marker = `name="csrf_token" value="`
		k := strings.Index(form, marker)
		if k < 0 {
			t.Fatalf("%s: a POST form rendered without a csrf_token field:\n%s", label, form)
		}
		value := form[k+len(marker):]
		if j := strings.IndexByte(value, '"'); j >= 0 {
			value = value[:j]
		}
		if value == "" {
			t.Fatalf("%s: a POST form rendered an empty csrf_token:\n%s", label, form)
		}
	}
	if posts == 0 {
		t.Fatalf("%s: no POST form rendered at all; the page or the nav lost its forms", label)
	}
}

// TestReplayPanelFragmentRendersAndCarriesTheToken renders the fragment on its
// own, the way an htmx swap will. Nothing renders it yet, but it must render: it
// is reached from the delivery log, and a fragment that only works wrapped in a
// page is a fragment that breaks the moment it is swapped in.
func TestReplayPanelFragmentRendersAndCarriesTheToken(t *testing.T) {
	r := mustRenderer(t)
	render := func(t *testing.T, data any) string {
		t.Helper()
		rec := httptest.NewRecorder()
		r.Fragment(rec, http.StatusOK, "frag:replay/panel", data)
		body := rec.Body.String()
		if strings.Contains(body, "<!-- render error -->") {
			t.Fatalf("frag:replay/panel failed to execute:\n%s", body)
		}
		if strings.Contains(body, "{{") || strings.Contains(body, "}}") {
			t.Fatalf("frag:replay/panel left an unexpanded action in the output:\n%s", body)
		}
		return body
	}

	t.Run("before the dry run", func(t *testing.T) {
		body := render(t, map[string]any{
			"Replay": replayView{
				Selection: "consumer con_1", Key: "portal-replay-abc",
				Hidden: []hiddenField{{Name: "idempotency_key", Value: "portal-replay-abc"}, {Name: "consumer_id", Value: "con_1"}},
			},
			"CSRFToken": testToken,
		})
		assertContainsAll(t, "frag:replay/panel", body, []string{
			`action="/deliveries/replay/confirm"`, "Review this replay",
			`name="idempotency_key" value="portal-replay-abc"`, `name="consumer_id" value="con_1"`,
			"consumer con_1", "never changes the history you already have", "FR-1107",
		})
		assertPostFormsCarryTokens(t, "frag:replay/panel", body)
	})

	t.Run("after the dry run", func(t *testing.T) {
		body := render(t, map[string]any{
			"Replay": replayView{
				Selection: "every delivery in this environment", Key: "portal-replay-abc",
				Confirmed: true, Matched: 3, Rate: 100,
				Hidden: []hiddenField{{Name: "idempotency_key", Value: "portal-replay-abc"}},
			},
			"CSRFToken": testToken,
		})
		assertContainsAll(t, "frag:replay/panel", body, []string{
			`action="/deliveries/replay"`, "Replay 3", "3", "100", "The API matched",
		})
		assertContainsNone(t, "frag:replay/panel", body, []string{`action="/deliveries/replay/confirm"`})
		assertPostFormsCarryTokens(t, "frag:replay/panel", body)
	})
}

// TestNavRendersTheEnvironmentSwitchAndEveryLink pins the nav's contract: the
// links the portal offers, and the environment switch, which is a POST because it
// changes the plane the next requests act in (FR-1009).
func TestNavRendersTheEnvironmentSwitchAndEveryLink(t *testing.T) {
	r := mustRenderer(t)
	body := renderPage(t, r, "problem", portalui.Page{Brand: portalui.NeutralFallback, Data: problemView{Title: "x"}, CSRFToken: testToken})
	assertContainsAll(t, "nav", body, []string{
		`href="/onboarding"`, `href="/consumers"`, `href="/deliveries"`, `href="/guide"`,
		`href="/snippet"`, `href="/usage"`, `href="/account"`,
		`action="/env"`, `name="env" value="test"`, `name="env" value="live"`,
	})
}
