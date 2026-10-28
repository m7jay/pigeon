package consumer

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

// This file is UI-15 and UI-16: the consumer portal's delivery log, attempt
// detail and replay. Both are thin-client uses of S7's inspect and replay API,
// which already exposes the consumer-scoped routes (§7.11, FR-1101/FR-1102/
// FR-1103/FR-1105/FR-1106/FR-1107/FR-1108/FR-1110):
//
//	GET  /v1/consumer/deliveries                 - the log, forced to the claim's consumer
//	GET  /v1/consumer/deliveries/{deliveryID}    - one delivery with its ordered attempts
//	POST /v1/consumer/deliveries/replay          - bounded, idempotent replay
//
// Three rules shape everything below, all of them §14's:
//
//   - The consumer id is never read from the browser. The portal sends no
//     consumer_id at all, in a path, a query or a body: the API takes it from
//     the session claim (FR-1105), so a foreign id in a request is *dropped*
//     here rather than forwarded (a forwarded one would be ignored by the API
//     anyway, and a portal that sent it would be pretending it had a say).
//   - The portal owns no filter, no bound and no rate. It forwards the request's
//     parameters verbatim - including `limit`, which the API bounds - and renders
//     the API's refusal where the user acted. FR-1107's ≤ 1,000 per request and
//     50/s per endpoint are the API's numbers, quoted back from its own answers.
//   - No free-text control offers a value the API decides. The endpoint filter is
//     a select over the caller's own endpoints and the status filter is a select
//     over the statuses the deliveries column defines; the response body, which
//     the API truncates, is rendered as text and never as markup.

// --- view models -----------------------------------------------------------

// deliveryRow is one line of the log (UI-15). It carries exactly what the log
// renders; a field nothing renders belongs on the detail page instead.
type deliveryRow struct {
	ID             int64
	EventType      string
	EndpointID     string
	Status         string
	Attempts       int
	FirstAttemptAt string
	DeliveredAt    string
	Duration       string
	// ReplayOf is the delivery this row was replayed from, when it is itself a
	// replay (FR-1108: the log distinguishes a replayed delivery).
	ReplayOf string
}

// problemView is the page for a request the portal cannot answer - a delivery
// outside the caller's consumer (AC-39's 404) or a core read that failed. It
// carries the status the API's answer maps to, so the portal's answer is the
// API's answer rather than a friendly 200 that reads as "you have none".
type problemView struct {
	Title     string
	Message   string
	BackURL   string
	BackLabel string
}

// deliveryLogView is the log page: the caller's deliveries, the filter set the
// request carried, the option sets its controls may offer, and the replay panel.
type deliveryLogView struct {
	Deliveries []deliveryRow
	Filter     deliveryFilterView
	Statuses   []string
	Endpoints  []endpointOption
	// Error is the API's own refusal of the read (a filter it rejected), shown on
	// the page instead of an empty table that would read as "no deliveries".
	Error string
	// NextPageURL and FilterURL are the API's cursor carried forward, and the
	// same page without it.
	NextPageURL string
	FilterURL   string
	Replay      replayView
}

// deliveryFilterView is the API's own delivery filter (FR-1101) plus its paging
// parameters, exactly as one request carried them. Every value is forwarded
// verbatim; the portal neither validates nor clamps one.
//
// `consumer_id` is deliberately absent. FR-1105 takes the consumer from the
// session claim, so a consumer id in a URL or a body is not a filter the portal
// may pass on: there is nothing here to pass it through with.
type deliveryFilterView struct {
	EndpointID string
	EventType  string
	Status     string
	From       string
	To         string
	Limit      string
	Cursor     string
}

// endpointOption is one choice of the log's endpoint filter: an endpoint of the
// caller's own consumer, read from the API, so the control cannot offer an id
// the caller does not have.
type endpointOption struct {
	ID   string
	Name string
}

// hiddenField is one hidden input. The replay panel carries the API's filters
// forward as hidden fields, built in Go so the page and the htmx fragment render
// the identical set.
type hiddenField struct {
	Name  string
	Value string
}

// replayView is UI-16's panel: the confirmation state, and the API's answers.
type replayView struct {
	// Selection describes what is about to be replayed, in the caller's words, so
	// the confirmation says which set it is about (UI-5's "explicit confirmation").
	Selection string
	// Key is FR-1107's request key for this confirmation. The portal mints it (the
	// user never types one) and sends it as the `Idempotency-Key` header; the API
	// is what enforces idempotency, so the same double-submit cannot create two
	// sets of deliveries (AC-12).
	Key string
	// Confirmed is true once the API's dry run has answered, so the panel shows
	// the API's own count and rate and offers the commit step.
	Confirmed bool
	// Matched and Rate are the API's numbers, quoted - not computed here.
	Matched int
	Rate    int
	// Message is what a committed replay did, again from the API.
	Message string
	// Error is the API's refusal, including FR-1107's bound, shown where the user
	// acted.
	Error string
	// Hidden are the filter fields both steps carry.
	Hidden []hiddenField
	// CSRFToken is FR-1009's token for the two POST forms (confirm and commit).
	CSRFToken string
}

// trailRow is one entry of the attempt trail: an attempt row, or the synthesized
// entry that stands for the attempt a delivery had without one (FR-1110).
type trailRow struct {
	Attempt        int
	OccurredAt     string
	Duration       string
	ResponseStatus string
	// ResponseBody is the point of UI-15's "including response bodies". The API
	// truncates it (FR-1103); the portal renders it as text.
	ResponseBody string
	ErrorClass   string
	Outcome      string
	NextRetryAt  string
	// Synthesized marks the entry built from the delivery row rather than from an
	// `attempts` row (FR-811, FR-1110).
	Synthesized bool
}

// deliveryDetailView is one delivery with its ordered attempts (FR-1102).
type deliveryDetailView struct {
	ID                string
	EventID           string
	EventType         string
	EndpointID        string
	Status            string
	Attempts          int
	FirstAttemptAt    string
	DeliveredAt       string
	CreatedAt         string
	Duration          string
	NonDeliveryReason string
	// ReplayOf is the delivery this one was replayed from (FR-1108, one direction).
	ReplayOf string
	// ReplayedBy are the deliveries a replay created from this one (the other).
	ReplayedBy []int64
	Trail      []trailRow
	BackURL    string
}

// --- core response shapes --------------------------------------------------

// listDeliveriesResponse is GET /v1/consumer/deliveries. Only the fields the log
// renders are decoded.
type listDeliveriesResponse struct {
	Deliveries []struct {
		ID                 int64   `json:"id"`
		EventType          string  `json:"event_type"`
		EndpointID         string  `json:"endpoint_id"`
		Status             string  `json:"status"`
		Attempts           int     `json:"attempts"`
		FirstAttemptAt     *string `json:"first_attempt_at"`
		DeliveredAt        *string `json:"delivered_at"`
		CreatedAt          string  `json:"created_at"`
		DurationMs         *int    `json:"duration_ms"`
		ReplayOfDeliveryID *int64  `json:"replay_of_delivery_id"`
	} `json:"deliveries"`
	NextCursor string `json:"next_cursor"`
}

type attemptJSON struct {
	Attempt        int     `json:"attempt"`
	OccurredAt     string  `json:"occurred_at"`
	DurationMs     int     `json:"duration_ms"`
	ResponseStatus *int    `json:"response_status"`
	ResponseBody   *string `json:"response_body"`
	ErrorClass     string  `json:"error_class"`
	Outcome        string  `json:"outcome"`
	NextRetryAt    *string `json:"next_retry_at"`
}

type effectiveAttemptJSON struct {
	Attempt    int    `json:"attempt"`
	OccurredAt string `json:"occurred_at"`
	DurationMs *int   `json:"duration_ms"`
	Status     string `json:"status"`
	Outcome    string `json:"outcome"`
}

// deliveryDetailResponse is GET /v1/consumer/deliveries/{deliveryID}.
type deliveryDetailResponse struct {
	ID                 int64                 `json:"id"`
	EventID            string                `json:"event_id"`
	EventType          string                `json:"event_type"`
	EndpointID         string                `json:"endpoint_id"`
	Status             string                `json:"status"`
	Attempts           int                   `json:"attempts"`
	FirstAttemptAt     *string               `json:"first_attempt_at"`
	DeliveredAt        *string               `json:"delivered_at"`
	CreatedAt          string                `json:"created_at"`
	DurationMs         *int                  `json:"duration_ms"`
	NonDeliveryReason  *string               `json:"non_delivery_reason"`
	ReplayOfDeliveryID *int64                `json:"replay_of_delivery_id"`
	AttemptsRows       []attemptJSON         `json:"attempts_rows"`
	EffectiveAttempt   *effectiveAttemptJSON `json:"effective_attempt"`
	ReplayedBy         []int64               `json:"replayed_by"`
}

// replayResponse is the API's answer to a dry run and to a committed replay: the
// count it matched, what it created, and the rate it releases at.
type replayResponse struct {
	Matched          int     `json:"matched"`
	Created          int     `json:"created"`
	DeliveryIDs      []int64 `json:"delivery_ids"`
	Repeated         bool    `json:"repeated"`
	ReleasePerSecond int     `json:"release_per_second"`
	DryRun           bool    `json:"dry_run"`
}

// --- the delivery log (UI-15) ---------------------------------------------

// deliveryStatuses is FR-1101's `status` filter as the log offers it. The four
// values are the deliveries column's own check constraint - the API's contract
// for the field - so the control lists them rather than accepting free text: a
// status the API does not define could only ever select nothing. 0007_routing.sql
// defined them first; S5's 0013 is the current one, because I-4/FR-1201 renamed
// the terminal value to `failed_terminal` and replaced the constraint with it.
var deliveryStatuses = []string{"pending", "delivered", "failed_terminal", "skipped"}

// deliveryFilterNames is exactly the set of read parameters the core API's
// delivery list defines (FR-1101 plus its paging). The portal forwards these and
// nothing else: a name outside this set would be a filter the API never agreed
// to, and the portal does not get to define one. Note there is no `consumer_id`
// - the claim decides that (FR-1105) - so a browser asking for another consumer
// cannot make the portal even mention one.
var deliveryFilterNames = []string{"endpoint_id", "event_type", "status", "from", "to", "limit", "cursor"}

// deliveryLog renders UI-15's log page.
func (h *Handler) deliveryLog(w http.ResponseWriter, r *http.Request) {
	h.renderDeliveryLog(w, r, http.StatusOK, readDeliveryFilters(r.URL.Query().Get), deliveryLogView{})
}

// renderDeliveryLog reads the caller's deliveries from the inspect API and
// renders the log. f is the filter set this render is about - the query string
// for a page view, the form body for a replay step - and base carries any replay
// panel state the caller already has (a confirmation, an outcome, a refusal).
// Everything else comes from the API.
func (h *Handler) renderDeliveryLog(w http.ResponseWriter, r *http.Request, status int,
	f deliveryFilterView, base deliveryLogView) {
	base.Filter = f
	base.Statuses = deliveryStatuses
	base.Endpoints = h.consumerEndpointOptions(r)
	base.FilterURL = "/deliveries" + querySuffix(deliveryLogQuery(base.Filter, false))

	resp, err := h.coreReq(r, http.MethodGet,
		"/v1/consumer/deliveries"+querySuffix(deliveryLogQuery(base.Filter, true)), nil)
	if notAuthed(resp, err, w, r, h) {
		return
	}
	switch resp.Status {
	case http.StatusOK:
		var page listDeliveriesResponse
		_ = json.Unmarshal(resp.Body, &page)
		for _, d := range page.Deliveries {
			base.Deliveries = append(base.Deliveries, deliveryRow{
				ID: d.ID, EventType: d.EventType, EndpointID: d.EndpointID,
				Status: d.Status, Attempts: d.Attempts,
				FirstAttemptAt: strOr(d.FirstAttemptAt, "-"), DeliveredAt: strOr(d.DeliveredAt, "-"),
				Duration: msOr(d.DurationMs), ReplayOf: idOr(d.ReplayOfDeliveryID),
			})
		}
		if page.NextCursor != "" {
			next := base.Filter
			next.Cursor = page.NextCursor
			base.NextPageURL = "/deliveries" + querySuffix(deliveryLogQuery(next, true))
		}
	default:
		// The API refused the request (a filter it does not accept, a limit out of
		// its range, a bad cursor). Show its reason rather than an empty log: an
		// empty log would read as "you have no deliveries".
		base.Error = coreErrorMessage(resp)
	}
	base.Replay = h.replayPanel(r, base.Filter, base.Replay)
	h.renderPageStatus(w, r, status, "deliveries", "Delivery log", "deliveries", base)
}

// deliveryFilterValues maps the API's read parameters to the values this request
// carries. deliveryLogQuery encodes only the names the whitelist above allows, so
// a parameter missing from that whitelist cannot be sent even if this map names
// it: sending one requires editing the whitelist, where the reason it is absent
// (`consumer_id`) is written down.
func deliveryFilterValues(f deliveryFilterView) map[string]string {
	return map[string]string{
		"endpoint_id": f.EndpointID,
		"event_type":  f.EventType,
		"status":      f.Status,
		"from":        f.From,
		"to":          f.To,
		"limit":       f.Limit,
		"cursor":      f.Cursor,
	}
}

// deliveryLogQuery builds the core API query for the log from the request's own
// parameters. Values are forwarded verbatim - including `limit`, which the API
// bounds itself - so what the user sees is the API's answer and never a
// portal-side clamp (§14: the portal passes the API's filters and limits
// through). Cursor is dropped when withCursor is false, so changing a filter
// starts at the newest page.
func deliveryLogQuery(f deliveryFilterView, withCursor bool) string {
	values := deliveryFilterValues(f)
	q := url.Values{}
	for _, name := range deliveryFilterNames {
		if name == "cursor" && !withCursor {
			continue
		}
		if v := values[name]; v != "" {
			q.Set(name, v)
		}
	}
	return q.Encode()
}

// readDeliveryFilters reads the API's read parameters from a request. It is the
// same function for the log page's query string and for the replay panel's form
// body because they are the same filter set, expressed the same way.
func readDeliveryFilters(get func(string) string) deliveryFilterView {
	return deliveryFilterView{
		EndpointID: get("endpoint_id"),
		EventType:  get("event_type"),
		Status:     get("status"),
		From:       get("from"),
		To:         get("to"),
		Limit:      get("limit"),
		Cursor:     get("cursor"),
	}
}

// consumerEndpointOptions reads the caller's own endpoints for the log's endpoint
// filter. A refusal or a network error yields no options, which leaves the
// "All endpoints" choice the API accepts: the filter is additive to the page.
func (h *Handler) consumerEndpointOptions(r *http.Request) []endpointOption {
	resp, err := h.coreReq(r, http.MethodGet, "/v1/consumer/endpoints", nil)
	if err != nil || resp.Status != http.StatusOK {
		return nil
	}
	var le listEndpointsResponse
	_ = json.Unmarshal(resp.Body, &le)
	out := make([]endpointOption, 0, len(le.Endpoints))
	for _, e := range le.Endpoints {
		out = append(out, endpointOption{ID: e.ID, Name: e.Name})
	}
	return out
}

// --- one delivery with its attempts (UI-15) -------------------------------

// deliveryDetail renders UI-15's attempt detail, response bodies included.
func (h *Handler) deliveryDetail(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	// The id is the API's own path parameter, escaped rather than pasted in: a
	// segment the caller crafted cannot then rewrite the core request's path.
	resp, err := h.coreReq(r, http.MethodGet, "/v1/consumer/deliveries/"+url.PathEscape(id), nil)
	if notAuthed(resp, err, w, r, h) {
		return
	}
	switch {
	case resp.Status == http.StatusNotFound:
		// AC-39: a delivery outside the caller's consumer is a miss in its own
		// scope, and the API answers 404 - never 403. The portal passes that
		// through at the same status instead of dressing it as a friendly page.
		h.deliveryProblem(w, r, http.StatusNotFound, "No such delivery",
			"That delivery does not exist for your consumer.")
		return
	case resp.Status != http.StatusOK:
		h.deliveryProblem(w, r, http.StatusBadGateway, "Cannot read that delivery",
			coreErrorMessage(resp))
		return
	}
	var d deliveryDetailResponse
	_ = json.Unmarshal(resp.Body, &d)
	h.renderPage(w, r, "delivery", "Delivery "+fmt.Sprint(d.ID), "deliveries", h.deliveryDetailView(d))
}

// deliveryDetailView turns the API's delivery into the page's view model. The
// trail is the API's attempt rows, and when there are none the API's effective
// attempt stands in for them: FR-1110 forbids rendering an empty trail for a
// delivery that succeeded on its first attempt (FR-811 keeps that attempt on the
// delivery row, not in an `attempts` row), and a skipped delivery gets the same
// treatment so an unroutable event does not read as "nothing happened".
func (h *Handler) deliveryDetailView(d deliveryDetailResponse) deliveryDetailView {
	view := deliveryDetailView{
		ID: fmt.Sprint(d.ID), EventID: d.EventID, EventType: d.EventType, EndpointID: d.EndpointID,
		Status: d.Status, Attempts: d.Attempts,
		FirstAttemptAt: strOr(d.FirstAttemptAt, "-"), DeliveredAt: strOr(d.DeliveredAt, "-"),
		CreatedAt: d.CreatedAt, Duration: msOr(d.DurationMs),
		NonDeliveryReason: strOr(d.NonDeliveryReason, "-"),
		ReplayOf:          idOr(d.ReplayOfDeliveryID),
		ReplayedBy:        d.ReplayedBy,
		BackURL:           "/deliveries",
	}
	for _, a := range d.AttemptsRows {
		view.Trail = append(view.Trail, trailRow{
			Attempt: a.Attempt, OccurredAt: a.OccurredAt, Duration: msOf(a.DurationMs),
			ResponseStatus: intOr(a.ResponseStatus), ResponseBody: strOr(a.ResponseBody, "(empty)"),
			ErrorClass: a.ErrorClass, Outcome: a.Outcome, NextRetryAt: strOr(a.NextRetryAt, "-"),
		})
	}
	if len(view.Trail) == 0 && d.EffectiveAttempt != nil {
		view.Trail = append(view.Trail, trailRow{
			Attempt: d.EffectiveAttempt.Attempt, OccurredAt: d.EffectiveAttempt.OccurredAt,
			Duration: msOr(d.EffectiveAttempt.DurationMs), ResponseStatus: "-",
			// No body was recorded, because no attempt row exists (§7.11 FR-811 /
			// FR-1110). The row is marked synthesized, and the template says why in
			// the attempt column, so an empty body cell cannot read as a body.
			ResponseBody: "-",
			ErrorClass:   "-", Outcome: d.EffectiveAttempt.Outcome, NextRetryAt: "-",
			Synthesized: true,
		})
	}
	return view
}

// deliveryProblem answers a request the portal cannot complete (AC-39's foreign
// id, or a core read that failed) with the status that answer deserves.
func (h *Handler) deliveryProblem(w http.ResponseWriter, r *http.Request, status int, title, message string) {
	h.renderPageStatus(w, r, status, "problem", title, "deliveries", problemView{
		Title: title, Message: message, BackURL: "/deliveries", BackLabel: "Back to the delivery log",
	})
}

// --- replay (UI-16) --------------------------------------------------------

// deliveryReplayConfirm is UI-16's first step, and UI-5's "explicit confirmation
// stating count and rate" applied to the consumer portal: the API is asked what
// the selection would replay, with dry_run, and its own count and rate are what
// the panel states. A selection over FR-1107's bound is refused by the API here,
// before anything is created, and its message is what the user reads.
func (h *Handler) deliveryReplayConfirm(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	f := readDeliveryFilters(r.FormValue)
	key := r.FormValue("idempotency_key")
	panel := replayView{Key: key}
	resp, err := h.coreReqHeaders(r, http.MethodPost, "/v1/consumer/deliveries/replay",
		replayBody(f, true), replayKeyHeader(key))
	if notAuthed(resp, err, w, r, h) {
		return
	}
	if resp.Status == http.StatusOK {
		var res replayResponse
		_ = json.Unmarshal(resp.Body, &res)
		panel.Confirmed, panel.Matched, panel.Rate = true, res.Matched, res.ReleasePerSecond
	} else {
		panel.Error = coreErrorMessage(resp)
	}
	if wantsFragment(r) {
		h.r.Fragment(w, http.StatusOK, "frag:replay/panel", h.replayPanel(r, f, panel))
		return
	}
	// A plain post gets the log page with the confirmation in place. It is
	// deliberately not a redirect: a redirect would drop the confirmed selection
	// and mint a second request key, so the confirmation the user is looking at
	// would not be the one their next click commits.
	h.renderDeliveryLog(w, r, http.StatusOK, f, deliveryLogView{Replay: panel})
}

// deliveryReplay is UI-16's commit step: the same selection, sent again with
// dry_run off and the confirmation's request key as the `Idempotency-Key`
// header. The portal adds no bound of its own - FR-1107's ≤ 1,000 and 50/s are
// the API's, and a refusal is the API's message shown where the user acted
// (in the panel for htmx, on the log page it redirects to otherwise).
func (h *Handler) deliveryReplay(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	f := readDeliveryFilters(r.FormValue)
	key := r.FormValue("idempotency_key")
	resp, err := h.coreReqHeaders(r, http.MethodPost, "/v1/consumer/deliveries/replay",
		replayBody(f, false), replayKeyHeader(key))
	if notAuthed(resp, err, w, r, h) {
		return
	}
	panel := replayView{Key: key, Confirmed: true}
	if resp.Status == http.StatusOK {
		var res replayResponse
		_ = json.Unmarshal(resp.Body, &res)
		panel.Matched, panel.Rate = res.Matched, res.ReleasePerSecond
		panel.Message = replayOutcome(res)
	} else {
		panel.Error = coreErrorMessage(resp)
	}
	if wantsFragment(r) {
		h.r.Fragment(w, http.StatusOK, "frag:replay/panel", h.replayPanel(r, f, panel))
		return
	}
	if panel.Error != "" {
		h.setFlash(w, "error", "The replay was refused: "+panel.Error)
	} else {
		h.setFlash(w, "success", panel.Message)
	}
	http.Redirect(w, r, "/deliveries"+querySuffix(deliveryLogQuery(f, false)), http.StatusSeeOther)
}

// replayBody is the selection the API's replay route acts on. It carries the
// list's filters and nothing invented: the same expression the log was showing
// is the one that gets replayed (UI-5). The consumer id is not among them - the
// claim decides it (FR-1105), so the portal cannot ask to replay another
// consumer's deliveries even by sending one.
func replayBody(f deliveryFilterView, dryRun bool) map[string]any {
	body := map[string]any{"dry_run": dryRun}
	if f.EndpointID != "" {
		body["endpoint_id"] = f.EndpointID
	}
	if f.EventType != "" {
		body["event_type"] = f.EventType
	}
	if f.Status != "" {
		body["status"] = f.Status
	}
	if f.From != "" {
		body["from"] = f.From
	}
	if f.To != "" {
		body["to"] = f.To
	}
	return body
}

// replayKeyHeader is FR-1107's request key, carried where the API documents it
// for a batch write: the `Idempotency-Key` header (as publish does, FR-603).
func replayKeyHeader(key string) map[string]string {
	if key == "" {
		return nil
	}
	return map[string]string{"Idempotency-Key": key}
}

// replayPanel completes a replay view from the filter set it acts on and the
// request's CSRF token, so the page and the htmx fragment always render the same
// hidden fields and the same token.
func (h *Handler) replayPanel(r *http.Request, f deliveryFilterView, v replayView) replayView {
	if v.Key == "" {
		v.Key = newRequestKey()
	}
	v.Selection = selectionWords(f)
	v.Hidden = replayHidden(f, v.Key)
	v.CSRFToken = h.csrfToken(r)
	return v
}

// replayHidden is the filter set both steps carry forward, plus the request key
// of this confirmation. The key is the portal's own (minted per confirmation) and
// never a field the user types: it is a request identifier, not a value the API
// decides and not a credential.
func replayHidden(f deliveryFilterView, key string) []hiddenField {
	var out []hiddenField
	for _, k := range []struct{ name, value string }{
		{"endpoint_id", f.EndpointID}, {"event_type", f.EventType}, {"status", f.Status},
		{"from", f.From}, {"to", f.To},
	} {
		if k.value != "" {
			out = append(out, hiddenField{Name: k.name, Value: k.value})
		}
	}
	if key != "" {
		out = append(out, hiddenField{Name: "idempotency_key", Value: key})
	}
	return out
}

// selectionWords describes the set a replay acts on, so the confirmation says
// which selection it is about rather than "some deliveries".
func selectionWords(f deliveryFilterView) string {
	var parts []string
	if f.EndpointID != "" {
		parts = append(parts, "endpoint "+f.EndpointID)
	}
	if f.EventType != "" {
		parts = append(parts, "event type "+f.EventType)
	}
	if f.Status != "" {
		parts = append(parts, "status "+f.Status)
	}
	if f.From != "" || f.To != "" {
		parts = append(parts, "the time range you asked for")
	}
	if len(parts) == 0 {
		return "your consumer's replayable deliveries"
	}
	return "your consumer's deliveries with " + strings.Join(parts, ", ")
}

// replayOutcome states what a committed replay did, in the API's numbers. A
// repeat of a request key is reported as such rather than as a second write
// (AC-12): the API answers `repeated` and creates nothing.
func replayOutcome(res replayResponse) string {
	if res.Repeated {
		return fmt.Sprintf("That replay already ran: the API created nothing new (%d deliveries from the first request).", len(res.DeliveryIDs))
	}
	return fmt.Sprintf("The API replayed %d deliveries at %d/s per endpoint.", res.Created, res.ReleasePerSecond)
}

// newRequestKey mints FR-1107's request key for one confirmation.
func newRequestKey() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// A key that cannot be minted is handed to the API empty, and the API's
		// own refusal ("send an Idempotency-Key header") is what the user sees.
		return ""
	}
	return "portal-replay-" + hex.EncodeToString(b[:])
}

// --- small formatters ------------------------------------------------------

func querySuffix(q string) string {
	if q == "" {
		return ""
	}
	return "?" + q
}

func strOr(s *string, fallback string) string {
	if s == nil || *s == "" {
		return fallback
	}
	return *s
}

func intOr(n *int) string {
	if n == nil {
		return "-"
	}
	return fmt.Sprint(*n)
}

func msOr(n *int) string {
	if n == nil {
		return "-"
	}
	return fmt.Sprintf("%d ms", *n)
}

func msOf(n int) string { return fmt.Sprintf("%d ms", n) }

func idOr(n *int64) string {
	if n == nil {
		return ""
	}
	return fmt.Sprint(*n)
}
