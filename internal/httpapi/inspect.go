package httpapi

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/m7jay/pigeon/internal/store"
)

// This file is S7's read and replay surface (§11, FR-1101..FR-1111):
//
//	producer:
//	  GET  /v1/producer/deliveries                  - list, filtered and cursor-paginated (FR-1101)
//	  GET  /v1/producer/deliveries/{deliveryID}     - detail with ordered attempts (FR-1102, FR-1103)
//	  GET  /v1/producer/deliveries/dedupe           - was this a duplicate? (FR-903)
//	  POST /v1/producer/deliveries/replay           - replay, bounded and idempotent (FR-1106..FR-1108)
//	  GET  /v1/producer/events/{eventID}            - the event-level view and its rollup (FR-1109, FR-1110, FR-1111)
//	consumer (FR-1105, and UI-15/UI-16's API):
//	  GET  /v1/consumer/deliveries                  - the same list, forced to the caller's own consumer
//	  GET  /v1/consumer/deliveries/{deliveryID}
//	  POST /v1/consumer/deliveries/replay
//
// Every consumer route takes its consumer from the session claim and never from
// the path, the query or the body (FR-110): the handlers below build the filter
// with the claim's consumer id *after* parsing the request, so a foreign id in a
// filter is overwritten rather than honoured, and a foreign id in a path is a
// miss in the caller's scope, which is a 404 (FR-108).

// deliveryJSON is the wire view of one delivery. It carries the FR-811 columns
// (status, attempts, first_attempt_at, delivered_at) plus the duration they imply,
// which is what makes FR-1110 answerable: a successful first attempt has no
// attempt row, and its status, timestamp and duration are here.
type deliveryJSON struct {
	ID                 int64   `json:"id"`
	EventID            string  `json:"event_id"`
	EventType          string  `json:"event_type"`
	EndpointID         string  `json:"endpoint_id"`
	ConsumerID         string  `json:"consumer_id"`
	Status             string  `json:"status"`
	Attempts           int     `json:"attempts"`
	FirstAttemptAt     *string `json:"first_attempt_at"`
	DeliveredAt        *string `json:"delivered_at"`
	CreatedAt          string  `json:"created_at"`
	DurationMs         *int    `json:"duration_ms"`
	NonDeliveryReason  *string `json:"non_delivery_reason"`
	ReplayOfDeliveryID *int64  `json:"replay_of_delivery_id"`
	// HeldBackSince and HeldBackReason are FR-1304: the delivery is being held
	// back by its endpoint's limits - not attempted, not failed, not slow - since
	// the given instant and for the given reason ("endpoint_concurrency_cap" from
	// FR-1302, "endpoint_rps" from FR-1306). Both are null on a delivery that is
	// not held, which is every delivery whose attempt has started.
	HeldBackSince  *string `json:"held_back_since"`
	HeldBackReason *string `json:"held_back_reason"`
	// AttemptsRows are the attempt ROWS (FR-1102/FR-1103), in attempt order.
	// Empty for a successful first attempt, which FR-811 records as columns on
	// the delivery instead of a row.
	AttemptsRows []attemptJSON `json:"attempts_rows"`
	// EffectiveAttempt is the attempt a delivery had with no row: the success
	// FR-811 stores on the delivery, or the skip of a matched endpoint that
	// could not take the event. FR-1110: when AttemptsRows is empty this is
	// present, so a trail is never rendered empty. The field is here even when
	// there are rows, because a retried delivery's last attempt is the success
	// and it has no row of its own either.
	EffectiveAttempt *effectiveAttemptJSON `json:"effective_attempt"`
	// ReplayedBy names the deliveries a replay created from this one (FR-1108).
	ReplayedBy []int64 `json:"replayed_by,omitempty"`
}

// attemptJSON is FR-1103 in full: number, timestamp, duration, status,
// truncated body, error class and next retry time.
type attemptJSON struct {
	Attempt        int     `json:"attempt"`
	OccurredAt     string  `json:"occurred_at"`
	DurationMs     int     `json:"duration_ms"`
	ResponseStatus *int    `json:"response_status"`
	ResponseBody   *string `json:"response_body"`
	ErrorClass     string  `json:"error_class"`
	Outcome        string  `json:"outcome"`
	NextRetryAt    *string `json:"next_retry_at"`
	// Synthesized marks the one entry that is not a row: see EffectiveAttempt.
	Synthesized bool `json:"synthesized"`
}

type effectiveAttemptJSON struct {
	Attempt     int    `json:"attempt"`
	OccurredAt  string `json:"occurred_at"`
	DurationMs  *int   `json:"duration_ms"`
	Status      string `json:"status"`
	Outcome     string `json:"outcome"`
	Synthesized bool   `json:"synthesized"`
}

func toDeliveryJSON(d store.DeliverySummary) deliveryJSON {
	return deliveryJSON{
		ID: d.ID, EventID: d.EventID, EventType: d.EventType, EndpointID: d.EndpointID,
		ConsumerID: d.ConsumerID, Status: d.Status, Attempts: d.Attempts,
		FirstAttemptAt: tsPtr(d.FirstAttemptAt), DeliveredAt: tsPtr(d.DeliveredAt),
		CreatedAt: ts(d.CreatedAt), DurationMs: d.DurationMs(),
		NonDeliveryReason: d.NonDeliveryReason, ReplayOfDeliveryID: d.ReplayOfDeliveryID,
		HeldBackSince: tsPtr(d.HeldBackSince), HeldBackReason: d.HeldBackReason,
		AttemptsRows: []attemptJSON{},
	}
}

func toDeliveryDetailJSON(d store.DeliveryDetail) deliveryJSON {
	out := toDeliveryJSON(d.Delivery)
	for _, a := range d.Attempts {
		out.AttemptsRows = append(out.AttemptsRows, attemptJSON{
			Attempt: a.Attempt, OccurredAt: ts(a.OccurredAt), DurationMs: a.DurationMs,
			ResponseStatus: a.ResponseStatus, ResponseBody: a.ResponseBody,
			ErrorClass: a.ErrorClass, Outcome: a.Outcome, NextRetryAt: tsPtr(a.NextRetryAt),
		})
	}
	if e := d.EffectiveAttempt; e != nil {
		out.EffectiveAttempt = &effectiveAttemptJSON{
			Attempt: e.Attempt, OccurredAt: ts(e.OccurredAt), DurationMs: e.DurationMs,
			Status: e.Status, Outcome: e.Outcome, Synthesized: true,
		}
	}
	out.ReplayedBy = d.ReplayedBy
	return out
}

// --- the delivery list (FR-1101, FR-1105) --------------------------------

// handleListDeliveries is GET /v1/producer/deliveries: the producer's whole
// environment, filterable by endpoint, consumer, event type, status and creation
// time, newest first, cursor-paginated.
func (s *Server) handleListDeliveries(w http.ResponseWriter, r *http.Request) {
	sc, err := s.memberScope(r)
	if err != nil {
		s.scopeError(w, r, err)
		return
	}
	s.listDeliveries(w, r, sc, "")
}

// handleConsumerListDeliveries is GET /v1/consumer/deliveries (FR-1105): the
// same list with the consumer forced from the session claim, so a consumer user
// cannot widen its own scope by asking for another consumer.
func (s *Server) handleConsumerListDeliveries(w http.ResponseWriter, r *http.Request) {
	sc, err := consumerScope(r)
	if err != nil {
		s.scopeError(w, r, err)
		return
	}
	sess, _ := sessionFrom(r.Context())
	s.listDeliveries(w, r, sc, sess.ConsumerID)
}

func (s *Server) listDeliveries(w http.ResponseWriter, r *http.Request, sc store.Scope, consumerID string) {
	f, err := deliveryFilterFrom(r, consumerID)
	if err != nil {
		s.filterError(w, r, err)
		return
	}
	limit, err := intParam(r, "limit")
	if err != nil {
		s.filterError(w, r, err)
		return
	}
	cursor, err := cursorParam(r)
	if err != nil {
		s.filterError(w, r, err)
		return
	}
	page, err := s.deps.Data.ListDeliveries(r.Context(), sc, f, limit, cursor)
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	out := make([]deliveryJSON, 0, len(page.Deliveries))
	for _, d := range page.Deliveries {
		out = append(out, toDeliveryJSON(d))
	}
	writeJSON(w, http.StatusOK, map[string]any{"deliveries": out, "next_cursor": page.NextCursor})
}

// handleGetDelivery is GET /v1/producer/deliveries/{deliveryID} (FR-1102).
func (s *Server) handleGetDelivery(w http.ResponseWriter, r *http.Request) {
	sc, err := s.memberScope(r)
	if err != nil {
		s.scopeError(w, r, err)
		return
	}
	s.getDelivery(w, r, sc)
}

// handleConsumerGetDelivery is the consumer-scoped detail (FR-1105): the same
// read, scoped by the session's producer+env, with the delivery's consumer
// checked against the claim.
func (s *Server) handleConsumerGetDelivery(w http.ResponseWriter, r *http.Request) {
	sc, err := consumerScope(r)
	if err != nil {
		s.scopeError(w, r, err)
		return
	}
	s.getDelivery(w, r, sc)
}

func (s *Server) getDelivery(w http.ResponseWriter, r *http.Request, sc store.Scope) {
	id, ok := deliveryIDParam(w, r)
	if !ok {
		return
	}
	detail, err := s.deps.Data.GetDelivery(r.Context(), sc, id)
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	// FR-1105: a consumer session sees only its own consumer's deliveries. The
	// row is readable in the tenant, so the check is here rather than in the
	// query: the answer must be the same 404 a foreign id gets, never a 403.
	if sess, ok := sessionFrom(r.Context()); ok && sess.IsConsumer() && detail.Delivery.ConsumerID != sess.ConsumerID {
		s.writeNotFound(w, r)
		return
	}
	writeJSON(w, http.StatusOK, toDeliveryDetailJSON(detail))
}

// --- the event-level view (FR-1109, FR-1110, FR-1111) --------------------

type eventJSON struct {
	ID                string  `json:"id"`
	EventType         string  `json:"event_type"`
	RoutingMode       string  `json:"routing_mode"`
	ConsumerID        *string `json:"consumer_id"`
	ConsumerRef       *string `json:"consumer_ref"`
	PayloadVersion    int     `json:"payload_version"`
	OccurredAt        string  `json:"occurred_at"`
	CreatedAt         string  `json:"created_at"`
	RoutingState      *string `json:"routing_state"`
	NonDeliveryReason *string `json:"non_delivery_reason"`
}

// handleEventView is GET /v1/producer/events/{eventID}: the event, its fan-out
// outcome, its rollup (FR-1109) and every delivery with its trail, which is how
// FR-1111's "did my event succeed?" is answered from the API rather than from
// logs.
func (s *Server) handleEventView(w http.ResponseWriter, r *http.Request) {
	sc, err := s.memberScope(r)
	if err != nil {
		s.scopeError(w, r, err)
		return
	}
	view, err := s.deps.Data.EventView(r.Context(), sc, r.PathValue("eventID"))
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	deliveries := make([]deliveryJSON, 0, len(view.Deliveries))
	for _, d := range view.Deliveries {
		deliveries = append(deliveries, toDeliveryDetailJSON(d))
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"event": eventJSON{
			ID: view.EventID, EventType: view.EventType, RoutingMode: view.RoutingMode,
			ConsumerID: view.ConsumerID, ConsumerRef: view.ConsumerRef,
			PayloadVersion: view.PayloadVersion, OccurredAt: ts(view.OccurredAt),
			CreatedAt: ts(view.CreatedAt), RoutingState: view.RoutingState,
			NonDeliveryReason: view.NonDeliveryReason,
		},
		"rollup":     view.Rollup,
		"deliveries": deliveries,
	})
}

// --- the dedupe answer (FR-903) -----------------------------------------

type dedupeJSON struct {
	EndpointID      string  `json:"endpoint_id"`
	EventID         string  `json:"event_id"`
	Duplicate       bool    `json:"duplicate"`
	FirstDeliveryID int64   `json:"first_delivery_id,omitempty"`
	FirstSeenAt     *string `json:"first_seen_at"`
	Deliveries      int     `json:"deliveries"`
}

// handleDedupeAnswer is GET /v1/producer/deliveries/dedupe?endpoint_id=&event_id=
// (FR-903): the support question "was this event a duplicate for this endpoint?"
// for the last 30 days. It is answered from the `deliveries` row while its
// partition exists and from S8's `delivery_dedupe` archive once it has been
// dropped (G2 gap 4, A-4), read through the single view `delivery_dedupe_all`,
// so this handler does not care which side answered.
func (s *Server) handleDedupeAnswer(w http.ResponseWriter, r *http.Request) {
	sc, err := s.memberScope(r)
	if err != nil {
		s.scopeError(w, r, err)
		return
	}
	q := r.URL.Query()
	endpointID, eventID := q.Get("endpoint_id"), q.Get("event_id")
	if endpointID == "" || eventID == "" {
		writeError(w, http.StatusBadRequest, "dedupe_key_required",
			"endpoint_id and event_id are both required: they are the dedupe key (FR-903)")
		return
	}
	answer, err := s.deps.Data.DedupeAnswer(r.Context(), sc, endpointID, eventID)
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, dedupeJSON{
		EndpointID: answer.EndpointID, EventID: answer.EventID, Duplicate: answer.Duplicate,
		FirstDeliveryID: answer.FirstDeliveryID, FirstSeenAt: tsPtr(answer.FirstSeenAt),
		Deliveries: answer.Deliveries,
	})
}

// --- replay (FR-1106, FR-1107, FR-1108) ----------------------------------

// replayRequest is the selection a replay acts on plus FR-1107's request key.
// The filters are the list's filters, so "replay what I was just looking at" is
// the same expression in both calls (UI-5).
type replayRequest struct {
	EndpointID string     `json:"endpoint_id"`
	ConsumerID string     `json:"consumer_id"`
	EventType  string     `json:"event_type"`
	Status     string     `json:"status"`
	From       *time.Time `json:"from"`
	To         *time.Time `json:"to"`
	// IdempotencyKey is FR-1107's request key. The `Idempotency-Key` header is
	// the documented place for it, as it is for publish (FR-603); the body copy
	// is accepted and the header wins.
	IdempotencyKey string `json:"idempotency_key"`
	// DryRun answers the count without creating anything, which is what a
	// confirmation step needs before it commits to a batch.
	DryRun bool `json:"dry_run"`
}

// handleReplayDeliveries is POST /v1/producer/deliveries/replay.
func (s *Server) handleReplayDeliveries(w http.ResponseWriter, r *http.Request) {
	sc, err := s.memberScope(r)
	if err != nil {
		s.scopeError(w, r, err)
		return
	}
	s.replayDeliveries(w, r, sc, "")
}

// handleConsumerReplayDeliveries is POST /v1/consumer/deliveries/replay
// (UI-16): a consumer user replays its own consumer's deliveries, under exactly
// the bounds the producer route enforces - the portal passes the API's bounds
// through rather than inventing its own (§14).
func (s *Server) handleConsumerReplayDeliveries(w http.ResponseWriter, r *http.Request) {
	sc, err := consumerScope(r)
	if err != nil {
		s.scopeError(w, r, err)
		return
	}
	sess, _ := sessionFrom(r.Context())
	s.replayDeliveries(w, r, sc, sess.ConsumerID)
}

func (s *Server) replayDeliveries(w http.ResponseWriter, r *http.Request, sc store.Scope, consumerID string) {
	var req replayRequest
	// An empty body is "no filter", like POST /v1/producer/keys: the bound is
	// what makes that safe, and a client that means "everything replayable in
	// this environment" does not have to invent a filter to say so.
	if r.ContentLength != 0 {
		if !s.decodeJSON(w, r, &req) {
			return
		}
	}
	if h := r.Header.Get("Idempotency-Key"); h != "" {
		req.IdempotencyKey = h
	}
	// FR-1107: a replay is a batch write whose retry semantics have to be
	// defined, so the key is required rather than optional. Without one, a
	// retried request would create a second set of deliveries, which is exactly
	// what AC-12 forbids.
	if req.IdempotencyKey == "" {
		writeError(w, http.StatusBadRequest, "idempotency_key_required",
			"replay is idempotent on a request key (FR-1107): send an Idempotency-Key header")
		return
	}
	f := store.DeliveryFilter{
		EndpointID: req.EndpointID, ConsumerID: req.ConsumerID, EventType: req.EventType,
		Status: req.Status, From: req.From, To: req.To,
	}
	// The consumer claim wins over anything the request said (FR-1105): the
	// portal cannot widen its own scope by naming another consumer.
	if consumerID != "" {
		f.ConsumerID = consumerID
	}
	result, err := s.deps.Data.ReplayDeliveries(r.Context(), sc, store.ReplayInput{
		Filter: f, RequestKey: req.IdempotencyKey, DryRun: req.DryRun,
	})
	if err != nil {
		if errors.Is(err, store.ErrReplayBound) {
			// AC-13: refused, naming the bound. The store's message carries the
			// 1000, and the UI shows it where the user acted (UI-5).
			writeError(w, http.StatusUnprocessableEntity, "replay_bound_exceeded", err.Error())
			return
		}
		s.storeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"matched":            result.Matched,
		"created":            result.Created,
		"delivery_ids":       result.DeliveryIDs,
		"repeated":           result.Repeated,
		"release_per_second": result.ReleasePerSecond,
		"dry_run":            req.DryRun,
	})
}

// --- request parsing -----------------------------------------------------

// deliveryFilterFrom builds the selection from the query string. consumerID is
// the claim's consumer for a consumer-scoped caller and "" for a producer, in
// which case `?consumer_id=` is honoured (UI-3 filters by consumer).
func deliveryFilterFrom(r *http.Request, consumerID string) (store.DeliveryFilter, error) {
	q := r.URL.Query()
	f := store.DeliveryFilter{
		EndpointID: q.Get("endpoint_id"),
		EventType:  q.Get("event_type"),
		Status:     q.Get("status"),
		ConsumerID: consumerID,
	}
	if f.ConsumerID == "" {
		f.ConsumerID = q.Get("consumer_id")
	}
	var err error
	if f.From, err = timeParam(q.Get("from")); err != nil {
		return f, err
	}
	if f.To, err = timeParam(q.Get("to")); err != nil {
		return f, err
	}
	return f, nil
}

var (
	errInvalidTime   = errors.New("time must be RFC 3339, e.g. 2026-10-27T12:00:00Z")
	errInvalidLimit  = errors.New("limit must be an integer between 1 and 200")
	errInvalidCursor = errors.New("cursor must be the next_cursor value from the previous page")
)

func timeParam(v string) (*time.Time, error) {
	if v == "" {
		return nil, nil
	}
	t, err := time.Parse(time.RFC3339, v)
	if err != nil {
		return nil, errInvalidTime
	}
	return &t, nil
}

// intParam reads ?limit=. Zero (absent) means the store's default.
func intParam(r *http.Request, name string) (int, error) {
	v := r.URL.Query().Get(name)
	if v == "" {
		return 0, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 || n > 200 {
		return 0, errInvalidLimit
	}
	return n, nil
}

// cursorParam reads ?cursor=. The cursor is a delivery id and the order is
// descending, so a page is "ids below the last one you saw".
func cursorParam(r *http.Request) (*int64, error) {
	v := r.URL.Query().Get("cursor")
	if v == "" {
		return nil, nil
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n < 1 {
		return nil, errInvalidCursor
	}
	return &n, nil
}

// deliveryIDParam parses the path's delivery id. A non-numeric id is answered
// as "no such resource" rather than a 400: it cannot name a delivery in any
// tenant, and FR-108's rule is that a miss looks like a miss.
func deliveryIDParam(w http.ResponseWriter, r *http.Request) (int64, bool) {
	id, err := strconv.ParseInt(r.PathValue("deliveryID"), 10, 64)
	if err != nil || id < 1 {
		writeError(w, http.StatusNotFound, "not_found", "no such resource")
		return 0, false
	}
	return id, true
}

// filterError answers a malformed read parameter.
func (s *Server) filterError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, errInvalidTime):
		writeError(w, http.StatusBadRequest, "invalid_time", err.Error())
	case errors.Is(err, errInvalidCursor):
		writeError(w, http.StatusBadRequest, "invalid_cursor", err.Error())
	default:
		writeError(w, http.StatusBadRequest, "invalid_limit", errInvalidLimit.Error())
	}
}
