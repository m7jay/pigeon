package httpapi

import (
	"net/http"
	"time"

	"github.com/m7jay/pigeon/internal/retention"
	"github.com/m7jay/pigeon/internal/store"
)

// This file is S9a's producer-portal surface (M2a, PLAN.md:29): the producer
// routes the portal needs that existed nowhere, added so that every panel it
// renders is a documented API call (§14: nothing a portal can do may be
// impossible from the API, and a portal holds no state of its own).
//
// A producer portal session is not a producer API key. FR-112 makes a portal
// session an opaque bearer, and FR-201 keeps the key as the machine credential,
// so the routes here are kindMember - a session whose claims name a producer and a
// role - and each takes the producer and the user from that claim, never from a
// request field. That is why they are their own path prefix rather than a
// relaxation of the key routes: POST /v1/events stays key-only (the ingest path a
// producer's own infrastructure uses), and the portal gets POST /v1/producer/events
// - the same work through servePublish, under a different credential, so the two
// cannot drift.
//
//   GET  /v1/producer/event-types                                    UI-1 step 2, UI-9
//   POST /v1/producer/event-types                                    UI-1 step 2
//   POST /v1/producer/events                                         UI-1 steps 5-6, UI-4
//   GET  /v1/producer/usage                                          UI-8 (SB-6)
//   GET  /v1/producer/consumers/{consumerID}/events                  UI-11
//   POST /v1/producer/consumers/{consumerID}/endpoints/{endpointID}/rotate  UI-6

// --- event types (UI-1 step 2, UI-9) -------------------------------------

// handleMemberListEventTypes is GET /v1/producer/event-types?env=test|live: the
// same list GET /v1/event-types answers, for a session instead of a key. UI-1
// needs it to check the second box ("an event type exists") and UI-9 needs it
// because the integration guide is rendered from these live contracts - the
// schemas are the source, and neither portal keeps a copy.
func (s *Server) handleMemberListEventTypes(w http.ResponseWriter, r *http.Request) {
	sc, err := s.memberScope(r)
	if err != nil {
		s.scopeError(w, r, err)
		return
	}
	s.serveListEventTypes(w, r, sc)
}

// handleMemberCreateEventType is POST /v1/producer/event-types?env=...: UI-1's
// second step. It is the key route's create with a session credential.
func (s *Server) handleMemberCreateEventType(w http.ResponseWriter, r *http.Request) {
	sc, err := s.memberScope(r)
	if err != nil {
		s.scopeError(w, r, err)
		return
	}
	s.serveCreateEventType(w, r, sc)
}

// --- publish, including UI-4's synthetic test event -----------------------

// handleMemberPublish is POST /v1/producer/events?env=...: publish from the
// portal's session. UI-1's last two steps and UI-4's "send test event" both come
// through here.
//
// The environment is named by the request (`?env=test|live`), exactly as on every
// other member route (memberScope, auth.go: the producer from the session claim,
// the environment from the query). A member session carries no environment claim
// of its own - that binding is FR-110's consumer one - so the portal chooses the
// plane per call through its remembered setting and never through a body field.
func (s *Server) handleMemberPublish(w http.ResponseWriter, r *http.Request) {
	sc, err := s.memberScope(r)
	if err != nil {
		s.scopeError(w, r, err)
		return
	}
	s.servePublish(w, r, sc)
}

// --- UI-8: the disk and retention pressure panel (SB-6) -------------------

// usagePressureJSON is the member half of the usage payload: the pressure and the
// windows, without the partition catalogue. The catalogue is the operator's view
// (it lists this instance's tables by day, with sizes), and SB-6's dashboard half
// only needs to answer "is the platform about to drop my history, and how much
// disk is left". One route answering two audiences would either give a tenant the
// operator's table listing or give the operator a payload that is not the one
// they alert on, so GET /v1/meta/usage stays admin-only and this is the producer's
// view of the same reading - one gauge, two shapes.
type usagePressureJSON struct {
	Disk               retention.Usage `json:"disk"`
	MaxDiskPct         int             `json:"max_disk_pct"`
	OverBudget         bool            `json:"over_budget"`
	RetentionDays      map[string]int  `json:"retention_days"`
	DroppedPartitions  int64           `json:"dropped_partitions_total"`
	DedupeArchiveBytes int64           `json:"dedupe_archive_bytes"`
	CheckedAt          time.Time       `json:"checked_at"`
	// ReadError carries SB-6's stale-reading signal through to the portal: the
	// disk figure is the last good one when it is set, and a dashboard that hides
	// that would report a calm number that stopped updating.
	ReadError string `json:"read_error,omitempty"`
	// Note says why there is no `partitions` key, so a client that compares the
	// two usage payloads reads the omission as a decision rather than a bug.
	Note string `json:"note,omitempty"`
}

// handleProducerUsage is GET /v1/producer/usage (UI-8): the disk pressure and
// retention windows the producer portal's pressure panel shows.
func (s *Server) handleProducerUsage(w http.ResponseWriter, r *http.Request) {
	if _, err := s.memberScope(r); err != nil {
		s.scopeError(w, r, err)
		return
	}
	if s.deps.Retention == nil {
		// Same as the admin route: "we cannot tell you how full the disk is" must
		// not be dressed up as a calm zero (SB-6).
		writeError(w, http.StatusServiceUnavailable, "retention_unavailable",
			"retention telemetry is not configured")
		return
	}
	st, err := s.deps.Retention.Status(r.Context())
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "disk_usage_unavailable", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, usagePressureJSON{
		Disk:               st.Disk,
		MaxDiskPct:         st.MaxDiskPct,
		OverBudget:         st.OverBudget,
		RetentionDays:      st.RetentionDays,
		DroppedPartitions:  st.DroppedPartitions,
		DedupeArchiveBytes: st.DedupeArchiveBytes,
		CheckedAt:          st.CheckedAt,
		ReadError:          st.ReadError,
		Note: "the partition catalogue is operator telemetry and stays on " +
			"GET /v1/meta/usage (SB-6, admin); this is the same reading, producer-visible",
	})
}

// --- UI-6: producer-side secret rotation ---------------------------------

// endpointRotatedWithWindowJSON is endpointRotatedJSON plus the overlap window,
// which is the half UI-6 has to explain: a rotation that showed only the new
// secret would invite a producer to believe the old one stopped working the
// moment they clicked. FR-1004 keeps the outgoing secret verifiable for
// SecretOverlapTTL (S10's semantics, PLAN.md:30 - this route does not invent a
// window, it reads the one the row was given).
type endpointRotatedWithWindowJSON struct {
	SigningSecret     string `json:"signing_secret"`
	SigningSecretHint string `json:"signing_secret_hint"`
	// PreviousSecretExpiresAt is when the outgoing secret stops verifying. Null
	// when there was no outgoing secret (the first rotation of an endpoint whose
	// secret is still the original), because "no window" and "a window that
	// closed" are different answers.
	PreviousSecretExpiresAt *string `json:"previous_secret_expires_at"`
	OverlapSeconds          int     `json:"overlap_seconds"`
}

// handleProducerRotateEndpointSecret is POST
// /v1/producer/consumers/{consumerID}/endpoints/{endpointID}/rotate: the
// producer-side rotation UI-6 needs. S9 shipped only the consumer's own rotation
// (POST /v1/consumer/endpoints/{endpointID}/rotate), so a producer who suspected a
// leaked secret could not rotate it from the producer portal at all - the
// consumer had to. Same store call, scoped by the member's producer, and the
// endpoint must belong to the named consumer (a foreign consumer id is a 404,
// AC-39's rule: never 403).
func (s *Server) handleProducerRotateEndpointSecret(w http.ResponseWriter, r *http.Request) {
	sc, err := s.memberScope(r)
	if err != nil {
		s.scopeError(w, r, err)
		return
	}
	sec, err := s.deps.Data.RotateConsumerEndpointSecret(r.Context(), sc,
		r.PathValue("consumerID"), r.PathValue("endpointID"))
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	out := endpointRotatedWithWindowJSON{
		SigningSecret:     sec.Secret,
		SigningSecretHint: sec.Hint,
		OverlapSeconds:    int(store.SecretOverlapTTL / time.Second),
	}
	if !sec.OverlapExpiresAt.IsZero() {
		v := ts(sec.OverlapExpiresAt)
		out.PreviousSecretExpiresAt = &v
	}
	writeJSON(w, http.StatusCreated, out)
}

// --- UI-11: one consumer's view ------------------------------------------

// consumerEventJSON is one event addressed to a consumer that produced no
// delivery to it (store.ConsumerEvent).
type consumerEventJSON struct {
	ID           string `json:"id"`
	EventType    string `json:"event_type"`
	RoutingState string `json:"routing_state"`
	// NonDeliveryReason is FR-508's reason (`unroutable:consumer_not_registered`)
	// or §7.5a's (`not_delivered:no_matching_subscription`). The panel shows it
	// verbatim: the two have different fixes, and a single "unroutable" label
	// would send the producer looking for the wrong one.
	NonDeliveryReason string `json:"non_delivery_reason"`
	OccurredAt        string `json:"occurred_at"`
	CreatedAt         string `json:"created_at"`
	IsTest            bool   `json:"is_test"`
}

// handleConsumerEvents is GET /v1/producer/consumers/{consumerID}/events?env=...
// (UI-11): the consumer's unroutable and undelivered events, newest first. It is
// a producer-scoped read of rows S3 already wrote; the consumer's own delivery log
// (S9's GET /v1/consumer/deliveries) cannot show them, because an event that never
// became a delivery has no delivery row to list.
func (s *Server) handleConsumerEvents(w http.ResponseWriter, r *http.Request) {
	sc, err := s.memberScope(r)
	if err != nil {
		s.scopeError(w, r, err)
		return
	}
	events, err := s.deps.Data.ConsumerEvents(r.Context(), sc, r.PathValue("consumerID"), 100)
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	out := make([]consumerEventJSON, 0, len(events))
	for _, e := range events {
		out = append(out, consumerEventJSON{
			ID: e.EventID, EventType: e.EventType, RoutingState: e.RoutingState,
			NonDeliveryReason: e.Reason, OccurredAt: ts(e.OccurredAt), CreatedAt: ts(e.CreatedAt),
			IsTest: e.IsTest,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"events": out})
}

// --- UI-2: endpoint health in the endpoint list ---------------------------

// endpointHealthJSON is UI-2's health for one endpoint: how many deliveries have
// failed in a row, and when the last one was. It is a read-time derivation from
// the delivery rows (store.EndpointHealth), not a stored counter, so it cannot
// disagree with the delivery log the same page links to.
type endpointHealthJSON struct {
	FailureStreak  int     `json:"failure_streak"`
	LastStatus     string  `json:"last_status"`
	LastDeliveryAt *string `json:"last_delivery_at"`
}

// attachHealth decorates a page of endpoints with UI-2's health, in one extra
// query for the whole page. Health is attached by the list route only: it is the
// route UI-2 and UI-11 read, and a per-endpoint read would pay for a figure about
// a history the caller is not looking at.
func (s *Server) attachHealth(r *http.Request, sc store.Scope, out []endpointJSON) error {
	ids := make([]string, 0, len(out))
	for _, ep := range out {
		ids = append(ids, ep.ID)
	}
	health, err := s.deps.Data.EndpointHealth(r.Context(), sc, ids)
	if err != nil {
		return err
	}
	for i := range out {
		h, ok := health[out[i].ID]
		if !ok {
			// No deliveries yet: zero streak and an empty status, which the
			// portal renders as "no attempts". Not an error, and not a health
			// value that could be mistaken for a successful delivery.
			h = store.EndpointHealth{EndpointID: out[i].ID}
		}
		j := endpointHealthJSON{FailureStreak: h.FailureStreak, LastStatus: h.LastStatus}
		if h.LastDeliveryAt != nil {
			v := ts(*h.LastDeliveryAt)
			j.LastDeliveryAt = &v
		}
		out[i].Health = &j
	}
	return nil
}
