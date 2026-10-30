package producer

import (
	"encoding/json"
	"fmt"
	"net/url"
	"sort"
	"strings"
)

// The views this portal renders, and the core API shapes they are built from.
// Every shape below is a *read* of a documented response: the portal parses what
// the API says and re-renders it. Where a page needs two resources it makes two
// calls and joins them here, because a join in the portal is a UI convenience,
// not a rule the API has to learn (§14).

// --- core response shapes -------------------------------------------------

// eventTypeJSON is GET /v1/producer/event-types (a bare array). The schema is the
// live contract UI-9's guide renders, and it is carried as raw JSON so the portal
// can pretty-print the producer's own document rather than a normalised copy.
type eventTypeJSON struct {
	Name           string          `json:"name"`
	RoutingMode    string          `json:"routing_mode"`
	EventClass     string          `json:"event_class"`
	PayloadSchema  json.RawMessage `json:"payload_schema"`
	PayloadVersion int             `json:"payload_version"`
	Enabled        bool            `json:"enabled"`
	CreatedAt      string          `json:"created_at"`
}

type consumerJSON struct {
	ID        string `json:"id"`
	Ref       string `json:"ref"`
	Name      string `json:"name"`
	Env       string `json:"env"`
	CreatedAt string `json:"created_at"`
}

type listConsumersResponse struct {
	Consumers []consumerJSON `json:"consumers"`
}

// endpointJSON is GET /v1/producer/consumers/{consumerID}/endpoints. Health is
// present on that list and absent elsewhere (S9a added it there), so it is a
// pointer: a missing health must read as "the API did not send one", not as a
// healthy endpoint with a zero streak.
type endpointJSON struct {
	ID                string              `json:"id"`
	ConsumerID        string              `json:"consumer_id"`
	Name              string              `json:"name"`
	URL               string              `json:"url"`
	NotificationEmail *string             `json:"notification_email"`
	Internal          bool                `json:"internal"`
	Status            string              `json:"status"`
	Verified          bool                `json:"verified"`
	CreatedAt         string              `json:"created_at"`
	Health            *endpointHealthJSON `json:"health"`
}

type endpointHealthJSON struct {
	FailureStreak  int     `json:"failure_streak"`
	LastStatus     string  `json:"last_status"`
	LastDeliveryAt *string `json:"last_delivery_at"`
}

type listEndpointsResponse struct {
	Endpoints []endpointJSON `json:"endpoints"`
}

type consumerUserJSON struct {
	UserID     string `json:"user_id"`
	Email      string `json:"email"`
	ConsumerID string `json:"consumer_id"`
	CreatedAt  string `json:"created_at"`
}

type listConsumerUsersResponse struct {
	Users []consumerUserJSON `json:"users"`
}

type inviteJSON struct {
	ID         string `json:"id"`
	Email      string `json:"email"`
	Role       string `json:"role"`
	ConsumerID string `json:"consumer_id"`
	ExpiresAt  string `json:"expires_at"`
	// Token has no consumer user value - there is no mailer in v1.0, so the
	// invitation token is returned to the inviter, who passes it on. The portal
	// shows it once, exactly as the API intends.
	Token *string `json:"token"`
}

type endpointCreatedJSON struct {
	endpointJSON
	SigningSecret     string `json:"signing_secret"`
	SigningSecretHint string `json:"signing_secret_hint"`
	VerificationToken string `json:"verification_token"`
}

type endpointRotatedJSON struct {
	SigningSecret           string  `json:"signing_secret"`
	SigningSecretHint       string  `json:"signing_secret_hint"`
	PreviousSecretExpiresAt *string `json:"previous_secret_expires_at"`
	OverlapSeconds          int     `json:"overlap_seconds"`
}

// endpointReissuedJSON is the 201 body of an ownership-challenge re-issue (FR-409):
// the new token and its safe-to-display prefix, named exactly as the creation
// response. Each call mints a fresh value, so the route is not idempotent.
type endpointReissuedJSON struct {
	VerificationToken string `json:"verification_token"`
	VerificationHint  string `json:"verification_hint"`
}

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
	HeldBackSince      *string `json:"held_back_since"`
	HeldBackReason     *string `json:"held_back_reason"`
	IsTest             bool    `json:"is_test"`
	AttemptsRows       []struct {
		Attempt        int     `json:"attempt"`
		OccurredAt     string  `json:"occurred_at"`
		DurationMs     int     `json:"duration_ms"`
		ResponseStatus *int    `json:"response_status"`
		ResponseBody   *string `json:"response_body"`
		ErrorClass     string  `json:"error_class"`
		Outcome        string  `json:"outcome"`
		NextRetryAt    *string `json:"next_retry_at"`
	} `json:"attempts_rows"`
	EffectiveAttempt *struct {
		Attempt    int    `json:"attempt"`
		OccurredAt string `json:"occurred_at"`
		DurationMs *int   `json:"duration_ms"`
		Status     string `json:"status"`
		Outcome    string `json:"outcome"`
	} `json:"effective_attempt"`
}

type listDeliveriesResponse struct {
	Deliveries []deliveryJSON `json:"deliveries"`
	NextCursor string         `json:"next_cursor"`
}

type eventViewResponse struct {
	Event struct {
		ID                string  `json:"id"`
		EventType         string  `json:"event_type"`
		RoutingMode       string  `json:"routing_mode"`
		ConsumerRef       *string `json:"consumer_ref"`
		PayloadVersion    int     `json:"payload_version"`
		OccurredAt        string  `json:"occurred_at"`
		CreatedAt         string  `json:"created_at"`
		RoutingState      *string `json:"routing_state"`
		NonDeliveryReason *string `json:"non_delivery_reason"`
		IsTest            bool    `json:"is_test"`
	} `json:"event"`
	Rollup     string         `json:"rollup"`
	Deliveries []deliveryJSON `json:"deliveries"`
}

type consumerEventJSON struct {
	ID                string `json:"id"`
	EventType         string `json:"event_type"`
	RoutingState      string `json:"routing_state"`
	NonDeliveryReason string `json:"non_delivery_reason"`
	OccurredAt        string `json:"occurred_at"`
	CreatedAt         string `json:"created_at"`
	IsTest            bool   `json:"is_test"`
}

type listConsumerEventsResponse struct {
	Events []consumerEventJSON `json:"events"`
}

type usageResponse struct {
	Disk struct {
		UsedBytes  int64   `json:"used_bytes"`
		TotalBytes int64   `json:"total_bytes"`
		UsedPct    float64 `json:"used_pct"`
		// Source is where the reading came from and, for a filesystem reading, which
		// directory was read ("statfs:/var/lib/pigeon"). There is no separate `path`
		// key in the API's payload (retention.Usage), so nothing here may invent one:
		// the panel rendered an always-empty row for a field the API never sends
		// until this was fixed.
		Source string `json:"source"`
	} `json:"disk"`
	MaxDiskPct         int            `json:"max_disk_pct"`
	OverBudget         bool           `json:"over_budget"`
	RetentionDays      map[string]int `json:"retention_days"`
	DroppedPartitions  int64          `json:"dropped_partitions_total"`
	DedupeArchiveBytes int64          `json:"dedupe_archive_bytes"`
	CheckedAt          string         `json:"checked_at"`
	ReadError          string         `json:"read_error"`
	Note               string         `json:"note"`
}

type publishResponseJSON struct {
	ID        string `json:"id"`
	EventType string `json:"event_type"`
	IsTest    bool   `json:"is_test"`
}

type replayResponse struct {
	Matched          int     `json:"matched"`
	Created          int     `json:"created"`
	DeliveryIDs      []int64 `json:"delivery_ids"`
	Repeated         bool    `json:"repeated"`
	ReleasePerSecond int     `json:"release_per_second"`
	DryRun           bool    `json:"dry_run"`
}

// sessionResponseJSON is the core's POST /v1/session reply.
type sessionResponseJSON struct {
	Token     string `json:"token"`
	ExpiresAt string `json:"expires_at"`
	User      struct {
		Email              string `json:"email"`
		MustChangePassword bool   `json:"must_change_password"`
	} `json:"user"`
	Claims struct {
		ProducerID string `json:"producer_id"`
		Role       string `json:"role"`
	} `json:"claims"`
}

// --- view models ----------------------------------------------------------

// stepView is one line of UI-1's checklist. Done is derived from what the API
// answers, never from a portal-side flag: a checklist that remembers "you clicked
// this" is a checklist that lies the moment anything is deleted.
type stepView struct {
	N      int
	Title  string
	Detail string
	Done   bool
}

type eventTypeRow struct {
	Name           string
	RoutingMode    string
	EventClass     string
	PayloadVersion int
	Enabled        bool
	Schema         string // pretty-printed
}

type endpointRow struct {
	ID                string
	ConsumerID        string
	ConsumerName      string
	ConsumerRef       string
	Name              string
	URL               string
	Status            string
	Verified          bool
	NotificationEmail string
	FailureStreak     int
		LastStatus        string
		LastDeliveryAt    string
	// RotatedSecret and RotatedOverlap carry UI-6's one-time response: the new
	// secret is shown where the rotation happened, with the window the API
	// reported, so the page can explain the overlap with the API's own number.
	RotatedSecret  string
	RotatedHint    string
	RotatedOverlap string
	RotatedUntil   string
	// VerificationToken is UI-1's step-4 value, shown once when an endpoint is
	// created (and only then: the API never returns it again).
	VerificationToken string
	// ReissuedToken and ReissuedHint are FR-409's one-time re-issue response:
	// each call to the ownership-challenge re-issue route mints a fresh token and
	// hint and answers 201; the page that follows shows the token exactly once,
	// with a copy warning and a Verify now button. They are nil/empty until the
	// re-issue handler sets them from the API's response.
	ReissuedToken string
	ReissuedHint  string
	}

type consumerRow struct {
	ID          string
	Ref         string
	Name        string
	Env         string
	CreatedAt   string
	EndpointCnt int
	Users       []consumerUserJSON
	Unroutable  []consumerEventJSON
}

type deliveryRow struct {
	ID                 int64
	EventID            string
	EventType          string
	EndpointID         string
	ConsumerID         string
	Status             string
	Attempts           int
	CreatedAt          string
	DurationMs         string
	NonDeliveryReason  string
	HeldBackReason     string
	IsTest             bool
	ReplayOfDeliveryID int64
	Trail              []trailRow
	RollupNote         string
}

// trailRow is one entry of an attempt trail: a row, or the synthesized entry that
// stands in for the attempt a delivery had without one (FR-1110). The portal does
// not decide whether a trail is empty - it renders what the API sent, which is the
// property FR-1110 asks for.
type trailRow struct {
	Attempt        int
	OccurredAt     string
	DurationMs     string
	ResponseStatus string
	ResponseBody   string
	ErrorClass     string
	Outcome        string
	NextRetryAt    string
	Synthesized    bool
}

// deliveryFilterView is FR-1101's filter set as a browser expresses it, plus the
// paging parameter. The producer version adds consumer_id, which the consumer
// portal deliberately does not have (its claim decides the consumer).
type deliveryFilterView struct {
	ConsumerID string
	EndpointID string
	EventType  string
	Status     string
	From       string
	To         string
	Limit      string
	Cursor     string
}

// deliveryFilterNames is exactly the set of read parameters the core API's
// delivery list defines (FR-1101 plus its paging). The portal forwards these and
// nothing else: a name outside the set would be a filter the API never agreed to.
var deliveryFilterNames = []string{"consumer_id", "endpoint_id", "event_type", "status", "from", "to", "limit", "cursor"}

// deliveryStatuses is FR-1101's `status` filter as the log offers it: the
// delivery status column's own check constraint (S5's 0013), so the control lists
// them instead of accepting free text that could only ever select nothing.
var deliveryStatuses = []string{"pending", "delivered", "failed_terminal", "skipped"}

func readDeliveryFilters(get func(string) string) deliveryFilterView {
	return deliveryFilterView{
		ConsumerID: get("consumer_id"),
		EndpointID: get("endpoint_id"),
		EventType:  get("event_type"),
		Status:     get("status"),
		From:       get("from"),
		To:         get("to"),
		Limit:      get("limit"),
		Cursor:     get("cursor"),
	}
}

func (f deliveryFilterView) values() url.Values {
	q := url.Values{}
	for _, name := range deliveryFilterNames {
		var v string
		switch name {
		case "consumer_id":
			v = f.ConsumerID
		case "endpoint_id":
			v = f.EndpointID
		case "event_type":
			v = f.EventType
		case "status":
			v = f.Status
		case "from":
			v = f.From
		case "to":
			v = f.To
		case "limit":
			v = f.Limit
		case "cursor":
			v = f.Cursor
		}
		if v != "" {
			q.Set(name, v)
		}
	}
	return q
}

// coreQuery is the filter set as the core API's query string, with the
// environment prepended (every member route takes it).
func (f deliveryFilterView) coreQuery(env string, withCursor bool) string {
	q := f.values()
	if !withCursor {
		q.Del("cursor")
	}
	q.Set("env", env)
	return "?" + q.Encode()
}

// pageQuery is the same filters as this portal's own URL, so "next page" and the
// filter form keep the selection.
func (f deliveryFilterView) pageQuery(withCursor bool) string {
	q := f.values()
	if !withCursor {
		q.Del("cursor")
	}
	if q.Encode() == "" {
		return ""
	}
	return "?" + q.Encode()
}

// selectionWords describes the filter set in the caller's own words, so UI-5's
// confirmation can say what it is about to replay rather than "these".
func (f deliveryFilterView) selectionWords() string {
	var parts []string
	if f.ConsumerID != "" {
		parts = append(parts, "consumer "+f.ConsumerID)
	}
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
		parts = append(parts, fmt.Sprintf("created %s..%s", orAny(f.From), orAny(f.To)))
	}
	if len(parts) == 0 {
		return "every delivery in this environment"
	}
	return strings.Join(parts, ", ")
}

func orAny(s string) string {
	if s == "" {
		return "*"
	}
	return s
}

// hiddenField is one <input type=hidden> a form carries forward.
type hiddenField struct{ Name, Value string }

func hiddenFrom(f deliveryFilterView, extra map[string]string) []hiddenField {
	out := make([]hiddenField, 0, len(f.values())+len(extra))
	names := make([]string, 0, len(f.values()))
	for name := range f.values() {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		out = append(out, hiddenField{Name: name, Value: f.values().Get(name)})
	}
	keys := make([]string, 0, len(extra))
	for k := range extra {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		out = append(out, hiddenField{Name: k, Value: extra[k]})
	}
	return out
}

// replayView is UI-5's confirmation panel. Matched and Rate are the API's own
// numbers from the dry run, quoted rather than computed here; Key is the portal's
// request key for this confirmation (the user never types one).
type replayView struct {
	Selection string
	Key       string
	Confirmed bool
	Matched   int
	Rate      int
	Message   string
	Error     string
	Hidden    []hiddenField
}

// --- page payloads --------------------------------------------------------

type problemView struct {
	Title   string
	Message string
	Detail  string
}

type loginView struct {
	Next       string
	Error      string
	ForgotOK   bool
	OIDCEnable bool
	Env        string
}

type forgotView struct {
	Sent  bool
	Error string
}

type resetView struct {
	Token string
	Done  bool
	Error string
}

type onboardingView struct {
	Env             string
	Steps           []stepView
	EventTypes      []eventTypeRow
	Consumers       []consumerRow
	Endpoints       []endpointRow
	EventTypesCount int
	EndpointsCount  int
	// TestEventBody is the JSON the test-event form starts from: the event type's
	// own example when it has one, else an empty object. A producer should not have
	// to guess their own contract from scratch.
	TestEventBody string
}

type consumersView struct {
	Env       string
	Consumers []consumerRow
	// Endpoints is every consumer's endpoints in one table: UI-2 asks for "an
	// endpoint list with health across all consumers", which is this page's join
	// of the API's per-consumer lists. It is a UI convenience, not a rule the API
	// has to learn (each row still comes from a documented per-consumer call).
	Endpoints []endpointRow
	Created   struct {
		Ref  string
		Name string
	}
	Error string
}

type consumerView struct {
	Env       string
	Consumer  consumerRow
	Endpoints []endpointRow
	Users     []consumerUserJSON
	Events    []consumerEventJSON
	// NewInviteToken is the invitation token the API returned once, shown so the
	// inviter can pass it on (there is no mailer in v1.0).
	NewInviteToken string
	NewInviteEmail string
	Error          string
	IsOwner        bool
}

type endpointView struct {
	Env      string
	Endpoint endpointRow
	Consumer consumerRow
	Error    string
}

type deliveriesView struct {
	Env        string
	Filter     deliveryFilterView
	Deliveries []deliveryRow
	NextCursor string
	Consumers  []consumerRow
	Endpoints  []endpointRow
	Statuses   []string
	Replay     replayView
	Error      string
}

type deliveryView struct {
	Env      string
	Delivery deliveryRow
	Error    string
}

type eventView struct {
	Env   string
	Event eventRowView
	Error string
}

type eventRowView struct {
	ID                string
	EventType         string
	RoutingMode       string
	ConsumerRef       string
	OccurredAt        string
	CreatedAt         string
	RoutingState      string
	NonDeliveryReason string
	IsTest            bool
	Rollup            string
	Deliveries        []deliveryRow
}

type testEventView struct {
	Env        string
	EventTypes []eventTypeRow
	Consumers  []consumerRow
	// Form state, so a refused publish keeps what the user typed.
	EventType   string
	ConsumerRef string
	Body        string
	Error       string
}

type snippetView struct {
	EndpointID string
	Whsec      string
	Snippet    []snippetRow
	Note       string
	Error      string
}

type snippetRow struct {
	Language string
	Label    string
	Source   string
	Note     string
}

type guideView struct {
	Env        string
	EventTypes []eventTypeRow
	BaseURL    string
	Error      string
}

type guideTypeView struct {
	Name           string
	RoutingMode    string
	PayloadVersion int
	Schema         string
	Examples       []string
}

type usageView struct {
	Env       string
	Usage     usageResponse
	Available bool
	Error     string
}

type accountView struct {
	Email              string
	Role               string
	ProducerID         string
	MustChangePassword bool
	Changed            bool
	ResetSent          bool
	Error              string
}
