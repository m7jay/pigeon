package httpapi

import "net/http"

// routeKind is the credential a route requires. It is the whole authorization
// policy of the service: each handler says what it needs, and authorize() is the
// only place that decides whether the caller has it.
//
// The kinds are ordered from least to most specific so that "which routes are
// reachable without a producer scope" is answerable by reading this list.
type routeKind int

const (
	// kindAnonymous needs no credential. Only /health, the login form and the
	// invitation-acceptance form are anonymous: accepting an invitation *is* the
	// credential, and login is how one is obtained.
	kindAnonymous routeKind = iota
	// kindSession needs any valid session, and is refused while FR-101's forced
	// password change is pending.
	kindSession
	// kindPasswordChange is the one route a session with a pending forced change
	// may still call.
	kindPasswordChange
	// kindAdmin needs an instance-admin session. Anything less is 404, not 403:
	// a producer owner must not learn that /v1/admin exists.
	kindAdmin
	// kindProducerKey needs a producer API key (FR-201) and is rate limited
	// (FR-207). This is the ingest credential.
	kindProducerKey
	// kindMember needs a session whose claims name a producer and a role
	// (FR-104): any owner or developer.
	kindMember
	// kindOwner needs the same, with role owner.
	kindOwner
	// kindConsumer needs a session carrying FR-110's consumer_id claim.
	kindConsumer
)

func (k routeKind) String() string {
	switch k {
	case kindAnonymous:
		return "anonymous"
	case kindSession:
		return "session"
	case kindPasswordChange:
		return "password_change"
	case kindAdmin:
		return "admin"
	case kindProducerKey:
		return "producer_key"
	case kindMember:
		return "member"
	case kindOwner:
		return "owner"
	case kindConsumer:
		return "consumer"
	}
	return "unknown"
}

// route is one entry of the service's surface.
type route struct {
	Method  string
	Path    string
	Kind    routeKind
	Handler http.HandlerFunc
}

// pattern is what net/http's mux is given: "POST /v1/admin/producers".
func (r route) pattern() string { return r.Method + " " + r.Path }

// routes is the single source of truth for the HTTP surface. The mux is built
// from it (Handler), the AC-20 cross-producer suite is generated from it, and
// the route-inventory test asserts that no route escapes the two.
//
// Adding a route means adding it here and adding it to the suite; a route that
// appears here without a suite case fails TestCrossProducerSuiteCoversEveryRoute.
func (s *Server) routes() []route {
	rs := []route{
		{http.MethodGet, "/health", kindAnonymous, s.handleLiveness},
		{http.MethodGet, "/health/ready", kindAnonymous, s.handleReadiness},

		// Identity (FR-101, FR-104, FR-110, FR-111).
		{http.MethodPost, "/v1/session", kindAnonymous, s.handleLogin},
		{http.MethodDelete, "/v1/session", kindSession, s.handleLogout},
		{http.MethodGet, "/v1/session", kindSession, s.handleSession},
		{http.MethodPost, "/v1/session/password", kindPasswordChange, s.handleChangePassword},
		{http.MethodPost, "/v1/invites/accept", kindAnonymous, s.handleAcceptInvite},

		// The ingest credential (FR-201..FR-207).
		{http.MethodGet, "/v1/whoami", kindProducerKey, s.handleWhoami},

		// Event types and ingest (§11). A producer registers the contracts it may
		// publish (FR-301..FR-306): POST/GET create and list them, PATCH bumps
		// version or disables (FR-304, FR-305). Then it publishes events against
		// those contracts; each accept writes the event and its outbox row in one
		// transaction (FR-604).
		{http.MethodPost, "/v1/event-types", kindProducerKey, s.handleCreateEventType},
		{http.MethodGet, "/v1/event-types", kindProducerKey, s.handleListEventTypes},
		{http.MethodPatch, "/v1/event-types/{name}", kindProducerKey, s.handlePatchEventType},
		{http.MethodPost, "/v1/events", kindProducerKey, s.handlePublish},

		// Instance administration (FR-102, FR-107, MT-5).
		{http.MethodPost, "/v1/admin/producers", kindAdmin, s.handleCreateProducer},
		{http.MethodGet, "/v1/admin/producers", kindAdmin, s.handleListProducers},
		{http.MethodGet, "/v1/admin/producers/{producerID}", kindAdmin, s.handleGetProducer},
		{http.MethodPost, "/v1/admin/producers/{producerID}/suspend", kindAdmin, s.handleSuspendProducer},
		{http.MethodPost, "/v1/admin/producers/{producerID}/resume", kindAdmin, s.handleResumeProducer},
		{http.MethodGet, "/v1/admin/producers/{producerID}/audit", kindAdmin, s.handleProducerAudit},
		// FR-508: the fan-out non-delivery counters, one series per reason. Operator
		// telemetry over the whole platform, so it is not tenant-scoped (admin only).
		{http.MethodGet, "/metrics", kindAdmin, s.handleMetrics},
		// SB-6: the storage budget as an operator sees it — volume usage against
		// max_disk_pct, the partition catalogue with sizes and row estimates, and
		// the retention windows. Infrastructure telemetry, so admin only.
		{http.MethodGet, "/v1/meta/usage", kindAdmin, s.handleUsage},
		// FR-1007: the addresses a receiver should allowlist. It describes the
		// deployment, not a tenant, and the people who need it are whoever
		// configures an allowlist on either side, so it needs a session and no
		// more; see handleEgressIPs.
		{http.MethodGet, "/v1/meta/egress-ips", kindSession, s.handleEgressIPs},

		// Producer membership and invitations (FR-104, FR-105, FR-109).
		{http.MethodGet, "/v1/producer/members", kindMember, s.handleListMembers},
		{http.MethodPost, "/v1/producer/invites", kindOwner, s.handleInviteMember},
		{http.MethodGet, "/v1/producer/invites", kindMember, s.handleListInvites},
		{http.MethodDelete, "/v1/producer/invites/{inviteID}", kindOwner, s.handleRevokeInvite},

		// Consumers and their users (FR-109, FR-110).
		{http.MethodGet, "/v1/producer/consumers", kindMember, s.handleListConsumers},
		{http.MethodPost, "/v1/producer/consumers", kindMember, s.handleCreateConsumer},
		{http.MethodGet, "/v1/producer/consumers/{consumerID}", kindMember, s.handleGetConsumer},
		// FR-506: the ref is immutable everywhere but this one re-key operation.
		{http.MethodPost, "/v1/producer/consumers/{consumerID}/rekey", kindMember, s.handleRekeyConsumer},
		// FR-401..FR-408: a consumer's endpoints. Registered under the consumer the
		// URL belongs to; SSRF-checked, challenge-gated before live delivery.
		{http.MethodGet, "/v1/producer/consumers/{consumerID}/endpoints", kindMember, s.handleListEndpoints},
		{http.MethodPost, "/v1/producer/consumers/{consumerID}/endpoints", kindMember, s.handleCreateEndpoint},
		{http.MethodGet, "/v1/producer/consumers/{consumerID}/endpoints/{endpointID}", kindMember, s.handleGetEndpoint},
		{http.MethodPatch, "/v1/producer/consumers/{consumerID}/endpoints/{endpointID}", kindMember, s.handlePatchEndpoint},
		{http.MethodDelete, "/v1/producer/consumers/{consumerID}/endpoints/{endpointID}", kindMember, s.handleDeleteEndpoint},
		{http.MethodPost, "/v1/producer/consumers/{consumerID}/endpoints/{endpointID}/verify", kindMember, s.handleVerifyEndpoint},
		// FR-501..FR-512: the (endpoint, event_type) subscriptions.
		{http.MethodGet, "/v1/producer/endpoints/{endpointID}/subscriptions", kindMember, s.handleListSubscriptions},
		{http.MethodPost, "/v1/producer/endpoints/{endpointID}/subscriptions", kindMember, s.handleCreateSubscription},
		{http.MethodDelete, "/v1/producer/endpoints/{endpointID}/subscriptions/{eventType}", kindMember, s.handleDeleteSubscription},
		{http.MethodPost, "/v1/producer/consumers/{consumerID}/invites", kindOwner, s.handleInviteConsumerUser},
		{http.MethodGet, "/v1/producer/consumers/{consumerID}/users", kindMember, s.handleListConsumerUsers},
		{http.MethodDelete, "/v1/producer/consumers/{consumerID}/users/{userID}", kindOwner, s.handleRevokeConsumerUser},

		// API keys (FR-201..FR-207).
		{http.MethodPost, "/v1/producer/keys", kindOwner, s.handleIssueKey},
		{http.MethodGet, "/v1/producer/keys", kindMember, s.handleListKeys},
		{http.MethodGet, "/v1/producer/keys/{keyID}", kindMember, s.handleGetKey},
		{http.MethodDelete, "/v1/producer/keys/{keyID}", kindOwner, s.handleRevokeKey},

		// The consumer portal's own scope (FR-110). GET /v1/consumer/me is the
		// session's self-description; the delivery routes below are FR-1105's
		// "a consumer user sees only its own consumer's deliveries", which is
		// also the API UI-15 and UI-16 are thin clients of (§14).
		{http.MethodGet, "/v1/consumer/me", kindConsumer, s.handleConsumerMe},

		// M2b, UI-13/UI-14: the consumer portal's self-service surface. Every route
		// is scoped by the consumer id on the session claim; the consumer id is the
		// session's, never a request field, and the store filters by it so a guessed
		// foreign id is a 404 (AC-39). The portal is a thin client: these are also
		// directly callable with the same scoped session, with the same answer.
		//
		// UI-14's gate is a read and a write over one set: event-types lists what
		// the producer has left publicly subscribable, and the subscription POST
		// accepts exactly that set.
		{http.MethodGet, "/v1/consumer/event-types", kindConsumer, s.handleConsumerListEventTypes},
		{http.MethodGet, "/v1/consumer/endpoints", kindConsumer, s.handleConsumerListEndpoints},
		{http.MethodPost, "/v1/consumer/endpoints", kindConsumer, s.handleConsumerCreateEndpoint},
		{http.MethodGet, "/v1/consumer/endpoints/{endpointID}", kindConsumer, s.handleConsumerGetEndpoint},
		{http.MethodPatch, "/v1/consumer/endpoints/{endpointID}", kindConsumer, s.handleConsumerPatchEndpoint},
		{http.MethodPost, "/v1/consumer/endpoints/{endpointID}/rotate", kindConsumer, s.handleConsumerRotateEndpointSecret},
		{http.MethodDelete, "/v1/consumer/endpoints/{endpointID}", kindConsumer, s.handleConsumerDeleteEndpoint},
		{http.MethodPost, "/v1/consumer/endpoints/{endpointID}/verify", kindConsumer, s.handleConsumerVerifyEndpoint},
		{http.MethodGet, "/v1/consumer/endpoints/{endpointID}/subscriptions", kindConsumer, s.handleConsumerListSubscriptions},
		{http.MethodPost, "/v1/consumer/endpoints/{endpointID}/subscriptions", kindConsumer, s.handleConsumerCreateSubscription},
		{http.MethodDelete, "/v1/consumer/endpoints/{endpointID}/subscriptions/{eventType}", kindConsumer, s.handleConsumerDeleteSubscription},

		// Inspection and replay (§7.11, FR-1101..FR-1111). The producer surface
		// is the management API UI-3 and UI-5 use; the consumer surface is
		// FR-1105's scoped read and UI-16's scoped replay, with the consumer
		// taken from the session claim rather than from the request.
		{http.MethodGet, "/v1/producer/deliveries", kindMember, s.handleListDeliveries},
		{http.MethodGet, "/v1/producer/deliveries/dedupe", kindMember, s.handleDedupeAnswer},
		{http.MethodGet, "/v1/producer/deliveries/{deliveryID}", kindMember, s.handleGetDelivery},
		{http.MethodPost, "/v1/producer/deliveries/replay", kindMember, s.handleReplayDeliveries},
		{http.MethodGet, "/v1/producer/events/{eventID}", kindMember, s.handleEventView},
		{http.MethodGet, "/v1/consumer/deliveries", kindConsumer, s.handleConsumerListDeliveries},
		{http.MethodGet, "/v1/consumer/deliveries/{deliveryID}", kindConsumer, s.handleConsumerGetDelivery},
		{http.MethodPost, "/v1/consumer/deliveries/replay", kindConsumer, s.handleConsumerReplayDeliveries},
	}

	if s.deps.SelfRegistration {
		// FR-102: self-registration is off by default and enabled by config. When
		// it is off the route does not exist at all, which is a stronger answer
		// than a 403 that advertises the feature.
		rs = append(rs, route{http.MethodPost, "/v1/producers", kindAnonymous, s.handleSelfRegister})
	}
	return rs
}
