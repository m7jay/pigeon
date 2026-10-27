package consumer

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/m7jay/pigeon/internal/portal"
)

// --- view models -----------------------------------------------------------
// These are what the templates render. Every value is already formatted (times,
// badges, statuses) so the templates do no computation.

type meView struct {
	Name      string
	Ref       string
	Env       string
	CreatedAt string
}

type createdView struct {
	ID                string
	Secret            string
	VerificationToken string
}

type endpointRow struct {
	ID        string
	Name      string
	URL       string
	Status    string
	Verified  bool
	Paused    bool
	CreatedAt string
}

type endpointsView struct {
	Endpoints []endpointRow
	Created   *createdView
}

type subRow struct {
	EndpointID string
	EventType  string
	Status     string
}

// subsView is the subscription panel: the caller's subscriptions plus, when a
// subscribe attempt was refused, the API's reason (UI-14's gate answers 422 for a
// type the producer has not left publicly subscribable). It is a struct rather
// than a bare slice so the panel can show that refusal where the user acted.
type subsView struct {
	Subscriptions []subRow
	Error         string
}

type endpointDetail struct {
	ID                string
	Name              string
	URL               string
	Status            string
	Verified          bool
	Paused            bool
	NotificationEmail string
	CreatedAt         string
	// RotatedSecret is set only on the render that follows a rotation (UI-13):
	// the new signing secret exists in exactly one API response, so the page that
	// shows it is rendered from that response rather than redirected to.
	RotatedSecret string
	// Subs is the subscription panel (UI-14).
	Subs subsView
	// AvailableTypes is the set the subscribe control offers: exactly the event
	// types the producer has left open to public subscription, from
	// GET /v1/consumer/event-types. Offering a free-text field would offer a control
	// the API refuses.
	AvailableTypes []string
}

type snippetView struct {
	EndpointID string
	Whsec      string
	Snippet    string
}

// --- core response shapes --------------------------------------------------

type listEndpointsResponse struct {
	Endpoints []struct {
		ID        string `json:"id"`
		Name      string `json:"name"`
		URL       string `json:"url"`
		Status    string `json:"status"`
		Verified  bool   `json:"verified"`
		CreatedAt string `json:"created_at"`
	} `json:"endpoints"`
}

// listSubscribableTypesResponse is GET /v1/consumer/event-types (UI-14): the types
// the producer has left open to public subscription.
type listSubscribableTypesResponse struct {
	EventTypes []struct {
		Name string `json:"name"`
	} `json:"event_types"`
}

type listSubscriptionsResponse struct {
	Subscriptions []struct {
		EndpointID string `json:"endpoint_id"`
		EventType  string `json:"event_type"`
		Status     string `json:"status"`
	} `json:"subscriptions"`
}

// coreErrorMessage turns a core error body into a human message, falling back to
// the status code when the body is not a §11 error object.
func coreErrorMessage(resp portal.Response) string {
	var e struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	_ = json.Unmarshal(resp.Body, &e)
	if e.Error.Message != "" {
		return e.Error.Message
	}
	if e.Error.Code != "" {
		return e.Error.Code
	}
	return fmt.Sprintf("the API returned an unexpected error (%d)", resp.Status)
}

// okStatus reports whether the core accepted a write. The API answers 201 with the
// created resource, 200 for an edit, and 204 for a delete or an unsubscribe, so a
// write is accepted if it is any of those.
func okStatus(status int) bool {
	return status == http.StatusOK || status == http.StatusCreated || status == http.StatusNoContent
}

// wantsFragment reports whether the request came from htmx, the only client that
// can swap a fragment into the page it is on. Every handler that answers with a
// fragment asks first: a plain form post answered with a fragment navigates the
// browser to a document that has no layout and no htmx script, so the controls
// inside that document - the "Opt out" form on the subscriptions panel, for
// one - become inert and the action the user asked for never happens.
func wantsFragment(r *http.Request) bool { return r.Header.Get("HX-Request") == "true" }

// --- auth ------------------------------------------------------------------

// loginForm renders the login page; a logged-in visitor is sent straight through.
func (h *Handler) loginForm(w http.ResponseWriter, r *http.Request) {
	if h.tokenFromCookie(r) != "" {
		http.Redirect(w, r, "/endpoints", http.StatusSeeOther)
		return
	}
	h.renderPage(w, r, "login", "Log in", "", nil)
}

// loginSubmit authenticates against the core API, stores the scoped session
// token in the encrypted cookie, and sends the visitor to where they were headed.
func (h *Handler) loginSubmit(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	email, password := r.FormValue("email"), r.FormValue("password")
	if email == "" || password == "" {
		h.renderPage(w, r, "login", "Log in", "", nil)
		return
	}
	resp, err := h.coreReq(r, http.MethodPost, "/v1/session", map[string]string{
		"email": email, "password": password,
	})
	if err != nil {
		http.Error(w, "cannot reach the API", http.StatusBadGateway)
		return
	}
	if resp.Status != http.StatusCreated {
		h.renderPage(w, r, "login", "Log in", "", nil) // the page flashes the reason
		return
	}
	var sess struct {
		Token string `json:"token"`
	}
	_ = json.Unmarshal(resp.Body, &sess)
	if err := h.setTokenCookie(w, sess.Token); err != nil {
		http.Error(w, "cannot start session", http.StatusInternalServerError)
		return
	}
	next := r.FormValue("next")
	if !isLocalPath(next) {
		next = "/endpoints"
	}
	http.Redirect(w, r, next, http.StatusSeeOther)
}

// logout ends the local session (drops the cookie); the core session token is
// left to expire on its own TTL, which is the standard one-device logout.
func (h *Handler) logout(w http.ResponseWriter, r *http.Request) {
	h.clearTokenCookie(w)
	h.setFlash(w, "info", "You have been logged out.")
	http.Redirect(w, r, "/login", http.StatusSeeOther)
}

// inviteForm renders the invitation-redeem form.
func (h *Handler) inviteForm(w http.ResponseWriter, r *http.Request) {
	if h.tokenFromCookie(r) != "" {
		http.Redirect(w, r, "/endpoints", http.StatusSeeOther)
		return
	}
	h.renderPage(w, r, "invite", "Redeem an invitation", "", nil)
}

// inviteSubmit redeems an emailed invitation (FR-111: no self-registration) then
// logs the new account in. Accepting creates the account bound to the consumer;
// logging in then yields the scoped session the portal holds.
func (h *Handler) inviteSubmit(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	token, email, password := r.FormValue("token"), r.FormValue("email"), r.FormValue("password")
	if token == "" || email == "" || password == "" {
		h.renderPage(w, r, "invite", "Redeem an invitation", "", nil)
		return
	}
	// Step 1: accept the invitation (anonymous).
	resp, err := h.coreReq(r, http.MethodPost, "/v1/invites/accept",
		map[string]string{"token": token, "password": password})
	if err != nil {
		http.Error(w, "cannot reach the API", http.StatusBadGateway)
		return
	}
	switch resp.Status {
	case http.StatusOK:
		// Step 2: log in with the account we just created/bound.
		login, err := h.coreReq(r, http.MethodPost, "/v1/session",
			map[string]string{"email": email, "password": password})
		if err != nil {
			http.Error(w, "cannot reach the API", http.StatusBadGateway)
			return
		}
		if login.Status != http.StatusCreated {
			h.renderPage(w, r, "invite", "Redeem an invitation", "", nil)
			return
		}
		var sess struct {
			Token string `json:"token"`
		}
		_ = json.Unmarshal(login.Body, &sess)
		if err := h.setTokenCookie(w, sess.Token); err != nil {
			http.Error(w, "cannot start session", http.StatusInternalServerError)
			return
		}
		http.Redirect(w, r, "/endpoints", http.StatusSeeOther)
	case http.StatusNotFound:
		h.renderPage(w, r, "invite", "Redeem an invitation", "", nil) // flashes invalid_invite
	case http.StatusForbidden:
		// An existing account: acceptance needs that account's own session. Tell
		// them to log in first (SPEC gap 7: no password-reset mailer yet).
		h.renderPage(w, r, "invite", "Redeem an invitation", "", nil)
	default:
		h.renderPage(w, r, "invite", "Redeem an invitation", "", nil)
	}
}

// --- pages -----------------------------------------------------------------

// me shows the caller's own consumer (UI-12): the only consumer visible here.
func (h *Handler) me(w http.ResponseWriter, r *http.Request) {
	resp, err := h.coreReq(r, http.MethodGet, "/v1/consumer/me", nil)
	if notAuthed(resp, err, w, r, h) {
		return
	}
	var c meView
	_ = json.Unmarshal(resp.Body, &c) // consumerJSON: id, ref, name, env, created_at
	h.renderPage(w, r, "me", "Your consumer", "me", c)
}

// endpointsList lists the caller's endpoints and shows a registration form (UI-13).
func (h *Handler) endpointsList(w http.ResponseWriter, r *http.Request) {
	resp, err := h.coreReq(r, http.MethodGet, "/v1/consumer/endpoints", nil)
	if notAuthed(resp, err, w, r, h) {
		return
	}
	var view endpointsView
	if resp.Status == http.StatusOK {
		var le listEndpointsResponse
		_ = json.Unmarshal(resp.Body, &le)
		for _, e := range le.Endpoints {
			view.Endpoints = append(view.Endpoints, endpointRow{
				ID: e.ID, Name: e.Name, URL: e.URL, Status: e.Status,
				Verified: e.Verified, CreatedAt: e.CreatedAt,
			})
		}
	}
	h.renderPage(w, r, "endpoints", "Your endpoints", "endpoints", view)
}

// endpointCreate registers an endpoint under the caller's own consumer (UI-13).
// On success it re-renders the endpoints page with the one-time signing secret
// and verification challenge, which the core returns exactly once.
func (h *Handler) endpointCreate(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	name, url := strings.TrimSpace(r.FormValue("name")), strings.TrimSpace(r.FormValue("url"))
	if name == "" || url == "" {
		h.renderPage(w, r, "endpoints", "Your endpoints", "endpoints", endpointsView{})
		return
	}
	resp, err := h.coreReq(r, http.MethodPost, "/v1/consumer/endpoints",
		map[string]string{"name": name, "url": url})
	if err != nil {
		http.Error(w, "cannot reach the API", http.StatusBadGateway)
		return
	}
	switch resp.Status {
	case http.StatusCreated:
		var created struct {
			ID                string `json:"id"`
			SigningSecret     string `json:"signing_secret"`
			VerificationToken string `json:"verification_token"`
		}
		_ = json.Unmarshal(resp.Body, &created)
		view := endpointsView{Created: &createdView{
			ID: created.ID, Secret: created.SigningSecret, VerificationToken: created.VerificationToken,
		}}
		h.renderPageStatus(w, r, http.StatusOK, "endpoints", "Your endpoints", "endpoints", view)
	default:
		// 422 (SSRF-blocked URL), 409, etc. Re-render with the API's reason.
		h.renderPageStatus(w, r, http.StatusOK, "endpoints", "Your endpoints", "endpoints", endpointsView{})
	}
}

// endpointDetail shows one endpoint with verify/pause/delete and its subscriptions.
func (h *Handler) endpointDetail(w http.ResponseWriter, r *http.Request) {
	detail, ok := h.endpointDetailView(w, r, r.PathValue("id"))
	if !ok {
		return
	}
	h.renderPage(w, r, "endpoint", detail.Name, "endpoints", detail)
}

// endpointDetailView reads everything the detail page renders for one of the
// caller's own endpoints: the row, the subscriptions panel and the event types
// open to public subscription. ok is false when the read could not be answered,
// in which case the caller has already had a response written for it (a redirect
// to login or to the list). It exists so the rotate handler can render the same
// page with the new secret attached.
func (h *Handler) endpointDetailView(w http.ResponseWriter, r *http.Request, id string) (endpointDetail, bool) {
	resp, err := h.coreReq(r, http.MethodGet, "/v1/consumer/endpoints/"+id, nil)
	if notAuthed(resp, err, w, r, h) {
		return endpointDetail{}, false
	}
	if resp.Status == http.StatusNotFound {
		http.Redirect(w, r, "/endpoints", http.StatusSeeOther)
		return endpointDetail{}, false
	}
	var ep struct {
		ID, Name, URL, Status, CreatedAt string
		Verified                         bool
		NotificationEmail                *string `json:"notification_email"`
	}
	_ = json.Unmarshal(resp.Body, &ep)
	detail := endpointDetail{
		ID: ep.ID, Name: ep.Name, URL: ep.URL, Status: ep.Status,
		Verified: ep.Verified, Paused: ep.Status == "paused", CreatedAt: ep.CreatedAt,
	}
	if ep.NotificationEmail != nil {
		detail.NotificationEmail = *ep.NotificationEmail
	}
	// The endpoint's subscriptions for the panel, and the types the subscribe
	// control may offer (UI-14). Both are plain API reads: the portal decides
	// nothing here that the API does not already answer.
	detail.Subs = subsView{Subscriptions: h.endpointSubscriptions(r, id)}
	detail.AvailableTypes = h.subscribableTypes(r)
	return detail, true
}

// endpointRotate mints a fresh signing secret for one of the caller's own
// endpoints (UI-13: "rotate their own secret"). The API answers with the new
// secret exactly once, so this handler renders the detail page straight from that
// response instead of redirecting - and it deliberately does not park the secret
// in the flash cookie, which is unsigned and would then carry a credential.
func (h *Handler) endpointRotate(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	resp, err := h.coreReq(r, http.MethodPost, "/v1/consumer/endpoints/"+id+"/rotate", nil)
	if notAuthed(resp, err, w, r, h) {
		return
	}
	if resp.Status != http.StatusCreated {
		h.setFlash(w, "error", coreErrorMessage(resp))
		http.Redirect(w, r, "/endpoints/"+id, http.StatusSeeOther)
		return
	}
	var rotated struct {
		SigningSecret string `json:"signing_secret"`
	}
	_ = json.Unmarshal(resp.Body, &rotated)
	detail, ok := h.endpointDetailView(w, r, id)
	if !ok {
		return
	}
	detail.RotatedSecret = rotated.SigningSecret
	h.renderPage(w, r, "endpoint", detail.Name, "endpoints", detail)
}

// endpointSubscriptions reads one of the caller's endpoints' subscriptions for the
// panel. A network error or a refusal renders an empty panel rather than an error
// page: the panel is additive to the endpoint the page is about.
func (h *Handler) endpointSubscriptions(r *http.Request, id string) []subRow {
	subs := []subRow{}
	resp, err := h.coreReq(r, http.MethodGet, "/v1/consumer/endpoints/"+id+"/subscriptions", nil)
	if err != nil || resp.Status != http.StatusOK {
		return subs
	}
	var ls listSubscriptionsResponse
	_ = json.Unmarshal(resp.Body, &ls)
	for _, s := range ls.Subscriptions {
		subs = append(subs, subRow{EndpointID: id, EventType: s.EventType, Status: s.Status})
	}
	return subs
}

// subscribableTypes reads UI-14's list: the event types the producer has left open
// to public subscription, so the subscribe control offers only what the API will
// accept.
func (h *Handler) subscribableTypes(r *http.Request) []string {
	resp, err := h.coreReq(r, http.MethodGet, "/v1/consumer/event-types", nil)
	if err != nil || resp.Status != http.StatusOK {
		return nil
	}
	var lt listSubscribableTypesResponse
	_ = json.Unmarshal(resp.Body, &lt)
	out := make([]string, 0, len(lt.EventTypes))
	for _, t := range lt.EventTypes {
		out = append(out, t.Name)
	}
	return out
}

// endpointEdit applies UI-13's edits - name, URL, notification email - to one of
// the caller's own endpoints. It is the same PATCH the API exposes, so the new URL
// is re-checked for SSRF (FR-402) and a foreign id is a 404 (AC-39) with no
// portal-side special case.
func (h *Handler) endpointEdit(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	id := r.PathValue("id")
	resp, err := h.coreReq(r, http.MethodPatch, "/v1/consumer/endpoints/"+id, map[string]string{
		"name":               strings.TrimSpace(r.FormValue("name")),
		"url":                strings.TrimSpace(r.FormValue("url")),
		"notification_email": strings.TrimSpace(r.FormValue("notification_email")),
	})
	if notAuthed(resp, err, w, r, h) {
		return
	}
	if resp.Status == http.StatusOK {
		h.setFlash(w, "success", "Endpoint updated.")
	} else {
		h.setFlash(w, "error", coreErrorMessage(resp))
	}
	http.Redirect(w, r, "/endpoints/"+id, http.StatusSeeOther)
}

// endpointVerify answers the ownership challenge (FR-403) so the endpoint can
// receive live events. The token is the one shown once at registration: the page
// asks the user to paste it, because the portal holds no copy of it (only its
// hash is stored) and a pre-filled hidden field would post an empty token.
func (h *Handler) endpointVerify(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	id := r.PathValue("id")
	resp, err := h.coreReq(r, http.MethodPost, "/v1/consumer/endpoints/"+id+"/verify",
		map[string]string{"token": r.FormValue("token")})
	if err != nil {
		http.Error(w, "cannot reach the API", http.StatusBadGateway)
		return
	}
	switch resp.Status {
	case http.StatusNoContent:
		h.setFlash(w, "success", "Endpoint verified. It can now receive live events.")
	case http.StatusNotFound:
		h.setFlash(w, "error", "That endpoint no longer exists.")
	default:
		h.setFlash(w, "error", "Verification failed: check the token.")
	}
	http.Redirect(w, r, "/endpoints/"+id, http.StatusSeeOther)
}

// endpointSetPaused pauses or resumes an endpoint (UI-13). The endpoints list
// carries this control as an htmx form, so an htmx request gets the refreshed list
// fragment; the detail page carries it as a plain form, and a plain post gets a 303
// back to the endpoint page rather than a fragment-only document.
func (h *Handler) endpointSetPaused(paused bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		resp, err := h.coreReq(r, http.MethodPatch, "/v1/consumer/endpoints/"+id,
			map[string]bool{"paused": paused})
		if notAuthed(resp, err, w, r, h) {
			return
		}
		if !wantsFragment(r) {
			if !okStatus(resp.Status) {
				h.setFlash(w, "error", coreErrorMessage(resp))
			}
			http.Redirect(w, r, "/endpoints/"+id, http.StatusSeeOther)
			return
		}
		h.fragEndpoints(w, r) // re-render the list with the new status
	}
}

// endpointDelete soft-deletes an endpoint (FR-407), consumer-scoped. An htmx
// request gets the refreshed list fragment; a plain post from the detail page gets
// a 303 to the list, where the endpoint is gone.
func (h *Handler) endpointDelete(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	resp, err := h.coreReq(r, http.MethodDelete, "/v1/consumer/endpoints/"+id, nil)
	if notAuthed(resp, err, w, r, h) {
		return
	}
	if !wantsFragment(r) {
		if !okStatus(resp.Status) {
			h.setFlash(w, "error", coreErrorMessage(resp))
			http.Redirect(w, r, "/endpoints/"+id, http.StatusSeeOther)
			return
		}
		h.setFlash(w, "success", "Endpoint deleted.")
		http.Redirect(w, r, "/endpoints", http.StatusSeeOther)
		return
	}
	h.fragEndpoints(w, r)
}

// subscribe opts an endpoint into an event type (UI-14). An htmx request gets the
// refreshed subscriptions fragment, so the panel updates where the user acted; any
// other caller gets a 303 back to the endpoint page. A refusal (the type is not
// publicly subscribable, or is disabled) is shown either way - in the panel, or as
// the flash on the page it redirects to - so the consumer sees the API's reason
// wherever they acted.
func (h *Handler) subscribe(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	id := r.PathValue("id")
	eventType := strings.TrimSpace(r.FormValue("event_type"))
	resp, err := h.coreReq(r, http.MethodPost, "/v1/consumer/endpoints/"+id+"/subscriptions",
		map[string]string{"event_type": eventType})
	if notAuthed(resp, err, w, r, h) {
		return
	}
	if !wantsFragment(r) {
		if resp.Status == http.StatusCreated {
			h.setFlash(w, "success", "Subscribed to "+eventType+".")
		} else {
			h.setFlash(w, "error", coreErrorMessage(resp))
		}
		http.Redirect(w, r, "/endpoints/"+id, http.StatusSeeOther)
		return
	}
	// Re-read after the attempt either way, so the panel shows the new state on
	// success and the unchanged state next to the reason on refusal.
	view := subsView{Subscriptions: h.endpointSubscriptions(r, id)}
	if resp.Status != http.StatusCreated {
		view.Error = coreErrorMessage(resp)
	}
	h.r.Fragment(w, http.StatusOK, "frag:subs/list", view)
}

// unsubscribe opts an endpoint out of an event type (UI-14). Same rule as
// subscribe: htmx gets the refreshed fragment, everyone else a 303 back to the
// page that owns the control.
func (h *Handler) unsubscribe(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	eventType := r.PathValue("event")
	resp, err := h.coreReq(r, http.MethodDelete,
		"/v1/consumer/endpoints/"+id+"/subscriptions/"+eventType, nil)
	if notAuthed(resp, err, w, r, h) {
		return
	}
	if !wantsFragment(r) {
		if !okStatus(resp.Status) {
			h.setFlash(w, "error", coreErrorMessage(resp))
		}
		http.Redirect(w, r, "/endpoints/"+id, http.StatusSeeOther)
		return
	}
	h.renderSubsFragment(w, r, id)
}

// deliveries is the delivery-log page (UI-15/UI-16). The inspect and replay API it
// reads is a separate slice (S7), so the page states the dependency plainly instead
// of rendering an empty log that would read as "you have no deliveries". The
// requirement is tracked as a card parented on S7, not by this notice.
func (h *Handler) deliveries(w http.ResponseWriter, r *http.Request) {
	h.renderPage(w, r, "deliveries", "Delivery log", "deliveries", nil)
}

// snippetForm renders the verification-snippet form (UI-18). A logged-in user only;
// the secret is entered on the next request, never in a query string.
func (h *Handler) snippetForm(w http.ResponseWriter, r *http.Request) {
	h.renderPage(w, r, "snippet", "Webhook snippet", "snippet", snippetView{
		EndpointID: r.URL.Query().Get("endpoint"),
	})
}

// snippetSubmit renders a Standard Webhooks verification snippet for a whsec secret
// (UI-18). The secret arrives in the POST body - not a GET query parameter, where it
// would land in access logs, browser history and any shared link - and it is used
// only to render this page: it is not forwarded to the core API and not stored.
func (h *Handler) snippetSubmit(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	view := snippetView{
		EndpointID: strings.TrimSpace(r.FormValue("endpoint")),
		Whsec:      strings.TrimSpace(r.FormValue("whsec")),
	}
	if view.Whsec != "" {
		view.Snippet = webhookSnippet(view.Whsec)
	}
	h.renderPage(w, r, "snippet", "Webhook snippet", "snippet", view)
}

// --- fragments -------------------------------------------------------------

// fragEndpoints renders the endpoints list fragment for htmx swaps.
func (h *Handler) fragEndpoints(w http.ResponseWriter, r *http.Request) {
	resp, err := h.coreReq(r, http.MethodGet, "/v1/consumer/endpoints", nil)
	if notAuthed(resp, err, w, r, h) {
		return
	}
	view := endpointsView{}
	if resp.Status == http.StatusOK {
		var le listEndpointsResponse
		_ = json.Unmarshal(resp.Body, &le)
		for _, e := range le.Endpoints {
			view.Endpoints = append(view.Endpoints, endpointRow{
				ID: e.ID, Name: e.Name, URL: e.URL, Status: e.Status,
				Verified: e.Verified, Paused: e.Status == "paused", CreatedAt: e.CreatedAt,
			})
		}
	}
	h.r.Fragment(w, http.StatusOK, "frag:endpoints/list", view)
}

// renderSubsFragment fetches an endpoint's subscriptions and renders the subs
// fragment for the htmx swap.
func (h *Handler) renderSubsFragment(w http.ResponseWriter, r *http.Request, id string) {
	h.r.Fragment(w, http.StatusOK, "frag:subs/list", subsView{Subscriptions: h.endpointSubscriptions(r, id)})
}

// notAuthed handles the "no valid session" case shared by every data handler: a
// network error is a 502, an unauthenticated core reply redirects to login with
// the next path preserved, and anything else is surfaced to the caller. It
// returns whether it already wrote the response.
func notAuthed(resp portal.Response, err error, w http.ResponseWriter, r *http.Request, h *Handler) bool {
	if err != nil {
		http.Error(w, "cannot reach the API", http.StatusBadGateway)
		return true
	}
	if resp.Status == http.StatusUnauthorized || resp.Status == http.StatusForbidden {
		next := r.URL.RequestURI()
		http.Redirect(w, r, "/login?next="+urlEscape(next), http.StatusSeeOther)
		return true
	}
	return false
}

// isLocalPath guards the post-login redirect against an open-redirect: only a
// path beginning with a single "/" is honoured.
func isLocalPath(p string) bool {
	return strings.HasPrefix(p, "/") && !strings.HasPrefix(p, "//")
}

// urlEscape percent-encodes a value for a query string. It backs the post-auth
// "next" redirect so a login round-trip preserves the target path without
// becoming an open-redirect.
func urlEscape(s string) string { return url.QueryEscape(s) }

// webhookSnippet renders a copy-paste Standard Webhooks verification snippet
// tailored to the caller's own endpoint signing secret (UI-18). It is generated
// server-side from the whsec the user already holds and is never sent to the
// core API; the verifier is the Standard Webhooks Go reference library, so
// signatures are validated by a maintained implementation rather than ad-hoc
// HMAC code. strconv.Quote embeds the secret as an inert Go string literal.
func webhookSnippet(whsec string) string {
	const head = `// Verify an incoming webhook signature with the Standard Webhooks Go library.
//   go get github.com/standard-webhooks/standard-webhooks/libraries/go

import (
	"io"
	"net/http"

	standardwebhooks "github.com/standard-webhooks/standard-webhooks/libraries/go"
)

var webhookSecret = `
	const tail = `
func verify(r *http.Request) error {
	wh, err := standardwebhooks.NewWebhook(webhookSecret)
	if err != nil {
		return err
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return err
	}
	return wh.Verify(body, r.Header)
}
`
	return head + strconv.Quote(whsec) + tail
}
