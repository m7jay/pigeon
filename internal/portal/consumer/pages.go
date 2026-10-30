package consumer

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

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

// headerRow is one configured custom header as the portal may show it (FR-405,
// FR-1006): the name, and the mask the API put in place of the value. The portal
// never sees a plaintext - it renders the mask the API sent, and a value it never
// received cannot be leaked by a template.
type headerRow struct {
	Name  string
	Value string
}

// customHeaderInput is one parsed line of the registration form's header field.
type customHeaderInput struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// parseCustomHeaders reads the registration form's "Name: value" lines. It does
// the mechanical part only: FR-405's rules (the bound, the forbidden names, the
// legality of the value) are the API's, applied at registration, so the portal
// cannot grow a second opinion about what is acceptable. A line with no colon is
// reported here, because that is a typo the API would report less helpfully as
// "the name is not a valid HTTP header name".
func parseCustomHeaders(text string) ([]customHeaderInput, error) {
	var out []customHeaderInput
	for i, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, value, ok := strings.Cut(line, ":")
		if !ok || strings.TrimSpace(name) == "" {
			return nil, fmt.Errorf("line %d is not \"Name: value\"", i+1)
		}
		out = append(out, customHeaderInput{Name: strings.TrimSpace(name), Value: strings.TrimSpace(value)})
	}
	return out, nil
}

type endpointsView struct {
	Endpoints []endpointRow
	Created   *createdView
	// CSRFToken is FR-1009's token. This view renders two POST forms (pause/resume
	// and delete) per row inside an htmx fragment, so it carries the token the same
	// way a Page does.
	CSRFToken string
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
	// CSRFToken is FR-1009's token, for the "opt out" POST each row renders.
	CSRFToken string
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
	// ReissuedToken and ReissuedHint are FR-409's one-time re-issue response:
	// each call to the ownership-challenge re-issue route mints a fresh token and
	// hint and answers 201; the page that follows shows the token exactly once,
	// with a copy warning and a Verify now button. They are empty until the
	// re-issue handler sets them from the API's response.
	ReissuedToken string
	ReissuedHint  string
	// RotatedSecret is set only on the render that follows a rotation (UI-13):
	// the new signing secret exists in exactly one API response, so the page that
	// shows it is rendered from that response rather than redirected to.
	RotatedSecret string
	// Subs is the subscription panel (UI-14).
	Subs subsView
	// CustomHeaders are the endpoint's FR-405 headers as configured: names in the
	// clear and the API's mask where the value was (FR-1006). There is no way for
	// this field to hold a value, which is what makes the page redactable by
	// construction.
	CustomHeaders []headerRow
	// AvailableTypes is the set the subscribe control offers: exactly the event
	// types the producer has left open to public subscription, from
	// GET /v1/consumer/event-types. Offering a free-text field would offer a control
	// the API refuses.
	AvailableTypes []string
}

// snippetView is UI-18's page: the endpoint id (carried forward), the submitted
// secret so the form can tell it was used, and the four rendered snippets. It no
// longer carries a single hand-written snippet - the shared renderer in
// internal/portal produces all four, so this portal and the producer portal
// implement the signature rules once rather than twice.
type snippetView struct {
	EndpointID string
	Whsec      string
	Snippet    []snippetRow
	Note       string
}

// snippetRow is one snippet as the template renders it. It mirrors
// portal.Snippet's shape so the shared renderer's output can be shown without the
// template reaching into another package's type.
type snippetRow struct {
	Language string
	Label    string
	Source   string
	Note     string
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

// listSubscribableTypesResponse is GET /v1/consumer/event-types: the types the
// producer has left open to public subscription (UI-14), each carrying the live
// contract UI-9's consumer guide renders. S9a added payload_version and
// payload_schema to this response; the schema is kept as raw JSON so the guide can
// pretty-print the API's own document rather than a normalised copy of it.
type listSubscribableTypesResponse struct {
	EventTypes []struct {
		Name           string          `json:"name"`
		PayloadVersion int             `json:"payload_version"`
		PayloadSchema  json.RawMessage `json:"payload_schema"`
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
//
// The cookie's lifetime comes from the core's own `expires_at` (AC-51): the portal
// no longer invents a TTL, so the cookie cannot outlive the session token it
// carries. A 201 whose body has no parsable expires_at is a broken core, not a
// reason to fall back to a constant - that fallback is precisely the defect
// FR-1009 closes - so the login fails with 502 instead.
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
	if !h.applyCoreSession(w, resp.Body) {
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

// applyCoreSession decodes a 201 from the core's POST /v1/session and starts the
// portal session from it: the token goes into the encrypted cookie and the
// cookie's Max-Age comes from the core's own `expires_at` (AC-51). It reports
// whether the session was started, having already written the error response when
// it was not.
//
// Both entry points that start a session - the login form and invitation
// acceptance, which logs in the account it just created - go through this one
// function, so neither can drift back to a portal-side TTL. A body the portal
// cannot read a session out of is a 502: the core promised a session and did not
// deliver one, and inventing a lifetime for it is the defect this closes.
func (h *Handler) applyCoreSession(w http.ResponseWriter, body []byte) bool {
	var sess struct {
		Token     string    `json:"token"`
		ExpiresAt time.Time `json:"expires_at"`
	}
	if err := json.Unmarshal(body, &sess); err != nil || sess.Token == "" || sess.ExpiresAt.IsZero() {
		http.Error(w, "the API did not return a usable session", http.StatusBadGateway)
		return false
	}
	if err := h.setTokenCookie(w, sess.Token, sess.ExpiresAt); err != nil {
		http.Error(w, "cannot start session", http.StatusInternalServerError)
		return false
	}
	return true
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
		if !h.applyCoreSession(w, login.Body) {
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
				Verified: e.Verified, Paused: e.Status == "paused", CreatedAt: e.CreatedAt,
			})
		}
	}
	view.CSRFToken = h.csrfToken(r)
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
		h.renderPage(w, r, "endpoints", "Your endpoints", "endpoints",
			endpointsView{CSRFToken: h.csrfToken(r)})
		return
	}
	// FR-405: the headers field is parsed here and validated by the API, which
	// owns the rules. A typo in the field itself is the portal's to report.
	headers, err := parseCustomHeaders(r.FormValue("custom_headers"))
	if err != nil {
		// A malformed field is the one thing the API cannot describe usefully, so
		// the portal reports it - then redirects, because a flash set on a response
		// is only rendered after the browser sends it back on the next request.
		h.setFlash(w, "error", "Custom headers: "+err.Error())
		http.Redirect(w, r, "/endpoints", http.StatusSeeOther)
		return
	}
	body := map[string]any{"name": name, "url": url}
	if len(headers) > 0 {
		body["custom_headers"] = headers
	}
	resp, err := h.coreReq(r, http.MethodPost, "/v1/consumer/endpoints", body)
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
		view := endpointsView{
			Created: &createdView{
				ID: created.ID, Secret: created.SigningSecret, VerificationToken: created.VerificationToken,
			},
			CSRFToken: h.csrfToken(r),
		}
		h.renderPageStatus(w, r, http.StatusOK, "endpoints", "Your endpoints", "endpoints", view)
	default:
		// 422 (an SSRF-blocked URL, or FR-405's refusal of a header name), 409, etc.
		// The API's reason is what the user has to act on, and it never quotes a
		// header value (FR-1006). It travels in the flash cookie, which the browser
		// sends back on the redirect below, so it actually renders - a flash set and
		// read in the same response would be silently dropped.
		h.setFlash(w, "error", coreErrorMessage(resp))
		http.Redirect(w, r, "/endpoints", http.StatusSeeOther)
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
		CustomHeaders                    []struct {
			Name  string `json:"name"`
			Value string `json:"value"`
		} `json:"custom_headers"`
	}
	_ = json.Unmarshal(resp.Body, &ep)
	detail := endpointDetail{
		ID: ep.ID, Name: ep.Name, URL: ep.URL, Status: ep.Status,
		Verified: ep.Verified, Paused: ep.Status == "paused", CreatedAt: ep.CreatedAt,
	}
	for _, h := range ep.CustomHeaders {
		detail.CustomHeaders = append(detail.CustomHeaders, headerRow{Name: h.Name, Value: h.Value})
	}
	if ep.NotificationEmail != nil {
		detail.NotificationEmail = *ep.NotificationEmail
	}
	// The endpoint's subscriptions for the panel, and the types the subscribe
	// control may offer (UI-14). Both are plain API reads: the portal decides
	// nothing here that the API does not already answer.
	detail.Subs = subsView{Subscriptions: h.endpointSubscriptions(r, id), CSRFToken: h.csrfToken(r)}
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

// endpointReissue is UI-13's re-issue: it asks the API for a fresh ownership
// challenge token and hint for one of the caller's own endpoints. The API answers
// 201 with the creation response's field names, so this handler renders the
// detail page from that response, showing the new token exactly once, with a
// copy warning and a Verify now button.
func (h *Handler) endpointReissue(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	resp, err := h.coreReq(r, http.MethodPost, "/v1/consumer/endpoints/"+id+"/challenge", nil)
	if notAuthed(resp, err, w, r, h) {
		return
	}
	if resp.Status != http.StatusCreated {
		h.setFlash(w, "error", coreErrorMessage(resp))
		http.Redirect(w, r, "/endpoints/"+id, http.StatusSeeOther)
		return
	}
	var chal struct {
		VerificationToken string `json:"verification_token"`
		VerificationHint  string `json:"verification_hint"`
	}
	if err := json.Unmarshal(resp.Body, &chal); err != nil {
		h.setFlash(w, "error", "The re-issued token could not be read.")
		http.Redirect(w, r, "/endpoints/"+id, http.StatusSeeOther)
		return
	}
	detail, ok := h.endpointDetailView(w, r, id)
	if !ok {
		return
	}
	detail.ReissuedToken = chal.VerificationToken
	detail.ReissuedHint = chal.VerificationHint
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

// snippetForm renders the verification-snippet form (UI-18). A logged-in user only;
// the secret is entered on the next request, never in a query string.
func (h *Handler) snippetForm(w http.ResponseWriter, r *http.Request) {
	h.renderPage(w, r, "snippet", "Verification snippets", "snippet", snippetView{
		EndpointID: r.URL.Query().Get("endpoint"),
		Note:       portal.SnippetTemplateNote,
	})
}

// snippetSubmit renders Standard Webhooks verification snippets for a whsec secret
// (UI-18). The secret arrives in the POST body - not a GET query parameter, where it
// would land in access logs, browser history and any shared link - and it is used
// only to render this page: it is not forwarded to the core API and not stored.
//
// The four samples come from the shared renderer in internal/portal, the same one
// the producer portal's UI-7 page calls: the signature rules have one
// implementation, so the two portals cannot drift about what a valid verifier is.
func (h *Handler) snippetSubmit(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	view := snippetView{
		EndpointID: strings.TrimSpace(r.FormValue("endpoint")),
		Whsec:      strings.TrimSpace(r.FormValue("whsec")),
		Note:       portal.SnippetTemplateNote,
	}
	if view.Whsec != "" {
		for _, s := range portal.Snippets(view.Whsec) {
			view.Snippet = append(view.Snippet, snippetRow{
				Language: string(s.Language), Label: s.Label, Source: s.Source, Note: s.Note,
			})
		}
	}
	h.renderPage(w, r, "snippet", "Verification snippets", "snippet", view)
}

// --- fragments -------------------------------------------------------------

// fragEndpoints renders the endpoints list fragment for htmx swaps.
func (h *Handler) fragEndpoints(w http.ResponseWriter, r *http.Request) {
	resp, err := h.coreReq(r, http.MethodGet, "/v1/consumer/endpoints", nil)
	if notAuthed(resp, err, w, r, h) {
		return
	}
	view := endpointsView{CSRFToken: h.csrfToken(r)}
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
	h.r.Fragment(w, http.StatusOK, "frag:subs/list",
		subsView{Subscriptions: h.endpointSubscriptions(r, id), CSRFToken: h.csrfToken(r)})
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
