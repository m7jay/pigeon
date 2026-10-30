package consumer

import (
	"encoding/json"
	"net/http"
)

// UI-9's consumer-side integration guide. It is a separate file from pages.go
// because the guide is one page with one read: it renders the live contract the
// producer exposes, so a consumer integrates against what the API will actually
// accept rather than a document that was true at some point.
//
// The read is GET /v1/consumer/event-types - the very same route UI-14's subscribe
// control uses to list what may be subscribed to, now carrying payload_version and
// payload_schema per type (S9a). The producer portal's UI-9 guide renders the same
// table through its own route; the two pages share one source, which is the point
// of the row: a schema the producer bumps changes both, so there is no copy to go
// stale.

// guideTypeRow is one event type as the guide shows it: the name, the version the
// core currently publishes, and the schema pretty-printed from the API's own
// document.
type guideTypeRow struct {
	Name           string
	PayloadVersion int
	Schema         string // pretty-printed; empty when the type carries none
}

type guideView struct {
	EventTypes []guideTypeRow
	Error      string
}

// guide is UI-9's consumer page: the integration guide rendered from the live
// event-type schemas the core exposes to this consumer. It reads exactly one
// route, and it renders the payload version and schema the API answered with, so
// what the page says is the live contract rather than a copy.
func (h *Handler) guide(w http.ResponseWriter, r *http.Request) {
	resp, err := h.coreReq(r, http.MethodGet, "/v1/consumer/event-types", nil)
	if notAuthed(resp, err, w, r, h) {
		return
	}
	var view guideView
	if resp.Status != http.StatusOK {
		view.Error = coreErrorMessage(resp)
		h.renderPageStatus(w, r, resp.Status, "guide", "Integration guide", "guide", view)
		return
	}
	var lt listSubscribableTypesResponse
	_ = json.Unmarshal(resp.Body, &lt)
	for _, t := range lt.EventTypes {
		view.EventTypes = append(view.EventTypes, guideTypeRow{
			Name:           t.Name,
			PayloadVersion: t.PayloadVersion,
			Schema:         prettyJSON(t.PayloadSchema),
		})
	}
	h.renderPage(w, r, "guide", "Integration guide", "guide", view)
}

// prettyJSON re-indents a JSON document for display, and returns it unchanged (as
// a string) when it is not JSON or is absent. The guide shows the producer's own
// schema text, indented - not a re-render of it - so a consumer reads the contract
// the API enforces.
func prettyJSON(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var buf any
	if err := json.Unmarshal(raw, &buf); err != nil {
		return string(raw)
	}
	out, err := json.MarshalIndent(buf, "", "  ")
	if err != nil {
		return string(raw)
	}
	return string(out)
}
