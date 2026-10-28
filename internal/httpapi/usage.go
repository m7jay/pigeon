package httpapi

import "net/http"

// handleUsage is GET /v1/meta/usage (SB-6). It answers with the volume reading
// against the budget, the partition catalogue (size and row estimate per day),
// and the configured retention windows — the payload an operator needs to see
// pressure before history disappears.
//
// It is kindAdmin: it reports the deployment's disk and its table sizes, which is
// infrastructure, not tenant data. A tenant credential has no business reading
// another tenant's storage footprint, so this is not a member route.
func (s *Server) handleUsage(w http.ResponseWriter, r *http.Request) {
	if s.deps.Retention == nil {
		writeError(w, http.StatusServiceUnavailable, "retention_unavailable",
			"retention telemetry is not configured")
		return
	}
	st, err := s.deps.Retention.Status(r.Context())
	if err != nil {
		// Status only errors when there has never been a usable reading; when a
		// later gauge read fails it keeps the last good one. So an error here is
		// "we cannot tell you how full the disk is", which must not be dressed up
		// as a calm 80%-of-nothing.
		writeError(w, http.StatusServiceUnavailable, "disk_usage_unavailable", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, st)
}
