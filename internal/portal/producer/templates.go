package producer

import "embed"

// templatesFS is the portal's own template tree: a nav override and fragments on
// top of the shared base, plus one file per page under templates/pages. The
// assets (htmx, the stylesheet) are embedded once in internal/portalui and
// served from there, so both portals render the same shell (§14).
//
//go:embed templates
var templatesFS embed.FS
