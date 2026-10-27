// Package portalui is the shared component library for the Pigeon portals.
//
// §14: the producer portal and the consumer portal are two separate deployables
// that share a component library and are each independently themeable. This
// package is that shared library: the base HTML layout, the reusable UI
// components (tables, buttons, badges, flash messages), the embedded static
// assets (htmx and the stylesheet), and the per-deployable branding with a
// neutral fallback (UI-17).
//
// It carries no business logic and makes no calls to the core API: each portal
// owns its own handlers and only uses portalui to render. That keeps the
// library shareable and unit-testable on its own.
package portalui

import (
	"embed"
	"io/fs"
	"net/http"
	"strings"
)

//go:embed static/*
var staticFS embed.FS

// StaticFS is the embedded asset tree, rooted at "static". It is served verbatim
// at /assets by each portal (htmx.min.js and styles.css). Everything the operator
// needs is embedded, so there is no Node toolchain and no build step on the host
// (the Do requirement: "assets embedded — no Node toolchain for the operator").
func StaticFS() fs.FS { return staticFS }

// AssetHandler serves the embedded static tree at the given URL prefix (usually
// "/assets"). Only the two shipped assets are served with a real content type;
// anything else is a 404. Being explicit about the MIME type keeps cache headers
// stable and stops a misnamed file from being served as a script.
func AssetHandler(prefix string) http.Handler {
	return http.StripPrefix(prefix, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name := strings.TrimPrefix(strings.TrimPrefix(r.URL.Path, "/"), "/")
		ct, ok := assetContentType(name)
		if !ok {
			http.NotFound(w, r)
			return
		}
		data, err := staticFS.ReadFile("static/" + name)
		if err != nil {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", ct)
		w.Header().Set("Cache-Control", "public, max-age=3600")
		_, _ = w.Write(data)
	}))
}

// assetContentType maps a static file name to its MIME type. Only the two assets
// we ship are recognised; everything else is reported as unknown so the caller
// can 404 rather than guess.
func assetContentType(name string) (string, bool) {
	switch {
	case strings.HasSuffix(name, "htmx.min.js"):
		return "text/javascript; charset=utf-8", true
	case strings.HasSuffix(name, "styles.css"):
		return "text/css; charset=utf-8", true
	default:
		return "", false
	}
}
