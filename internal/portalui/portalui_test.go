package portalui

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// renderPage builds a renderer over Base() plus one inline page body and returns
// the rendered HTML for a Page.
func renderPage(t *testing.T, body string, page Page) string {
	t.Helper()
	base, err := Base()
	if err != nil {
		t.Fatalf("Base(): %v", err)
	}
	r, err := NewRenderer(base, map[string]string{"x": `{{define "content"}}` + body + `{{end}}`})
	if err != nil {
		t.Fatalf("NewRenderer(): %v", err)
	}
	rec := httptest.NewRecorder()
	r.Page(rec, http.StatusOK, "x", page)
	if rec.Code != http.StatusOK {
		t.Fatalf("Page status = %d", rec.Code)
	}
	return rec.Body.String()
}

// TestNeutralFallbackHasNoPigeonBranding is UI-17's guarantee: a portal whose
// operator configured no brand carries no Pigeon branding.
func TestNeutralFallbackHasNoPigeonBranding(t *testing.T) {
	html := renderPage(t, "<p>home</p>", Page{Brand: NeutralFallback, Flash: "hi"})
	for _, forbidden := range []string{"Pigeon", "pigeon", "pgn_"} {
		if strings.Contains(html, forbidden) {
			t.Fatalf("neutral fallback leaked %q branding (UI-17): %s", forbidden, html)
		}
	}
	if !strings.Contains(html, NeutralFallback.Name) {
		t.Fatalf("neutral fallback did not render its wordmark %q", NeutralFallback.Name)
	}
	if !strings.Contains(html, "--brand-accent:"+NeutralFallback.AccentColor+";") {
		t.Fatalf("neutral fallback did not set its accent variable")
	}
}

// TestProducerBrandOverrides renders a producer-themed portal and checks every
// brand field reaches the page.
func TestProducerBrandOverrides(t *testing.T) {
	brand := Brand{Name: "Acme", AccentColor: "#4f46e5", LogoURL: "/logo.png", Company: "Acme Inc."}
	html := renderPage(t, "<p>home</p>", Page{Brand: brand, Title: "Endpoints"})
	for _, want := range []string{"Acme", "#4f46e5", "/logo.png", "Acme Inc.", "Endpoints"} {
		if !strings.Contains(html, want) {
			t.Fatalf("producer brand page missing %q", want)
		}
	}
}

// TestContentTemplateReceivesPage proves the content template sees the Page, so
// a page can read Data alongside the layout fields.
func TestContentTemplateReceivesPage(t *testing.T) {
	html := renderPage(t, "<p>hello {{.Email}} / {{.Brand.Name}}</p>",
		Page{Brand: NeutralFallback, Email: "j@acme.test"})
	if !strings.Contains(html, "hello j@acme.test / "+NeutralFallback.Name) {
		t.Fatalf("content template did not receive the Page: %s", html)
	}
}

// TestFlashRendersKind checks the flash banner uses its kind for the class.
func TestFlashRendersKind(t *testing.T) {
	html := renderPage(t, "", Page{Brand: NeutralFallback, Flash: "Saved", FlashKind: "success"})
	if !strings.Contains(html, `class="flash flash-success"`) || !strings.Contains(html, "Saved") {
		t.Fatalf("flash banner wrong: %s", html)
	}
}

// TestFragmentRendersWithoutLayout proves htmx fragments skip the document shell.
func TestFragmentRendersWithoutLayout(t *testing.T) {
	base, err := Base()
	if err != nil {
		t.Fatalf("Base(): %v", err)
	}
	if _, err := base.Parse(`{{define "frag:row"}}<tr><td>{{.}}</td></tr>{{end}}`); err != nil {
		t.Fatalf("parse frag: %v", err)
	}
	r := NewRendererNoPages(base)
	rec := httptest.NewRecorder()
	r.Fragment(rec, http.StatusOK, "frag:row", "acme")
	if strings.Contains(rec.Body.String(), "<!doctype html>") {
		t.Fatalf("fragment rendered the full layout: %s", rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "<td>acme</td>") {
		t.Fatalf("fragment body wrong: %s", rec.Body.String())
	}
}

// TestUnsafeAccentFallsBack proves a hostile PORTAL_BRAND_ACCENT cannot break
// out of the style element: it is replaced by the neutral accent.
func TestUnsafeAccentFallsBack(t *testing.T) {
	html := renderPage(t, "", Page{Brand: Brand{Name: "X", AccentColor: "</style><script>alert(1)</script>"}})
	if strings.Contains(html, "<script>") {
		t.Fatalf("accent colour broke out of <style>: %s", html)
	}
	if !strings.Contains(html, "--brand-accent:"+NeutralFallback.AccentColor+";") {
		t.Fatalf("unsafe accent did not fall back to neutral")
	}
}

func TestAssetHandlerServesEmbeddedAssets(t *testing.T) {
	h := AssetHandler("/assets")
	srv := httptest.NewServer(h)
	defer srv.Close()

	cases := []struct {
		path, ctype string
		wantBody    bool
	}{
		{"/assets/htmx.min.js", "text/javascript", true},
		{"/assets/styles.css", "text/css", true},
		{"/assets/missing.js", "", false},
	}
	for _, c := range cases {
		resp, err := http.Get(srv.URL + c.path)
		if err != nil {
			t.Fatalf("GET %s: %v", c.path, err)
		}
		if c.wantBody {
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("GET %s status = %d, want 200", c.path, resp.StatusCode)
			}
			if !strings.HasPrefix(resp.Header.Get("Content-Type"), c.ctype) {
				t.Fatalf("GET %s content-type = %q, want prefix %q", c.path, resp.Header.Get("Content-Type"), c.ctype)
			}
		} else if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("GET %s status = %d, want 404", c.path, resp.StatusCode)
		}
		resp.Body.Close()
	}
}
