package portalui

import (
	"fmt"
	"html/template"
	"io"
	"net/http"
)

// Page is everything the base layout needs to render a full page. A page handler
// fills it and names the content template to drop into <main> via ContentTmpl;
// the content template reads the same Page, so it can reach Data plus every
// layout field.
type Page struct {
	Brand     Brand
	Title     string
	ActiveNav string // nav id that is highlighted
	Flash     string // one-shot message shown above the page
	FlashKind string // "success" | "error" | "info"
	Email     string // current user, shown in the top-right box
	Data      any    // page payload, read by the content template
	// CSRFToken is FR-1009's token for this request, rendered into every form by
	// the shared "csrf_field" component. It lives on Page (and on each fragment
	// view that renders a form) rather than being minted per template, so adding a
	// form cannot silently omit it: a form without a token is refused by the
	// portal's middleware with a 403.
	CSRFToken string
}

// Table is the payload for the shared "table" component: a header row plus
// pre-rendered string cells. Keeping cells as strings means the caller has
// already formatted every value (times, badges, links-as-markup), which is the
// only thing html/template cannot do for you.
type Table struct {
	Columns []string
	Rows    [][]string
	Empty   string // message when Rows is empty
}

// Funcs are the template helpers shared by every portal. html/template handles
// all HTML escaping; these only cover the couple of shapes it cannot express.
var Funcs = template.FuncMap{
	// dict builds a map[string]any from an even-length arg list. Lets a content
	// template range over a stable key/value pair without a helper type.
	"dict": func(args ...any) map[string]any {
		m := make(map[string]any, len(args)/2)
		for i := 0; i+1 < len(args); i += 2 {
			if k, ok := args[i].(string); ok {
				m[k] = args[i+1]
			}
		}
		return m
	},
	// eqs reports whether s equals any of cs. Small string membership test for
	// highlighting nav items and status classes.
	"eqs": func(s string, cs ...string) bool {
		for _, c := range cs {
			if s == c {
				return true
			}
		}
		return false
	},
}

// Base returns the shared template set: the base layout plus the reusable
// components. Each portal Clones it and ParseFS its own nav and page templates
// on top, overriding "nav" and adding "content:*" / "frag:*" defines. Splitting
// the shared shell from the portal-specific pages is what lets two deployables
// share one library while staying independently themeable (§14).
func Base() (*template.Template, error) {
	return template.New("base").Funcs(Funcs).ParseFS(TemplateFS,
		"templates/base.html", "templates/components.html")
}

// TemplateFS is the embedded shared template tree, rooted at "templates". It is
// defined in templates.go so the go:embed directive sits next to its embed.FS.

// Renderer renders pages and fragments from one parsed template set.
//
// html/template's {{template}} action takes a *static* name, so a page cannot be
// selected by a runtime string. Instead each named page is its own parsed set:
// a Clone of the shared base (layout + components) with exactly one "content"
// define added, cached at construction. Fragments (htmx swaps) are additive
// defines on the shared base and are rendered without the layout wrapper.
type Renderer struct {
	base  *template.Template            // layout + components + fragments; "content" undefined
	pages map[string]*template.Template // base.Clone() + one "content" define, per page name
}

// NewRendererNoPages wraps a parsed base for fragment rendering when a portal
// serves only htmx fragments (or a test needs a renderer without pages).
func NewRendererNoPages(base *template.Template) *Renderer {
	return &Renderer{base: base, pages: map[string]*template.Template{}}
}

// NewRenderer builds a Renderer from a parsed base (portalui.Base() with a
// portal's nav and fragments ParseFS'd on top) and a map of page names to their
// template text. Each page text is parsed verbatim into a Clone of the base, so
// it must define the "content" template the layout renders:
//
//	{{define "content"}} … {{end}}
//
// The body reads the Page via ".", so it can reach Data alongside the layout
// fields. Because each page is a separate clone with exactly one "content",
// many pages can share one base without the names colliding.
func NewRenderer(base *template.Template, pages map[string]string) (*Renderer, error) {
	r := &Renderer{base: base, pages: make(map[string]*template.Template, len(pages))}
	for name, body := range pages {
		tmpls, err := base.Clone()
		if err != nil {
			return nil, err
		}
		if _, err := tmpls.Parse(body); err != nil {
			return nil, fmt.Errorf("parse page %q: %w", name, err)
		}
		r.pages[name] = tmpls
	}
	return r, nil
}

// Page renders the named page as a full HTML document.
func (r *Renderer) Page(w http.ResponseWriter, status int, name string, page Page) {
	tmpls, ok := r.pages[name]
	if !ok {
		http.Error(w, "no such page: "+name, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)
	if err := tmpls.ExecuteTemplate(w, "layout", page); err != nil {
		_, _ = io.WriteString(w, "\n<!-- render error -->")
	}
}

// Fragment renders a single named fragment (a "frag:*" define) with no layout
// wrapper, for htmx in-place swaps. A missing template is a 500, which is a
// programmer error, not a user-facing condition.
func (r *Renderer) Fragment(w http.ResponseWriter, status int, name string, data any) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)
	if err := r.base.ExecuteTemplate(w, name, data); err != nil {
		_, _ = io.WriteString(w, "\n<!-- render error -->")
	}
}
