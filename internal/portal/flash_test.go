package portal

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestFlashCookieCarriesEveryByteNetHTTPDrops is the regression test for the
// truncation both portal flashes used to ship (internal/portal/flash.go): a
// Set-Cookie value containing ';' - or '"', '\', a control byte, or any non-ASCII
// byte - is not legal, and net/http drops the illegal bytes instead of failing,
// so the message the next request reads back is not the message that was set.
//
// The value carries one of every class net/http cannot serialise, and it makes the
// round trip through a real Set-Cookie header and a real Cookie request header:
// net/http's own writer and parser, not string surgery on the raw header. It first
// proves the mechanism on the unencoded value, so a pass here means the encoding
// is load-bearing rather than that net/http has become permissive.
func TestFlashCookieCarriesEveryByteNetHTTPDrops(t *testing.T) {
	const (
		name = "pigeon_flash"
		kind = "error"
		// One of every byte class validCookieValueByte rejects - '"', ';', '\', a
		// control byte, non-ASCII - plus a space and the '|' the value is split on
		// (escaped, so it cannot be mistaken for the separator).
		msg = "the value a;b|c is refused by https://x.example/hook?q=a;b: " +
			`say "yes\no"` + ", a control byte \x07 and non-ASCII é ☃ ride along"
	)

	// The defect, reproduced: written raw, the message really does arrive mangled,
	// so this test can tell the fix from the bug it fixes.
	raw := httptest.NewRecorder()
	http.SetCookie(raw, &http.Cookie{Name: name, Value: kind + string(flashSeparator) + msg, Path: "/"})
	if _, got := pullFlashFrom(t, cookieFrom(t, raw, name), name); got == msg {
		t.Fatalf("net/http carried the unencoded message intact (%q), so this test cannot distinguish the fix from the defect it fixes", got)
	}

	// The fix: the same message through the writer both portals use.
	rec := httptest.NewRecorder()
	SetFlashCookie(rec, name, kind, msg)
	c := cookieFrom(t, rec, name)

	// AC-49 on the shared writer itself: this cookie is not excused the three
	// flags, and moving the writer here must not have dropped one.
	if !c.HttpOnly || !c.Secure || c.SameSite != http.SameSiteLaxMode {
		t.Fatalf("flash cookie flags = (HttpOnly=%v, Secure=%v, SameSite=%v), want all three set",
			c.HttpOnly, c.Secure, c.SameSite)
	}

	gotKind, gotMsg := pullFlashFrom(t, c, name)
	if gotMsg != msg {
		t.Fatalf("the flash message did not survive the cookie:\n got %q\nwant %q", gotMsg, msg)
	}
	if gotKind != kind {
		t.Fatalf("flash kind = %q, want %q", gotKind, kind)
	}

	// No cookie is not a message, and neither is a value that is not a pair: both
	// read as "no flash" rather than as a message with an empty half.
	if k, m := PullFlashCookie(httptest.NewRequest(http.MethodGet, "/", nil), name); k != "" || m != "" {
		t.Fatalf("a request with no flash cookie read as (%q, %q), want empty", k, m)
	}
	bare := httptest.NewRequest(http.MethodGet, "/", nil)
	bare.AddCookie(&http.Cookie{Name: name, Value: "not-a-flash"})
	if k, m := PullFlashCookie(bare, name); k != "" || m != "" {
		t.Fatalf("a value with no separator read as (%q, %q), want empty", k, m)
	}
}

// TestFlashCookieRoundTripsAnUnencodedLegacyValue pins the compatibility half of
// PullFlashCookie: a message that is not percent-encoded - an older build's flash
// still in a browser's jar, or a hand-edited cookie - is shown as it stands rather
// than dropped. The bug this codec fixes was a message that arrived wrong; a
// reader that answers "no flash" to every value it cannot decode would be the same
// bug with a different symptom.
func TestFlashCookieRoundTripsAnUnencodedLegacyValue(t *testing.T) {
	const name = "pigeon_flash"
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.AddCookie(&http.Cookie{Name: name, Value: "info|100% of the way there"})

	k, m := PullFlashCookie(req, name)
	if k != "info" || m != "100% of the way there" {
		t.Fatalf("PullFlashCookie of an unencoded value = (%q, %q), want (info, 100%% of the way there)", k, m)
	}
}

// cookieFrom parses the named cookie out of a recorder's Set-Cookie header the way
// a browser does - through net/http's own parser, which also unquotes a value
// net/http quoted on the way out - and fails when the response never set it, so an
// assertion cannot pass vacuously on an absent cookie.
func cookieFrom(t *testing.T, rec *httptest.ResponseRecorder, name string) *http.Cookie {
	t.Helper()
	for _, c := range (&http.Response{Header: rec.Header()}).Cookies() {
		if c.Name == name {
			return c
		}
	}
	t.Fatalf("the response set no %s cookie (Set-Cookie: %v)", name, rec.Header().Values("Set-Cookie"))
	return nil
}

// pullFlashFrom sends a parsed cookie back on a request and reads it with the
// reader both portals use: the full round trip, writer to header to request to
// PullFlashCookie.
func pullFlashFrom(t *testing.T, c *http.Cookie, name string) (kind, msg string) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/endpoints", nil)
	req.AddCookie(c)
	return PullFlashCookie(req, name)
}
