package portal

import (
	"net/http"
	"net/url"
	"strings"
)

// Flash cookies: the one-shot message a portal carries across a redirect.
//
// The value on the wire is a percent-encoded (kind, message) pair joined by "|",
// because a cookie value is a restricted byte string. net/http does not refuse
// the bytes it cannot carry - it DROPS them on the way out, silently: '"', ';',
// '\', every control byte and every byte >= 0x80 (net/http's
// validCookieValueByte). An unencoded message therefore arrives truncated, with
// no error anywhere - the header is written, the browser stores it, and the next
// request reads back a message nobody wrote. It is invisible until a dropped byte
// is load-bearing, which is how the producer portal shipped an invitation token
// in a flash that could then not be accepted. Encoding makes the value legal
// without changing a byte of what is displayed.
//
// The codec lives here rather than once per portal because both portal flashes
// were the same shape - same separator, same encoding, same flags - and that fix
// landed in one copy only. One writer, one reader, one test, no second copy to
// miss.
//
// The three flags are AC-49's and this cookie is not excused them: a one-shot
// message is still a cookie a portal emits. SameSite is Lax rather than Strict
// because a flash legitimately arrives on the redirect the user just made -
// Strict would drop it on the first cross-site navigation and the message would
// silently disappear.

const (
	// flashSeparator joins the kind to the message on the wire. Every "|" inside
	// either half is escaped, so the first one in a value is always the separator
	// and the split is unambiguous whatever the message contains.
	flashSeparator = '|'

	// flashMaxAge is how long the message waits for the request that renders it:
	// long enough for the redirect the browser is about to make, short enough that
	// a message cannot go stale in a tab that has been open all day.
	flashMaxAge = 60
)

// SetFlashCookie writes a one-shot (kind, message) cookie under name. The kind is
// the portal UI's banner class ("success", "error" or "info"); the caller owns the
// message text, which is frequently a string the API supplied and may contain any
// byte at all.
func SetFlashCookie(w http.ResponseWriter, name, kind, msg string) {
	http.SetCookie(w, &http.Cookie{
		Name:     name,
		Value:    url.QueryEscape(kind) + string(flashSeparator) + url.QueryEscape(msg),
		Path:     "/",
		MaxAge:   flashMaxAge,
		HttpOnly: true,
		Secure:   true,
		SameSite: http.SameSiteLaxMode,
	})
}

// PullFlashCookie reads the pair back, or ("", "") when the request carries no
// such cookie or the value is not a flash at all. A malformed value is a missing
// flash, never a panic and never half a message.
//
// The halves are decoded with QueryUnescape, the inverse of what SetFlashCookie
// writes, so this reader is compatible with a value already in flight when the
// code moves between packages. A value that does not decode - an unencoded flash
// from an older build, or a cookie someone edited by hand - is returned as it
// stands rather than dropped: a message that is wrong in its whitespace is worth
// more to the person reading it than no message at all.
func PullFlashCookie(r *http.Request, name string) (kind, msg string) {
	c, err := r.Cookie(name)
	if err != nil {
		return "", ""
	}
	i := strings.IndexByte(c.Value, flashSeparator)
	if i < 0 {
		return "", ""
	}
	k, err := url.QueryUnescape(c.Value[:i])
	if err != nil {
		k = c.Value[:i]
	}
	m, err := url.QueryUnescape(c.Value[i+1:])
	if err != nil {
		m = c.Value[i+1:]
	}
	return k, m
}
