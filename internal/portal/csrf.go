package portal

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"io"
	"net/http"
	"strings"
)

// CSRF protection for the portal deployables (FR-1009, AC-50).
//
// The token is a stateless double-submit value: a random nonce, plus an
// HMAC-SHA256 over "<nonce>|<session cookie value>" keyed with the portal's own
// session secret (PORTAL_SESSION_SECRET) - no new secret, no server-side storage,
// no new dependency. The wire form is base64url(nonce) + "." + base64url(mac).
//
// The middleware runs on EVERY state-changing request in a portal's Handler(),
// the anonymous /login and /invite included: logging a victim into the
// attacker's account is itself the attack, and it needs no session to succeed.
//
// Two properties matter, and SameSite=Lax supplies neither:
//
//   - The HMAC binds the token to the session cookie, so a token minted for one
//     session does not verify for another: a token captured from the attacker's
//     own login cannot be replayed against a victim.
//   - The double submit (the request must carry both the cookie and a matching
//     form field) means a cross-origin page cannot produce the pair at all - and
//     Chrome's Lax+POST window, which sends the session cookie on a cross-site
//     POST for ~2 minutes after it is set, therefore cannot forge a
//     state-changing request. UI-19 puts the consumer portal on the producer's
//     own domain, where same-site siblings are not all trusted either.
const (
	// CSRFCookieName is the cookie the token travels in. It carries the same three
	// flags as every other portal cookie (AC-49). HttpOnly is not an exception
	// here: the server renders the hidden field itself, so no script ever needs to
	// read this cookie, and keeping it out of reach of script removes the one
	// read primitive a same-site XSS would otherwise have.
	CSRFCookieName = "pigeon_csrf"

	// csrfFieldName is the form field, and csrfHeaderName the header, a token may
	// be submitted in. Form posts (plain and htmx) carry the field; the header
	// exists for a caller that is not a rendered form.
	csrfFieldName  = "csrf_token"
	csrfHeaderName = "X-CSRF-Token"

	// csrfNonceBytes is the nonce's entropy. 128 bits matches the order of the
	// session credential and keeps the token short enough for a hidden input.
	csrfNonceBytes = 16
)

// mintedToken is the context key carrying the token the middleware minted (on a
// safe request) or validated (on a state-changing one), so a handler can render
// the same token into the form it answers with.
type mintedToken struct{}

// Mint returns the token for one nonce bound to sessionCookie. An empty
// sessionCookie is legitimate - an anonymous form has no session yet, and the
// secret, not the binding, is what an attacker cannot guess.
func Mint(secret string, nonce []byte, sessionCookie string) string {
	return base64.RawURLEncoding.EncodeToString(nonce) + "." +
		base64.RawURLEncoding.EncodeToString(csrfMAC(secret, nonce, sessionCookie))
}

// NewCSRFNonce returns the random nonce a token is minted from.
func NewCSRFNonce() ([]byte, error) {
	nonce := make([]byte, csrfNonceBytes)
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}
	return nonce, nil
}

// MintRandom mints a token with a fresh nonce for the given session cookie value.
func MintRandom(secret, sessionCookie string) (string, error) {
	nonce, err := NewCSRFNonce()
	if err != nil {
		return "", err
	}
	return Mint(secret, nonce, sessionCookie), nil
}

// Verify reports whether token is a well-formed token minted under secret and
// bound to sessionCookie. The MAC comparison is constant time, so a caller learns
// only whether the token verified.
func Verify(secret, token, sessionCookie string) bool {
	nonceB64, macB64, ok := strings.Cut(token, ".")
	if !ok {
		return false
	}
	nonce, err := base64.RawURLEncoding.DecodeString(nonceB64)
	if err != nil || len(nonce) != csrfNonceBytes {
		return false
	}
	got, err := base64.RawURLEncoding.DecodeString(macB64)
	if err != nil || len(got) != sha256.Size {
		return false
	}
	return subtle.ConstantTimeCompare(got, csrfMAC(secret, nonce, sessionCookie)) == 1
}

// csrfMAC is HMAC-SHA256(secret, nonce || "|" || sessionCookie).
func csrfMAC(secret string, nonce []byte, sessionCookie string) []byte {
	m := hmac.New(sha256.New, []byte(secret))
	m.Write(nonce)
	m.Write([]byte("|"))
	m.Write([]byte(sessionCookie))
	return m.Sum(nil)
}

// CSRFMiddleware wraps a portal's whole handler chain.
//
// On a safe request (GET/HEAD/OPTIONS) it ensures a valid token cookie exists and
// stashes the token in the request context, so every form the handler renders
// carries the token the browser will submit back. Re-minting only happens when
// there is no valid token for the current session cookie, so a page reload does
// not invalidate a form the user is already looking at.
//
// On a state-changing request (POST as this platform uses it, plus PUT/PATCH/
// DELETE for the routes a later portal might add) it requires a token that is
// present, equal to the cookie, and verifies under the secret against the session
// cookie - and answers 403 without calling the handler when any of that fails.
// "No state change" is therefore structural rather than a promise: nothing has run
// yet. There is no retry path and no repair path.
//
// A malformed body is a 403, not a 400: the token is the thing being refused, and
// an unparseable body is indistinguishable from an absent one for that purpose.
func CSRFMiddleware(secret, sessionCookieName string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		session := cookieValue(r, sessionCookieName)
		switch r.Method {
		case http.MethodGet, http.MethodHead, http.MethodOptions:
			token := cookieValue(r, CSRFCookieName)
			if !Verify(secret, token, session) {
				fresh, err := MintRandom(secret, session)
				if err != nil {
					http.Error(w, "csrf: cannot mint a token", http.StatusInternalServerError)
					return
				}
				token = fresh
				setCSRFCookie(w, token)
			}
			next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), mintedToken{}, token)))
		default:
			if err := r.ParseForm(); err != nil {
				csrfRefused(w, "the request body could not be read")
				return
			}
			submitted := r.PostFormValue(csrfFieldName)
			if submitted == "" {
				submitted = r.Header.Get(csrfHeaderName)
			}
			cookie := cookieValue(r, CSRFCookieName)
			// Constant-time on the double-submit pair, and no early return that
			// would make the three failure modes distinguishable by timing.
			pairOK := cookie != "" && subtle.ConstantTimeCompare([]byte(submitted), []byte(cookie)) == 1
			if !pairOK || !Verify(secret, submitted, session) {
				csrfRefused(w, "the CSRF token is missing or does not match this session")
				return
			}
			next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), mintedToken{}, submitted)))
		}
	})
}

// TokenFromRequest returns the token CSRFMiddleware minted or validated for this
// request, or "" when the request did not pass through it.
func TokenFromRequest(r *http.Request) string {
	if r == nil {
		return ""
	}
	if tok, ok := r.Context().Value(mintedToken{}).(string); ok {
		return tok
	}
	return ""
}

// setCSRFCookie writes the token with the three flags AC-49 requires of every
// portal cookie. Secure is unconditional: the portal is a browser surface and the
// compose file terminates TLS in front of it, so a cleartext deployment is a
// configuration error rather than a reason to drop the flag on the cookie that
// guards every write.
func setCSRFCookie(w http.ResponseWriter, token string) {
	http.SetCookie(w, &http.Cookie{
		Name:     CSRFCookieName,
		Value:    token,
		Path:     "/",
		HttpOnly: true,
		Secure:   true,
		SameSite: http.SameSiteLaxMode,
		// Session cookie: it expires with the browser session, and a token that
		// outlived the form it was minted for buys nothing.
		MaxAge: 0,
	})
}

// csrfRefused answers a refused request: 403, one line, and no state change -
// the handler never ran.
func csrfRefused(w http.ResponseWriter, reason string) {
	http.Error(w, "forbidden: "+reason, http.StatusForbidden)
}

// cookieValue reads one cookie's value, or "" when it is absent.
func cookieValue(r *http.Request, name string) string {
	c, err := r.Cookie(name)
	if err != nil {
		return ""
	}
	return c.Value
}
