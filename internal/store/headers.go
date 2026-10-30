package store

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/m7jay/pigeon/internal/atrest"
)

// Custom endpoint headers (FR-405) and their at-rest encryption (FR-1006).
//
// An endpoint may carry up to five headers that the delivery worker adds to
// every request it sends it. Four names are refused outright by FR-405 and four
// more by the platform's own wire contract (FR-1001, FR-810) - refused at
// registration, never dropped silently, because a header a consumer believes it
// configured and the platform quietly discards is worse than a refusal.
//
// The values are stored encrypted (FR-1006) and there is no path in this package
// that reads one back: a read produces a CustomHeaderView, whose value is always
// MaskedHeaderValue. Only two places ever hold the plaintext - the seal in
// CreateEndpoint/CreateConsumerEndpoint, and the decrypt the delivery claim does
// for the request it is about to send.

const (
	// MaxCustomHeaders is FR-405's per-endpoint bound.
	MaxCustomHeaders = 5

	// MaskedHeaderValue is what every read returns in place of a value: the API
	// and the portal are structurally unable to render a plaintext, because this
	// is the only value a read can produce (FR-1006).
	MaskedHeaderValue = "••••••••"

	// maxHeaderNameLen and maxHeaderValueLen bound one stored entry. HTTP itself
	// sets no limit on either; the platform does, so one endpoint cannot turn the
	// column (or the request) into a payload. Both are far above any real header.
	maxHeaderNameLen  = 128
	maxHeaderValueLen = 4096
)

// CustomHeader is one custom header on a WRITE path: the name and the value in
// the clear. It exists only between the API layer and the seal, and between the
// delivery claim and the outbound request. No read produces one.
type CustomHeader struct {
	Name  string
	Value string
}

// CustomHeaderView is one custom header as it is READ BACK: the name in the
// clear (it is not secret and the operator needs to know what is configured) and
// the value replaced by MaskedHeaderValue (FR-1006).
type CustomHeaderView struct {
	Name string
	// Value is always MaskedHeaderValue. It is a field rather than an implicit
	// fact about the name so that a client sees that a value is configured
	// without ever being told what it is.
	Value string
}

// reservedHeaderNames are the names registration refuses, with the reason the
// API shows the caller. FR-405 mandates the first four; the platform's own four
// are here because the delivery worker sets them itself (FR-1001, FR-810), and
// a custom header that could overwrite a signature header would break the
// consumer's verifier in a way no error message would explain.
//
// The map is keyed by the lowercased name: header names are case-insensitive
// (RFC 9110), so `content-TYPE` must not slip past as an unknown name.
var reservedHeaderNames = map[string]string{
	"host":              "a custom Host header would let an endpoint's request be addressed somewhere other than its registered URL (FR-405)",
	"content-length":    "Content-Length is set by the transport from the body it sends (FR-405)",
	"content-type":      "Content-Type is the platform's own, application/json (FR-405, FR-801)",
	"authorization":     "an Authorization header set here would be a credential the platform stores, logs and cannot rotate (FR-405)",
	"webhook-id":        "Webhook-Id identifies the event and is set by the signer (FR-1001)",
	"webhook-timestamp": "Webhook-Timestamp is set by the signer (FR-1001)",
	"webhook-signature": "Webhook-Signature is the Standard Webhooks signature (FR-1001)",
	"user-agent":        "User-Agent identifies the platform (FR-810)",
}

// validateCustomHeaders applies FR-405 to a registration request. Every refusal
// wraps ErrInvalid, so the API answers 422 with the reason (and never with the
// value: a message that quoted one would put the secret in the client's logs,
// which is exactly what FR-1006 forbids).
func validateCustomHeaders(in []CustomHeader) error {
	if len(in) > MaxCustomHeaders {
		return fmt.Errorf("%w: at most %d custom headers per endpoint, got %d (FR-405)",
			ErrInvalid, MaxCustomHeaders, len(in))
	}
	seen := make(map[string]bool, len(in))
	for i, h := range in {
		name := strings.TrimSpace(h.Name)
		if name == "" {
			return fmt.Errorf("%w: custom_headers[%d].name is required", ErrInvalid, i)
		}
		if len(name) > maxHeaderNameLen {
			return fmt.Errorf("%w: custom_headers[%d].name is %d bytes, over the %d-byte limit",
				ErrInvalid, i, len(name), maxHeaderNameLen)
		}
		if !isToken(name) {
			return fmt.Errorf("%w: custom_headers[%d].name %q is not a valid HTTP header name",
				ErrInvalid, i, name)
		}
		canonical := strings.ToLower(name)
		if reason, reserved := reservedHeaderNames[canonical]; reserved {
			return fmt.Errorf("%w: the %s header is reserved and cannot be set per endpoint: %s",
				ErrInvalid, name, reason)
		}
		if seen[canonical] {
			return fmt.Errorf("%w: custom_headers names %q more than once", ErrInvalid, name)
		}
		seen[canonical] = true
		if len(h.Value) > maxHeaderValueLen {
			return fmt.Errorf("%w: custom_headers[%d] (%s) is %d bytes, over the %d-byte limit",
				ErrInvalid, i, name, len(h.Value), maxHeaderValueLen)
		}
		if bad, ok := firstControlByte(h.Value); ok {
			// The message names the byte, never the value: the value is the
			// secret (FR-1006), and a CR or an LF here is a header-injection
			// attempt (or a config file pasted into the wrong field).
			return fmt.Errorf("%w: custom_headers[%d] (%s) contains control byte 0x%02x: "+
				"a header value may not carry CR, LF or NUL", ErrInvalid, i, name, bad)
		}
	}
	return nil
}

// isToken reports whether s is a non-empty RFC 9110 token (`tchar`). Anything
// else - a colon, a space, a non-ASCII byte - is not a header name, and letting
// it through would produce a request the transport refuses at delivery time,
// long after the caller could fix it.
func isToken(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		case strings.IndexByte("!#$%&'*+-.^_`|~", c) >= 0:
		default:
			return false
		}
	}
	return len(s) > 0
}

// firstControlByte returns the first byte that may not appear in a header value
// (RFC 9110: field values carry SP, HTAB, VCHAR and obs-text). It returns the
// byte and true when one is present.
func firstControlByte(v string) (byte, bool) {
	for i := 0; i < len(v); i++ {
		c := v[i]
		if c == '\t' {
			continue // HTAB is legal in a field value
		}
		if c < 0x20 || c == 0x7f {
			return c, true
		}
	}
	return 0, false
}

// sealedHeader is one entry of the endpoints.custom_headers column: the name in
// the clear and the value as ciphertext (FR-1006).
type sealedHeader struct {
	Name    string `json:"name"`
	ValueCT string `json:"value_ct"`
}

// headerAAD names the row AND the field a sealed value belongs to. One endpoints
// row holds up to five encrypted values (FR-405's bound), so without the name a
// ciphertext could be copied from one header onto another inside the same row and
// would still authenticate - the value would then be delivered under a name it
// was never sealed for. It is built here, next to the SQL that names the row, so
// the write and the read cannot drift.
func headerAAD(endpointID, name string) string {
	return "pigeon:endpoint-custom-headers:" + endpointID + ":" + name
}

// sealHeaders renders the column for a write. Names are kept as given (trimmed),
// so a read shows back what the caller wrote; values are sealed under the AAD of
// the endpoint AND the header name, so a value cannot be moved between names in
// the row it lives in. An empty list is the column default and needs no key: an
// endpoint with no custom headers must be registrable on a deployment that has
// not configured one (there is nothing to encrypt).
func (s *Store) sealHeaders(endpointID string, in []CustomHeader) ([]byte, error) {
	if len(in) == 0 {
		return []byte("[]"), nil
	}
	if !s.headersKey.Configured() {
		// Not ErrInvalid: this is the server missing its key, not the caller's
		// mistake. config.Validate refuses to boot without one, so reaching this
		// means a caller built a Store with no key.
		return nil, fmt.Errorf("store: cannot store custom header values: %w", atrest.ErrNoKey)
	}
	out := make([]sealedHeader, 0, len(in))
	for _, h := range in {
		// The name that is stored is the name that goes into the AAD: the read
		// authenticates against what the row holds, so the two cannot drift by a
		// trim or a re-render.
		name := strings.TrimSpace(h.Name)
		ct, err := s.headersKey.Seal([]byte(h.Value), headerAAD(endpointID, name))
		if err != nil {
			// err never carries the plaintext (internal/atrest guarantees it);
			// the name is not secret.
			return nil, fmt.Errorf("store: seal custom header %q: %w", name, err)
		}
		out = append(out, sealedHeader{Name: name, ValueCT: ct})
	}
	return json.Marshal(out)
}

// openSealedHeaders decrypts the column for the DELIVERY path alone (FR-405).
// It returns the headers that authenticated plus the names of those that did
// not: a value sealed for another endpoint, or for another header name in this
// one, or under a key this process does not have, is dropped rather than sent,
// and the delivery still goes out - a header nobody can authenticate must never
// reach a receiver, and one bad value must not wedge a delivery.
func (s *Store) openSealedHeaders(endpointID string, raw []byte) ([]CustomHeader, []string, error) {
	entries, err := sealedHeaderEntries(raw)
	if err != nil {
		return nil, nil, err
	}
	var out []CustomHeader
	var dropped []string
	for _, e := range entries {
		plaintext, err := s.headersKey.Open(e.ValueCT, headerAAD(endpointID, e.Name))
		if err != nil {
			dropped = append(dropped, e.Name)
			continue
		}
		out = append(out, CustomHeader{Name: e.Name, Value: string(plaintext)})
	}
	return out, dropped, nil
}

// summariseHeaders renders the column for every READ path: names in the clear,
// values masked (FR-1006). It never decrypts, so a read cannot leak a value even
// if the key is wrong, missing or rotated.
func summariseHeaders(raw []byte) ([]CustomHeaderView, error) {
	entries, err := sealedHeaderEntries(raw)
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		return nil, nil
	}
	out := make([]CustomHeaderView, 0, len(entries))
	for _, e := range entries {
		out = append(out, CustomHeaderView{Name: e.Name, Value: MaskedHeaderValue})
	}
	return out, nil
}

// sealedHeaderEntries parses the column. A malformed column is an error rather
// than an empty list: it means something wrote the column outside this package,
// and pretending the endpoint has no headers would hide that.
func sealedHeaderEntries(raw []byte) ([]sealedHeader, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var entries []sealedHeader
	if err := json.Unmarshal(raw, &entries); err != nil {
		return nil, fmt.Errorf("store: custom_headers is not a JSON array of header entries: %w", err)
	}
	return entries, nil
}
