// Package ssrf rejects webhook URLs that point at infrastructure the platform
// must never call (FR-402, AC-19). It is the registration-time half of the SSRF
// story; the delivery-time half (not following redirects, FR-803) lives with the
// worker.
//
// The check is deliberately fail-closed: a host that does not resolve, or that
// resolves to more than one address of which any is internal, is rejected. An
// endpoint the platform cannot prove is public is an endpoint it will not call.
package ssrf

import (
	"errors"
	"fmt"
	"net"
	"net/netip"
	"net/url"
	"strings"
)

// ErrBlocked is what a disallowed URL returns. The caller decides the status
// code; the store maps it to a 422 (invalid input the caller can fix).
var ErrBlocked = errors.New("url blocked: SSRF protection")

// BlockedReason narrows ErrBlocked for the response body so a producer learns
// whether it sent http in live or an internal address.
var (
	ErrNotHTTP        = fmt.Errorf("%w: scheme must be http or https", ErrBlocked)
	ErrBadScheme      = fmt.Errorf("%w: https is required for a live endpoint", ErrBlocked)
	ErrNoHost         = fmt.Errorf("%w: url has no host", ErrBlocked)
	ErrDoesNotResolve = fmt.Errorf("%w: host does not resolve", ErrBlocked)
	ErrPrivateRange   = fmt.Errorf("%w: address is in a private, loopback, link-local or metadata range", ErrBlocked)
)

// CheckEndpointURL validates a webhook URL for an endpoint registered in env.
// internal is FR-402's escape hatch: an endpoint explicitly marked internal may
// address a private range (an on-prem receiver), and the operator has opted in.
func CheckEndpointURL(raw, env string, internal bool) error {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return fmt.Errorf("%w: %v", ErrBlocked, err)
	}
	scheme := strings.ToLower(u.Scheme)
	if scheme != "http" && scheme != "https" {
		return ErrNotHTTP
	}
	// FR-402: live endpoints must be https. Test endpoints may be http.
	if env == "live" && scheme != "https" {
		return ErrBadScheme
	}
	host := u.Hostname()
	if host == "" {
		return ErrNoHost
	}

	addrs, err := resolve(host)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrDoesNotResolve, err)
	}
	for _, ip := range addrs {
		if !internal && isBlocked(ip) {
			return fmt.Errorf("%w: %s (%s)", ErrPrivateRange, ip, host)
		}
	}
	return nil
}

// resolve returns the IPs a host must be checked against. An IP literal is used
// directly, so the check costs no DNS; a hostname is looked up and every address
// it returns is inspected, because a rebinding host can answer one public and one
// internal address.
func resolve(host string) ([]netip.Addr, error) {
	if ip, err := netip.ParseAddr(host); err == nil {
		return []netip.Addr{ip}, nil
	}
	ips, err := net.LookupIP(host)
	if err != nil {
		return nil, err
	}
	out := make([]netip.Addr, 0, len(ips))
	for _, ip := range ips {
		if a, ok := netip.AddrFromSlice(ip); ok {
			out = append(out, a.Unmap())
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no IP addresses returned")
	}
	return out, nil
}

// isBlocked reports whether an addr falls in a range the platform will not call.
// Loopback, private, link-local (unicast and multicast, which covers the
// 169.254.169.254 cloud metadata address), unspecified and any interface-local
// multicast are all rejected. A global unicast address is allowed.
func isBlocked(a netip.Addr) bool {
	return a.IsLoopback() || a.IsPrivate() || a.IsLinkLocalUnicast() ||
		a.IsLinkLocalMulticast() || a.IsUnspecified() || a.IsInterfaceLocalMulticast()
}
