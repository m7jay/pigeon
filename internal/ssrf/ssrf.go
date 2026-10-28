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
			// The reason travels with the rejection: "169.254.169.254 is
			// link-local, which includes the cloud metadata services" tells a
			// producer more than "blocked" ever will, and tells a reviewer
			// reading the 422 that the right rule fired.
			return fmt.Errorf("%w: %s resolves to %s (%s)", ErrPrivateRange, host, ip, BlockedReasonFor(ip))
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
	ips, err := lookupIP(host)
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

// blockedRanges are the ranges the platform will not call that netip's own
// predicates do not name. They are listed as prefixes rather than tested bit by
// bit so that each one is visible and citable: a reviewer checking "is the cloud
// metadata range covered?" should be able to read the answer.
//
//   - 169.254.0.0/16 is link-local. netip reports it via IsLinkLocalUnicast, and
//     it contains the cloud metadata services (169.254.169.254 for the instance
//     metadata, 169.254.170.2 for ECS task credentials, 169.254.169.123 for the
//     Amazon time service). AC-19's case is in here.
//   - 100.64.0.0/10 is carrier-grade NAT. IsPrivate deliberately does not cover
//     it (it is not RFC 1918), but on a cloud network it is where a provider's
//     internal services live.
//   - 192.0.0.0/24 is IETF protocol assignments, which includes the NAT64 and
//     DNS64 discovery addresses.
//   - 198.18.0.0/15 is benchmarking.
//   - 240.0.0.0/4 is reserved, and 255.255.255.255 is the broadcast address.
//   - 2002::/16 is 6to4, which embeds an IPv4 address: ::169.254.169.254 is
//     reachable through it on a host with 6to4 configured, so blocking the v4
//     form alone is not enough. 64:ff9b::/96 is NAT64, which embeds it too.
var blockedRanges = []netip.Prefix{
	netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("192.0.0.0/24"),
	netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("192.0.2.0/24"),
	netip.MustParsePrefix("198.51.100.0/24"),
	netip.MustParsePrefix("203.0.113.0/24"),
	netip.MustParsePrefix("240.0.0.0/4"),
	netip.MustParsePrefix("2002::/16"),
	netip.MustParsePrefix("64:ff9b::/96"),
}

// isBlocked reports whether an addr falls in a range the platform will not call.
//
// Loopback, private (RFC 1918 and fc00::/7), link-local unicast and multicast
// (which covers the 169.254.169.254 cloud metadata address), unspecified, any
// interface-local multicast, and the ranges blockedRanges names are all rejected.
// A global unicast address is allowed. There is no allowlist: the rule is "not
// provably public", because an endpoint the platform cannot prove is public is one
// it will not call.
func isBlocked(a netip.Addr) bool {
	a = a.Unmap()
	if a.IsLoopback() || a.IsPrivate() || a.IsLinkLocalUnicast() ||
		a.IsLinkLocalMulticast() || a.IsUnspecified() || a.IsInterfaceLocalMulticast() ||
		a.IsMulticast() {
		return true
	}
	for _, p := range blockedRanges {
		if p.Contains(a) {
			return true
		}
	}
	return false
}

// BlockedReasonFor is the human half of isBlocked, for a check that wants to say
// which rule rejected an address rather than only that one did.
func BlockedReasonFor(a netip.Addr) string {
	a = a.Unmap()
	switch {
	case a.IsLoopback():
		return "loopback"
	case a.IsPrivate():
		return "private (RFC 1918 / fc00::/7)"
	case a.IsLinkLocalUnicast():
		return "link-local, which includes the cloud metadata services"
	case a.IsUnspecified():
		return "unspecified (0.0.0.0 / ::)"
	case a.IsMulticast():
		return "multicast"
	}
	for _, p := range blockedRanges {
		if p.Contains(a) {
			return "reserved range " + p.String()
		}
	}
	return "not blocked"
}
