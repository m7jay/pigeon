package ssrf

import (
	"fmt"
	"net"
	"net/netip"
	"strings"
	"syscall"
)

// This file is FR-1005's second and third call sites. Registration alone is not
// SSRF protection: a host can be public when the endpoint is registered and
// internal when the delivery is attempted (DNS rebinding), and a name can resolve
// to a different address on every query. So the check runs again at verification
// time, and again on the address the transport is actually about to connect to.

// lookupIP is the resolver, as a variable so a test can drive it. Production uses
// the platform resolver; nothing else in the package reads the network.
var lookupIP = net.LookupIP

// CheckVerificationURL re-runs the registration check when an endpoint answers
// its ownership challenge (FR-1005).
//
// It is a named entry point rather than a second call to CheckEndpointURL so that
// the three call sites FR-1005 names are each greppable - a reviewer asking "is
// the check at verification time?" should find the answer in the code, not infer
// it from a call graph. The check itself is identical; what differs is when it
// runs, and a name is what makes that visible to the next reader.
func CheckVerificationURL(raw, env string, internal bool) error {
	if err := CheckEndpointURL(raw, env, internal); err != nil {
		return fmt.Errorf("re-checked at verification time: %w", err)
	}
	return nil
}

// Control is the signature net.Dialer.Control wants: a hook that runs for every
// connection attempt, with the address about to be dialled.
type Control func(network, address string, c syscall.RawConn) error

// DialControl returns the hook a delivery transport installs so no attempt can
// reach a blocked address. This is the check that catches rebinding, and the only
// one that can: net.Dialer.Control is called *after* resolution and immediately
// before connect(2), with the literal IP the kernel is about to be asked for, so
// there is no window between the check and the connection for a name to change
// underneath it. A DNS-based check at any earlier point has that window by
// construction.
//
// allowInternal is FR-402's escape hatch, and it is the caller's job to establish
// it from the endpoint's own row - not from the URL, which the endpoint controls.
func DialControl(allowInternal bool) Control {
	return func(network, address string, _ syscall.RawConn) error {
		return CheckDialAddress(network, address, allowInternal)
	}
}

// CheckDialAddress validates one dial target. It is exported because the delivery
// worker calls it directly as well, and because it is the unit a test can drive
// without a socket.
//
// address arrives as host:port with the host already resolved - that is the
// contract of net.Dialer.Control - but a literal host is accepted too, since the
// same function is used by the pre-attempt check where the caller passes what the
// URL says.
func CheckDialAddress(network, address string, allowInternal bool) error {
	if allowInternal {
		return nil
	}
	ip, err := addrFromDialTarget(address)
	if err != nil {
		// Fail closed: an address this function cannot understand is one it cannot
		// prove is public, and the cost of being wrong here is a request to the
		// cloud metadata service.
		return fmt.Errorf("%w: %v", ErrPrivateRange, err)
	}
	if isBlocked(ip) {
		return fmt.Errorf("%w: %s is the address the connection would use", ErrPrivateRange, ip)
	}
	return nil
}

// addrFromDialTarget extracts the IP from a dial address. Unix sockets and other
// non-IP networks have no IP to judge and are refused: they are not a webhook
// target, and this hook exists precisely to constrain what a webhook target can
// be.
func addrFromDialTarget(address string) (netip.Addr, error) {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		// No port: treat the whole string as a host.
		host = address
	}
	host = strings.Trim(host, "[]")
	if ip, err := netip.ParseAddr(host); err == nil {
		return ip.Unmap(), nil
	}
	// The dialer resolved already, so this only happens for a hostname that was
	// never resolved - which means the caller passed a URL host rather than a dial
	// target. Resolve it here so the answer is still about addresses, not names.
	ips, err := lookupIP(host)
	if err != nil {
		return netip.Addr{}, fmt.Errorf("resolve %s: %w", host, err)
	}
	if len(ips) == 0 {
		return netip.Addr{}, fmt.Errorf("resolve %s: no addresses", host)
	}
	a, ok := netip.AddrFromSlice(ips[0])
	if !ok {
		return netip.Addr{}, fmt.Errorf("resolve %s: unreadable address", host)
	}
	return a.Unmap(), nil
}
