package ssrf

import (
	"errors"
	"net"
	"net/netip"
	"strings"
	"testing"
)

// withResolver swaps the package's resolver for the duration of a test, so the
// rebinding cases are deterministic instead of depending on the network.
func withResolver(t *testing.T, fn func(string) ([]net.IP, error)) {
	t.Helper()
	old := lookupIP
	lookupIP = fn
	t.Cleanup(func() { lookupIP = old })
}

// TestRebindingIsCaughtAtEveryCallSite is FR-1005's whole argument: the same name
// answers a public address at one moment and an internal one at the next, so a
// check that ran once is not protection.
//
// The three call sites are exercised in the order they run in production -
// registration, verification, request time - with the resolver's answer changing
// between them. Registration passes, verification passes on a re-query of the same
// public answer, and then the answer flips: the request-time check must refuse.
func TestRebindingIsCaughtAtEveryCallSite(t *testing.T) {
	var answer []net.IP
	calls := 0
	withResolver(t, func(string) ([]net.IP, error) {
		calls++
		return answer, nil
	})

	answer = []net.IP{net.ParseIP("93.184.216.34")} // a public address
	if err := CheckEndpointURL("https://hook.example.com/x", "live", false); err != nil {
		t.Fatalf("registration rejected a public name: %v", err)
	}
	if err := CheckVerificationURL("https://hook.example.com/x", "live", false); err != nil {
		t.Fatalf("verification rejected a public name: %v", err)
	}

	// The name is rebound to the instance metadata service.
	answer = []net.IP{net.ParseIP("169.254.169.254")}
	err := CheckEndpointURL("https://hook.example.com/x", "live", false)
	if err == nil {
		t.Fatal("the request-time check accepted a rebinding name; AC-19's address is reachable through it")
	}
	if !errors.Is(err, ErrBlocked) {
		t.Fatalf("err = %v, want it to wrap ErrBlocked so the caller answers 422", err)
	}
	if !strings.Contains(err.Error(), "link-local") {
		t.Errorf("refusal %q does not say which rule fired", err)
	}
	if calls < 3 {
		t.Fatalf("the resolver was queried %d times; each call site must re-resolve or the answers could be stale", calls)
	}
}

// TestCheckVerificationURLRejectsAChangedAnswer is the second call site on its own:
// a name that was public at registration and is internal by the time the endpoint
// answers its challenge is not verified.
func TestCheckVerificationURLRejectsAChangedAnswer(t *testing.T) {
	withResolver(t, func(string) ([]net.IP, error) {
		return []net.IP{net.ParseIP("10.1.2.3")}, nil
	})
	err := CheckVerificationURL("https://hook.example.com/x", "live", false)
	if err == nil {
		t.Fatal("verification accepted a name that resolves into RFC 1918")
	}
	if !strings.Contains(err.Error(), "verification") {
		t.Errorf("refusal %q does not say which call site refused", err)
	}
	// FR-402's escape hatch still applies: the operator opted this endpoint in.
	if err := CheckVerificationURL("https://hook.example.com/x", "live", true); err != nil {
		t.Fatalf("an internal endpoint was refused at verification: %v", err)
	}
}

// TestDialControlIsTheOnlyCheckWithoutAWindow drives the hook net.Dialer.Control
// installs. It is handed the *resolved* address, which is why it is the check that
// closes the rebinding window rather than narrowing it: there is no gap between
// the address it judges and the address the kernel dials.
func TestDialControlIsTheOnlyCheckWithoutAWindow(t *testing.T) {
	blocked := []struct {
		address string
		why     string
	}{
		{"169.254.169.254:80", "the instance metadata service (AC-19)"},
		{"169.254.170.2:80", "the ECS task-credential service"},
		{"127.0.0.1:8080", "loopback"},
		{"10.0.0.5:443", "RFC 1918"},
		{"192.168.1.1:80", "RFC 1918"},
		{"172.16.0.1:80", "RFC 1918"},
		{"100.64.0.1:80", "carrier-grade NAT"},
		{"0.0.0.0:80", "the unspecified address"},
		{"[::1]:80", "IPv6 loopback"},
		{"[::ffff:169.254.169.254]:80", "the IPv4 metadata address, mapped"},
		{"[fd00::1]:80", "IPv6 unique-local"},
		{"[fe80::1]:80", "IPv6 link-local"},
		{"[2002:7f00:1::1]:80", "6to4 wrapping 127.0.0.1"},
		{"[64:ff9b::a9fe:a9fe]:80", "NAT64 wrapping 169.254.169.254"},
		{"garbage", "an address the check cannot parse, which fails closed"},
	}
	for _, tc := range blocked {
		if err := CheckDialAddress("tcp", tc.address, false); err == nil {
			t.Errorf("dial to %s was allowed: it is %s", tc.address, tc.why)
		}
	}

	allowed := []string{"93.184.216.34:443", "[2606:2800:220:1:248:1893:25c8:1946]:443"}
	for _, address := range allowed {
		if err := CheckDialAddress("tcp", address, false); err != nil {
			t.Errorf("dial to the public address %s was refused: %v", address, err)
		}
	}

	// FR-402: the operator's opt-in is the only thing that widens this.
	if err := CheckDialAddress("tcp", "10.0.0.5:80", true); err != nil {
		t.Errorf("an internal endpoint could not dial a private address: %v", err)
	}
}

// TestDialControlResolvesAHostname covers the pre-attempt caller, which passes the
// host the URL names rather than a resolved address.
func TestDialControlResolvesAHostname(t *testing.T) {
	withResolver(t, func(host string) ([]net.IP, error) {
		if host != "hook.example.com" {
			t.Fatalf("unexpected lookup for %q", host)
		}
		return []net.IP{net.ParseIP("127.0.0.1")}, nil
	})
	if err := CheckDialAddress("tcp", "hook.example.com:80", false); err == nil {
		t.Fatal("a hostname resolving to loopback was dialable")
	}
}

// TestIsBlockedCoversTheRangesThatMatter is the table the reviewers will read. It
// states the classification of one address per rule, so a regression in isBlocked
// names the range it broke rather than only that something did.
func TestIsBlockedCoversTheRangesThatMatter(t *testing.T) {
	cases := []struct {
		addr    string
		blocked bool
	}{
		{"169.254.169.254", true},  // link-local: cloud metadata (AC-19)
		{"169.254.169.123", true},  // Amazon time service
		{"100.100.100.200", true},  // Alibaba Cloud metadata (in 100.64/10)
		{"192.0.0.192", true},      // Oracle Cloud metadata (IETF assignments)
		{"127.0.0.1", true},        // loopback
		{"10.1.1.1", true},         // RFC 1918
		{"172.31.255.255", true},   // RFC 1918 upper bound
		{"192.168.0.1", true},      // RFC 1918
		{"fd12:3456::1", true},     // IPv6 unique-local
		{"fe80::1", true},          // IPv6 link-local
		{"::", true},               // unspecified
		{"224.0.0.1", true},        // multicast
		{"255.255.255.255", true},  // broadcast (in 240/4)
		{"93.184.216.34", false},   // example.com
		{"8.8.8.8", false},         // public DNS
		{"2606:4700::1111", false}, // public IPv6
	}
	for _, tc := range cases {
		addr := netip.MustParseAddr(tc.addr)
		if got := isBlocked(addr); got != tc.blocked {
			t.Errorf("isBlocked(%s) = %v, want %v (%s)", tc.addr, got, tc.blocked, BlockedReasonFor(addr))
		}
	}
}
