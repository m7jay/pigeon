package httpapi

import (
	"net"
	"net/http"
	"sort"
)

// egressPayload is FR-1007's answer: the addresses this instance delivers from,
// so a consumer can allowlist them before an endpoint starts refusing traffic.
type egressPayload struct {
	// IPs is the allowlist. An entry may be a bare address ("203.0.113.7") or a
	// CIDR block ("203.0.113.0/24"), exactly as the operator configured it.
	IPs []string `json:"ips"`
	// Source says where IPs came from: "configured" when PIGEON_EGRESS_IPS is
	// set, "discovered" when they were read off the host's interfaces.
	Source string `json:"source"`
	// Note is present only for the discovered case, and says why an operator
	// should still set the knob. A bare address list that silently means "the
	// container's private address" is worse than no answer: a consumer would
	// allowlist something that never appears as the source of a delivery.
	Note string `json:"note,omitempty"`
}

// egressDiscoveredNote is FR-1007's honesty clause. Behind NAT, a load balancer,
// a proxy or a container network the interface addresses are *not* what a
// receiver sees, and the instance cannot find out what that is - so it says so
// rather than publishing a list that looks authoritative.
const egressDiscoveredNote = "these are this host's own interface addresses, read at request time; behind NAT, a load balancer, a proxy or a container network they are not the addresses a receiver sees - set PIGEON_EGRESS_IPS to the address list your receivers should allow"

// handleEgressIPs is GET /v1/meta/egress-ips (FR-1007).
//
// It is kindSession rather than kindAdmin or kindMember: the answer is identical
// for every tenant (it describes the deployment, not tenant data), and the
// people who need it are whoever configures an allowlist - a producer member
// adding an endpoint and a consumer user running one are both authenticated
// sessions, and a producer API key has no business here because it cannot read
// another producer's configuration either.
func (s *Server) handleEgressIPs(w http.ResponseWriter, _ *http.Request) {
	ips, source := s.egressIPs()
	payload := egressPayload{IPs: ips, Source: source}
	if source == egressSourceDiscovered {
		payload.Note = egressDiscoveredNote
	}
	writeJSON(w, http.StatusOK, payload)
}

const (
	egressSourceConfigured = "configured"
	egressSourceDiscovered = "discovered"
)

// egressIPs prefers the operator's declaration and falls back to discovery.
// Both paths are sorted so the same instance answers the same way twice: a
// receiver comparing two responses is looking for "did the egress changed", and
// an unstable order would read as one.
func (s *Server) egressIPs() ([]string, string) {
	if len(s.deps.EgressIPs) > 0 {
		out := append([]string(nil), s.deps.EgressIPs...)
		sort.Strings(out)
		return out, egressSourceConfigured
	}
	discover := s.deps.DiscoverEgress
	if discover == nil {
		discover = hostInterfaceAddresses
	}
	ips := discover()
	if len(ips) == 0 {
		// An empty array, not JSON null: a consumer's allowlist tooling should
		// not have to tell "no addresses" from "a bug in the encoder".
		ips = []string{}
	}
	sort.Strings(ips)
	return ips, egressSourceDiscovered
}

// hostInterfaceAddresses returns the host's non-loopback, non-link-local unicast
// addresses - what an outbound connection would source from on a flat network.
// It is deliberately not an egress lookup: the public address of a NAT'd
// instance is not discoverable from inside it.
func hostInterfaceAddresses() []string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil
	}
	out := make([]string, 0, len(addrs))
	for _, a := range addrs {
		var ip net.IP
		switch v := a.(type) {
		case *net.IPNet:
			ip = v.IP
		case *net.IPAddr:
			ip = v.IP
		}
		if ip == nil || ip.IsUnspecified() || ip.IsLoopback() ||
			ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsMulticast() {
			continue
		}
		out = append(out, ip.String())
	}
	return out
}
