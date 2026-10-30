package delivery

import (
	"fmt"

	"github.com/m7jay/pigeon/internal/ssrf"
	"github.com/m7jay/pigeon/internal/store"
)

// This file is FR-1005's third call site, request time, in two halves.
//
//  1. ssrfRefusal re-runs the registration check against the endpoint's stored URL
//     before every attempt. It re-resolves the name, so a host that has since been
//     pointed at an internal address is refused here rather than at the first
//     connect.
//  2. The transport installed by clientFor also carries ssrf.DialControl, which
//     validates the address that is actually being connected to. That one closes
//     the remaining window: a name can change between the check in (1) and the
//     connection, and the dial hook has no such gap because it sees the resolved
//     address immediately before connect(2).
//
// Both take the endpoint's FR-402 escape hatch from the claim row, never from
// anything the endpoint can influence at request time, and never from a read of
// their own: the flag arrives beside the URL and the signing secret, read by
// pigeon_claim_delivery under its own definer rights (0022). A worker-side read by
// endpoint id cannot work - the runtime pool is RLS-constrained, the policies on
// `endpoints` are scoped by producer and env, and the worker has no tenant at that
// point. Worse, it fails in the direction that silently disables the hatch: the
// read finds no row, the flag reads false, and an endpoint the operator explicitly
// opted in is treated as public and refused, terminally, before a socket is
// opened. Carrying the flag in the claim removes that failure mode rather than
// handling it - a claim that cannot return the flag yields no delivery to attempt -
// and it costs no extra query, because the claim already reads this row.

// ssrfRefusal returns the terminal attempt result for a URL that must not be
// called, and whether it refused. A terminal result is the right classification:
// the address is either permanently blocked or being rebound, and neither gets
// better by retrying it fifteen times over §9's 72 h window.
func ssrfRefusal(d store.LeasedDelivery, allowPrivate bool) (attemptResult, bool) {
	if allowPrivate {
		return attemptResult{}, false
	}
	if err := ssrf.CheckEndpointURL(d.EndpointURL, d.Env, false); err != nil {
		return attemptResult{
			outcome:    outcomeTerminal,
			errorClass: classDialError,
			// No status: the request was never sent, so there is no response to
			// record, and inventing one would put a number in the attempt trail
			// that no peer chose.
			sendErr: fmt.Errorf("request-time SSRF check: %w", err),
		}, true
	}
	return attemptResult{}, false
}
