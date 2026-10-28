package delivery

import (
	"context"
	"fmt"
	"sync"

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
// Both take the endpoint's FR-402 escape hatch from its own row, never from
// anything the endpoint can influence at request time.

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

// endpointInternalCache remembers FR-402's flag per endpoint. The claim path does
// not carry it, and reading it per attempt would put a query on the hot path for a
// value that changes only when an operator changes it.
//
// A lookup that fails is cached as "not internal": a database that cannot answer
// must not be able to widen what the worker may dial, so the failure mode is the
// restrictive one.
type endpointInternalCache struct {
	mu sync.RWMutex
	m  map[string]bool
}

func newEndpointInternalCache() *endpointInternalCache {
	return &endpointInternalCache{m: map[string]bool{}}
}

func (c *endpointInternalCache) get(endpointID string) (bool, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	v, ok := c.m[endpointID]
	return v, ok
}

func (c *endpointInternalCache) set(endpointID string, internal bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.m[endpointID] = internal
}

// endpointAllowsPrivate reports whether the attempt may dial a private address.
//
// A lookup error is logged at debug and treated as "not internal". Refusing to
// deliver because a flag could not be read would be a worse failure than
// delivering to a public address, and the dial-time control still blocks the
// private one - so the only outcome of a failed lookup is that an internal
// endpoint's delivery fails closed.
func (w *Worker) endpointAllowsPrivate(ctx context.Context, endpointID string) bool {
	if v, ok := w.internalFlags.get(endpointID); ok {
		return v
	}
	internal, err := w.store.EndpointIsInternal(ctx, endpointID)
	if err != nil {
		w.log.Debug("delivery: endpoint internal flag unreadable; treating as public (FR-402)",
			"endpoint_id", endpointID, "error", err)
		// Cache the restrictive answer: a broken read should not cost a query per
		// attempt for a value that is probably still false.
		w.internalFlags.set(endpointID, false)
		return false
	}
	w.internalFlags.set(endpointID, internal)
	return internal
}
