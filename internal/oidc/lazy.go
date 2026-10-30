package oidc

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// FR-106 is an *optional* login, and that has to hold at boot as well as in the
// UI: an instance whose IdP happens to be unreachable must still start, keep
// serving password logins, and recover on its own when the IdP comes back. So
// both halves hold a LazyProvider rather than a *Provider from Discover: the
// discovery call happens on the first use, is cached once it succeeds, and is
// retried after a failure no more often than discoverRetryInterval - so a broken
// issuer cannot be turned into a request amplifier by an anonymous caller who
// posts id_tokens at the login route (the core) or follows the login link (the
// portal).
//
// None of this weakens verification: a LazyVerifier that has never discovered a
// provider refuses every token. Failing closed here means "no OIDC login right
// now", never "no verification right now".

// discoverRetryInterval bounds how often a failed discovery is retried.
const discoverRetryInterval = 30 * time.Second

// LazyProvider is an OpenID Connect provider discovered on first use. It is safe
// for concurrent use.
type LazyProvider struct {
	cfg Config

	mu          sync.Mutex
	provider    *Provider
	lastAttempt time.Time
	lastErr     error
}

// NewLazyProvider returns a provider for cfg. It performs no I/O, so a caller can
// build one before the network is up - which is the case it exists for.
func NewLazyProvider(cfg Config) *LazyProvider { return &LazyProvider{cfg: cfg} }

// Config is the configuration this provider was built from.
func (l *LazyProvider) Config() Config { return l.cfg }

// Enabled reports whether the configuration can verify id_tokens at all.
func (l *LazyProvider) Enabled() bool { return l.cfg.ServerEnabled() }

// Provider returns the discovered provider, discovering it if there is none and
// the retry interval has elapsed since the last failure.
func (l *LazyProvider) Provider(ctx context.Context) (*Provider, error) {
	if !l.cfg.ServerEnabled() {
		// Not "an unknown issuer": the process was never given one. Saying so keeps
		// a misconfiguration out of the log as a token failure.
		return nil, fmt.Errorf("%w: no issuer configured", ErrInvalidToken)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.provider != nil {
		return l.provider, nil
	}
	if !l.lastAttempt.IsZero() && l.cfg.now().Sub(l.lastAttempt) < discoverRetryInterval {
		// Recently failed: answer from the recorded error rather than hammering an
		// issuer that is already down.
		return nil, fmt.Errorf("%w (retrying in %s): %v",
			ErrDiscoveryFailed, discoverRetryInterval-l.cfg.now().Sub(l.lastAttempt), l.lastErr)
	}
	l.lastAttempt = l.cfg.now()
	p, err := Discover(ctx, l.cfg)
	if err != nil {
		l.lastErr = err
		return nil, err
	}
	l.provider = p
	l.lastErr = nil
	return p, nil
}

// LazyVerifier is the core API server's trust anchor: a LazyProvider plus the
// id_token verification the login route needs. It implements httpapi.OIDCVerifier.
type LazyVerifier struct{ *LazyProvider }

// NewLazyVerifier returns a verifier for cfg.
func NewLazyVerifier(cfg Config) *LazyVerifier { return &LazyVerifier{NewLazyProvider(cfg)} }

// VerifyIDToken discovers the issuer when it has to, then verifies against the
// provider it found.
func (l *LazyVerifier) VerifyIDToken(ctx context.Context, raw string) (Claims, error) {
	p, err := l.Provider(ctx)
	if err != nil {
		return Claims{}, err
	}
	return p.VerifyIDToken(ctx, raw)
}
