package main

import (
	"context"
	"log/slog"

	"github.com/m7jay/pigeon/internal/config"
	"github.com/m7jay/pigeon/internal/httpapi"
	"github.com/m7jay/pigeon/internal/oidc"
)

// S9a's two auth features are wired here rather than inline in run(), because
// both of them are "configured or absent" and the decision to be absent deserves
// to be readable in one place.

// oidcVerifier builds FR-106's trust anchor, or returns nil when the operator
// configured no issuer. The verifier discovers the IdP lazily (internal/oidc), so
// this does no I/O: an instance whose IdP is unreachable still starts and still
// serves password logins, and no anonymous caller can turn the login route into a
// discovery amplifier.
func oidcVerifier(cfg config.Config) httpapi.OIDCVerifier {
	if cfg.OIDCIssuer == "" {
		// Off, and the API says so: POST /v1/session/oidc answers 404 and neither
		// portal renders an OIDC control.
		return nil
	}
	return oidc.NewLazyVerifier(oidc.Config{
		Issuer:   cfg.OIDCIssuer,
		ClientID: cfg.OIDCClientID,
	})
}

// resetNotifier is FR-111's delivery transport for v1.0, which ships no mailer.
//
// The seam exists so a deployment can wire its own (an SMTP client, a queue, a
// transactional-mail API); this default is deliberately narrow. Unless the
// operator asks for it, there is NO transport: the reset row is minted and the
// API logs that nothing could deliver it. With PIGEON_PASSWORD_RESET_LOG_TOKENS
// on, the full link is written to the service log, which is a single-operator
// convenience (a self-hosted instance whose only operator reads the logs) and not
// a safe default on a shared one - a reset link in a shared log is a working
// credential in a shared log.
func resetNotifier(log *slog.Logger, logTokens bool) httpapi.PasswordResetNotifier {
	if !logTokens {
		return nil
	}
	return func(ctx context.Context, email, token string) error {
		log.Warn("password reset link minted (PIGEON_PASSWORD_RESET_LOG_TOKENS is on: "+
			"this puts a working credential in this log, do not enable it on a shared instance)",
			"email", email, "token", token,
			"accept_with", "POST /v1/password-resets/accept {\"token\": \"...\", \"new_password\": \"...\"}")
		return nil
	}
}
