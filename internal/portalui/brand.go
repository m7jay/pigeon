package portalui

import (
	"html/template"
	"os"
	"regexp"
	"strings"
)

// Brand is the visual identity a portal presents (UI-17). The consumer portal
// carries the producer's brand, not Pigeon's: the producer operates the consumer
// portal deployable and configures its name, accent colour and logo through the
// environment (PORTAL_BRAND_NAME, PORTAL_BRAND_ACCENT, PORTAL_BRAND_LOGO_URL).
//
// There is deliberately no per-producer branding row in the API, so the brand is
// a property of the portal deployable, not something the core serves. That is
// consistent with §14's "independently themeable": each portal is themed by the
// party that runs it.
type Brand struct {
	Name        string // product name shown in the top bar; empty -> neutral fallback
	AccentColor string // CSS colour for the active nav item, buttons, links
	LogoURL     string // optional logo image src; empty -> no <img>
	Company     string // small caption under the name, e.g. the producer's legal name
}

// NeutralFallback is what a portal shows when the operator configures no brand.
// It carries no "Pigeon" branding (UI-17): a plain wordmark and a neutral slate
// accent, and no logo. A producer that supplies its own brand overrides every
// field.
var NeutralFallback = Brand{
	Name:        "Events",
	AccentColor: "#334155", // slate-700
	LogoURL:     "",
	Company:     "",
}

// FromEnv builds a Brand from the environment, layering the supplied values over
// the neutral fallback so an unset field falls back rather than going blank.
func FromEnv() Brand {
	b := NeutralFallback
	if v := strings.TrimSpace(os.Getenv("PORTAL_BRAND_NAME")); v != "" {
		b.Name = v
	}
	if v := strings.TrimSpace(os.Getenv("PORTAL_BRAND_ACCENT")); v != "" {
		b.AccentColor = v
	}
	if v := strings.TrimSpace(os.Getenv("PORTAL_BRAND_LOGO_URL")); v != "" {
		b.LogoURL = v
	}
	if v := strings.TrimSpace(os.Getenv("PORTAL_BRAND_COMPANY")); v != "" {
		b.Company = v
	}
	return b.withDefaults()
}

// withDefaults fills any still-empty field from the neutral fallback, so a
// partial configuration never renders an empty brand name or a blank accent.
func (b Brand) withDefaults() Brand {
	if b.Name == "" {
		b.Name = NeutralFallback.Name
	}
	if b.AccentColor == "" {
		b.AccentColor = NeutralFallback.AccentColor
	}
	return b
}

// accentPattern is a deliberately tight allowlist for the accent colour: a
// 3-, 4-, 6- or 8-digit hex hash, or one of the handful of CSS named colours a
// brand colour could reasonably be. Anything else falls back to the neutral
// accent, so a misconfigured (or injected) PORTAL_BRAND_ACCENT cannot break out
// of the <style> element or render garbage.
var accentPattern = regexp.MustCompile(`^(#[0-9a-fA-F]{3,8}|[a-z-]{3,20})$`)

// safeAccent returns the accent if it is a permitted colour, else the fallback.
func safeAccent(color, fallback string) string {
	color = strings.TrimSpace(color)
	if accentPattern.MatchString(strings.ToLower(color)) {
		return color
	}
	return fallback
}

// CSS returns the deployable-specific custom properties as a template.CSS value
// the base layout injects into <head>. Returning template.CSS (not a bare
// string) is what lets html/template emit it inside <style> without blanking it
// as unsafe; the accent is validated by safeAccent first, so the only values
// that reach here are inert.
func (b Brand) CSS() template.CSS {
	accent := safeAccent(b.AccentColor, NeutralFallback.AccentColor)
	return template.CSS(":root{--brand-accent:" + accent + ";--brand-accent-dim:" + accent + "cc}")
}
