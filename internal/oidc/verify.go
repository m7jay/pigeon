package oidc

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rsa"
	"encoding/asn1"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
	"time"
)

// This file is the verifier: the half the API server runs. Its contract is that
// nothing in a token is believed until the signature has been checked against a
// key the IdP published and every claim rule below holds. The claim rules are
// the part a library would hide, and they are the part that decides whether a
// token is a login, so they are written out here with the reason for each.

// jwsHeader is the protected header of the compact JWS.
type jwsHeader struct {
	// Alg is the signing algorithm. Only the six RSA/ECDSA values the package
	// implements are accepted; see hashForAlg.
	Alg string `json:"alg"`
	// Kid names the key in the IdP's JWKS. It is required: a token that does not
	// say which key signed it cannot be verified against an anchor.
	Kid string `json:"kid"`
	// Typ is "JWT" in practice and is not used for any decision.
	Typ string `json:"typ"`
}

// VerifyIDToken verifies a compact JWS id_token and returns the identity it
// asserts.
//
// It is the whole of the API server's OIDC surface. Every path out of it that is
// not a verified Claims wraps ErrInvalidToken: a caller can map that to 401 and
// everything else to 500 without reading messages.
func (p *Provider) VerifyIDToken(ctx context.Context, raw string) (Claims, error) {
	if p.jwks == nil {
		return Claims{}, fmt.Errorf("%w: the provider was not built by Discover, so it has no key set", ErrInvalidToken)
	}
	header, payload, signingInput, signature, err := splitJWS(raw)
	if err != nil {
		return Claims{}, err
	}
	// The algorithm is checked before anything else is trusted, and the check is
	// an allow-list: a token that names an algorithm we did not choose to
	// implement is rejected whatever the JWKS happens to contain.
	if _, err := hashForAlg(header.Alg); err != nil {
		return Claims{}, err
	}
	if header.Kid == "" {
		return Claims{}, fmt.Errorf("%w: the token header names no kid, so there is no key to verify it against", ErrInvalidToken)
	}
	key, err := p.jwks.keyByID(ctx, header.Kid)
	if err != nil {
		return Claims{}, err
	}
	if err := verifySignature(header.Alg, key, signingInput, signature); err != nil {
		return Claims{}, err
	}

	// Only now, with the signature verified against the IdP's key, is the payload
	// read. Until this point it is attacker-controlled text.
	var wire rawClaims
	if err := json.Unmarshal(payload, &wire); err != nil {
		return Claims{}, fmt.Errorf("%w: decoding claims: %v", ErrInvalidToken, err)
	}
	claims, err := wire.claims()
	if err != nil {
		return Claims{}, err
	}
	if err := p.validateClaims(claims); err != nil {
		return Claims{}, err
	}
	return claims, nil
}

// splitJWS splits a compact JWS into its header, its payload, the signing input
// the signature covers and the signature itself. Anything that is not exactly
// three non-empty base64url parts is rejected: a JWS with five parts is a JWE,
// and one with two is a token that was never signed.
func splitJWS(raw string) (jwsHeader, []byte, []byte, []byte, error) {
	var zero jwsHeader
	parts := strings.Split(raw, ".")
	if len(parts) != 3 {
		return zero, nil, nil, nil, fmt.Errorf("%w: a compact JWS has three dot-separated parts, this one has %d", ErrInvalidToken, len(parts))
	}
	if parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return zero, nil, nil, nil, fmt.Errorf("%w: the compact JWS has an empty part", ErrInvalidToken)
	}
	headerBytes, err := decodeBase64URL(parts[0])
	if err != nil {
		return zero, nil, nil, nil, fmt.Errorf("%w: the header is not base64url: %v", ErrInvalidToken, err)
	}
	payload, err := decodeBase64URL(parts[1])
	if err != nil {
		return zero, nil, nil, nil, fmt.Errorf("%w: the payload is not base64url: %v", ErrInvalidToken, err)
	}
	signature, err := decodeBase64URL(parts[2])
	if err != nil {
		return zero, nil, nil, nil, fmt.Errorf("%w: the signature is not base64url: %v", ErrInvalidToken, err)
	}
	var header jwsHeader
	if err := json.Unmarshal(headerBytes, &header); err != nil {
		return zero, nil, nil, nil, fmt.Errorf("%w: decoding the header: %v", ErrInvalidToken, err)
	}
	signingInput := []byte(parts[0] + "." + parts[1])
	return header, payload, signingInput, signature, nil
}

// hashForAlg maps a JWS algorithm to the digest it uses, and is where the
// allow-list lives.
//
// HMAC is rejected here rather than anywhere else, and not because this package
// cannot compute HMAC: it is the key-confusion attack. The "key" in an HMAC JWS
// is a symmetric secret, but the only bytes an attacker would have to know are
// the IdP's public RSA modulus, which is public by design. An implementation that
// picks the verification routine from the token's own alg header therefore lets
// anyone mint a token whose signature "verifies" under RS256's key material. The
// algorithm is allowed to be a hint about formatting, never about which key type
// to trust, and "alg":"none" is the same attack with the work done for us.
func hashForAlg(alg string) (crypto.Hash, error) {
	switch alg {
	case "RS256", "ES256":
		return crypto.SHA256, nil
	case "RS384", "ES384":
		return crypto.SHA384, nil
	case "RS512", "ES512":
		return crypto.SHA512, nil
	case "HS256", "HS384", "HS512":
		return 0, fmt.Errorf("%w: %s is a MAC algorithm and this package will not verify tokens with it (the IdP's public key would become the shared secret)", ErrInvalidToken, alg)
	case "none", "":
		return 0, fmt.Errorf("%w: the token is unsigned or does not name an algorithm", ErrInvalidToken)
	default:
		return 0, fmt.Errorf("%w: unsupported signature algorithm %q", ErrInvalidToken, alg)
	}
}

// verifySignature checks the signature over the signing input with the key the
// JWKS supplied. The algorithm must match the key type, which is what stops a
// header claiming RS256 from being verified with EC material or vice versa.
func verifySignature(alg string, key jsonWebKey, signingInput, signature []byte) error {
	hash, err := hashForAlg(alg)
	if err != nil {
		return err
	}
	if key.Alg != "" && key.Alg != alg {
		return fmt.Errorf("%w: the token names %s but the key with kid %q is published for %s", ErrInvalidToken, alg, key.Kid, key.Alg)
	}
	digest := hash.New()
	digest.Write(signingInput)

	switch {
	case strings.HasPrefix(alg, "RS"):
		if key.Kty != "RSA" {
			return fmt.Errorf("%w: %s needs an RSA key but kid %q is %s", ErrInvalidToken, alg, key.Kid, key.Kty)
		}
		pub, err := rsaPublicKey(key)
		if err != nil {
			return err
		}
		if err := rsa.VerifyPKCS1v15(pub, hash, digest.Sum(nil), signature); err != nil {
			return fmt.Errorf("%w: the RSA signature does not verify under the key with kid %q", ErrInvalidToken, key.Kid)
		}
		return nil

	case strings.HasPrefix(alg, "ES"):
		if key.Kty != "EC" {
			return fmt.Errorf("%w: %s needs an EC key but kid %q is %s", ErrInvalidToken, alg, key.Kid, key.Kty)
		}
		curve, err := curveForAlg(alg)
		if err != nil {
			return err
		}
		if key.Crv != "" && key.Crv != curveName(curve) {
			return fmt.Errorf("%w: %s needs curve %s but kid %q is on %s", ErrInvalidToken, alg, curveName(curve), key.Kid, key.Crv)
		}
		pub, err := ecPublicKey(key, curve)
		if err != nil {
			return err
		}
		size := (curve.Params().BitSize + 7) / 8
		r, s, err := decodeRawECDSASignature(signature, size)
		if err != nil {
			return err
		}
		der, err := encodeECDSASignature(r, s)
		if err != nil {
			return fmt.Errorf("%w: encoding the ECDSA signature: %v", ErrInvalidToken, err)
		}
		if !ecdsa.VerifyASN1(pub, digest.Sum(nil), der) {
			return fmt.Errorf("%w: the ECDSA signature does not verify under the key with kid %q", ErrInvalidToken, key.Kid)
		}
		return nil

	default:
		return fmt.Errorf("%w: unsupported signature algorithm %q", ErrInvalidToken, alg)
	}
}

// curveForAlg names the curve each ES* algorithm is defined over. JWS pairs them
// one to one; there is no negotiation to do.
func curveForAlg(alg string) (elliptic.Curve, error) {
	switch alg {
	case "ES256":
		return elliptic.P256(), nil
	case "ES384":
		return elliptic.P384(), nil
	case "ES512":
		return elliptic.P521(), nil
	default:
		return nil, fmt.Errorf("%w: unsupported ECDSA algorithm %q", ErrInvalidToken, alg)
	}
}

func curveName(c elliptic.Curve) string {
	switch c {
	case elliptic.P256():
		return "P-256"
	case elliptic.P384():
		return "P-384"
	case elliptic.P521():
		return "P-521"
	default:
		return "unknown"
	}
}

// decodeRawECDSASignature splits the fixed-width r||s pair a JWS carries into the
// two integers ECDSA wants.
//
// This is the detail that breaks naive verifiers: JWS ES256/384/512 signatures
// are the raw concatenation of r and s padded to the curve size, while
// crypto/ecdsa.VerifyASN1 expects the DER SEQUENCE { r, s } of X9.62 and
// crypto/ecdsa.Sign produces that DER form. Feeding a raw signature straight to
// VerifyASN1 fails for every valid token, and the usual "fix" - trying ASN.1
// first and then the raw form - accepts two encodings of one signature, so this
// package converts in exactly one direction and rejects any other length.
func decodeRawECDSASignature(signature []byte, size int) (*big.Int, *big.Int, error) {
	if len(signature) != 2*size {
		return nil, nil, fmt.Errorf("%w: an ECDSA signature on this curve is %d bytes, this one is %d", ErrInvalidToken, 2*size, len(signature))
	}
	r := new(big.Int).SetBytes(signature[:size])
	s := new(big.Int).SetBytes(signature[size:])
	return r, s, nil
}

// encodeECDSASignature converts the r||s pair into the DER SEQUENCE
// crypto/ecdsa.VerifyASN1 expects. asn1.Marshal cannot fail for two integers of
// this size, but it returns an error and the caller propagates it rather than
// panicking on an impossible path.
func encodeECDSASignature(r, s *big.Int) ([]byte, error) {
	return asn1.Marshal(struct {
		R *big.Int
		S *big.Int
	}{R: r, S: s})
}

// rsaPublicKey rebuilds an RSA public key from the JWK's modulus and exponent.
// Keys below 2048 bits are refused: JOSE requires them, and an IdP offering one
// is a misconfiguration worth surfacing rather than a key worth trusting.
func rsaPublicKey(k jsonWebKey) (*rsa.PublicKey, error) {
	nBytes, err := decodeBase64URL(k.N)
	if err != nil {
		return nil, fmt.Errorf("%w: the RSA modulus of kid %q is not base64url: %v", ErrInvalidToken, k.Kid, err)
	}
	eBytes, err := decodeBase64URL(k.E)
	if err != nil {
		return nil, fmt.Errorf("%w: the RSA exponent of kid %q is not base64url: %v", ErrInvalidToken, k.Kid, err)
	}
	if len(eBytes) == 0 || len(eBytes) > 8 {
		return nil, fmt.Errorf("%w: the RSA exponent of kid %q is not a plausible number", ErrInvalidToken, k.Kid)
	}
	exponent := 0
	for _, b := range eBytes {
		exponent = exponent<<8 | int(b)
	}
	if exponent < 3 {
		return nil, fmt.Errorf("%w: the RSA exponent of kid %q is %d", ErrInvalidToken, k.Kid, exponent)
	}
	modulus := new(big.Int).SetBytes(nBytes)
	if modulus.BitLen() < 2048 {
		return nil, fmt.Errorf("%w: the RSA key with kid %q is %d bits, 2048 is the floor", ErrInvalidToken, k.Kid, modulus.BitLen())
	}
	return &rsa.PublicKey{N: modulus, E: exponent}, nil
}

// ecPublicKey rebuilds an EC public key from the JWK's coordinates and refuses a
// point that is not on the curve - the check crypto/ecdsa assumes its caller made.
func ecPublicKey(k jsonWebKey, curve elliptic.Curve) (*ecdsa.PublicKey, error) {
	xBytes, err := decodeBase64URL(k.X)
	if err != nil {
		return nil, fmt.Errorf("%w: the x coordinate of kid %q is not base64url: %v", ErrInvalidToken, k.Kid, err)
	}
	yBytes, err := decodeBase64URL(k.Y)
	if err != nil {
		return nil, fmt.Errorf("%w: the y coordinate of kid %q is not base64url: %v", ErrInvalidToken, k.Kid, err)
	}
	x := new(big.Int).SetBytes(xBytes)
	y := new(big.Int).SetBytes(yBytes)
	if !curve.IsOnCurve(x, y) {
		return nil, fmt.Errorf("%w: the point published for kid %q is not on %s", ErrInvalidToken, k.Kid, curveName(curve))
	}
	return &ecdsa.PublicKey{Curve: curve, X: x, Y: y}, nil
}

// rawClaims is the wire form of the claims, decoded before any rule is applied.
// exp and iat are pointers so that "absent" is distinguishable from "zero": a
// token with no exp is rejected, and a token without iat is merely unusual.
type rawClaims struct {
	Iss           string          `json:"iss"`
	Sub           string          `json:"sub"`
	Aud           json.RawMessage `json:"aud"`
	Exp           *float64        `json:"exp"`
	Iat           *float64        `json:"iat"`
	Email         string          `json:"email"`
	EmailVerified json.RawMessage `json:"email_verified"`
	Name          string          `json:"name"`
	Nonce         string          `json:"nonce"`
}

// claims converts the wire form to the four strings, the audience list and the
// two instants the caller sees, rejecting the values that have no agreed type:
// aud may be a string or an array of strings and nothing else, email_verified
// may be a boolean or the string "true"/"false" that some IdPs send and nothing
// else. A "null" member is not a value and is rejected too.
func (r rawClaims) claims() (Claims, error) {
	c := Claims{
		Subject:   r.Sub,
		Email:     r.Email,
		Name:      r.Name,
		Issuer:    r.Iss,
		Nonce:     r.Nonce,
		ExpiresAt: instant(r.Exp),
		IssuedAt:  instant(r.Iat),
	}
	audience, err := parseAudience(r.Aud)
	if err != nil {
		return Claims{}, err
	}
	c.Audience = audience
	verified, err := parseEmailVerified(r.EmailVerified)
	if err != nil {
		return Claims{}, err
	}
	c.EmailVerified = verified
	return c, nil
}

// instant converts an epoch-second claim, of which the zero value means the
// member was absent.
func instant(seconds *float64) time.Time {
	if seconds == nil {
		return time.Time{}
	}
	whole := int64(*seconds)
	return time.Unix(whole, 0).UTC()
}

// parseAudience accepts both wire forms the specification allows for aud: a
// string when there is one audience, an array of strings when there are several.
func parseAudience(raw json.RawMessage) ([]string, error) {
	b := trimJSON(raw)
	if len(b) == 0 {
		return nil, fmt.Errorf("%w: the claims carry no aud", ErrInvalidToken)
	}
	switch b[0] {
	case '"':
		var one string
		if err := json.Unmarshal(b, &one); err != nil {
			return nil, fmt.Errorf("%w: aud is not a string: %v", ErrInvalidToken, err)
		}
		return []string{one}, nil
	case '[':
		var list []string
		if err := json.Unmarshal(b, &list); err != nil {
			return nil, fmt.Errorf("%w: aud is not an array of strings: %v", ErrInvalidToken, err)
		}
		return list, nil
	default:
		return nil, fmt.Errorf("%w: aud is neither a string nor an array of strings", ErrInvalidToken)
	}
}

// parseEmailVerified accepts a JSON boolean, or the strings "true" and "false"
// that some IdPs send where the specification says boolean. Anything else is
// rejected rather than coerced: this member decides whether the address is
// trusted, so guessing at it is the one thing not to do.
func parseEmailVerified(raw json.RawMessage) (bool, error) {
	b := trimJSON(raw)
	if len(b) == 0 {
		return false, fmt.Errorf("%w: the claims carry no email_verified", ErrInvalidToken)
	}
	switch b[0] {
	case 't', 'f':
		var value bool
		if err := json.Unmarshal(b, &value); err != nil {
			return false, fmt.Errorf("%w: email_verified is not a boolean: %v", ErrInvalidToken, err)
		}
		return value, nil
	case '"':
		var value string
		if err := json.Unmarshal(b, &value); err != nil {
			return false, fmt.Errorf("%w: email_verified is not a string: %v", ErrInvalidToken, err)
		}
		switch value {
		case "true":
			return true, nil
		case "false":
			return false, nil
		}
		return false, fmt.Errorf("%w: email_verified is %q, which is neither \"true\" nor \"false\"", ErrInvalidToken, value)
	default:
		return false, fmt.Errorf("%w: email_verified is neither a boolean nor a string", ErrInvalidToken)
	}
}

// validateClaims applies the rules that make a signed token a login. Each one
// rejects; none of them is advisory.
func (p *Provider) validateClaims(c Claims) error {
	// iss: the token must claim the issuer we discovered, compared with one
	// trailing slash trimmed from each side so that the two spellings of one
	// issuer cannot disagree here. A token from a different IdP must not become
	// a login on this one just because it is signed by someone.
	if trimSlash(c.Issuer) != p.Config.issuer() {
		return fmt.Errorf("%w: iss is %q, expected %q", ErrInvalidToken, c.Issuer, p.Config.Issuer)
	}
	// sub: the identity. Without it there is nobody to log in.
	if c.Subject == "" {
		return fmt.Errorf("%w: the claims carry no sub", ErrInvalidToken)
	}
	// aud: the token must have been minted for this client. A token the IdP
	// issued to a different application is a valid token for that application,
	// and accepting it here would let any registered client impersonate a user.
	if !contains(c.Audience, p.Config.ClientID) {
		return fmt.Errorf("%w: aud %v does not contain client_id %q", ErrInvalidToken, c.Audience, p.Config.ClientID)
	}
	// exp: the token must still be valid, with clockSkew of tolerance for the
	// gap between the IdP's clock and ours.
	now := p.Config.now()
	if c.ExpiresAt.IsZero() {
		return fmt.Errorf("%w: the claims carry no exp", ErrInvalidToken)
	}
	if !now.Add(-clockSkew).Before(c.ExpiresAt) {
		return fmt.Errorf("%w: the token expired at %s", ErrInvalidToken, c.ExpiresAt.Format(time.RFC3339))
	}
	// iat: a token minted in the future is not a slow clock, it is a token we
	// cannot date; maxIATFuture plus clockSkew is the window allowed for skew.
	if !c.IssuedAt.IsZero() && c.IssuedAt.After(now.Add(maxIATFuture+clockSkew)) {
		return fmt.Errorf("%w: iat %s is more than %s in the future", ErrInvalidToken, c.IssuedAt.Format(time.RFC3339), maxIATFuture)
	}
	// email and email_verified: an account is keyed by address here, so an
	// unverified or absent address is not an identity.
	if c.Email == "" {
		return fmt.Errorf("%w: the claims carry no email", ErrInvalidToken)
	}
	if !c.EmailVerified {
		return fmt.Errorf("%w: email_verified is not true", ErrInvalidToken)
	}
	return nil
}

func contains(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}
