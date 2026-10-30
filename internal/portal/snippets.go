package portal

import (
	"encoding/json"
	"strconv"
	"strings"
)

// UI-7 and UI-18's verification snippets, in one place.
//
// UI-7 asks the producer portal for copy-paste snippets "using the Standard
// Webhooks verifier libraries" in curl, Go, Node and Python; UI-18 asks the
// consumer portal for the same thing for the secret a consumer holds. The two
// pages are the same page with a different audience, so the renderer lives here
// and both portals call it: a fork would be two implementations of the signature
// rules, and the one that drifts is the one nobody is reading.
//
// Two properties are structural, not editorial:
//
//   - **Every sample verifies against the published libraries**, except the shell
//     one, for which no maintained verifier exists - and it says so rather than
//     hand-rolling HMAC while pretending otherwise. The spec's own algorithm is
//     what it implements, and the note names the one thing it cannot do
//     (constant-time comparison in a shell).
//   - **The secret is embedded as an inert literal** (`strconv.Quote` for Go, a
//     repeated backslash-escape for the others): a secret is attacker-known data
//     in exactly one sense - it is chosen by the platform - but a snippet
//     generator that interpolates raw text into a shell fragment is a command
//     injection waiting for a `whsec` that carries a quote or a `$(...)`.

// SnippetLanguage is one of UI-7's four languages, as the UI spells them.
type SnippetLanguage string

// The four languages UI-7 names. The values are also the URL/query values the
// pages use, so they are stable identifiers rather than labels.
const (
	SnippetCurl   SnippetLanguage = "curl"
	SnippetGo     SnippetLanguage = "go"
	SnippetNode   SnippetLanguage = "node"
	SnippetPython SnippetLanguage = "python"
)

// SnippetLanguages is the ordered set the pages offer. Go first: it is the
// reference implementation and the one the core itself uses.
func SnippetLanguages() []SnippetLanguage {
	return []SnippetLanguage{SnippetGo, SnippetNode, SnippetPython, SnippetCurl}
}

// Snippet is one rendered sample.
type Snippet struct {
	Language SnippetLanguage
	// Label is what the tab or heading says.
	Label string
	// Source is the copy-paste text.
	Source string
	// Note says what this sample does and does not do, so the page does not have
	// to explain the difference for one language and not the others.
	Note string
}

// Snippets renders all four samples for one signing secret.
func Snippets(whsec string) []Snippet {
	out := make([]Snippet, 0, 4)
	for _, lang := range SnippetLanguages() {
		s, ok := SnippetFor(lang, whsec)
		if !ok {
			continue
		}
		out = append(out, s)
	}
	return out
}

// SnippetFor renders one language. An unknown language is reported rather than
// defaulted: a page that silently rendered Go for `?lang=rust` would be a control
// that lies about what it did.
func SnippetFor(language SnippetLanguage, whsec string) (Snippet, bool) {
	literal := jsStringLiteral(whsec)
	switch language {
	case SnippetGo:
		return Snippet{
			Language: SnippetGo,
			Label:    "Go",
			Note: "The Standard Webhooks Go library. `wh.Verify` checks the signature and " +
				"the timestamp (default tolerance 5 minutes) and uses a constant-time comparison.",
			Source: `// Verify an incoming webhook signature with the Standard Webhooks Go library.
//   go get github.com/standard-webhooks/standard-webhooks/libraries/go

import (
	"io"
	"net/http"

	standardwebhooks "github.com/standard-webhooks/standard-webhooks/libraries/go"
)

var webhookSecret = ` + strconv.Quote(whsec) + `

func verify(r *http.Request) error {
	wh, err := standardwebhooks.NewWebhook(webhookSecret)
	if err != nil {
		return err
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return err
	}
	// Verify against the RAW body: re-serialising the parsed JSON changes the
	// bytes and the signature will not match.
	return wh.Verify(body, r.Header)
}
`,
		}, true
	case SnippetNode:
		return Snippet{
			Language: SnippetNode,
			Label:    "Node",
			Note: "The Standard Webhooks npm package. Pass the raw request body, not a " +
				"parsed object: `express.raw` below keeps the bytes the sender signed.",
			Source: `// Verify an incoming webhook signature with the Standard Webhooks npm package.
//   npm install standardwebhooks

const { Webhook } = require("standardwebhooks");
const express = require("express");

const webhookSecret = ` + literal + `;
const wh = new Webhook(webhookSecret);

const app = express();

// express.raw keeps the body as a Buffer: the signature covers the bytes as sent.
app.post("/webhook", express.raw({ type: "*/*" }), (req, res) => {
  try {
    wh.verify(req.body, req.headers);
  } catch (err) {
    return res.status(400).send("invalid signature");
  }
  // Verified: handle the event.
  res.status(204).end();
});

app.listen(3000);
`,
		}, true
	case SnippetPython:
		return Snippet{
			Language: SnippetPython,
			Label:    "Python",
			Note: "The Standard Webhooks Python package. Pass the raw bytes " +
				"(`request.get_data()`, not `request.json`) so the signed bytes are the " +
				"bytes you verify.",
			Source: `# Verify an incoming webhook signature with the Standard Webhooks Python package.
#   pip install standardwebhooks

import os

from flask import Flask, request
from standardwebhooks import Webhook

webhook = Webhook(os.environ["WEBHOOK_SECRET"])
app = Flask(__name__)


@app.post("/webhook")
def receive():
    try:
        # Raises WebhookVerificationError when the signature or timestamp is bad.
        webhook.verify(request.get_data(), request.headers)
    except Exception:
        return "invalid signature", 400
    return "", 204
`,
		}, true
	case SnippetCurl:
		return Snippet{
			Language: SnippetCurl,
			Label:    "curl / shell",
			Note: "There is no maintained curl verifier: the Standard Webhooks project ships " +
				"libraries for Go, Node, Python, Rust, Java, PHP and others, but nothing for a " +
				"shell. This is the spec's own algorithm (HMAC-SHA256 over " +
				"\"id.timestamp.body\", base64, `v1,` prefix) for the box that has nothing but " +
				"a shell - and it compares with `case`, which is NOT constant time. Use a " +
				"library wherever you can.",
			Source: `#!/bin/sh
# Verify a Standard Webhooks delivery with only a shell, curl and openssl.
# The sender's headers and body are needed: webhook-id, webhook-timestamp,
# webhook-signature, and the raw body exactly as received.
set -eu

secret='` + shSingleQuoteEscape(whsec) + `'
body=$(cat body.json)

signed="$WEBHOOK_ID.$WEBHOOK_TIMESTAMP.$body"
# The signing key is the whsec_ secret with its prefix removed and base64-decoded.
keyhex=$(printf '%s' "${secret#whsec_}" | openssl base64 -d -A | xxd -p -c 256)
expected="v1,$(printf '%s' "$signed" | openssl dgst -sha256 -mac hmac -macopt "hexkey:$keyhex" -binary | openssl base64 -A)"

# webhook-signature may carry several space-separated signatures: accept any match.
case " $WEBHOOK_SIGNATURE " in
  *" $expected "*) echo "verified" ;;
  *) echo "invalid signature" >&2; exit 1 ;;
esac
`,
		}, true
	default:
		return Snippet{}, false
	}
}

// jsStringLiteral renders s as a double-quoted JavaScript string literal. JSON's
// escaping rules are a subset of JavaScript's for the characters that matter here
// (quote, backslash, control characters), and JSON escapes the U+2028/U+2029 pair
// that older JavaScript treated as line terminators.
func jsStringLiteral(s string) string {
	raw, err := json.Marshal(s)
	if err != nil {
		return `""`
	}
	return string(raw)
}

// shSingleQuoteEscape renders s inside a single-quoted shell word: the only
// character a single-quoted string cannot contain is the quote itself, which is
// spliced as four characters.
func shSingleQuoteEscape(s string) string {
	return strings.ReplaceAll(s, "'", `'\''`)
}

// SnippetTemplateNote is the sentence both pages show above the language tabs.
const SnippetTemplateNote = "The snippet is rendered by this portal from the secret you " +
	"submit: the secret is never sent to the core API and it is not stored."
