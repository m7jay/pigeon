package schema

import (
	"strings"
	"testing"
)

// paymentSchema requires an object with amount (integer) and currency (string).
const paymentSchema = `{
  "type": "object",
  "required": ["amount", "currency"],
  "properties": {
    "amount":     { "type": "integer" },
    "currency":   { "type": "string", "enum": ["INR", "USD"] },
    "reference":  { "type": "string" }
  }
}`

func TestValidateAcceptsConformingPayload(t *testing.T) {
	err := Validate([]byte(paymentSchema), []byte(`{"amount":49900,"currency":"INR"}`))
	if err != nil {
		t.Fatalf("conforming payload rejected: %v", err)
	}
}

func TestValidateMissingRequiredNamesItsPointer(t *testing.T) {
	err := Validate([]byte(paymentSchema), []byte(`{"currency":"INR"}`))
	ve, ok := err.(*ValidationError)
	if !ok {
		t.Fatalf("want *ValidationError, got %T: %v", err, err)
	}
	// A missing property fails at the object that lacks it (the root here), and
	// the keyword names the assertion; the message names the missing property.
	if ve.Keyword != "required" {
		t.Fatalf("keyword = %q, want required", ve.Keyword)
	}
	if ve.Pointer != "" {
		t.Fatalf("pointer = %q, want root (\"\")", ve.Pointer)
	}
	if !strings.Contains(ve.Message, "amount") {
		t.Fatalf("message %q does not name the missing property", ve.Message)
	}
}

func TestValidateWrongTypeNamesTheField(t *testing.T) {
	err := Validate([]byte(paymentSchema), []byte(`{"amount":"not-a-number","currency":"INR"}`))
	ve, ok := err.(*ValidationError)
	if !ok {
		t.Fatalf("want *ValidationError, got %T: %v", err, err)
	}
	if ve.Pointer != "/amount" {
		t.Fatalf("pointer = %q, want /amount", ve.Pointer)
	}
	if ve.Keyword != "type" {
		t.Fatalf("keyword = %q, want type", ve.Keyword)
	}
}

func TestValidateNestedPointer(t *testing.T) {
	schema := `{"type":"object","properties":{"data":{"type":"object","required":["id"],"properties":{"id":{"type":"string"}}}}}`
	err := Validate([]byte(schema), []byte(`{"data":{"id":42}}`))
	ve, ok := err.(*ValidationError)
	if !ok {
		t.Fatalf("want *ValidationError, got %T: %v", err, err)
	}
	if ve.Pointer != "/data/id" {
		t.Fatalf("pointer = %q, want /data/id", ve.Pointer)
	}
}

func TestValidateBadSchemaIsNotAValidationError(t *testing.T) {
	// A schema whose "type" is a non-string is a definition error, surfaced as a
	// plain error, not a *ValidationError against the payload.
	err := Validate([]byte(`{"type": 123}`), []byte(`{}`))
	if err == nil {
		t.Fatal("expected a schema error, got nil")
	}
	if _, ok := err.(*ValidationError); ok {
		t.Fatal("malformed schema reported as a payload validation error")
	}
}
