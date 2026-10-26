// Package schema validates an event payload against its event type's JSON
// Schema (FR-303). The failing location is reported as a JSON pointer so the API
// can name exactly which field a producer got wrong, rather than a bare "does
// not conform".
package schema

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/santhosh-tekuri/jsonschema/v5"
)

// ValidationError is a single failing assertion: the JSON pointer into the
// payload that failed, the human message, and the schema keyword that rejected
// it. FR-303 wants the pointer, so it is the first field anything surfaces.
type ValidationError struct {
	// Pointer is the RFC 6901 pointer to the failing value, e.g. "/amount" or
	// "" for the document root.
	Pointer string
	// Message is a plain-English description of the failure.
	Message string
	// Keyword is the schema keyword that failed ("type", "required", ...).
	Keyword string
}

func (e *ValidationError) Error() string {
	if e.Pointer == "" {
		return fmt.Sprintf("payload %s", e.Message)
	}
	return fmt.Sprintf("payload at %s %s", e.Pointer, e.Message)
}

// Validate compiles rawSchema (a JSON Schema document) and validates payload
// against it. A nil return means the payload conforms. A *ValidationError names
// the failing pointer; any other error means the schema itself is malformed and
// belongs to the event-type the producer defined, not the payload they sent.
func Validate(rawSchema, rawPayload []byte) error {
	compiler := jsonschema.NewCompiler()
	// A schema that is not even JSON is a definition bug, reported distinctly
	// from a payload that violates a valid schema.
	if err := compiler.AddResource("event-type-schema.json", bytes.NewReader(rawSchema)); err != nil {
		return fmt.Errorf("event type schema is not a valid resource: %w", err)
	}
	sch, err := compiler.Compile("event-type-schema.json")
	if err != nil {
		return fmt.Errorf("event type schema is invalid: %w", err)
	}

	var payload any
	dec := json.NewDecoder(bytes.NewReader(rawPayload))
	if err := dec.Decode(&payload); err != nil {
		return fmt.Errorf("payload is not valid JSON: %w", err)
	}
	if dec.More() {
		return errors.New("payload has trailing data after the JSON value")
	}

	if err := sch.Validate(payload); err != nil {
		var ve *jsonschema.ValidationError
		if errors.As(err, &ve) {
			leaf, keyword := mostSpecific(ve)
			return &ValidationError{
				Pointer: leaf.InstanceLocation,
				Message: leaf.Message,
				Keyword: keyword,
			}
		}
		return err
	}
	return nil
}

// mostSpecific walks the ValidationError tree and returns the deepest leaf (the
// most specific failing assertion) plus the last segment of its keyword
// location, which is the human-readable reason ("type", "required", ...). A
// deeper pointer is a more useful message than the root, so depth wins; ties go
// to the first found.
func mostSpecific(ve *jsonschema.ValidationError) (*jsonschema.ValidationError, string) {
	best := ve
	bestDepth := len(strings.Split(ve.InstanceLocation, "/")) - 1
	var bestKeyword string
	if ve.KeywordLocation != "" {
		bestKeyword = lastSegment(ve.KeywordLocation)
	}

	var walk func(n *jsonschema.ValidationError, depth int)
	walk = func(n *jsonschema.ValidationError, depth int) {
		for _, c := range n.Causes {
			d := depth + 1
			if d > bestDepth {
				best = c
				bestDepth = d
				bestKeyword = ""
				if c.KeywordLocation != "" {
					bestKeyword = lastSegment(c.KeywordLocation)
				}
			}
			walk(c, d)
		}
	}
	walk(ve, 0)
	return best, bestKeyword
}

func lastSegment(s string) string {
	if i := strings.LastIndex(s, "/"); i >= 0 {
		return s[i+1:]
	}
	return s
}
