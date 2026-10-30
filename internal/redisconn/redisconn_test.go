package redisconn

import (
	"testing"
)

// The parser is the whole shared decision, so it is pinned here as well as
// through each constructor that uses it: a constructor test can only show that
// *its* caller got the right options, while this one fixes the contract the three
// callers share.

// Both documented forms are accepted, and each yields the address go-redis
// should dial - never the raw value, which is the defect this package exists to
// prevent (go-redis dials Options.Addr verbatim, so a URL in it becomes a
// hostname and fails with "too many colons in address").
func TestOptionsReadsTheDocumentedURLForm(t *testing.T) {
	o, err := Options("redis://redis:6379/0")
	if err != nil {
		t.Fatalf("Options(redis://redis:6379/0): %v", err)
	}
	if o.Addr != "redis:6379" {
		t.Fatalf("Options(redis://redis:6379/0).Addr = %q, want %q", o.Addr, "redis:6379")
	}
	if o.DB != 0 {
		t.Fatalf("Options(redis://redis:6379/0).DB = %d, want 0", o.DB)
	}
}

func TestOptionsAcceptsABareHostPort(t *testing.T) {
	o, err := Options("127.0.0.1:6399")
	if err != nil {
		t.Fatalf("Options(127.0.0.1:6399): %v", err)
	}
	if o.Addr != "127.0.0.1:6399" {
		t.Fatalf("Options(127.0.0.1:6399).Addr = %q, want %q", o.Addr, "127.0.0.1:6399")
	}
	if o.DB != 0 {
		t.Fatalf("Options(127.0.0.1:6399).DB = %d, want 0", o.DB)
	}
}

// The quieter half of the defect, and the reason a passing PING is not evidence:
// a client that connects fine can still be in the wrong database. Dropping the
// index is invisible from the connection alone, so it is asserted here.
func TestOptionsCarriesTheDatabaseIndex(t *testing.T) {
	o, err := Options("redis://127.0.0.1:6399/9")
	if err != nil {
		t.Fatalf("Options(redis://127.0.0.1:6399/9): %v", err)
	}
	if o.Addr != "127.0.0.1:6399" {
		t.Fatalf("Options(redis://127.0.0.1:6399/9).Addr = %q, want %q", o.Addr, "127.0.0.1:6399")
	}
	if o.DB != 9 {
		t.Fatalf("Options(redis://127.0.0.1:6399/9).DB = %d, want 9", o.DB)
	}

	// A URL with no database path means DB 0, not "no database".
	o, err = Options("redis://127.0.0.1:6399")
	if err != nil {
		t.Fatalf("Options(redis://127.0.0.1:6399): %v", err)
	}
	if o.DB != 0 {
		t.Fatalf("Options(redis://127.0.0.1:6399).DB = %d, want 0", o.DB)
	}
}

// A malformed URL is an error the boot gate can report, not a value dialled
// verbatim (which is how the defect survived: nothing tried to parse it).
func TestOptionsRejectsAMalformedURL(t *testing.T) {
	if _, err := Options("redis://%zz"); err == nil {
		t.Fatal("Options(redis://%zz) returned nil error, want a parse failure")
	}
}

// ContextTimeoutEnabled stays off: the shared helper must not change how a
// caller's deadline bounds a command. internal/attemptbuf turns it on for its own
// reason (AckTimeout is the bound it reads as, D-9) and internal/broker and
// internal/limit keep the behaviour they had, where their own ReadTimeout bounds
// the I/O.
func TestOptionsLeavesTheDeadlineFlagToTheCaller(t *testing.T) {
	for _, raw := range []string{"redis://127.0.0.1:6399/9", "127.0.0.1:6399"} {
		o, err := Options(raw)
		if err != nil {
			t.Fatalf("Options(%q): %v", raw, err)
		}
		if o.ContextTimeoutEnabled {
			t.Fatalf("Options(%q) enabled ContextTimeoutEnabled; the shared helper must leave that decision to its caller", raw)
		}
	}
}
