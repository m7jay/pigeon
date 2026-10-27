package delivery

import (
	"io"
	"strings"
	"testing"
	"time"
)

// TestMaxBodyBytesZeroDisablesCapture is FR-804/SB-4: the response-body cap is
// configurable down to 0, where 0 means "capture nothing" — at the config layer
// PIGEON_DELIVERY_MAX_BODY_BYTES=0 must reach the worker as 0. Asking for the
// 2 KB default is therefore a negative value, not zero, which is the one place
// where a zero value of Options is not "unset".
func TestMaxBodyBytesZeroDisablesCapture(t *testing.T) {
	if got := New(nil, Options{MaxBodyBytes: -1}); got.opts.MaxBodyBytes != 2048 {
		t.Fatalf("negative MaxBodyBytes = %d, want the 2048 default", got.opts.MaxBodyBytes)
	}
	if got := New(nil, Options{MaxBodyBytes: 0}); got.opts.MaxBodyBytes != 0 {
		t.Fatalf("MaxBodyBytes 0 = %d, want 0 (capture disabled, SB-4)", got.opts.MaxBodyBytes)
	}
	if got := New(nil, Options{}); got.opts.MaxBodyBytes != 0 {
		t.Fatalf("zero-valued Options MaxBodyBytes = %d, want 0: 0 is a setting, not \"unset\" (SB-4)", got.opts.MaxBodyBytes)
	}
	// And the cap is honoured where it is applied: a zero cap captures nothing but
	// still consumes the body, so the connection can be reused (FR-817).
	body := io.NopCloser(strings.NewReader("a 500 response body"))
	if got := readAndDrain(body, 0); got != "" {
		t.Fatalf("readAndDrain(max=0) = %q, want nothing captured", got)
	}
}

// TestSweeperIntervalIsClampedToTheD6Ceiling: the sweeper's cadence is D-6's
// recovery bound, so a coarser interval is clamped rather than honoured — a direct
// constructor call must not be able to widen the guarantee the config clamp
// protects.
func TestSweeperIntervalIsClampedToTheD6Ceiling(t *testing.T) {
	if got := NewSweeper(nil, SweeperOptions{}).opts.Interval; got != 5*time.Second {
		t.Fatalf("default sweep interval = %s, want 5s", got)
	}
	if got := NewSweeper(nil, SweeperOptions{Interval: time.Minute}).opts.Interval; got != MaxSweepInterval {
		t.Fatalf("sweep interval = %s, want it clamped to %s (D-6)", got, MaxSweepInterval)
	}
	if got := NewSweeper(nil, SweeperOptions{Interval: time.Second}).opts.Interval; got != time.Second {
		t.Fatalf("sweep interval = %s, want the configured 1s", got)
	}
}
