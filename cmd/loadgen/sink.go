package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"
)

// cmdSink is the receiving end of a load run: a consumer endpoint that answers
// 200 immediately.
//
// It exists because a load test without a fast receiver measures the retry path,
// not the delivery path. With no sink the deliveries fail, every attempt writes an
// attempt row, FR-805's retry schedule takes over and the delivery unit becomes a
// statement about failure handling. The sink makes the same number mean
// "delivered".
//
// -latency adds a deliberate per-request delay, which is how the concurrency
// ceiling (AC-34) is explored: a 5 s timeout with a 100 ms sink holds at most 50
// attempts in flight per endpoint, whatever the offered rate.
func cmdSink(args []string) error {
	fs := flag.NewFlagSet("sink", flag.ContinueOnError)
	addr := fs.String("addr", "127.0.0.1:9001", "listen address")
	latency := fs.Duration("latency", 0, "artificial per-request delay")
	status := fs.Int("status", http.StatusOK, "status to answer with")
	body := fs.String("body", `{"ok":true}`, "response body")
	if err := fs.Parse(args); err != nil {
		return err
	}

	var received atomic.Int64
	var refused atomic.Int64

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		received.Add(1)
		// The signature headers are what a real consumer would verify; the sink
		// checks they are present but not their value, so a run proves the
		// envelope went out without the sink becoming a second implementation of
		// FR-1001 that could disagree with the first (§16's independence rule).
		if r.Header.Get("webhook-signature") == "" || r.Header.Get("webhook-id") == "" {
			refused.Add(1)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if *latency > 0 {
			time.Sleep(*latency)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(*status)
		_, _ = w.Write([]byte(*body))
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/count", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `{"received":%d,"unsigned":%d}`, received.Load(), refused.Load())
	})

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		return err
	}
	srv := &http.Server{
		Handler: mux,
		// The sink must not be the bottleneck: no per-connection read/write
		// deadline, and the idle pool is left at the default because a stuck
		// connection here would look like a delivery failure upstream.
		ReadHeaderTimeout: 10 * time.Second,
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		t := time.NewTicker(15 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				fmt.Printf("sink: received %d (unsigned %d)\n", received.Load(), refused.Load())
			}
		}
	}()

	fmt.Printf("sink: listening on %s (latency %s, status %d)\n", ln.Addr(), *latency, *status)
	go func() { <-ctx.Done(); _ = srv.Close() }()
	if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
		return err
	}
	fmt.Printf("sink: received %d requests, %d unsigned\n", received.Load(), refused.Load())
	return nil
}
