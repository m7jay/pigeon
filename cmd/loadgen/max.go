package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"time"
)

type maxStage struct {
	OfferedRate   float64 `json:"offered_events_per_s"`
	Accepted      float64 `json:"accepted_events_per_s"`
	Shed          float64 `json:"shed_events_per_s"`
	Errors        int64   `json:"errors"`
	P99MS         float64 `json:"p99_ms"`
	PacerLag      int64   `json:"pacer_lag"`
	DBReconciled  bool    `json:"db_reconciled"`
	DBAccepted    int64   `json:"db_generator_accepted"`
	DBEvents      int64   `json:"db_events"`
	DBOutbox      int64   `json:"db_outbox"`
	DBEventsMatch bool    `json:"db_events_match"`
	DBOutboxMatch bool    `json:"db_outbox_match"`
	DBNote        string  `json:"db_note,omitempty"`
	Safe          bool    `json:"safe"`
	Pressure      bool    `json:"pressure"`
	Reason        string  `json:"reason,omitempty"`
}

type maxSummary struct {
	StartedAt         time.Time  `json:"started_at"`
	FinishedAt        time.Time  `json:"finished_at"`
	Machine           machine    `json:"machine"`
	SustainableRate   float64    `json:"sustainable_events_per_s"`
	AcceptedAtMax     float64    `json:"accepted_events_per_s"`
	FirstPressureRate float64    `json:"first_pressure_events_per_s"`
	Complete          bool       `json:"maximum_found"`
	Reason            string     `json:"stop_reason"`
	Stages            []maxStage `json:"stages"`
}

// cmdMax performs short, sequential load stages. It increases the offered rate
// until it sees shedding, errors, latency above the service objective, or a load
// generator limit. Once it brackets a service limit, it refines the boundary by
// testing between the highest safe rate and the first pressured rate.
func cmdMax(args []string) error {
	fs := flag.NewFlagSet("max", flag.ContinueOnError)
	var c config
	fs.StringVar(&c.BaseURL, "base", "http://127.0.0.1:8080", "instance base URL")
	fs.StringVar(&c.Key, "key", os.Getenv("PIGEON_LOADGEN_KEY"), "producer API key (or PIGEON_LOADGEN_KEY)")
	fs.StringVar(&c.EventType, "event-type", "", "event type to publish")
	fs.StringVar(&c.ConsumerRef, "consumer-ref", "", "consumer_ref to route to")
	fs.IntVar(&c.PayloadBytes, "payload-bytes", referencePayloadBytes, "payload size in bytes")
	fs.IntVar(&c.Subs, "subs", 10, "subscriptions per event")
	fs.StringVar(&c.DB, "db", os.Getenv("PIGEON_LOADGEN_DB"), "Postgres DSN for reconciliation")
	fs.StringVar(&c.Out, "out", "load-test.json", "write the ramp summary JSON here")
	fs.IntVar(&c.Concurrency, "concurrency", 0, "in-flight requests (default: based on available CPUs)")
	fs.DurationVar(&c.RequestTimeout, "timeout", 10*time.Second, "per-request timeout")
	fs.DurationVar(&c.Settle, "settle", 10*time.Second, "wait after each stage for delivery fan-out")
	startRate := fs.Float64("start-rate", 0, "initial offered events/s (default: based on available CPUs)")
	maxRate := fs.Float64("max-rate", 100000, "upper safety cap for offered events/s")
	step := fs.Float64("step", 1.5, "rate multiplier after each safe stage")
	maxStages := fs.Int("max-stages", 20, "maximum number of stages, including boundary refinement")
	refineStages := fs.Int("refine-stages", 4, "boundary refinement stages after pressure is found")
	fs.DurationVar(&c.Warmup, "warmup", 15*time.Second, "warm-up at each rate stage")
	fs.DurationVar(&c.Measure, "measure", 45*time.Second, "measurement duration at each rate stage")
	fs.DurationVar(&c.Evaluate, "evaluate", 15*time.Second, "final evaluation window within each stage")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if c.Key == "" || c.EventType == "" || c.DB == "" {
		return errors.New("-key, -event-type and -db are required for a reconciled maximum-load run")
	}
	if c.Subs < 1 || c.PayloadBytes < 1 || c.Measure <= 0 || c.Evaluate <= 0 || c.Evaluate > c.Measure || c.Warmup < 0 || c.Settle < 0 || c.RequestTimeout <= 0 {
		return errors.New("-subs, -payload-bytes, -measure, -evaluate and -timeout must be positive; -warmup and -settle cannot be negative; -evaluate cannot exceed -measure")
	}
	if math.IsNaN(*step) || math.IsInf(*step, 0) || *step <= 1 || math.IsNaN(*maxRate) || math.IsInf(*maxRate, 0) || *maxRate <= 0 || math.IsNaN(*startRate) || math.IsInf(*startRate, 0) || *startRate < 0 || *maxStages < 1 || *refineStages < 0 || c.Concurrency < 0 {
		return errors.New("-step must be finite and greater than 1; -max-rate must be finite and positive; -start-rate and -concurrency cannot be negative; -max-stages must be positive; -refine-stages cannot be negative")
	}
	m := describeMachine()
	if c.Concurrency <= 0 {
		c.Concurrency = m.CPUs * 32
		if c.Concurrency < 64 {
			c.Concurrency = 64
		}
		if c.Concurrency > 512 {
			c.Concurrency = 512
		}
	}
	rate := *startRate
	if rate <= 0 {
		rate = float64(m.CPUs * 25)
		if rate < 10 {
			rate = 10
		}
	}
	if rate > *maxRate {
		return fmt.Errorf("-start-rate %.0f exceeds -max-rate %.0f", rate, *maxRate)
	}

	summary := maxSummary{StartedAt: time.Now().UTC(), Machine: m}
	var best, pressured float64
	var terminalReason string
	for len(summary.Stages) < *maxStages {
		r, err := runMaxStage(c, rate, len(summary.Stages)+1)
		if err != nil {
			return err
		}
		summary.Stages = append(summary.Stages, r)
		if r.Safe {
			best = rate
			summary.SustainableRate = rate
			summary.AcceptedAtMax = r.Accepted
			if rate >= *maxRate {
				terminalReason = "rate safety cap reached before the service limit"
				break
			}
			rate = math.Min(rate*(*step), *maxRate)
			continue
		}
		terminalReason = r.Reason
		if r.PacerLag > 0 {
			terminalReason = "load generator could not sustain the offered rate; move the generator off this machine or increase its resources"
			break
		}
		if !r.Pressure {
			break
		}
		pressured = rate
		if best == 0 {
			// The resource-based starting estimate may already be above a small
			// instance's limit. Step down until we establish a safe lower bound.
			for best == 0 && pressured > 1 && len(summary.Stages) < *maxStages {
				lower := math.Max(1, pressured/(*step))
				r, err := runMaxStage(c, lower, len(summary.Stages)+1)
				if err != nil {
					return err
				}
				summary.Stages = append(summary.Stages, r)
				if r.Safe {
					best = lower
					summary.SustainableRate = lower
					summary.AcceptedAtMax = r.Accepted
					break
				}
				if r.PacerLag > 0 || !r.Pressure {
					pressured = 0
					terminalReason = r.Reason
					if r.PacerLag > 0 {
						terminalReason = "load generator could not sustain the offered rate while establishing a safe baseline"
					}
					break
				}
				pressured = lower
				terminalReason = r.Reason
			}
		}
		if best == 0 || pressured == 0 {
			break
		}
		for i := 0; i < *refineStages && len(summary.Stages) < *maxStages; i++ {
			if pressured <= best*1.05 {
				break
			}
			mid := math.Sqrt(best * pressured)
			r, err := runMaxStage(c, mid, len(summary.Stages)+1)
			if err != nil {
				return err
			}
			summary.Stages = append(summary.Stages, r)
			if r.Safe {
				best = mid
				summary.SustainableRate = mid
				summary.AcceptedAtMax = r.Accepted
			} else if r.PacerLag > 0 || !r.Pressure {
				pressured = 0
				terminalReason = r.Reason
				if r.PacerLag > 0 {
					terminalReason = "load generator could not sustain the offered rate during boundary refinement"
				}
				break
			} else {
				pressured = mid
				terminalReason = r.Reason
			}
		}
		break
	}
	if terminalReason == "" {
		terminalReason = "stage limit reached before the service limit"
	}
	summary.FinishedAt = time.Now().UTC()
	summary.FirstPressureRate = pressured
	summary.Reason = terminalReason
	summary.Complete = summary.SustainableRate > 0 && pressured > 0 && terminalReason != "" && summary.Stages[len(summary.Stages)-1].PacerLag == 0
	printMaxSummary(summary)
	if c.Out != "" {
		b, err := json.MarshalIndent(summary, "", "  ")
		if err != nil {
			return err
		}
		if err := os.WriteFile(c.Out, append(b, '\n'), 0o644); err != nil {
			return fmt.Errorf("write ramp summary: %w", err)
		}
		fmt.Printf("\nramp summary: %s\n", c.Out)
	}
	if !summary.Complete {
		return fmt.Errorf("maximum load not established: %s", terminalReason)
	}
	return nil
}

func runMaxStage(c config, rate float64, number int) (maxStage, error) {
	c.Rate = rate
	c.Label = fmt.Sprintf("max-load-stage-%02d-%.0feps", number, rate)
	r, err := runLoad(c)
	if err != nil {
		return maxStage{}, err
	}
	printReport(r)
	s := maxStage{
		OfferedRate:   rate,
		Accepted:      r.Evaluated.AcceptRate,
		Shed:          r.Measured.ShedRate,
		Errors:        r.Measured.Errors,
		P99MS:         r.Evaluated.P99MS,
		PacerLag:      r.Measured.PacerLag,
		DBReconciled:  r.Reconcile.Ran,
		DBAccepted:    r.Reconcile.Accepted,
		DBEvents:      r.Reconcile.Events,
		DBOutbox:      r.Reconcile.Outbox,
		DBEventsMatch: r.Reconcile.EventsMatch,
		DBOutboxMatch: r.Reconcile.OutboxMatch,
		DBNote:        r.Reconcile.Note,
		Safe:          true,
	}
	switch {
	case !r.Reconcile.Ran || !r.Reconcile.EventsMatch || !r.Reconcile.OutboxMatch:
		s.Safe, s.Reason = false, "database reconciliation did not match accepted events and outbox rows"
	case r.Measured.Shed > 0:
		s.Safe, s.Pressure, s.Reason = false, true, "service shed requests with HTTP 429"
	case r.Measured.Errors > 0:
		s.Safe, s.Pressure, s.Reason = false, true, "service returned request errors"
	case r.Measured.PacerLag > 0:
		s.Safe, s.Reason = false, "load generator could not sustain the offered rate"
	case r.Evaluated.AcceptRate < rate*0.98:
		s.Safe, s.Pressure, s.Reason = false, true, "accepted rate fell below 98% of the offered rate"
	case r.Evaluated.P99MS > 100:
		s.Safe, s.Pressure, s.Reason = false, true, "p99 latency exceeded 100 ms"
	}
	return s, nil
}

func printMaxSummary(s maxSummary) {
	fmt.Printf("\nmaximum-load ramp on %s: %d vCPU / %.1f GiB RAM\n", s.Machine.Hostname, s.Machine.CPUs, float64(s.Machine.MemMiB)/1024)
	fmt.Printf("%-10s %-12s %-12s %-10s %-10s %-8s\n", "offered/s", "accepted/s", "shed/s", "errors", "p99 ms", "result")
	for _, stage := range s.Stages {
		result := "safe"
		if stage.Pressure {
			result = "pressure"
		} else if !stage.Safe {
			result = "invalid"
		}
		fmt.Printf("%-10.0f %-12.1f %-12.1f %-10d %-10.1f %-8s\n", stage.OfferedRate, stage.Accepted, stage.Shed, stage.Errors, stage.P99MS, result)
		if !stage.DBReconciled || !stage.DBEventsMatch || !stage.DBOutboxMatch {
			fmt.Printf("  reconciliation: accepted %d; events %d; outbox %d; note: %s\n",
				stage.DBAccepted, stage.DBEvents, stage.DBOutbox, stage.DBNote)
		}
	}
	if s.SustainableRate > 0 {
		fmt.Printf("highest safe offered rate: %.0f events/s (accepted %.1f/s)\n", s.SustainableRate, s.AcceptedAtMax)
	}
	if s.FirstPressureRate > 0 {
		fmt.Printf("service limit found at %.0f events/s: %s\n", s.FirstPressureRate, s.Reason)
	} else {
		fmt.Printf("service limit not established: %s\n", s.Reason)
	}
}
