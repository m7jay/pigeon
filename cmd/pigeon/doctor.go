package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/m7jay/pigeon/internal/config"
	"github.com/m7jay/pigeon/internal/doctor"
)

// runDoctor is `pigeon doctor` (OPS-4, OPS-10, OPS-11).
//
// It is deliberately forgiving about the configuration: on a box that will not
// start, `config.Load` is the thing that refused, so doctor records the refusal as
// a failing check and then reaches past it with the URLs read from the environment.
// A doctor that only works on a healthy box is a doctor nobody calls.
//
// The exit codes are OPS-11's: 0 nothing failed, 1 a failure, 2 warnings only,
// 3 doctor could not observe anything at all. They travel through run()'s error
// path as a typed exit so main does not print a second line over the report.
func runDoctor(args []string) error {
	fs := flag.NewFlagSet("doctor", flag.ContinueOnError)
	asJSON := fs.Bool("json", false, "emit the report as JSON")
	if err := fs.Parse(args); err != nil {
		return exitError{code: doctor.ExitCannotRun, err: err}
	}

	ctx := context.Background()
	cfg, cfgErr := config.Load()
	deps := doctor.Deps{
		Cfg:         cfg,
		DatabaseURL: func() string { u, _ := doctor.Env(); return u }(),
		RedisURL:    func() string { _, u := doctor.Env(); return u }(),
		Statfs:      doctor.Filesystem,
		FDLimit:     doctor.FileDescriptors,
	}
	if cfgErr != nil {
		deps.CfgErr = cfgErr.Error()
	}

	// The probes are opened out of band and closed here. A probe that will not
	// open is recorded as a failure for the dependency it belongs to - "I asked
	// and it did not answer" is the single most useful thing doctor can say - and
	// a dependency that was never configured stays a nil probe, which the checks
	// report as a skip.
	if pg, err := doctor.OpenPostgres(ctx, deps.DatabaseURL); err == nil {
		deps.DB = pg
		defer pg.Close()
	} else if deps.DatabaseURL != "" {
		deps.DBErr = err
	}
	if rd, err := doctor.OpenRedis(ctx, deps.RedisURL); err == nil {
		deps.Redis = rd
		defer rd.Close()
	} else if deps.RedisURL != "" {
		deps.RedisErr = err
	}

	report := deps.Run(ctx)
	if *asJSON {
		b, err := report.JSON()
		if err != nil {
			return exitError{code: doctor.ExitCannotRun, err: err}
		}
		fmt.Fprintln(os.Stdout, string(b))
	} else if err := report.Print(os.Stdout); err != nil {
		return exitError{code: doctor.ExitCannotRun, err: err}
	}
	if code := report.ExitCode(); code != doctor.ExitOK {
		return exitError{code: code}
	}
	return nil
}

// exitError carries a specific exit status without an error message: the report
// has already said everything worth saying, and main must not add a line to it.
type exitError struct {
	code int
	err  error
}

func (e exitError) Error() string {
	if e.err != nil {
		return e.err.Error()
	}
	return ""
}
