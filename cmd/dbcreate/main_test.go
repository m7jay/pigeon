package main

import "testing"

// TestTargetDatabaseReadsTheNameTheDSNNames pins the one decision this tool can get
// wrong quietly: which database it is about to create. Every shape below is one the
// Makefile, a developer or CI can hand it - the wait-and-see failure mode is a
// create against the wrong name, which then reds sixty acceptance tests in
// internal/httpapi for a reason that has nothing to do with them.
func TestTargetDatabaseReadsTheNameTheDSNNames(t *testing.T) {
	for _, tc := range []struct {
		name string
		dsn  string
		want string
	}{
		{"url", "postgres://user:pw@localhost:5432/pigeon_test?sslmode=disable", "pigeon_test"},
		{
			"url with a unix-socket host in the query",
			"postgres://hermes@localhost:55432/pigeon_sock_test?host=/var/run/postgresql&port=55432&sslmode=disable",
			"pigeon_sock_test",
		},
		{"url with no credentials", "postgres://hermes@localhost/pigeon_x_test", "pigeon_x_test"},
		{"url with a percent-escaped name", "postgres://user@localhost/pigeon%5Fesc_test", "pigeon_esc_test"},
		{"keyword form", "host=localhost port=5432 dbname=pigeon_kw_test sslmode=disable", "pigeon_kw_test"},
		{"keyword form, quoted", "dbname='pigeon_quoted_test'", "pigeon_quoted_test"},
		{"keyword form, last one wins", "dbname=first dbname=pigeon_last_test", "pigeon_last_test"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := targetDatabase(tc.dsn)
			if err != nil {
				t.Fatalf("targetDatabase(%q) = error %v, want %q", tc.dsn, err, tc.want)
			}
			if got != tc.want {
				t.Fatalf("targetDatabase(%q) = %q, want %q", tc.dsn, got, tc.want)
			}
		})
	}
}

// TestTargetDatabaseRefusesADSNSThatNamesNoDatabase is the guard the tool exists
// for: libpq's own default for an unnamed database is the connecting user's name,
// so a DSN that lost its /dbname on the way here would have this create a database
// nobody asked for instead of failing.
func TestTargetDatabaseRefusesADSNSThatNamesNoDatabase(t *testing.T) {
	for _, dsn := range []string{
		"postgres://user:pw@localhost:5432",
		"postgres://user:pw@localhost:5432?sslmode=disable",
		"host=localhost port=5432 user=hermes",
		"",
	} {
		if got, err := targetDatabase(dsn); err == nil {
			t.Errorf("targetDatabase(%q) = %q, want an error naming the missing database", dsn, got)
		}
	}
}

// TestRedactKeepsAPasswordOutOfAMessage: the DSN in an error message is the
// developer's own, and it can carry a password.
func TestRedactKeepsAPasswordOutOfAMessage(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"postgres://user:hunter2@localhost:5432/pigeon_test", "postgres://user:***@localhost:5432/pigeon_test"},
		{"postgres://user@localhost:5432/pigeon_test", "postgres://user@localhost:5432/pigeon_test"},
		{"postgres://user@localhost:5432/pigeon_test?passfile=/tmp/p", "postgres://user@localhost:5432/pigeon_test?passfile=/tmp/p"},
	} {
		if got := redact(tc.in); got != tc.want {
			t.Errorf("redact(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
