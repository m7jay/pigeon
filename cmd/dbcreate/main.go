// Command dbcreate creates the Postgres database a DSN names, if it is missing.
//
// `make test` runs it through `make create-test-db` before the suite, because the
// DB-backed harnesses all prepare the database they are handed - they migrate and
// truncate it - and none of them can create it. internal/httpapi's harness refuses
// to skip without one ("the AC-15/AC-20 suite must run", by design), so a fresh
// machine's first `make test` used to hit a database that was not there yet, and
// every developer (and every worker on this board) hand-rolled one with psql or a
// throwaway tool first. This is that step, in the repository, so the Makefile can
// do it without assuming psql, docker, or an admin DSN the operator was never
// given.
//
// It is a development tool, not a deployment one: compose.yaml's Postgres image
// creates the product database and deploy/postgres/01-roles.sql provisions that
// instance's roles, and nothing in the shipped image links this.
//
// Three properties `make test` depends on:
//
//   - The DSN is the only input. No second connection is invented: the database is
//     created over the DSN's own server, user, password and TLS settings, so the
//     step needs no credentials the developer did not already have.
//   - Idempotent. A database that is already there is reported and left alone, and
//     two runs that race each other are both successes, so `make test` is safe to
//     re-run - which is the whole point of doing this inside the target.
//   - Narrow. It connects to the `postgres` maintenance database and runs exactly
//     two kinds of statement: a read of pg_catalog.pg_database, and at most one
//     CREATE DATABASE for the database the DSN names. Nothing is dropped, altered
//     or truncated, so pointing it at a live instance by mistake finds that
//     database already there and changes nothing.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// maintenanceDatabase is a database every PostgreSQL cluster has, so it is the one
// connection that can be opened before the database under management exists. The
// DSN's own database cannot be used to create itself.
const maintenanceDatabase = "postgres"

// statementTimeout bounds the catalog read and the create once a connection is up,
// so a server that accepts connections and then stalls reports a timeout instead of
// hanging the Makefile.
const statementTimeout = 30 * time.Second

// retryInterval is how often an unreachable server is retried until -wait elapses.
const retryInterval = 500 * time.Millisecond

func main() {
	dsn := flag.String("dsn", "", "the DSN whose database must exist; read from stdin when empty, so a password in it stays out of the process list")
	wait := flag.Duration("wait", 30*time.Second, "how long to wait for the server to accept connections")
	flag.Parse()

	if err := run(context.Background(), *dsn, *wait); err != nil {
		fmt.Fprintf(os.Stderr, "dbcreate: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, dsn string, wait time.Duration) error {
	dsn, err := readDSN(dsn)
	if err != nil {
		return err
	}
	target, err := targetDatabase(dsn)
	if err != nil {
		return err
	}

	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return fmt.Errorf("parse the DSN: %w", err)
	}
	cfg.RuntimeParams["application_name"] = "pigeon-dbcreate"
	// The maintenance connection: the DSN's server and credentials, a database that
	// is always there. Assigning this field rather than rewriting the URL keeps that
	// true for every shape pgx accepts - a URL, a keyword string, a unix-socket host
	// - including the shapes whose database name is not where a rewrite would look.
	cfg.Database = maintenanceDatabase
	where := net.JoinHostPort(cfg.Host, strconv.FormatUint(uint64(cfg.Port), 10))

	conn, err := connect(ctx, cfg, where, wait)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(context.Background()) }()

	ctx, cancel := context.WithTimeout(ctx, statementTimeout)
	defer cancel()

	found, err := databaseExists(ctx, conn, target)
	if err != nil {
		return fmt.Errorf("read pg_catalog.pg_database: %w", err)
	}
	if found {
		fmt.Printf("dbcreate: database %q already exists; nothing to do\n", target)
		return nil
	}

	if err := createDatabase(ctx, conn, target); err != nil {
		// A create can lose a race: another run created the same database between
		// the read above and this statement, which two workers' `make test` on one
		// box is enough to do. PostgreSQL answers that with duplicate_database
		// (42P04) - "the database is there now, somebody else put it there", which
		// is success for this tool. The re-read below is what decides, rather than a
		// SQLSTATE list: the question this tool answers is "does the database
		// exist", and the catalog is the only authority on that. The read gets its
		// own context because the failing create is exactly the case where the
		// deadline above may already have passed.
		vctx, cancel := context.WithTimeout(context.Background(), statementTimeout)
		defer cancel()
		after, readErr := databaseExists(vctx, conn, target)
		if readErr != nil || !after {
			return fmt.Errorf("create database %q: %w", target, err)
		}
		fmt.Printf("dbcreate: database %q was created by another run\n", target)
		return nil
	}

	fmt.Printf("dbcreate: created database %q\n", target)
	return nil
}

// readDSN takes the DSN from the flag, or from stdin when the flag is empty. The
// stdin path is what the Makefile uses: a DSN carries a password, and an argv value
// is readable by every process on the box for as long as this tool runs.
func readDSN(flagValue string) (string, error) {
	if dsn := strings.TrimSpace(flagValue); dsn != "" {
		return dsn, nil
	}
	raw, err := io.ReadAll(os.Stdin)
	if err != nil {
		return "", fmt.Errorf("read the DSN from stdin: %w", err)
	}
	dsn := strings.TrimSpace(string(raw))
	if dsn == "" {
		return "", errors.New("no DSN: pass -dsn, or write one to stdin")
	}
	return dsn, nil
}

// targetDatabase reads the database name a DSN names. A DSN that names none is
// refused rather than guessed: libpq's own default is the connecting user's name,
// so a DSN that lost its /dbname on the way to the Makefile would have this tool
// quietly prepare a database nobody asked for, and the suite would then red sixty
// acceptance tests against the wrong thing.
func targetDatabase(dsn string) (string, error) {
	if u, err := url.Parse(dsn); err == nil && (u.Scheme == "postgres" || u.Scheme == "postgresql") {
		name, err := url.PathUnescape(strings.TrimPrefix(u.Path, "/"))
		if err != nil {
			return "", fmt.Errorf("the DSN's database name is not valid URL escaping: %w", err)
		}
		if name == "" {
			return "", fmt.Errorf("the DSN names no database; add /<name> to %s", redact(dsn))
		}
		return name, nil
	}
	// The keyword form (`host=... dbname=...`), which pgx accepts as well. The last
	// dbname wins, as it does in libpq.
	name := ""
	for _, field := range strings.Fields(dsn) {
		for _, key := range []string{"dbname=", "database="} {
			if rest, ok := strings.CutPrefix(field, key); ok {
				name = strings.Trim(rest, `'"`)
			}
		}
	}
	if name == "" {
		return "", fmt.Errorf("the DSN names no database; add dbname=<name> to %s", redact(dsn))
	}
	return name, nil
}

// connect dials the server until it answers or wait has elapsed. A failure here has
// to say what it was waiting for: `make test` calls this against a container that
// has just reported healthy (or a server the developer started by hand), and
// "connection refused" with no address and no budget is the difference between a
// clear failure and a mystery.
func connect(ctx context.Context, cfg *pgx.ConnConfig, where string, wait time.Duration) (*pgx.Conn, error) {
	deadline := time.Now().Add(wait)
	var last error
	for attempt := 0; ; attempt++ {
		attemptCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		conn, err := pgx.ConnectConfig(attemptCtx, cfg)
		cancel()
		if err == nil {
			return conn, nil
		}
		last = err
		if attempt == 0 && wait > 0 {
			fmt.Printf("dbcreate: waiting up to %s for %s to accept connections\n", wait, where)
		}
		if wait <= 0 || !time.Now().Add(retryInterval).Before(deadline) {
			break
		}
		select {
		case <-time.After(retryInterval):
		case <-ctx.Done():
			return nil, fmt.Errorf("connect to %s: %w", where, ctx.Err())
		}
	}
	return nil, fmt.Errorf("connect to %s: %w", where, last)
}

// databaseExists reads the catalog explicitly rather than through the unqualified
// `pg_database`, so a search_path in the DSN cannot make this read a different
// relation - the same reason internal/dbtest reads pg_catalog.pg_roles by name.
func databaseExists(ctx context.Context, conn *pgx.Conn, name string) (bool, error) {
	var found bool
	err := conn.QueryRow(ctx,
		`select exists(select 1 from pg_catalog.pg_database where datname = $1)`, name).Scan(&found)
	return found, err
}

// createDatabase renders the CREATE DATABASE itself. PostgreSQL accepts $n
// placeholders only in SELECT/INSERT/UPDATE/DELETE/VALUES, so a utility statement
// like CREATE DATABASE cannot take the name as a bind parameter; the identifier is
// quoted here instead, by the same helper internal/dbtest quotes a role with, so a
// name that needs quoting cannot build broken SQL.
func createDatabase(ctx context.Context, conn *pgx.Conn, name string) error {
	_, err := conn.Exec(ctx, `create database `+pgx.Identifier{name}.Sanitize())
	return err
}

// redact keeps a DSN out of an error message with its password in it. Only a colon
// in the userinfo is a password separator - the scheme's own colon and a `passfile`
// in the query are not - and a DSN with no password is returned unchanged. The
// same shape .env.example and docs/ use for a published credential.
func redact(dsn string) string {
	scheme := strings.Index(dsn, "://")
	if scheme < 0 {
		return dsn
	}
	at := strings.Index(dsn, "@")
	if at < 0 {
		return dsn
	}
	colon := strings.LastIndex(dsn[scheme+3:at], ":")
	if colon < 0 {
		return dsn
	}
	return dsn[:scheme+3+colon+1] + "***" + dsn[at:]
}
