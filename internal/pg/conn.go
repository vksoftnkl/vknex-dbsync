// Package pg holds the low-level PostgreSQL helpers: connections, quoting and COPY streaming.
package pg

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Describe prints user@host:port/db of a URL, without the password.
func Describe(url string) string {
	c, err := pgx.ParseConfig(url)
	if err != nil {
		return "(unparseable URL)"
	}
	return fmt.Sprintf("%s@%s:%d/%s", c.User, c.Host, c.Port, c.Database)
}

// Querier is satisfied by *pgx.Conn and pgx.Tx.
type Querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Connect opens a connection that runs each statement in one round trip.
// Nearly every statement here is different, so preparing them first (pgx's
// default) only doubles the round trips, which dominate over a slow link.
func Connect(ctx context.Context, url string) (*pgx.Conn, error) {
	return ConnectTo(ctx, url, "")
}

// ConnectTo is Connect to another database on url's server; "" = url's own.
func ConnectTo(ctx context.Context, url, database string) (*pgx.Conn, error) {
	cc, err := pgx.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	if database != "" {
		cc.Database = database
	}
	cc.DefaultQueryExecMode = pgx.QueryExecModeExec
	conn, err := pgx.ConnectConfig(ctx, cc)
	if err != nil {
		return nil, explain(err, cc)
	}
	return conn, nil
}

// explain says what to check when the server refused the URL's database,
// user or password: waiting does not fix those, unlike a tunnel that is down.
func explain(err error, cc *pgx.ConnConfig) error {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return err
	}
	var hint string
	switch pgErr.Code {
	case "3D000": // invalid_catalog_name
		hint = "check the database name and port in the URL"
		if cc.Port == 5432 && loopback(cc.Host) {
			hint += fmt.Sprintf(": %s:5432 is this machine's own PostgreSQL; a database behind an SSH tunnel is on the tunnel's local port", cc.Host)
		}
	case "28P01": // invalid_password
		hint = "check the password in the URL"
	case "28000": // invalid_authorization_specification: unknown user, or no pg_hba.conf entry
		hint = "check the user in the URL and the server's pg_hba.conf"
	default:
		return err
	}
	return fmt.Errorf("%w; %s", err, hint)
}

// loopback reports whether host is this machine: localhost, a loopback
// address or a Unix socket directory.
func loopback(host string) bool {
	if host == "localhost" || strings.HasPrefix(host, "/") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
