// Package pg holds the low-level PostgreSQL helpers: connections, quoting and COPY streaming.
package pg

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// Querier is satisfied by *pgx.Conn and pgx.Tx.
type Querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Connect opens a connection that runs each statement in one round trip.
// Nearly every statement here is different, so preparing them first (pgx's
// default) only doubles the round trips, which dominate over a slow link.
func Connect(ctx context.Context, url string) (*pgx.Conn, error) {
	cc, err := pgx.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	cc.DefaultQueryExecMode = pgx.QueryExecModeExec
	return pgx.ConnectConfig(ctx, cc)
}
