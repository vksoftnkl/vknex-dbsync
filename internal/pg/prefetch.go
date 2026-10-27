package pg

import (
	"context"
	"fmt"
	"reflect"
	"slices"

	"github.com/jackc/pgx/v5"
)

// Prefetched is a Querier that sent a known sequence of queries in one batch,
// so that together they cost one round trip, and hands out their results in
// that order. A query that is not the next one queued (the caller asks in
// another order than it said) ends the batch, and it and every query after it
// go to the transaction one at a time: slower, but the same results.
type Prefetched struct {
	tx    pgx.Tx
	br    pgx.BatchResults // nil once closed
	queue []string         // SQL of the queued queries not handed out yet
	args  []any            // the arguments of every queued query
}

// Prefetch runs the statements of before and then queues queries, each with
// args, in one batch on tx. The caller asks for the results with Query and
// calls Close when done; until then tx can run nothing else.
func Prefetch(ctx context.Context, tx pgx.Tx, before, queries []string, args ...any) (*Prefetched, error) {
	b := &pgx.Batch{}
	for _, q := range before {
		b.Queue(q)
	}
	for _, q := range queries {
		b.Queue(q, args...)
	}
	p := &Prefetched{tx: tx, br: tx.SendBatch(ctx, b), queue: slices.Clone(queries), args: args}
	for _, q := range before {
		if _, err := p.br.Exec(); err != nil {
			p.Close()
			return nil, fmt.Errorf("%s: %w", q, err)
		}
	}
	return p, nil
}

// next reports whether sql with args is the next queued query, and takes it.
func (p *Prefetched) next(sql string, args []any) bool {
	if p.br == nil || len(p.queue) == 0 || p.queue[0] != sql || !reflect.DeepEqual(args, p.args) {
		return false
	}
	p.queue = p.queue[1:]
	return true
}

func (p *Prefetched) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if p.next(sql, args) {
		return p.br.Query()
	}
	if err := p.Close(); err != nil {
		return nil, err
	}
	return p.tx.Query(ctx, sql, args...)
}

func (p *Prefetched) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	if p.next(sql, args) {
		return p.br.QueryRow()
	}
	p.Close() // an error here comes back from the query itself
	return p.tx.QueryRow(ctx, sql, args...)
}

// Close ends the batch; results not handed out are dropped.
func (p *Prefetched) Close() error {
	if p.br == nil {
		return nil
	}
	err := p.br.Close()
	p.br, p.queue = nil, nil
	return err
}
