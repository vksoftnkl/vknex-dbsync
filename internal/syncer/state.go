package syncer

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/vksoftwares/vknex-dbsync/internal/pg"
	"github.com/vksoftwares/vknex-dbsync/internal/schema"
)

// ensureMeta creates the sync-state tables on the target when they are
// missing, in a short transaction of its own: created inside a sync
// transaction, they would hold up the other kind of run until that commits.
func (s *Syncer) ensureMeta(ctx context.Context, dst *pgx.Conn) error {
	var ok bool
	if err := dst.QueryRow(ctx, "SELECT to_regclass($1) IS NOT NULL AND to_regclass($2) IS NOT NULL",
		pg.QName(s.cfg.MetaSchema, "table_state"), pg.QName(s.cfg.MetaSchema, "run_log")).Scan(&ok); err != nil {
		return err
	}
	if ok {
		return nil
	}
	tx, err := dst.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	m := pg.Ident(s.cfg.MetaSchema)
	stmts := []string{
		// An interval run and Watch start together: let one create, the other wait.
		fmt.Sprintf("SELECT pg_advisory_xact_lock(%d)", metaLockKey),
		"CREATE SCHEMA IF NOT EXISTS " + m,
		`CREATE TABLE IF NOT EXISTS ` + m + `.table_state (
			schema_name    text        NOT NULL,
			table_name     text        NOT NULL,
			last_synced_at timestamptz NOT NULL,
			updated_at     timestamptz NOT NULL DEFAULT now(),
			PRIMARY KEY (schema_name, table_name))`,
		`CREATE TABLE IF NOT EXISTS ` + m + `.run_log (
			id           bigserial PRIMARY KEY,
			mode         text        NOT NULL,
			snapshot_at  timestamptz NOT NULL,
			started_at   timestamptz NOT NULL,
			finished_at  timestamptz NOT NULL DEFAULT now(),
			tables       int         NOT NULL,
			rows_read    bigint      NOT NULL,
			rows_written bigint      NOT NULL,
			rows_deleted bigint      NOT NULL)`,
	}
	for _, q := range stmts {
		if _, err := tx.Exec(ctx, q); err != nil {
			return fmt.Errorf("prepare %s schema: %w", s.cfg.MetaSchema, err)
		}
	}
	return tx.Commit(ctx)
}

func (s *Syncer) loadState(ctx context.Context, q pg.Querier) (map[string]time.Time, error) {
	rows, err := q.Query(ctx, fmt.Sprintf("SELECT schema_name, table_name, last_synced_at FROM %s WHERE schema_name = ANY($1)",
		pg.QName(s.cfg.MetaSchema, "table_state")), s.cfg.Schemas)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]time.Time{}
	for rows.Next() {
		var ns, n string
		var ts time.Time
		if err := rows.Scan(&ns, &n, &ts); err != nil {
			return nil, err
		}
		out[schema.Key(ns, n)] = ts
	}
	return out, rows.Err()
}

// saveState records the watermark of every table of the run and the run
// itself; mode is the -mode, or "instant" for a run of Watch.
func (s *Syncer) saveState(ctx context.Context, tx pgx.Tx, tables map[string]*schema.Table, mode string, snapshot, started time.Time, st *runStats) error {
	q := fmt.Sprintf(`INSERT INTO %s (schema_name, table_name, last_synced_at) VALUES ($1, $2, $3)
		ON CONFLICT (schema_name, table_name) DO UPDATE SET last_synced_at = EXCLUDED.last_synced_at, updated_at = now()`,
		pg.QName(s.cfg.MetaSchema, "table_state"))
	batch := &pgx.Batch{}
	for _, t := range tables {
		batch.Queue(q, t.Schema, t.Name, snapshot)
	}
	batch.Queue(fmt.Sprintf(`INSERT INTO %s (mode, snapshot_at, started_at, finished_at, tables, rows_read, rows_written, rows_deleted)
		VALUES ($1, $2, $3, clock_timestamp(), $4, $5, $6, $7)`, pg.QName(s.cfg.MetaSchema, "run_log")),
		mode, snapshot, started, st.Tables, st.Copied, st.Upserted, st.Deleted)
	if err := tx.SendBatch(ctx, batch).Close(); err != nil {
		return fmt.Errorf("save sync state: %w", err)
	}
	return nil
}
