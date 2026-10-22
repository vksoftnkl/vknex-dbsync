// Package syncer runs one sync of the source database into the target.
package syncer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/vksoftwares/vknex-dbsync/internal/config"
	"github.com/vksoftwares/vknex-dbsync/internal/ddl"
	"github.com/vksoftwares/vknex-dbsync/internal/pg"
	"github.com/vksoftwares/vknex-dbsync/internal/schema"
)

// Any constant works; it only stops two sync processes from writing at once.
const advisoryLockKey int64 = 0x564B4E4558 // "VKNEX"

// Syncer mirrors the source database into the target, one run at a time.
type Syncer struct {
	cfg *config.Config
	log *slog.Logger
}

// New returns a Syncer for cfg that logs to log.
func New(cfg *config.Config, log *slog.Logger) *Syncer {
	return &Syncer{cfg: cfg, log: log}
}

type runStats struct {
	Copied   int64
	Upserted int64
	Deleted  int64
	Tables   int
	ReAdd    []string // NOT VALID constraints to put back after the data
}

// RunOnce performs one complete, atomic sync. Either every table on the target
// is updated in one transaction, or nothing changes.
func (s *Syncer) RunOnce(ctx context.Context) error {
	started := time.Now()
	cfg := s.cfg

	src, err := pg.Connect(ctx, cfg.SourceURL)
	if err != nil {
		return fmt.Errorf("connect source: %w", err)
	}
	defer src.Close(context.Background())

	dst, err := pg.Connect(ctx, cfg.TargetURL)
	if err != nil {
		return fmt.Errorf("connect target: %w", err)
	}
	defer dst.Close(context.Background())

	if err := guardDifferent(ctx, src, dst); err != nil {
		return err
	}
	if _, err := dst.Exec(ctx, "SELECT now() AT TIME ZONE $1", cfg.NaiveTZ); err != nil {
		return fmt.Errorf("invalid -naive-tz %q: %w", cfg.NaiveTZ, err)
	}

	// One consistent snapshot of the source for the whole run.
	srcTx, err := src.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return fmt.Errorf("begin source snapshot: %w", err)
	}
	defer srcTx.Rollback(context.Background())

	var snapshot time.Time
	if err := srcTx.QueryRow(ctx, "SELECT now()").Scan(&snapshot); err != nil {
		return err
	}

	var srcAll map[string]*schema.Table
	var srcInfo *ddl.Info
	if err := schema.CatalogPath(ctx, srcTx, func() error {
		var err error
		if srcAll, err = schema.Load(ctx, srcTx, cfg.Schemas); err != nil {
			return fmt.Errorf("source schema: %w", err)
		}
		if cfg.DDL {
			srcInfo, err = ddl.LoadInfo(ctx, srcTx, cfg.Schemas)
		}
		return err
	}); err != nil {
		return err
	}
	dstAll, dstInfo, err := readTarget(ctx, dst, cfg)
	if err != nil {
		return err
	}

	tables := schema.Select(srcAll, cfg)
	if len(tables) == 0 {
		return errors.New("no tables selected; check -schema / -include / -exclude")
	}

	changes := &ddl.Plan{}
	if cfg.DDL {
		if changes, err = ddl.Build(ctx, dst, tables, dstAll, srcInfo, dstInfo, cfg); err != nil {
			return err
		}
	} else {
		problems, warnings := schema.Compare(tables, dstAll)
		for _, w := range warnings {
			s.log.Warn("schema", "detail", w)
		}
		if len(problems) > 0 {
			return fmt.Errorf("source and target schemas differ; re-run with -ddl to apply the source's structure to the target:\n  - %s",
				strings.Join(problems, "\n  - "))
		}
	}

	order, cyclic := schema.FKOrder(tables, !cfg.DisableTriggers)
	if len(cyclic) > 0 && !cfg.DisableTriggers {
		return fmt.Errorf("tables %s form a foreign-key cycle with no nullable column to break it; run with -disable-triggers", strings.Join(cyclic, ", "))
	}
	for _, name := range order {
		if t := tables[name]; len(t.Break) > 0 {
			if len(t.PK) == 0 {
				return fmt.Errorf("table %s is in a foreign-key cycle but has no primary key; run with -disable-triggers", name)
			}
			s.log.Info("foreign-key cycle: these columns are filled in a second pass", "table", name, "columns", strings.Join(t.Break, ","))
		}
	}

	srcSeq, err := readSourceSequences(ctx, srcTx, tables)
	if err != nil {
		return err
	}
	marks := &syncMarks{}
	if cfg.MarkSynced {
		if marks, err = readPending(ctx, srcTx, tables); err != nil {
			return err
		}
	}

	if cfg.DryRun {
		for _, q := range slices.Concat(changes.EnumValues, changes.Before) {
			s.log.Info("schema change (before data)", "sql", q)
		}
		for _, q := range changes.After {
			s.log.Info("schema change (after data)", "sql", q)
		}
		if cfg.MarkSynced {
			s.log.Info("DRY RUN: sync time would be set", "rows", marks.count(), "tables", len(marks.rows), "deletes_up_to_id", marks.deletesUpTo)
		}
		return s.plan(ctx, srcTx, dst, tables, order, changes.Reset)
	}

	// A new enum value cannot be used in the transaction that adds it.
	for _, q := range changes.EnumValues {
		s.log.Info("schema change", "sql", q)
		if _, err := dst.Exec(ctx, q); err != nil {
			return fmt.Errorf("%s: %w", q, err)
		}
	}

	dstTx, err := dst.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin target: %w", err)
	}
	defer dstTx.Rollback(context.Background())

	var locked bool
	if err := dstTx.QueryRow(ctx, "SELECT pg_try_advisory_xact_lock($1)", advisoryLockKey).Scan(&locked); err != nil {
		return err
	}
	if !locked {
		s.log.Warn("another sync is already running against this target; skipping this run")
		return nil
	}

	setup := []string{
		fmt.Sprintf("SET LOCAL lock_timeout = %d", cfg.LockTimeout.Milliseconds()),
		"SET LOCAL statement_timeout = 0",
	}
	if cfg.DisableTriggers {
		setup = append(setup, "SET LOCAL session_replication_role = replica")
	}
	for _, q := range setup {
		if _, err := dstTx.Exec(ctx, q); err != nil {
			return fmt.Errorf("%s: %w", q, err)
		}
	}

	if err := s.applyDDL(ctx, dstTx, changes.Before); err != nil {
		return err
	}
	if len(changes.Before) > 0 {
		// Make sure the target can now take every source column before copying.
		var now map[string]*schema.Table
		if err := schema.CatalogPath(ctx, dstTx, func() error {
			var err error
			now, err = schema.Load(ctx, dstTx, cfg.Schemas)
			return err
		}); err != nil {
			return fmt.Errorf("target schema: %w", err)
		}
		if problems, _ := schema.Compare(tables, now); len(problems) > 0 {
			return fmt.Errorf("target still differs after the schema changes:\n  - %s", strings.Join(problems, "\n  - "))
		}
	}

	if err := s.ensureMeta(ctx, dstTx); err != nil {
		return err
	}

	var st runStats
	switch cfg.Mode {
	case "full":
		err = s.full(ctx, src, dstTx, tables, order, &st)
	default:
		var state map[string]time.Time
		if state, err = s.loadState(ctx, dstTx); err == nil {
			for name := range changes.Reset {
				delete(state, name)
			}
			err = s.incremental(ctx, src, dstTx, tables, order, state, &st)
		}
	}
	if err != nil {
		return err
	}
	if err := s.applyDDL(ctx, dstTx, slices.Concat(st.ReAdd, changes.After)); err != nil {
		return err
	}

	syncedAt := time.Now().Truncate(time.Microsecond)
	if err := s.markTarget(ctx, dstTx, tables, marks, syncedAt); err != nil {
		return err
	}
	if err := setSequences(ctx, dstTx, tables, srcSeq); err != nil {
		return err
	}
	if err := s.saveState(ctx, dstTx, tables, snapshot, started, &st); err != nil {
		return err
	}
	if err := dstTx.Commit(ctx); err != nil {
		return fmt.Errorf("commit target: %w", err)
	}

	s.log.Info("sync complete",
		"mode", cfg.Mode,
		"schema_changes", changes.Count(),
		"tables", st.Tables,
		"rows_read", st.Copied,
		"rows_written", st.Upserted,
		"rows_deleted", st.Deleted,
		"took", time.Since(started).Round(time.Millisecond).String())

	if cfg.MarkSynced {
		if err := srcTx.Rollback(ctx); err != nil { // end the read-only snapshot
			return err
		}
		// The data is on the target already. If this fails, the rows stay waiting
		// on the source and the next run copies them (and their sync time) again.
		if err := s.markSource(ctx, src, tables, marks, syncedAt); err != nil {
			s.log.Warn("could not set the sync time on the source; the next run retries", "err", err)
		}
	}
	return nil
}

// guardDifferent refuses to run when both URLs reach the same database.
func guardDifferent(ctx context.Context, src, dst *pgx.Conn) error {
	const q = `SELECT current_database() || '@' || COALESCE(host(inet_server_addr()), 'socket') || ':' || current_setting('port')`
	var a, b string
	if err := src.QueryRow(ctx, q).Scan(&a); err != nil {
		return err
	}
	if err := dst.QueryRow(ctx, q).Scan(&b); err != nil {
		return err
	}
	if a == b {
		return fmt.Errorf("source and target are the same database (%s)", a)
	}
	return nil
}

func (s *Syncer) applyDDL(ctx context.Context, tx pgx.Tx, stmts []string) error {
	for _, q := range stmts {
		s.log.Info("schema change", "sql", q)
		if _, err := tx.Exec(ctx, q); err != nil {
			return fmt.Errorf("schema change failed: %s: %w", q, err)
		}
	}
	return nil
}

// readTarget introspects the target in a short read-only transaction.
func readTarget(ctx context.Context, dst *pgx.Conn, cfg *config.Config) (map[string]*schema.Table, *ddl.Info, error) {
	tx, err := dst.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, nil, err
	}
	defer tx.Rollback(context.Background())
	var all map[string]*schema.Table
	var info *ddl.Info
	err = schema.CatalogPath(ctx, tx, func() error {
		var err error
		if all, err = schema.Load(ctx, tx, cfg.Schemas); err != nil {
			return fmt.Errorf("target schema: %w", err)
		}
		if cfg.DDL {
			info, err = ddl.LoadInfo(ctx, tx, cfg.Schemas)
		}
		return err
	})

	return all, info, err
}
